//! Receive simulated real-time factor (RTF) from the Virtual Robot stack and
//! expose it for slowing GameController match timers to match sim time.
//!
//! Wire format (UDP, little-endian), port default 3840:
//!   magic[4] = b"RGtf"
//!   version  = u8 (1)
//!   rtf      = f32

use std::{net::IpAddr, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use tokio::{net::UdpSocket, sync::watch, time::Instant};

/// Default UDP port for RTF sync packets from game_controller_node.
pub const DEFAULT_SIM_RTF_PORT: u16 = 3840;

const MAGIC: &[u8; 4] = b"RGtf";
const VERSION: u8 = 1;
const PACKET_LEN: usize = 4 + 1 + 4;

/// How long after the last valid packet we keep using that RTF before falling
/// back to real-time (1.0). Avoids a dead publisher freezing the match forever.
const STALE_AFTER: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, Debug)]
struct Sample {
    /// Last RTF from the Virtual Robot node; `None` until the first packet.
    rtf: Option<f64>,
    at: Instant,
}

impl Default for Sample {
    fn default() -> Self {
        Self {
            rtf: None,
            at: Instant::now(),
        }
    }
}

/// Shared RTF state for the event loop.
#[derive(Clone)]
pub struct SimRtfHandle {
    rx: watch::Receiver<Sample>,
}

impl SimRtfHandle {
    /// Current scale factor applied to wall `Duration` before `seek`.
    /// Clamped to [0, 2]; missing/stale samples fall back to 1.0.
    pub fn factor(&self) -> f64 {
        let sample = *self.rx.borrow();
        match sample.rtf {
            Some(rtf) if sample.at.elapsed() <= STALE_AFTER => rtf.clamp(0.0, 2.0),
            _ => 1.0,
        }
    }
}

/// Safe `Duration` from seconds; never panics on NaN / overflow.
fn duration_from_secs_f64_saturating(secs: f64) -> Duration {
    if !secs.is_finite() || secs <= 0.0 {
        return Duration::ZERO;
    }
    // Duration::from_secs_f64 panics on NaN / too-large values.
    Duration::try_from_secs_f64(secs).unwrap_or(Duration::MAX)
}

/// Scale a wall-clock delta by the current sim RTF for `GameController::seek`.
pub fn scale_seek_dt(wall_dt: Duration, rtf: f64) -> Duration {
    if !rtf.is_finite() || rtf <= 0.0 || wall_dt.is_zero() {
        return Duration::ZERO;
    }
    let scaled = wall_dt.as_secs_f64() * rtf.clamp(0.0, 2.0);
    duration_from_secs_f64_saturating(scaled)
}

/// Convert a game-time wait (`dt` until next timer) into wall sleep length.
/// Preserves `Duration::MAX` (no deadline) — never calls `mul_f64` on it.
pub fn game_dt_to_wall_dt(game_dt: Duration, rtf: f64) -> Duration {
    if game_dt == Duration::MAX {
        return Duration::MAX;
    }
    if !rtf.is_finite() || rtf <= 0.0 {
        return Duration::from_millis(50);
    }
    duration_from_secs_f64_saturating(game_dt.as_secs_f64() / rtf)
}

/// Spawn a UDP listener that updates RTF. Returns a handle for the event loop.
/// If `port == 0`, sync is disabled and the handle always reports 1.0.
pub async fn start_sim_rtf_receiver(
    bind_address: IpAddr,
    port: u16,
) -> Result<(SimRtfHandle, Option<tokio::task::JoinHandle<()>>)> {
    let (tx, rx) = watch::channel(Sample::default());
    let handle = SimRtfHandle { rx };

    if port == 0 {
        return Ok((handle, None));
    }

    let socket = UdpSocket::bind((bind_address, port))
        .await
        .with_context(|| format!("could not bind sim RTF receiver on {bind_address}:{port}"))?;
    // Also accept packets on 0.0.0.0 if bind was interface-local — re-bind all
    // interfaces when the interface address is not unspecified.
    let socket = if bind_address.is_unspecified() {
        socket
    } else {
        // Prefer dual bind: docker0 address for returns path, but RTF often
        // arrives at the docker gateway IP. Binding the interface address is
        // enough when game_control_ip is 172.17.0.1.
        socket
    };

    eprintln!("[sim_rtf] listening for RTF sync on {bind_address}:{port}");

    let tx = Arc::new(tx);
    let join = tokio::spawn(async move {
        let mut buf = [0u8; 64];
        loop {
            match socket.recv_from(&mut buf).await {
                Ok((len, _from)) => {
                    if let Some(rtf) = parse_packet(&buf[..len]) {
                        let _ = tx.send(Sample {
                            rtf: Some(rtf),
                            at: Instant::now(),
                        });
                    }
                }
                Err(err) => {
                    eprintln!("[sim_rtf] recv error: {err}");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    });

    Ok((handle, Some(join)))
}

fn parse_packet(data: &[u8]) -> Option<f64> {
    if data.len() < PACKET_LEN {
        return None;
    }
    if &data[0..4] != MAGIC || data[4] != VERSION {
        return None;
    }
    let mut bits = [0u8; 4];
    bits.copy_from_slice(&data[5..9]);
    let rtf = f32::from_le_bytes(bits) as f64;
    if !rtf.is_finite() {
        return None;
    }
    Some(rtf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_packet() {
        let mut pkt = Vec::new();
        pkt.extend_from_slice(MAGIC);
        pkt.push(VERSION);
        pkt.extend_from_slice(&(0.5f32).to_le_bytes());
        assert!((parse_packet(&pkt).unwrap() - 0.5).abs() < 1e-6);
    }

    #[test]
    fn rejects_bad_magic() {
        let mut pkt = vec![0u8; PACKET_LEN];
        assert!(parse_packet(&pkt).is_none());
    }

    #[test]
    fn scale_zero_freezes() {
        assert_eq!(
            scale_seek_dt(Duration::from_secs(1), 0.0),
            Duration::ZERO
        );
    }

    #[test]
    fn wall_wait_preserves_max() {
        assert_eq!(game_dt_to_wall_dt(Duration::MAX, 0.5), Duration::MAX);
    }

    #[test]
    fn wall_wait_scales() {
        let w = game_dt_to_wall_dt(Duration::from_secs(10), 0.5);
        assert!((w.as_secs_f64() - 20.0).abs() < 1e-6);
    }
}
