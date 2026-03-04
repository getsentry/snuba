use std::os::unix::net::UnixDatagram;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use statsdproxy::middleware::Middleware;
use statsdproxy::types::Metric;

const BUFSIZE: usize = 512;

pub struct UnixUpstream {
    socket: UnixDatagram,
    buffer: [u8; BUFSIZE],
    buf_used: usize,
    last_sent_at: SystemTime,
}

impl UnixUpstream {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error> {
        let socket = UnixDatagram::unbound()?;
        socket.connect(path)?;
        socket.set_nonblocking(true)?;
        Ok(UnixUpstream {
            socket,
            buffer: [0; BUFSIZE],
            buf_used: 0,
            last_sent_at: UNIX_EPOCH,
        })
    }

    fn flush(&mut self) {
        if self.buf_used > 0 {
            self.send_buffer(&self.buffer[..self.buf_used]);
            self.buf_used = 0;
        }
        self.last_sent_at = SystemTime::now();
    }

    fn timed_flush(&mut self) {
        let now = SystemTime::now();
        if now
            .duration_since(self.last_sent_at)
            .map_or(true, |x| x > Duration::from_secs(1))
        {
            self.flush();
        }
    }

    fn send_buffer(&self, buf: &[u8]) {
        match self.socket.send(buf) {
            Ok(bytes) => {
                if bytes != buf.len() {
                    tracing::error!("tried to send {} bytes but only sent {}.", buf.len(), bytes);
                }
            }
            Err(e) => {
                tracing::error!("failed to send to UDS upstream: {}", e);
            }
        }
    }
}

impl Middleware for UnixUpstream {
    fn poll(&mut self) {
        self.timed_flush();
    }

    fn submit(&mut self, metric: &mut Metric) {
        let metric_len = metric.raw.len();

        if metric_len + 1 > BUFSIZE - self.buf_used {
            self.flush();
        }

        if metric_len > BUFSIZE {
            self.send_buffer(&metric.raw);
        } else {
            if self.buf_used > 0 {
                self.buffer[self.buf_used] = b'\n';
                self.buf_used += 1;
            }
            self.buffer[self.buf_used..self.buf_used + metric_len].copy_from_slice(&metric.raw);
            self.buf_used += metric_len;
        }
    }
}

impl Drop for UnixUpstream {
    fn drop(&mut self) {
        self.flush();
    }
}
