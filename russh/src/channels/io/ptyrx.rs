use std::borrow::BorrowMut;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{ready, Context, Poll};

use e4pty::prelude::PtyReader;
use log::info;
use tokio::io::AsyncRead;

use super::{ChannelMsg, ChannelReadHalf};

#[derive(Debug)]
pub struct PtyRx<R> {
    channel: R,
    buffer: Option<(ChannelMsg, usize)>,

    ext: Option<u32>,

    exit_status: Arc<Mutex<Option<i32>>>,
    eof: bool,
}

impl<R> PtyRx<R> {
    pub fn new(channel: R, ext: Option<u32>, exit_status: Arc<Mutex<Option<i32>>>) -> Self {
        Self {
            channel,
            buffer: None,
            ext,
            exit_status,
            eof: false,
        }
    }
}

impl<R> AsyncRead for PtyRx<R>
where
    R: BorrowMut<ChannelReadHalf> + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let (msg, mut idx) = match self.buffer.take() {
            Some(msg) => msg,
            None => match ready!(self.channel.borrow_mut().receiver.poll_recv(cx)) {
                Some(msg) => (msg, 0),
                None => return Poll::Ready(Ok(())),
            },
        };
        info!("msg: {:?}", msg);

        match (&msg, self.ext) {
            (ChannelMsg::Data { data }, None) => {
                let readable = buf.remaining().min(data.len() - idx);

                // Clamped to maximum `buf.remaining()` and `data.len() - idx` with `.min`
                #[allow(clippy::indexing_slicing)]
                buf.put_slice(&data[idx..idx + readable]);
                idx += readable;

                if idx != data.len() {
                    self.buffer = Some((msg, idx));
                }

                Poll::Ready(Ok(()))
            }
            (ChannelMsg::ExtendedData { data, ext }, Some(target)) if *ext == target => {
                let readable = buf.remaining().min(data.len() - idx);

                // Clamped to maximum `buf.remaining()` and `data.len() - idx` with `.min`
                #[allow(clippy::indexing_slicing)]
                buf.put_slice(&data[idx..idx + readable]);
                idx += readable;

                if idx != data.len() {
                    self.buffer = Some((msg, idx));
                }

                Poll::Ready(Ok(()))
            }
            (ChannelMsg::Eof, _) => {
                // because exit status may be sent after EOF
                if self.exit_status.lock().unwrap().is_some() {
                    self.channel.borrow_mut().receiver.close();
                    Poll::Ready(Ok(()))
                } else {
                    self.eof = true;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
            (ChannelMsg::ExitStatus { exit_status }, _) => {
                self.exit_status
                    .lock()
                    .unwrap()
                    .replace(*exit_status as i32);
                if self.eof {
                    self.channel.borrow_mut().receiver.close();
                    Poll::Ready(Ok(()))
                } else {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
            _ => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

impl<R> PtyReader for PtyRx<R> where R: BorrowMut<ChannelReadHalf> + Unpin {}
