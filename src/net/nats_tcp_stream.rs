#[cfg(feature = "tls")]
use native_tls::{self, TlsConnector};
use pin_project::{pin_project, project};
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{
    io::{self, AsyncRead, AsyncWrite},
    net::TcpStream,
};
#[cfg(feature = "tls")]
use tokio_tls::{TlsConnector as TokioTlsConnector, TlsStream};
use futures::{Stream, Sink};
use crate::ops::Op;
use bytes::BytesMut;

use futures_core::ready;
use crate::parser::operation;
use nom::Err as NomErr;
use crate::error::RatsioError;
use std::fmt::Debug;
use failure::_core::fmt::{Formatter, Error};

/// A simple wrapper type that can either be a raw TCP stream or a TCP stream with TLS enabled.
#[pin_project]
#[derive(Debug)]
pub enum NatsTcpStreamInner {
    PlainStream(#[pin] TcpStream),
    #[cfg(feature = "tls")]
    TlsStream(#[pin] TlsStream<TcpStream>),
}

#[pin_project]
pub struct NatsTcpStream {
    #[pin]
    stream_inner: NatsTcpStreamInner,
    read_buffer: BytesMut,
    write_buffer: BytesMut,
    flushed: bool,
}

impl NatsTcpStreamInner {
    pub fn new(stream: TcpStream) -> Self {
        Self::PlainStream(stream)
    }

    #[cfg(feature = "tls")]
    pub async fn upgrade(
        self,
        tls_connector: TlsConnector,
        domain: &str,
    ) -> Result<Self, native_tls::Error> {
        Ok(match self {
            Self::PlainStream(stream) => {
                let tokio_tls_connector = TokioTlsConnector::from(tls_connector);
                let tls_stream = tokio_tls_connector.connect(domain, stream).await?;
                Self::TlsStream(tls_stream)
            }
            Self::TlsStream(stream) => Self::TlsStream(stream),
        })
    }
}


impl AsyncRead for NatsTcpStreamInner {
    #[project]
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        #[project]
            match self.project() {
            NatsTcpStreamInner::PlainStream(stream) => stream.poll_read(cx, buf),
            #[cfg(feature = "tls")]
            NatsTcpStreamInner::TlsStream(stream) => stream.poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for NatsTcpStreamInner {
    #[project]
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<io::Result<usize>> {
        #[project]
            match self.project() {
            NatsTcpStreamInner::PlainStream(stream) => stream.poll_write(cx, buf),
            #[cfg(feature = "tls")]
            NatsTcpStreamInner::TlsStream(stream) => stream.poll_write(cx, buf),
        }
    }

    #[project]
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        #[project]
            match self.project() {
            NatsTcpStreamInner::PlainStream(stream) => stream.poll_flush(cx),
            #[cfg(feature = "tls")]
            NatsTcpStreamInner::TlsStream(stream) => stream.poll_flush(cx),
        }
    }


    #[project]
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        #[project]
        match self.project() {
            NatsTcpStreamInner::PlainStream(stream) => stream.poll_shutdown(cx),
            #[cfg(feature = "tls")]
            NatsTcpStreamInner::TlsStream(stream) => stream.poll_shutdown(cx),
        }
    }
}


impl Stream for NatsTcpStream {
    type Item = Op;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if let Some(item) = NatsTcpStream::decode(&mut this.read_buffer) {
            debug!("Decoded {:?}, buffer is {}B", item, this.read_buffer.len());
            return Poll::Ready(Some(item));
        }

        let mut read_buffer = this.read_buffer;
        // Spurious EOF protection
        read_buffer.reserve(1);

        let mut buff: [u8; 2048] = [0; 2048];
        loop {
            match this.stream_inner.as_mut().poll_read(cx, &mut buff) {
                Poll::Ready(Ok(size)) => {
                    read_buffer.extend(&buff[0..size]);
                    debug!("Read {}B, buffer is {}B", size, read_buffer.len());
                    //println!(" ----- buffer [{}]\n\t'{}'", size, std::str::from_utf8(read_buffer.as_ref()).unwrap());
                    if size > 0 {
                        if let Some(item) = NatsTcpStream::decode(&mut read_buffer) {
                            debug!("Decoded {:?}, buffer is {}B", item, read_buffer.len());
                            return Poll::Ready(Some(item));
                        }
                    } else {
                        return Poll::Ready(None);
                    }
                }
                Poll::Ready(Err(err)) => {
                    if err.kind() == std::io::ErrorKind::WouldBlock {
                        warn!("Inner stream would block");
                        return Poll::Pending;
                    } else {
                        error!(target: "ratsio", "poll_next stream error - {:?}", err);
                        return Poll::Ready(None);
                    }
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
    }
}

impl Sink<Op> for NatsTcpStream {
    type Error = RatsioError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut this = self.project();
        if !*this.flushed {
            match this.stream_inner.as_mut().poll_flush(cx)? {
                Poll::Ready(()) => Poll::Ready(Ok(())),
                Poll::Pending => return Poll::Pending,
            }
        }else {
            Poll::Ready(Ok(()))
        }
    }

    fn start_send(self: Pin<&mut Self>, item: Op) -> Result<(), Self::Error> {
        let this = self.project();
        let buff = item.into_bytes()?;
        this.write_buffer.extend(buff);
        *this.flushed = false;
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut this = self.project();
        if *this.flushed {
            return Poll::Ready(Ok(()));
        }
        let len = ready!(this.stream_inner.as_mut().poll_write(cx, this.write_buffer.as_ref()))?;
        let wrote_all = len == this.write_buffer.len();
        *this.flushed = true;
        this.write_buffer.clear();

        let res = if wrote_all {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "failed to write entire datagram to socket",
            )
            .into())
        };

        Poll::Ready(res)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        ready!(self.poll_flush(cx))?;
        Poll::Ready(Ok(()))
    }
}
impl std::fmt::Debug for NatsTcpStream {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "NatsTcpStream()")
    }
}

const INITIAL_CAPACITY: usize = 8 * 1024;

impl NatsTcpStream {
    pub async fn new(tcp_stream: TcpStream) -> Self {
        let stream = NatsTcpStreamInner::new(tcp_stream);
        NatsTcpStream {
            stream_inner: stream,
            read_buffer: BytesMut::with_capacity(INITIAL_CAPACITY),
            write_buffer: BytesMut::with_capacity(INITIAL_CAPACITY),
            flushed: false,
        }
    }

    fn decode(src: &mut BytesMut) -> Option<Op> {
        if src.len() == 0 {
            return None;
        }
        let (op_item, offset) = match operation(src.as_ref()) {
            Err(NomErr::Incomplete(_)) => {
                (None, None)
            }
            Ok((remaining, item)) => {
                (Some(item), Some(src.len() - remaining.len()))
            }
            Err(NomErr::Error(err)) => {
                let txt = String::from(&(*String::from_utf8_lossy(src.as_ref())));
                error!(target: "ratsio", " Error parsing => {:?}\n{}", err, txt);
                if let Some(offset) = src[..].windows(2).position(|w| w == b"\r\n") {
                    (None, Some(offset))
                } else {
                    (None, Some(src.len()))
                }
            }
            Err(NomErr::Failure(err)) => {
                //scan for \r\n and recover there.
                let txt = String::from(&(*String::from_utf8_lossy(src.as_ref())));
                error!(target: "ratsio", " Failure parsing => {:?}\n{}", err, txt);
                if let Some(offset) = src[..].windows(2).position(|w| w == b"\r\n") {
                    (None, Some(offset))
                } else {
                    (None, Some(src.len()))
                }
            }
        };

        match (op_item, offset) {
            (Some(item), Some(offset)) => {
                src.split_to(offset);
                Some(item)
            }
            (_, Some(offset)) => {
                src.split_to(offset);
                None
            }
            _ => {
                None
            }
        }
    }
}
