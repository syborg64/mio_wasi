#![allow(dead_code)]
use std::cmp::{max, min};
use std::collections::HashMap;
use std::io;
use std::os::fd::RawFd;
use std::sync::atomic::AtomicBool;
#[cfg(feature = "net")]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::Registry;
#[cfg(feature = "net")]
use crate::{Interest, Token};
use wasmedge_wasi_socket::wasi_poll as wasi;

cfg_net! {

pub mod tcp {
    use std::io;
    use std::net::SocketAddr;
    use wasmedge_wasi_socket::socket::{self, Socket};

    pub(crate) use wasmedge_wasi_socket::TcpListener;
    pub(crate) use wasmedge_wasi_socket::TcpStream;

    pub(crate) fn new_for_addr(address: SocketAddr) -> io::Result<Socket> {
        let domain = match address {
            SocketAddr::V4(_) => socket::AddressFamily::Inet4,
            SocketAddr::V6(_) => socket::AddressFamily::Inet6,
        };

        let s = socket::Socket::new(domain, socket::SocketType::Stream)?;
        s.set_nonblocking(true)?;
        Ok(s)
    }

    pub(crate) fn accept(listener: &wasmedge_wasi_socket::TcpListener) -> io::Result<(wasmedge_wasi_socket::TcpStream, SocketAddr)> {
        listener.accept(true)
    }

    pub(crate) fn connect(socket: &Socket, addr: SocketAddr) -> io::Result<()> {
        match socket.connect(&addr) {
            Err(err) if err.raw_os_error() != Some(libc::EINPROGRESS) => Err(err),
            _ => Ok(()),
        }
    }
}
}

/// Unique id for use as `SelectorId`.
#[cfg(feature = "net")]
static NEXT_ID: AtomicUsize = AtomicUsize::new(1);

mod waker {
    use super::{Selector, Token};
    use std::io;
    #[derive(Debug)]
    pub struct Waker {}

    impl Waker {
        pub fn new(_: &Selector, _: Token) -> io::Result<Waker> {
            Ok(Waker {})
        }

        pub fn wake(&self) -> io::Result<()> {
            Ok(())
        }
    }
}
#[allow(unused_imports)]
pub use waker::Waker;

pub struct Selector {
    #[cfg(feature = "net")]
    id: usize,
    /// Subscriptions (reads events) we're interested in.
    subscriptions:
        Arc<Mutex<HashMap<wasi::Fd, (Token, Interest, Arc<AtomicBool>, Arc<AtomicUsize>)>>>,
}

impl Selector {
    fn subscriptions(&self) -> Vec<wasi::Subscription> {
        let subscriptions = self.subscriptions.lock().unwrap();
        let mut subs = Vec::with_capacity(subscriptions.len() * 2);
        for (fd, (_token, insterest, read_state, write_state)) in subscriptions.iter() {
            if insterest.is_readable() && read_state.load(Ordering::Acquire) {
                let s = wasi::Subscription {
                    userdata: *fd as wasi::Userdata,
                    u: wasi::SubscriptionU {
                        tag: wasi::EVENTTYPE_FD_READ,
                        u: wasi::SubscriptionUU {
                            fd_read: wasi::SubscriptionFdReadwrite {
                                file_descriptor: *fd,
                            },
                        },
                    },
                };
                subs.push(s);
            }

            if insterest.is_writable() && write_state.load(Ordering::Acquire) > 0 {
                let s = wasi::Subscription {
                    userdata: *fd as wasi::Userdata,
                    u: wasi::SubscriptionU {
                        tag: wasi::EVENTTYPE_FD_WRITE,
                        u: wasi::SubscriptionUU {
                            fd_read: wasi::SubscriptionFdReadwrite {
                                file_descriptor: *fd,
                            },
                        },
                    },
                };
                subs.push(s);
            }
        }

        subs
    }

    pub fn new() -> io::Result<Selector> {
        Ok(Selector {
            #[cfg(feature = "net")]
            id: NEXT_ID.fetch_add(1, Ordering::Relaxed),
            subscriptions: Default::default(),
        })
    }

    pub fn try_clone(&self) -> io::Result<Selector> {
        Ok(Selector {
            id: self.id,
            subscriptions: self.subscriptions.clone(),
        })
    }

    #[cfg(feature = "net")]
    pub fn id(&self) -> usize {
        self.id
    }

    pub fn select(&self, events: &mut Events, timeout: Option<Duration>) -> io::Result<()> {
        events.clear();

        let mut closing = vec![];

        let mut subscriptions = self.subscriptions();

        // If we want to a use a timeout in the `wasi_poll_oneoff()` function
        // we need another subscription to the list.
        if let Some(timeout) = timeout {
            subscriptions.push(timeout_subscription(timeout));
        }

        // `poll_oneoff` needs the same number of events as subscriptions.
        let length = subscriptions.len();
        events.reserve(length);

        debug_assert!(events.capacity() >= length);

        let res = unsafe { wasi::poll(subscriptions.as_ptr(), events.as_mut_ptr(), length) };

        // Remove the timeout subscription we possibly added above.
        if timeout.is_some() {
            let timeout_sub = subscriptions.pop();
            debug_assert_eq!(
                timeout_sub.unwrap().u.tag,
                wasi::EVENTTYPE_CLOCK,
                "failed to remove timeout subscription"
            );
        }

        match res {
            Ok(n_events) => {
                // Safety: `poll_oneoff` initialises the `events` for us.
                unsafe { events.set_len(n_events) };

                let mut subscriptions = self.subscriptions.lock().unwrap();

                let mut timeout_index = None;

                for (i, ev) in events.iter_mut().enumerate() {
                    let fd = ev.userdata as wasi::Fd;

                    if is_timeout_event(ev) {
                        timeout_index = Some(i);
                        continue;
                    }

                    if ev.fd_readwrite.flags & wasi::EVENTRWFLAGS_FD_READWRITE_HANGUP != 0 {
                        closing.push(i);
                    }

                    if let Some((token, _interest, read_state, write_state)) =
                        subscriptions.get(&fd)
                    {
                        if ev.type_ == wasi::EVENTTYPE_FD_READ {
                            ev.userdata = token.0 as wasi::Userdata;
                            read_state.store(false, Ordering::Release);
                            continue;
                        }

                        if ev.type_ == wasi::EVENTTYPE_FD_WRITE {
                            ev.userdata = token.0 as wasi::Userdata;
                            write_state.fetch_sub(1, Ordering::Release);
                            continue;
                        }
                    }
                }

                // Remove the timeout event.
                if timeout.is_some() {
                    if let Some(index) = timeout_index {
                        events.swap_remove(index);
                    }
                }

                if !closing.is_empty() {
                    let closing_fds: Vec<wasi::Fd> = closing
                        .iter()
                        .map(|i| events[*i].userdata as wasi::Fd)
                        .collect();
                    
                    subscriptions.retain(|k, _s| !closing_fds.contains(&k));

                    *events = events
                        .into_iter()
                        .enumerate()
                        .filter_map(|(i, e)| if closing.contains(&i) { None } else { Some(*e) })
                        .collect();
                }

                Ok(())
            }
            Err(err) if err.kind() == io::ErrorKind::InvalidInput && length == 0 => {
                // return Ok when there is no subscriptions
                // otherwise, poll will return an error.
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    #[cfg(feature = "net")]
    pub fn register(
        &self,
        fd: wasi::Fd,
        token: Token,
        interests: Interest,
        (read_state, write_state): (Arc<AtomicBool>, Arc<AtomicUsize>),
    ) -> io::Result<()> {
        let mut subscriptions = self.subscriptions.lock().unwrap();
        subscriptions.insert(fd, (token, interests, read_state, write_state));

        Ok(())
    }

    #[cfg(feature = "net")]
    pub fn reregister(
        &self,
        fd: wasi::Fd,
        token: Token,
        interests: Interest,
        (read_state, write_state): (Arc<AtomicBool>, Arc<AtomicUsize>),
    ) -> io::Result<()> {
        self.deregister(fd)
            .and_then(|()| self.register(fd, token, interests, (read_state, write_state)))
    }

    #[cfg(feature = "net")]
    pub fn deregister(&self, fd: wasi::Fd) -> io::Result<()> {
        let mut ret = Err(io::ErrorKind::NotFound.into());

        let s = self.subscriptions.lock().unwrap().remove(&fd);

        if s.is_some() {
            ret = Ok(())
        }

        ret
    }
    pub fn register_waker(&self) -> bool {
        false
    }
}

/// Token used to a add a timeout subscription, also used in removing it again.
const TIMEOUT_TOKEN: wasi::Userdata = wasi::Userdata::max_value();

/// Returns a `wasi::Subscription` for `timeout`.
fn timeout_subscription(timeout: Duration) -> wasi::Subscription {
    wasi::Subscription {
        userdata: TIMEOUT_TOKEN,
        u: wasi::SubscriptionU {
            tag: wasi::EVENTTYPE_CLOCK,
            u: wasi::SubscriptionUU {
                clock: wasi::SubscriptionClock {
                    id: wasi::CLOCKID_MONOTONIC,
                    // Timestamp is in nanoseconds.
                    timeout: max(
                        min(wasi::Timestamp::MAX as u128, timeout.as_nanos()) as wasi::Timestamp,
                        10,
                    ),
                    // Give the implementation another millisecond to coalesce
                    // events.
                    precision: Duration::from_millis(1).as_nanos() as wasi::Timestamp,
                    // Zero means the `timeout` is considered relative to the
                    // current time.
                    flags: 0,
                },
            },
        },
    }
}

fn is_timeout_event(event: &wasi::Event) -> bool {
    event.type_ == wasi::EVENTTYPE_CLOCK && event.userdata == TIMEOUT_TOKEN
}

/// Check all events for possible errors, it returns the first error found.
fn check_errors(events: &[Event]) -> io::Result<()> {
    for event in events {
        if event.error != 0 {
            return Err(io_err(event.error));
        }
    }
    Ok(())
}

/// Convert `wasi::Errno` into an `io::Error`.
fn io_err(errno: wasi::Errno) -> io::Error {
    // TODO: check if this is valid.
    io::Error::from_raw_os_error(errno as i32)
}

pub type Events = Vec<Event>;
pub type Event = wasi::Event;

pub mod event {
    use std::fmt;

    use crate::sys::Event;
    use crate::Token;
    use wasmedge_wasi_socket::wasi_poll as wasi;

    pub fn token(event: &Event) -> Token {
        Token(event.userdata as usize)
    }

    pub fn is_readable(event: &Event) -> bool {
        event.type_ == wasi::EVENTTYPE_FD_READ
    }

    pub fn is_writable(event: &Event) -> bool {
        event.type_ == wasi::EVENTTYPE_FD_WRITE
    }

    pub fn is_error(event: &Event) -> bool {
        event.error != 0
    }

    pub fn is_read_closed(event: &Event) -> bool {
        event.type_ == wasi::EVENTTYPE_FD_READ
            // Safety: checked the type of the union above.
            && (event.fd_readwrite.flags & wasi::EVENTRWFLAGS_FD_READWRITE_HANGUP) != 0
    }

    pub fn is_write_closed(event: &Event) -> bool {
        event.type_ == wasi::EVENTTYPE_FD_WRITE
            // Safety: checked the type of the union above.
            && (event.fd_readwrite.flags & wasi::EVENTRWFLAGS_FD_READWRITE_HANGUP) != 0
    }

    pub fn is_priority(_: &Event) -> bool {
        // Not supported.
        false
    }

    pub fn is_aio(_: &Event) -> bool {
        // Not supported.
        false
    }

    pub fn is_lio(_: &Event) -> bool {
        // Not supported.
        false
    }

    pub fn debug_details(f: &mut fmt::Formatter<'_>, event: &Event) -> fmt::Result {
        debug_detail!(
            TypeDetails(wasi::Eventtype),
            PartialEq::eq,
            wasi::EVENTTYPE_CLOCK,
            wasi::EVENTTYPE_FD_READ,
            wasi::EVENTTYPE_FD_WRITE,
        );

        #[allow(clippy::trivially_copy_pass_by_ref)]
        fn check_flag(got: &wasi::Eventrwflags, want: &wasi::Eventrwflags) -> bool {
            (got & want) != 0
        }
        debug_detail!(
            EventrwflagsDetails(wasi::Eventrwflags),
            check_flag,
            wasi::EVENTRWFLAGS_FD_READWRITE_HANGUP,
        );

        struct EventFdReadwriteDetails(wasi::EventFdReadwrite);

        impl fmt::Debug for EventFdReadwriteDetails {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_struct("EventFdReadwrite")
                    .field("nbytes", &self.0.nbytes)
                    .field("flags", &self.0.flags)
                    .finish()
            }
        }

        f.debug_struct("Event")
            .field("userdata", &event.userdata)
            .field("error", &event.error)
            .field("type", &TypeDetails(event.type_))
            .field("fd_readwrite", &EventFdReadwriteDetails(event.fd_readwrite))
            .finish()
    }
}

cfg_os_poll! {
    cfg_io_source! {

pub struct IoSourceState {
    readstate: Arc<AtomicBool>,
    writestate: Arc<AtomicUsize>,
}

impl IoSourceState {
    pub fn new() -> IoSourceState {
        IoSourceState {
            readstate: Arc::new(AtomicBool::new(true)),
            writestate: Arc::new(AtomicUsize::new(2)),
        }
    }

    pub fn do_io<T, F, R>(&self, f: F, io: &T) -> io::Result<R>
    where
        F: FnOnce(&T) -> io::Result<R>,
    {
        // We don't hold state, so we can just call the function and
        // return.
        let r = f(io);
        match &r {
            Ok(_) => {
                self.readstate.store(true, Ordering::Release);
                self.writestate.store(2, Ordering::Release);
            }
            Err(e) => {
                if e.kind() == std::io::ErrorKind::WouldBlock {
                    self.readstate.store(true, Ordering::Release);
                    self.writestate.store(2, Ordering::Release);
                }
            }
        }

        r
    }

    pub fn register(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
        fd: RawFd,
    ) -> io::Result<()> {
        registry.selector().register(
            fd as _,
            token,
            interests,
            (self.readstate.clone(), self.writestate.clone()),
        )
    }

    pub fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
        fd: RawFd,
    ) -> io::Result<()> {
        registry.selector().reregister(
            fd as _,
            token,
            interests,
            (self.readstate.clone(), self.writestate.clone()),
        )
    }

    pub fn deregister(&mut self, registry: &Registry, fd: RawFd) -> io::Result<()> {
        registry.selector().deregister(fd as _)
    }
}

    }
}
