#![allow(dead_code)]
use std::cmp::{max, min};
use std::collections::HashMap;
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::atomic::AtomicBool;
#[cfg(feature = "net")]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use crate::Registry;
#[cfg(feature = "net")]
use crate::{Interest, Token};
use ::wasi::ERRNO_BADF;
use wamr_wasi_socket::socket::{AddressFamily, Socket, SocketType};
use wamr_wasi_socket::wasi_poll as wasi;

#[derive(Clone, Default)]
pub struct Waker(pub(self) Arc<Mutex<WakerInner>>);

impl std::fmt::Debug for Waker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Ok(waker) = self.0.try_lock() {
            waker.fmt(f)
        } else {
            f.debug_tuple("Waker (locked)").field(&self.0).finish()
        }
    }
}
impl Waker {
    pub fn new(selector: &Selector, token: Token) -> io::Result<Waker> {
        selector.waker(token)
    }

    pub fn wake(&self) -> io::Result<()> {
        self.0.lock().unwrap().wake()?;
        return Ok(());
    }
}

/// Poll State machine
/// 
/// Transitioning out of the [`PollState::Polling`] state will [`Drop::drop`]
/// the socket which will wake the poll call
#[derive(Debug, Default)]
pub enum PollState {
    #[default]
    /// Default state: causes next [`Selector::select`] call to run poll to completion
    /// 
    /// Next state:
    ///  - -> Polling : when the [`Selector::do_poll`] function is called
    Reset,

    /// Polling in an other thread
    /// 
    /// Next states:
    ///  - -> Finished : when the poll call terminates
    ///  - -> Reset : when an internal event causes the poll to rerun
    ///  - -> WakerEvent : when a waker wakes
    Polling(Socket),

    /// Waking state: the current or next call to [`Selector::select`] will return early with a Waker event
    /// 
    /// Next states:
    ///  - -> Reset : when the waker event is processed and the [`Selector::select`] function returns
    WakerEvent,

    /// Bookkeeping: [`Selector::do_poll`] is returning and hasn't yet rearmed the state
    /// 
    /// Next states:
    ///  - -> Reset : when the output is processed and the [`Selector::select`] function returns
    Finished,
}

#[derive(Debug, Default)]
struct WakerInner {
    pub(self) token: Option<Token>,
    pub(self) state: PollState,
}

impl WakerInner {
    pub fn wake(&mut self) -> io::Result<()> {
        self.state = PollState::WakerEvent;
        Ok(())
    }

    pub fn wake_rerun(&mut self) -> io::Result<()> {
        // only set rerun if the socket is currently polling
        // avoid a race condition between [`wake`] and [`wake_rerun`] leading to a rerun
        if matches!(self.state, PollState::Polling(_)) {
            self.state = PollState::Reset;
        }
        Ok(())
    }

    pub fn event(&self) -> wasi::Event {
        wasi::Event {
            userdata: self.token.map(|t| t.0).unwrap_or_default() as u64,
            error: 0,
            type_: wasi::EVENTTYPE_FD_READ,
            fd_readwrite: wasi::EventFdReadwrite {
                nbytes: 0,
                flags: 0,
            },
        }
    }
}

cfg_net! {

pub mod tcp {
    use std::io;
    use std::net::SocketAddr;
    use wamr_wasi_socket::socket::{self, Socket};
    use std::convert::TryInto;

    pub(crate) use wamr_wasi_socket::TcpListener;
    pub(crate) use wamr_wasi_socket::TcpStream;

    pub(crate) fn new_for_addr(address: SocketAddr) -> io::Result<Socket> {
        let domain = match address {
            SocketAddr::V4(_) => socket::AddressFamily::Inet4,
            SocketAddr::V6(_) => socket::AddressFamily::Inet6,
        };

        let s = socket::Socket::new(domain, socket::SocketType::Stream)?;
        s.set_nonblocking(true)?;
        Ok(s)
    }


    /// probably very unsafe ??
    pub(crate) fn bind(socket: &wamr_wasi_socket::TcpListener, addr: SocketAddr) -> io::Result<()> {
        socket.as_ref().bind(&addr)
    }


    pub(crate) fn connect(socket: &Socket, addr: SocketAddr) -> io::Result<()> {
        match socket.connect(&addr) {
            Err(err) if err.raw_os_error() == Some(libc::EINPROGRESS) => {
                Ok(())
            },
            Err(err) => {
                Err(err)
            }
            Ok(()) => Ok(()),
        }
    }

    pub(crate) fn accept(listener: &wamr_wasi_socket::TcpListener) -> io::Result<(wamr_wasi_socket::TcpStream, SocketAddr)> {
        let s = listener.accept()?;
        s.0.set_nonblocking(true)?;
        Ok(s)
    }


    pub(crate) fn listen(socket: &wamr_wasi_socket::TcpListener, backlog: u32) -> io::Result<()> {
        let backlog = backlog.try_into().unwrap_or(i32::max_value());
        socket.as_ref().listen(backlog)
    }

    pub(crate) fn set_reuseaddr(socket: &wamr_wasi_socket::TcpListener, reuseaddr: bool) -> io::Result<()> {
        socket.as_ref().set_reuse_addr(reuseaddr)
    }
}

pub mod udp {
    use std::io;
    use std::net;

    pub(crate) use wamr_wasi_socket::UdpSocket;

    pub fn bind(addr: net::SocketAddr) -> io::Result<wamr_wasi_socket::UdpSocket> {
        let s = wamr_wasi_socket::UdpSocket::bind(addr)?;
        s.set_nonblocking(true)?;
        Ok(s)
    }

    pub(crate) fn only_v6(socket: &wamr_wasi_socket::UdpSocket) -> io::Result<bool> {
        socket.as_ref().ipv6_only()
    }
}

}

/// Unique id for use as `SelectorId`.
#[cfg(feature = "net")]
static NEXT_ID: AtomicUsize = AtomicUsize::new(1);

#[allow(unused)]
/// Safety: lock order dependency: self.subscriptions can only be locked after self.waker
pub struct Selector {
    #[cfg(feature = "net")]
    id: usize,
    /// Subscriptions (reads events) we're interested in.
    subscriptions:
        Arc<Mutex<HashMap<wasi::Fd, (Token, Interest, Arc<AtomicBool>, Arc<AtomicUsize>)>>>,
    /// waker system for this Selector
    waker: Waker,
}

impl Selector {
    pub(crate) fn waker(&self, token: Token) -> io::Result<Waker> {
        self.waker.0.lock().unwrap().token = Some(token);
        Ok(self.waker.clone())
    }

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
            waker: Default::default(),
        })
    }

    pub fn try_clone(&self) -> io::Result<Selector> {
        Ok(Selector {
            id: self.id,
            subscriptions: self.subscriptions.clone(),
            waker: self.waker.clone(),
        })
    }

    #[cfg(feature = "net")]
    pub fn id(&self) -> usize {
        self.id
    }

    pub fn select(&self, events: &mut Events, mut timeout: Option<Duration>) -> io::Result<()> {
        loop {
            let mut waker = self.waker.0.lock().unwrap();
            match waker.state {
                PollState::Reset => {
                    let duration = self.do_poll(events, timeout, waker)?;

                    if let Some(timeout) = &mut timeout {
                        *timeout = timeout.saturating_sub(duration);
                    }
                }
                PollState::WakerEvent => {
                    // this path should only happen if the waker event was triggered outside of the polling call
                    waker.state = PollState::Reset;
                    events.clear();
                    events.push(waker.event());
                    return Ok(());
                }
                PollState::Finished => {
                    waker.state = PollState::Reset;
                    return Ok(());
                }
                PollState::Polling(_) => {
                    return Err(io::ErrorKind::ResourceBusy.into());
                }
            }
        }
    }

    ///
    /// # Summary
    /// Collect the subscriptions and reset the locks for a poll to occur
    /// poll needs to be called after this funtion returns
    ///
    /// # Details
    /// by taking the MutexGuard, it atomicise the following:
    /// - check if a (re)run needs to happen (already done)
    /// - fetch the current list of subscriptions
    /// - set the Polling state
    ///
    /// if anything goes out of sync, a race condition could loose a wake call
    fn prepare_poll(
        &self,
        timeout: Option<Duration>,
        mut waker: MutexGuard<'_, WakerInner>,
        waker_socket: Socket,
    ) -> Vec<wasi::Subscription> {
        let waker_fd = waker_socket.as_raw_fd();

        waker.state = PollState::Polling(waker_socket);

        let mut subscriptions = self.subscriptions();

        subscriptions.push(wasi::Subscription {
            userdata: waker_fd as wasi::Userdata,
            u: wasi::SubscriptionU {
                tag: wasi::EVENTTYPE_FD_READ,
                u: wasi::SubscriptionUU {
                    fd_read: wasi::SubscriptionFdReadwrite {
                        file_descriptor: waker_fd as _,
                    },
                },
            },
        });

        // If we want to a use a timeout in the `wasi_poll_oneoff()` function
        // we need another subscription to the list.
        if let Some(timeout) = timeout {
            subscriptions.push(timeout_subscription(timeout));
        }

        subscriptions
    }

    ///
    /// Returns the duration the Poll actually ran for
    ///
    /// when [`Self::do_poll`] returns, the [`PollState`] is either [`PollState::Finished`] or [`PollState::Reset`]
    fn do_poll(
        &self,
        events: &mut Vec<wasi::Event>,
        timeout: Option<Duration>,
        waker: MutexGuard<'_, WakerInner>,
    ) -> Result<Duration, io::Error> {
        let waker_socket = Socket::new(AddressFamily::Inet4, SocketType::Datagram)?;
        let waker_fd = waker_socket.as_raw_fd() as wasi::Fd;

        let subscriptions = self.prepare_poll(timeout, waker, waker_socket);

        let mut closing = vec![];
        let mut deregistered = vec![];

        // `poll_oneoff` needs the same number of events as subscriptions.
        let length = subscriptions.len();
        events.clear();
        // footgun: the 'additional' is compared to [`Vec::len`] not [`Vec::capacity`]
        events.reserve(length);

        debug_assert!(events.capacity() >= length);

        let now = std::time::Instant::now();

        let res = unsafe { wasi::poll(subscriptions.as_ptr(), events.as_mut_ptr(), length) };

        let mut waker = self.waker.0.lock().unwrap();
        let duration = std::time::Instant::now() - now;

        if matches!(waker.state, PollState::Polling(_)) {
            waker.state = PollState::Finished;
        }

        let n_events = res?;

        // Safety: `poll_oneoff` initialises the `events` for us.
        unsafe { events.set_len(n_events) };

        let mut subscriptions = self.subscriptions.lock().unwrap();
        let waker_token = waker.token.as_ref().map(|t| t.0).unwrap_or_default();

        let mut timeout_index = None;

        for (i, ev) in events.iter_mut().enumerate() {
            let fd = ev.userdata as wasi::Fd;
            if fd == waker_fd {
                ev.fd_readwrite.flags = 0;
                ev.userdata = waker_token as _;
                continue;
            }

            if is_timeout_event(ev) {
                timeout_index = Some(i);
                continue;
            }

            if ev.fd_readwrite.flags & wasi::EVENTRWFLAGS_FD_READWRITE_HANGUP != 0
                || ev.error == ERRNO_BADF.raw()
            {
                closing.push(i);
            }

            if let Some((token, _interest, read_state, write_state)) = subscriptions.get(&fd) {
                if ev.type_ == wasi::EVENTTYPE_FD_READ {
                    ev.userdata = token.0 as wasi::Userdata;
                    read_state.store(false, Ordering::Release);
                    continue;
                }

                if ev.type_ == wasi::EVENTTYPE_FD_WRITE {
                    ev.userdata = token.0 as wasi::Userdata;
                    write_state.store(0, Ordering::Release);
                    continue;
                }
            } else {
                deregistered.push(i);
            }
        }

        // Remove the timeout event.
        if timeout.is_some() {
            if let Some(index) = timeout_index {
                events.swap_remove(index);
            }
        }

        if !closing.is_empty() || !deregistered.is_empty() {
            let closing_fds: Vec<wasi::Fd> = closing
                .iter()
                .map(|i| events[*i].userdata as wasi::Fd)
                .collect();

            subscriptions.retain(|k, _s| !closing_fds.contains(&k));

            *events = events
                .into_iter()
                .enumerate()
                .filter_map(|(i, e)| {
                    if closing.contains(&i) || deregistered.contains(&i) {
                        None
                    } else {
                        Some(*e)
                    }
                })
                .collect();
        }


        Ok(duration)
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
        let mut waker = self.waker.0.lock().unwrap();
        subscriptions.insert(fd, (token, interests, read_state, write_state));
        waker.wake_rerun()?;

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
        self.waker.0.lock().unwrap().token.is_some()
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
    use wamr_wasi_socket::wasi_poll as wasi;

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
