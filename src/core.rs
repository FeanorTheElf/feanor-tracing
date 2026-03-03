use std::sync::atomic::{AtomicU32, AtomicUsize, AtomicU64};
use std::sync::atomic::Ordering::SeqCst;
use std::sync::{Mutex, RwLock};
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::time::{Instant, Duration};
use thread_local::ThreadLocal;
use std::cell::Cell;

#[repr(u32)]
#[derive(PartialEq, Eq, Copy, Clone)]
enum SpanState {
    SilentPeriod = 0,
    PermissionAcquired = 1,
    PermissionDenied = 2,
    PermissionBorrowed = 3,
    Inactive = 4
}

impl TryFrom<u32> for SpanState {

    type Error = ();

    fn try_from(value: u32) -> Result<Self, ()> {
        match value {
            0 => Ok(SpanState::SilentPeriod),
            1 => Ok(SpanState::PermissionAcquired),
            2 => Ok(SpanState::PermissionDenied),
            3 => Ok(SpanState::PermissionBorrowed),
            4 => Ok(SpanState::Inactive),
            _ => Err(())
        }
    }
}

impl Into<u32> for SpanState {

    fn into(self) -> u32 {
        match self {
            SpanState::SilentPeriod => 0,
            SpanState::PermissionAcquired => 1,
            SpanState::PermissionDenied => 2,
            SpanState::PermissionBorrowed => 3,
            SpanState::Inactive => 4
        }
    }
}

///
/// State transitions:
///  - Inactive -> SilentPeriod: when entering a span, assumed to never be concurrent (one span is only active in one thread)
///  - Inactive -> PermissionAcquired: when entering a root span
///  - SilentPeriod -> PermissionAcquired: when borrowing permission from parent span; always log queued messages in this case!
///  - SilentPeriod -> PermissionBorrowed: when borrowing permission from parent span and directly passing it to child span; always log queued messages in this case!
///  - SilentPeriod -> PermissionDenied: when failing to borrow permission from parent span
///  - PermissionAcquired -> PermissionBorrowed: when child span borrows permission
///  - PermissionBorrowed -> PermissionAcquired: when child span returns permission
///  - PermissionAcquired | PermissionDenied | SilentPeriod -> Inactive: when leaving a span
///
struct AtomicSpanState {
    state: AtomicU32
}

impl AtomicSpanState {

    fn new() -> Self {
        Self { state: AtomicU32::new(SpanState::Inactive.into()) }
    }

    fn init(&self) {
        assert!(self.state.swap(SpanState::SilentPeriod.into(), SeqCst) == SpanState::Inactive.into());
    }

    fn init_and_give_permission(&self) {
        assert!(self.state.swap(SpanState::PermissionAcquired.into(), SeqCst) == SpanState::Inactive.into());
    }

    fn reset(&self) -> SpanState {
        SpanState::try_from(self.state.swap(SpanState::Inactive.into(), SeqCst)).unwrap()
    }

    fn get(&self) -> SpanState {
        SpanState::try_from(self.state.load(SeqCst)).unwrap()
    }

    fn give_permission(&self) {
        // we have to be careful about concurrency here. it can happen that multiple child spans
        // simultaneously try to borrow the permission for parent (to get it afterwards themselves).
        // in that case, if the span state is not SilentPeriod anymore, another thread came earlier;
        // however, that earlier thread may have failed to borrow the permission since we already did!
        // thus, overwrite a borrowed permission here
        match SpanState::try_from(self.state.swap(SpanState::PermissionAcquired.into(), SeqCst)).unwrap() {
            SpanState::Inactive => unreachable!(),
            _ => {}
        }
    }

    fn give_and_borrow_permission(&self) {
        // we have to be careful about concurrency here. it can happen that multiple child spans
        // simultaneously try to borrow the permission for parent (to get it afterwards themselves).
        // in that case, if the span state is not SilentPeriod anymore, another thread came earlier;
        // however, that earlier thread may have failed to borrow the permission since we already did!
        // thus, overwrite a borrowed permission here
        match SpanState::try_from(self.state.swap(SpanState::PermissionBorrowed.into(), SeqCst)).unwrap() {
            SpanState::Inactive => unreachable!(),
            _ => {}
        }
    }

    fn deny_permission(&self) {
        // we have to be careful about concurrency here. it can happen that multiple child spans
        // simultaneously try to borrow the permission for parent (to get it afterwards themselves).
        // in that case, if the span state is not SilentPeriod anymore, another thread came earlier
        match self.state.compare_exchange(SpanState::SilentPeriod.into(), SpanState::PermissionDenied.into(), SeqCst, SeqCst).map_err(|state| SpanState::try_from(state).unwrap()) {
            Err(SpanState::Inactive) => unreachable!(),
            _ => {}
        }
    }

    fn borrow_permission(&self) -> bool {
        match self.state.compare_exchange(SpanState::PermissionAcquired.into(), SpanState::PermissionBorrowed.into(), SeqCst, SeqCst) {
            Ok(_) => true,
            Err(value) => match SpanState::try_from(value).unwrap() {
                SpanState::PermissionDenied |SpanState::PermissionBorrowed => false,
                SpanState::SilentPeriod | SpanState::PermissionAcquired | SpanState::Inactive => unreachable!()
            }
        }
    }
    
    fn return_permission(&self) {
        assert!(self.state.swap(SpanState::PermissionAcquired.into(), SeqCst) == SpanState::PermissionBorrowed.into())
    }
}

struct SpanData<T> {
    data: T,
    id: NonZeroUsize,
    parent_id: Option<NonZeroUsize>,
    state: AtomicSpanState,
    /// accumulated messages; these will be printed as soon as state goes from
    /// [`SpanState::SilentPeriod`] to [`SpanState::PermissionAcquired`]
    messages: Mutex<Vec<String>>,
    /// the time that the last thread entered this span, in microseconds since baseline;
    /// this must be zero if no thread is currently in the span
    entered_time: AtomicU64,
    ref_counter: AtomicUsize
}

impl<T> SpanData<T> {

    fn new(id: NonZeroUsize, parent_id: Option<NonZeroUsize>, data: T) -> Self {
        Self {
            data: data,
            id: id,
            state: AtomicSpanState::new(),
            entered_time: AtomicU64::new(0),
            messages: Mutex::new(Vec::new()),
            parent_id: parent_id,
            ref_counter: AtomicUsize::new(0)
        }
    }

    fn data(&self) -> &T {
        &self.data
    }

    fn ref_counter(&self) -> &AtomicUsize {
        &self.ref_counter
    }

    fn log_queued_messages<F>(&self, forward: F)
        where F: Copy + Fn(&str)
    {
        for message in self.messages.lock().unwrap().drain(..) {
            forward(&message)
        }
    }

    fn queue_message(&self, message: &str) {
        self.messages.lock().unwrap().push(message.to_owned())
    }

    fn discard_queued_messages(&self) {
        self.messages.lock().unwrap().drain(..);
    }

    ///
    /// Borrows the permission, if possible. If the current span is still in the silent
    /// period, it will assume that the period is over (since a child span tries to get
    /// permission), and will try to obtain permission from the parent, and then pass it on.
    ///
    /// Returns true if the permission could be acquired. In that case, its state afterwards
    /// will be [`SpanState::PermissionBorrowed`]. Otherwise, it will usually be
    /// [`SpanState::PermissionDenied`], but can also have a different state if another
    /// concurrent borrow_permission() was successful.
    ///
    fn borrow_permission<F>(&self, span_map: &HashMap<NonZeroUsize, SpanData<T>>, forward: F) -> bool
        where F: Copy + Fn(&str)
    {
        match self.state.get() {
            SpanState::SilentPeriod => {
                let parent = span_map.get(&self.parent_id.unwrap()).unwrap();
                if parent.borrow_permission(span_map, forward) {
                    self.state.give_and_borrow_permission();
                    self.log_queued_messages(forward);
                    true
                } else {
                    self.state.deny_permission();
                    false
                }
            },
            SpanState::PermissionAcquired => self.state.borrow_permission(),
            SpanState::PermissionBorrowed | SpanState::PermissionDenied => false,
            SpanState::Inactive => unreachable!()
        }
    }

    ///
    /// This doesn't perform any logging, use [`SpanData::send_message()`] after entering!
    ///
    fn enter(&self, baseline: Instant) {
        self.entered_time.store(baseline.elapsed().as_micros() as u64, SeqCst);
        if self.parent_id.is_none() {
            self.state.init_and_give_permission();
        } else {
            self.state.init();
        }
    }

    ///
    /// This doesn't perform any logging, use [`SpanData::send_message()`] before exiting!
    ///
    fn exit(&self, span_map: &HashMap<NonZeroUsize, SpanData<T>>) {
        self.discard_queued_messages();
        self.entered_time.store(0, SeqCst);
        match self.state.reset() {
            SpanState::SilentPeriod | SpanState::PermissionDenied => {},
            SpanState::PermissionBorrowed => panic!("span closed despite active child"),
            SpanState::Inactive => unreachable!(),
            SpanState::PermissionAcquired => if let Some(parent_id) = self.parent_id {
                let parent = span_map.get(&parent_id).unwrap();
                parent.state.return_permission();
            }
        }
    }

    fn send_message<F>(&self, message: &str, span_map: &HashMap<NonZeroUsize, SpanData<T>>, baseline: Instant, forward: F, silent_duration: u64)
        where F: Copy + Fn(&str)
    {
        match self.state.get() {
            SpanState::SilentPeriod => {
                let entered_time = self.entered_time.load(SeqCst);
                let current_time = baseline.elapsed().as_micros() as u64;
                if current_time >= silent_duration + entered_time {
                    let parent = span_map.get(&self.parent_id.unwrap()).unwrap();
                    if parent.borrow_permission(span_map, forward) {
                        self.state.give_permission();
                        self.log_queued_messages(forward);
                        forward(message)
                    } else {
                        self.state.deny_permission();
                    }
                } else {
                    self.queue_message(message);
                }
            },
            SpanState::PermissionAcquired => {
                forward(message)
            },
            SpanState::PermissionBorrowed => {
                // not sure what to do here; should we log or shouldn't we?
                // we might also panic, but it does seem possible that a parent
                // span gets events while child spans are active
            },
            SpanState::PermissionDenied => {},
            SpanState::Inactive => unreachable!()
        }
    }
}

struct LoggerCore<T, F>
    where F: Fn(&str)
{
    id_generator: AtomicUsize,
    all_spans: RwLock<HashMap<NonZeroUsize, SpanData<T>>>,
    current_id: ThreadLocal<Cell<Option<NonZeroUsize>>>,
    forward: F,
    baseline: Instant,
    silent_duration: u64
}

impl<T, F> LoggerCore<T, F>
    where F: Fn(&str)
{
    fn new(silent_duration: u64, forward: F) -> Self {
        Self {
            baseline: Instant::now() - Duration::from_micros(10),
            id_generator: AtomicUsize::new(1),
            all_spans: RwLock::new(HashMap::new()),
            current_id: ThreadLocal::new(),
            silent_duration: silent_duration,
            forward: forward
        }
    }

    fn current_span(&self) -> &Cell<Option<NonZeroUsize>> {
        self.current_id.get_or(|| Cell::new(None))
    }

    fn create_span(&self, data: T) -> NonZeroUsize {
        return self.create_span_with_parent(data, self.current_span().get());
    }

    fn create_span_with_parent(&self, data: T, parent: Option<NonZeroUsize>) -> NonZeroUsize {
        let id = NonZeroUsize::try_from(self.id_generator.fetch_add(1, SeqCst)).unwrap();
        let mut span_map = self.all_spans.write().unwrap();
        let span = SpanData::new(id, parent, data);
        span.ref_counter().fetch_add(1, SeqCst);
        span_map.insert(id, span);
        return id;
    }

    fn clone_span(&self, id: NonZeroUsize) {
        let span_map = self.all_spans.read().unwrap();
        span_map.get(&id).unwrap().ref_counter().fetch_add(1, SeqCst);
    }

    fn delete_span(&self, id: NonZeroUsize) {
        let span_map = self.all_spans.read().unwrap();
        let last_ref = span_map.get(&id).unwrap().ref_counter().fetch_sub(1, SeqCst) == 1;
        drop(span_map);
        if last_ref {
            let mut span_map = self.all_spans.write().unwrap();
            _ = span_map.remove(&id);
        }
    }

    fn send_message<M>(&self, message: M, span_id: NonZeroUsize)
        where M: FnOnce(&T) -> String
    {    
        let span_map = self.all_spans.read().unwrap();
        let span = span_map.get(&span_id).unwrap();
        span.send_message(&message(span.data()), &*span_map, self.baseline, &self.forward, self.silent_duration);
    }

    fn enter(&self, span_id: NonZeroUsize) {
        let span_map = self.all_spans.read().unwrap();
        let span = span_map.get(&span_id).unwrap();
        self.current_span().set(Some(span_id));
        span.enter(self.baseline);
    }

    fn exit(&self, span_id: NonZeroUsize) {
        let span_map = self.all_spans.read().unwrap();
        let span = span_map.get(&span_id).unwrap();
        self.current_span().set(span.parent_id);
        span.exit(&*span_map);
    }
}

#[cfg(test)]
use std::thread::sleep;

#[test]
fn test_spans() {
    let log = Mutex::new(Vec::new());
    let logger = LoggerCore::new(0, |m: &str| log.lock().unwrap().push(m.to_owned()));

    let a = logger.create_span("a");
    logger.enter(a);
    logger.send_message(|name| format!("enter {}", name), a);

    let b = logger.create_span("b");
    logger.enter(b);
    logger.send_message(|name| format!("enter {}", name), b);
    
    logger.send_message(|name| format!("exit {}", name), b);
    logger.exit(b);
    logger.delete_span(b);

    logger.send_message(|name| format!("exit {}", name), a);
    logger.exit(a);
    logger.delete_span(a);

    assert_eq!(0, logger.all_spans.read().unwrap().len());
    drop(logger);
    let log = log.into_inner().unwrap();
    assert_eq!(vec!["enter a", "enter b", "exit b", "exit a"], log);
}

#[test]
fn test_concurrent_spans() {
    let log = Mutex::new(Vec::new());
    let logger = LoggerCore::new(0, |m: &str| log.lock().unwrap().push(m.to_owned()));

    let a = logger.create_span("a");
    logger.enter(a);
    logger.send_message(|name| format!("enter {}", name), a);

    let b = logger.create_span("b");
    logger.enter(b);
    logger.send_message(|name| format!("enter {}", name), b);
    
    let c = logger.create_span_with_parent("c", Some(a));
    logger.enter(c);
    logger.send_message(|name| format!("enter {}", name), c);
    
    logger.send_message(|name| format!("exit {}", name), b);
    logger.exit(b);
    logger.delete_span(b);

    logger.send_message(|name| format!("exit {}", name), c);
    logger.exit(c);
    logger.delete_span(c);
    
    let d = logger.create_span("d");
    logger.enter(d);
    logger.send_message(|name| format!("enter {}", name), d);
    
    logger.send_message(|name| format!("exit {}", name), d);
    logger.exit(d);
    logger.delete_span(d);

    logger.send_message(|name| format!("exit {}", name), a);
    logger.exit(a);
    logger.delete_span(a);

    assert_eq!(0, logger.all_spans.read().unwrap().len());
    drop(logger);
    let log = log.into_inner().unwrap();
    assert_eq!(vec!["enter a", "enter b", "exit b", "enter d", "exit d", "exit a"], log);
}

#[test]
fn test_skip_short_spans() {
    let log = Mutex::new(Vec::new());
    let logger = LoggerCore::new(1000, |m: &str| log.lock().unwrap().push(m.to_owned()));

    let a = logger.create_span("a");
    logger.enter(a);
    logger.send_message(|name| format!("enter {}", name), a);

    let b = logger.create_span("b");
    logger.enter(b);
    logger.send_message(|name| format!("enter {}", name), b);
    
    let c = logger.create_span_with_parent("c", Some(a));
    logger.enter(c);
    logger.send_message(|name| format!("enter {}", name), c);
    
    logger.send_message(|name| format!("exit {}", name), b);
    logger.exit(b);
    logger.delete_span(b);

    sleep(Duration::from_micros(2000));

    logger.send_message(|name| format!("exit {}", name), c);
    logger.exit(c);
    logger.delete_span(c);
    
    let d = logger.create_span("d");
    logger.enter(d);
    logger.send_message(|name| format!("enter {}", name), d);
    
    logger.send_message(|name| format!("exit {}", name), d);
    logger.exit(d);
    logger.delete_span(d);

    logger.send_message(|name| format!("exit {}", name), a);
    logger.exit(a);
    logger.delete_span(a);

    assert_eq!(0, logger.all_spans.read().unwrap().len());
    drop(logger);
    let log = log.into_inner().unwrap();
    assert_eq!(vec!["enter a", "enter c", "exit c", "exit a"], log);
}