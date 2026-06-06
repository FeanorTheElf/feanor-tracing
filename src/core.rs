use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::atomic::Ordering::SeqCst;
use std::sync::*;
use std::collections::HashMap;
use std::num::NonZeroU64;
use std::time::{Instant, Duration};
use thread_local::ThreadLocal;
use std::cell::RefCell;

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
    id: NonZeroU64,
    parent_id: Option<NonZeroU64>,
    state: AtomicSpanState,
    /// accumulated messages; these will be printed as soon as state goes from
    /// [`SpanState::SilentPeriod`] to [`SpanState::PermissionAcquired`]
    messages: Mutex<Vec<String>>,
    /// the time that the last thread entered this span, in microseconds since baseline;
    /// this must be zero if no thread is currently in the span
    entered_time: AtomicU64,
    ref_counter: AtomicU64
}

impl<T> SpanData<T> {

    fn new(id: NonZeroU64, parent_id: Option<NonZeroU64>, data: T) -> Self {
        Self {
            data: data,
            id: id,
            state: AtomicSpanState::new(),
            entered_time: AtomicU64::new(0),
            messages: Mutex::new(Vec::new()),
            parent_id: parent_id,
            ref_counter: AtomicU64::new(0)
        }
    }

    fn data(&self) -> &T {
        &self.data
    }

    fn ref_counter(&self) -> &AtomicU64 {
        &self.ref_counter
    }

    fn log_queued_messages<F>(&self, forward: F)
        where F: Copy + Fn(String)
    {
        for message in self.messages.lock().unwrap().drain(..) {
            forward(message)
        }
    }

    fn queue_message(&self, message: String) {
        self.messages.lock().unwrap().push(message)
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
    fn borrow_permission<F>(&self, span_map: &HashMap<NonZeroU64, SpanData<T>>, forward: F) -> bool
        where F: Copy + Fn(String)
    {
        match self.state.get() {
            SpanState::SilentPeriod => if let Some(parent_id) = self.parent_id {
                let parent = span_map.get(&parent_id).unwrap();
                if parent.borrow_permission(span_map, forward) {
                    self.state.give_and_borrow_permission();
                    self.log_queued_messages(forward);
                    true
                } else {
                    self.state.deny_permission();
                    false
                }
            } else {
                self.state.give_and_borrow_permission();
                self.log_queued_messages(forward);
                true
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
        self.state.init();
    }

    ///
    /// This doesn't perform any logging, use [`SpanData::send_message()`] before exiting!
    ///
    fn exit(&self, span_map: &HashMap<NonZeroU64, SpanData<T>>) {
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

    fn send_message<F>(&self, message: String, span_map: &HashMap<NonZeroU64, SpanData<T>>, baseline: Instant, forward: F, silent_duration: u64)
        where F: Copy + Fn(String)
    {
        match self.state.get() {
            SpanState::SilentPeriod => {
                let entered_time = self.entered_time.load(SeqCst);
                let current_time = baseline.elapsed().as_micros() as u64;
                if current_time >= silent_duration + entered_time {
                    if let Some(parent_id) = self.parent_id {
                        let parent = span_map.get(&parent_id).unwrap();
                        if parent.borrow_permission(span_map, forward) {
                            self.state.give_permission();
                            self.log_queued_messages(forward);
                            forward(message)
                        } else {
                            self.state.deny_permission();
                        }
                    } else {
                        self.state.give_permission();
                        self.log_queued_messages(forward);
                        forward(message)
                    }
                } else {
                    self.queue_message(message);
                    // in case another thread concurrently updated state but was too early
                    // to print these messages
                    match self.state.get() {
                        SpanState::PermissionAcquired | SpanState::PermissionBorrowed => self.log_queued_messages(forward),
                        SpanState::Inactive | SpanState::PermissionDenied | SpanState::SilentPeriod => {}
                    }
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

pub(crate) struct DelayedLoggerImpl<T, F>
    where F: Fn(String)
{
    id_generator: AtomicU64,
    all_spans: RwLock<HashMap<NonZeroU64, SpanData<T>>>,
    // deleted_spans: RwLock<HashMap<NonZeroU64, SpanData<T>>>,
    current_id: ThreadLocal<RefCell<Vec<NonZeroU64>>>,
    forward: F,
    baseline: Instant,
    silent_duration: u64
}

impl<T, F> DelayedLoggerImpl<T, F>
    where F: Fn(String)
{
    pub(crate) fn new(silent_duration: u64, forward: F) -> Self {
        Self {
            baseline: Instant::now() - Duration::from_micros(10),
            id_generator: AtomicU64::new(1),
            all_spans: RwLock::new(HashMap::new()),
            current_id: ThreadLocal::new(),
            silent_duration: silent_duration,
            forward: forward
        }
    }

    #[allow(unused)]
    pub(crate) fn current_span(&self) -> Option<NonZeroU64> {
        self.span_stack().borrow().last().copied()
    }

    pub(crate) fn span_stack(&self) -> &RefCell<Vec<NonZeroU64>> {
        self.current_id.get_or(|| RefCell::new(Vec::new()))
    }

    ///
    /// Be careful about re-entrancy! Don't call other functions of this [`DelayedLogger`]
    /// in the closure, as this might lead to deadlocks!
    ///
    pub(crate) fn span_data<G>(&self, id: NonZeroU64, accept: G)
        where G: FnOnce(/* data = */ &T, /* parent id = */ Option<NonZeroU64>, /* time running = */ Duration)
    {
        let span_map = self.all_spans.read().unwrap();
        let span = span_map.get(&id).unwrap();
        let running_for = span.entered_time.load(SeqCst);
        accept(span.data(), span.parent_id, Duration::from_micros((self.baseline.elapsed().as_micros() as u64).saturating_sub(running_for)))
    }

    ///
    /// Be careful about re-entrancy! Don't call other functions of this [`DelayedLogger`]
    /// in the closure, as this might lead to deadlocks!
    ///
    pub(crate) fn create_span<G>(&self, create_data: G) -> NonZeroU64
        where G: FnOnce(Option<(/* parent data = */ &T, /* parent id = */ NonZeroU64)>) -> T
    {
        let id = NonZeroU64::try_from(self.id_generator.fetch_add(1, SeqCst)).unwrap();
        let mut span_map = self.all_spans.write().unwrap();
        let (span_data, parent_id) = if let Some(parent_id) = self.span_stack().borrow().last().copied() {
            let parent = span_map.get(&parent_id).unwrap();
            (create_data(Some((parent.data(), parent.id))), Some(parent_id))
        } else {
            (create_data(None), None)
        };
        let span = SpanData::new(id, parent_id, span_data);
        span.ref_counter().fetch_add(1, SeqCst);
        span_map.insert(id, span);
        return id;
    }

    pub(crate) fn create_span_with_parent(&self, data: T, parent: Option<NonZeroU64>) -> NonZeroU64 {
        let id = NonZeroU64::try_from(self.id_generator.fetch_add(1, SeqCst)).unwrap();
        let mut span_map = self.all_spans.write().unwrap();
        let span = SpanData::new(id, parent, data);
        span.ref_counter().fetch_add(1, SeqCst);
        span_map.insert(id, span);
        return id;
    }

    pub(crate) fn clone_span(&self, id: NonZeroU64) {
        let span_map = self.all_spans.read().unwrap();
        span_map.get(&id).unwrap().ref_counter().fetch_add(1, SeqCst);
    }

    pub(crate) fn delete_span(&self, id: NonZeroU64) -> bool {
        let span_map = self.all_spans.read().unwrap();
        let last_ref = span_map.get(&id).unwrap().ref_counter().fetch_sub(1, SeqCst) == 1;
        drop(span_map);
        if last_ref {
            let mut span_map = self.all_spans.write().unwrap();
            _ = span_map.remove(&id);
            return true;
        } else {
            return false;
        }
    }

    pub(crate) fn send_message(&self, message: String, span_id: NonZeroU64) {    
        let span_map = self.all_spans.read().unwrap();
        let span = span_map.get(&span_id).unwrap();
        span.send_message(message, &*span_map, self.baseline, &self.forward, self.silent_duration);
    }

    pub(crate) fn enter(&self, span_id: NonZeroU64) {
        let span_map = self.all_spans.read().unwrap();
        let span = span_map.get(&span_id).unwrap();
        self.span_stack().borrow_mut().push(span_id);
        span.enter(self.baseline);
    }

    pub(crate) fn exit(&self, span_id: NonZeroU64) {
        let span_map = self.all_spans.read().unwrap();
        let span = span_map.get(&span_id).unwrap();
        self.span_stack().borrow_mut().pop();
        span.exit(&*span_map);
    }
}

#[cfg(test)]
use std::thread::{sleep, spawn};

#[test]
fn test_spans() {
    let log = Mutex::new(Vec::new());
    let logger = DelayedLoggerImpl::new(0, |m: String| log.lock().unwrap().push(m));

    let a = logger.create_span(|_| "a".to_owned());
    logger.enter(a);
    logger.send_message("enter a".to_owned(), a);

    let b = logger.create_span(|_| "b".to_owned());
    logger.enter(b);
    logger.send_message("enter b".to_owned(), b);
    
    logger.send_message("exit b".to_owned(), b);
    logger.exit(b);
    logger.delete_span(b);

    logger.send_message("exit a".to_owned(), a);
    logger.exit(a);
    logger.delete_span(a);

    assert_eq!(0, logger.all_spans.read().unwrap().len());
    drop(logger);
    let log = log.into_inner().unwrap();
    assert_eq!(vec!["enter a".to_owned(), "enter b".to_owned(), "exit b".to_owned(), "exit a".to_owned()], log);
}

#[test]
fn test_concurrent_spans() {
    let log = Mutex::new(Vec::new());
    let logger = DelayedLoggerImpl::new(0, |m: String| log.lock().unwrap().push(m));

    let a = logger.create_span(|_| "a".to_owned());
    logger.enter(a);
    logger.send_message("enter a".to_owned(), a);

    let b = logger.create_span(|_| "b".to_owned());
    logger.enter(b);
    logger.send_message("enter b".to_owned(), b);
    
    let c = logger.create_span_with_parent("c".to_owned(), Some(a));
    logger.enter(c);
    logger.send_message("enter c".to_owned(), c);
    
    logger.send_message("exit b".to_owned(), b);
    logger.exit(b);
    logger.delete_span(b);

    logger.send_message("exit c".to_owned(), c);
    logger.exit(c);
    logger.delete_span(c);
    
    let d = logger.create_span(|_| "d".to_owned());
    logger.enter(d);
    logger.send_message("enter d".to_owned(), d);
    
    logger.send_message("exit d".to_owned(), d);
    logger.exit(d);
    logger.delete_span(d);

    logger.send_message("exit a".to_owned(), a);
    logger.exit(a);
    logger.delete_span(a);

    assert_eq!(0, logger.all_spans.read().unwrap().len());
    drop(logger);
    let log = log.into_inner().unwrap();
    assert_eq!(vec!["enter a".to_owned(), "enter b".to_owned(), "exit b".to_owned(), "enter d".to_owned(), "exit d".to_owned(), "exit a".to_owned()], log);
}

#[test]
fn test_skip_short_spans() {
    let log = Mutex::new(Vec::new());
    let logger = DelayedLoggerImpl::new(1000, |m: String| log.lock().unwrap().push(m));

    let a = logger.create_span(|_| "a".to_owned());
    logger.enter(a);
    logger.send_message("enter a".to_owned(), a);

    let b = logger.create_span(|_| "b".to_owned());
    logger.enter(b);
    logger.send_message("enter b".to_owned(), b);
    
    sleep(Duration::from_micros(100));
    
    logger.send_message("exit b".to_owned(), b);
    logger.exit(b);
    logger.delete_span(b);

    logger.send_message("exit a".to_owned(), a);
    logger.exit(a);
    logger.delete_span(a);

    assert_eq!(0, logger.all_spans.read().unwrap().len());
    drop(logger);
    let log = log.into_inner().unwrap();
    assert_eq!(Vec::<String>::new(), log);


    let log = Mutex::new(Vec::new());
    let logger = DelayedLoggerImpl::new(1000, |m: String| log.lock().unwrap().push(m));

    let a = logger.create_span(|_| "a".to_owned());
    logger.enter(a);
    logger.send_message("enter a".to_owned(), a);

    let b = logger.create_span(|_| "b".to_owned());
    logger.enter(b);
    logger.send_message("enter b".to_owned(), b);
    
    logger.send_message("exit b".to_owned(), b);
    logger.exit(b);
    logger.delete_span(b);
    
    sleep(Duration::from_micros(2000));

    logger.send_message("exit a".to_owned(), a);
    logger.exit(a);
    logger.delete_span(a);

    assert_eq!(0, logger.all_spans.read().unwrap().len());
    drop(logger);
    let log = log.into_inner().unwrap();
    assert_eq!(vec!["enter a".to_owned(), "exit a".to_owned()], log);
}

#[test]
fn test_log_first_long_span() {
    let log = Mutex::new(Vec::new());
    let logger = DelayedLoggerImpl::new(1000, |m: String| log.lock().unwrap().push(m));

    let a = logger.create_span(|_| "a".to_owned());
    logger.enter(a);
    logger.send_message("enter a".to_owned(), a);

    let b = logger.create_span(|_| "b".to_owned());
    logger.enter(b);
    logger.send_message("enter b".to_owned(), b);
    
    let c = logger.create_span_with_parent("c".to_owned(), Some(a));
    logger.enter(c);
    logger.send_message("enter c".to_owned(), c);
    
    logger.send_message("exit b".to_owned(), b);
    logger.exit(b);
    logger.delete_span(b);

    sleep(Duration::from_micros(2000));

    logger.send_message("exit c".to_owned(), c);
    logger.exit(c);
    logger.delete_span(c);
    
    let d = logger.create_span(|_| "d".to_owned());
    logger.enter(d);
    logger.send_message("enter d".to_owned(), d);
    
    logger.send_message("exit d".to_owned(), d);
    logger.exit(d);
    logger.delete_span(d);

    logger.send_message("exit a".to_owned(), a);
    logger.exit(a);
    logger.delete_span(a);

    assert_eq!(0, logger.all_spans.read().unwrap().len());
    drop(logger);
    let log = log.into_inner().unwrap();
    assert_eq!(vec!["enter a".to_owned(), "enter c".to_owned(), "exit c".to_owned(), "exit a".to_owned()], log);
}

#[test]
fn test_span_tree_not_execution_stack() {
    let log = Mutex::new(Vec::new());
    let logger = Arc::new(DelayedLoggerImpl::new(1000, move |m: String| log.lock().unwrap().push(m)));
    let barrier = Arc::new(Barrier::new(2));

    let a = logger.create_span(|_| "/a".to_owned());
    logger.enter(a);
    let logger_copy = logger.clone();
    let barrier_copy = barrier.clone();
    let helper_thread = spawn(move || {
        let b = logger_copy.create_span_with_parent("/a/b".to_owned(), Some(a));
        logger_copy.enter(b);
        logger_copy.span_data(logger_copy.current_span().unwrap(), |data, _, _| assert_eq!("/a/b", data));
        logger_copy.exit(b);
        logger_copy.delete_span(b);
        barrier_copy.wait();
        barrier_copy.wait();
        let c = logger_copy.create_span(|parent| format!("{}/c", parent.map(|(s, _)| s.as_str()).unwrap_or("")));
        logger_copy.enter(c);
        logger_copy.span_data(logger_copy.current_span().unwrap(), |data, _, _| assert_eq!("/c", data));
        logger_copy.exit(c);
        logger_copy.delete_span(c);
    });
    barrier.wait();
    logger.exit(a);
    logger.delete_span(a);
    barrier.wait();
    helper_thread.join().unwrap();
}