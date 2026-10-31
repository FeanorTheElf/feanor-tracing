use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::atomic::Ordering::SeqCst;
use std::sync::*;
use std::hash::{BuildHasherDefault, Hasher};
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

///
/// Span ids are already unique, so use them directly as hash. This also keeps the ids reserved
/// by one thread (see [`ID_BLOCK_LEN`]) close together in the map, which avoids false sharing.
///
#[derive(Default)]
struct IdHasher(u64);

impl Hasher for IdHasher {

    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, _bytes: &[u8]) {
        unreachable!("only span ids are hashed")
    }

    fn write_u64(&mut self, value: u64) {
        self.0 = value;
    }
}

type IdHashBuilder = BuildHasherDefault<IdHasher>;

type SpanMap<'a, Data, Message> = papaya::HashMapRef<'a, NonZeroU64, SpanData<Data, Message>, IdHashBuilder, papaya::LocalGuard<'a>>;

struct SpanData<Data, Message> {
    data: Data,
    id: NonZeroU64,
    parent_id: Option<NonZeroU64>,
    state: AtomicSpanState,
    /// accumulated messages; these will be printed as soon as state goes from
    /// [`SpanState::SilentPeriod`] to [`SpanState::PermissionAcquired`]
    messages: Mutex<Vec<Message>>,
    /// the time that the last thread entered this span, in microseconds since baseline;
    /// this must be zero if no thread is currently in the span
    entered_time: AtomicU64,
    /// the number of span ids referencing this span, plus the number of its child spans;
    /// the span is removed when this reaches zero
    ref_counter: AtomicU64
}

impl<Data, Message> SpanData<Data, Message> {

    fn new(id: NonZeroU64, parent_id: Option<NonZeroU64>, data: Data) -> Self {
        Self {
            data: data,
            id: id,
            state: AtomicSpanState::new(),
            entered_time: AtomicU64::new(0),
            messages: Mutex::new(Vec::new()),
            parent_id: parent_id,
            ref_counter: AtomicU64::new(1)
        }
    }

    fn parent<'a>(&self, span_map: &'a SpanMap<Data, Message>) -> Option<&'a SpanData<Data, Message>> {
        // the parent cannot have been removed, since this span holds a reference to it
        self.parent_id.map(|parent_id| span_map.get(&parent_id).unwrap())
    }

    fn log_queued_messages<Forward>(&self, forward: &Forward)
        where Forward: Fn(&Data, Message)
    {
        for message in self.messages.lock().unwrap().drain(..) {
            forward(&self.data, message)
        }
    }

    fn queue_message(&self, message: Message) {
        self.messages.lock().unwrap().push(message)
    }

    fn discard_queued_messages(&self) {
        self.messages.lock().unwrap().clear();
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
    fn borrow_permission<Forward>(&self, span_map: &SpanMap<Data, Message>, forward: &Forward) -> bool
        where Forward: Fn(&Data, Message)
    {
        match self.state.get() {
            SpanState::SilentPeriod => if let Some(parent) = self.parent(span_map) {
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
    fn exit(&self, span_map: &SpanMap<Data, Message>) {
        self.discard_queued_messages();
        self.entered_time.store(0, SeqCst);
        match self.state.reset() {
            SpanState::SilentPeriod | SpanState::PermissionDenied => {},
            SpanState::PermissionBorrowed => panic!("span closed despite active child"),
            SpanState::Inactive => unreachable!(),
            SpanState::PermissionAcquired => if let Some(parent) = self.parent(span_map) {
                parent.state.return_permission();
            }
        }
    }

    ///
    /// Forwards or queues the message created by `message`. If it is clear that the message
    /// will never be forwarded, `message` is not called at all.
    ///
    fn send_message<Forward, MakeMessage>(&self, message: MakeMessage, span_map: &SpanMap<Data, Message>, baseline: Instant, forward: &Forward, silent_duration: u64)
        where Forward: Fn(&Data, Message),
            MakeMessage: FnOnce(&Data) -> Message
    {
        match self.state.get() {
            SpanState::SilentPeriod => {
                let entered_time = self.entered_time.load(SeqCst);
                let current_time = baseline.elapsed().as_micros() as u64;
                if current_time >= silent_duration + entered_time {
                    let has_permission = if let Some(parent) = self.parent(span_map) {
                        parent.borrow_permission(span_map, forward)
                    } else {
                        true
                    };
                    if has_permission {
                        self.state.give_permission();
                        self.log_queued_messages(forward);
                        forward(&self.data, message(&self.data))
                    } else {
                        self.state.deny_permission();
                    }
                } else {
                    self.queue_message(message(&self.data));
                    // in case another thread concurrently updated state but was too early
                    // to print these messages
                    match self.state.get() {
                        SpanState::PermissionAcquired | SpanState::PermissionBorrowed => self.log_queued_messages(forward),
                        SpanState::Inactive | SpanState::PermissionDenied | SpanState::SilentPeriod => {}
                    }
                }
            },
            SpanState::PermissionAcquired => {
                forward(&self.data, message(&self.data))
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

///
/// [`ThreadLocal`] stores the values of different threads next to each other, so
/// pad them to avoid false sharing.
///
#[repr(align(128))]
struct CachePadded<Inner>(Inner);

///
/// Span ids are reserved by threads in blocks of this size, to avoid contention.
///
const ID_BLOCK_LEN: u64 = 64;

#[derive(Default)]
struct ThreadState {
    span_stack: Vec<NonZeroU64>,
    reserved_ids: std::ops::Range<u64>
}

///
/// Ids of tracked spans are even, ids of ignored spans are odd.
///
const IGNORED_SPAN_TAG: u64 = 1;

///
/// The data of the parent span, as passed to the closure in [`DelayedLoggerImpl::create_span()`].
///
pub(crate) enum ParentData<'a, Data> {
    Root,
    Tracked(&'a Data),
    Ignored
}

///
/// The data of a newly created span, as returned by the closure in [`DelayedLoggerImpl::create_span()`].
///
/// Ignored spans are not stored in the span map and never log anything; all their child
/// spans must also be ignored. Their only payload is a static reference, and multiple ignored
/// spans with the same payload get the same id.
///
pub(crate) enum NewSpan<Data, IgnoredData: 'static> {
    Tracked(Data),
    Ignored(&'static IgnoredData)
}

pub(crate) enum SpanView<'a, Data, IgnoredData: 'static> {
    Tracked(&'a Data),
    Ignored(&'static IgnoredData)
}

///
/// All functions taking a span id panic if the id is invalid, i.e. was not created by this
/// logger or has already been closed via [`DelayedLoggerImpl::delete_span()`].
///
pub(crate) struct DelayedLoggerImpl<Data, IgnoredData, Message, Forward>
    where IgnoredData: 'static,
        Forward: Fn(&Data, Message)
{
    id_generator: CachePadded<AtomicU64>,
    all_spans: CachePadded<papaya::HashMap<NonZeroU64, SpanData<Data, Message>, IdHashBuilder>>,
    /// maps the id of ignored spans to their payload
    ignored_spans: CachePadded<papaya::HashMap<NonZeroU64, &'static IgnoredData, IdHashBuilder>>,
    thread_state: ThreadLocal<CachePadded<RefCell<ThreadState>>>,
    forward: Forward,
    baseline: Instant,
    silent_duration: u64
}

impl<Data, IgnoredData, Message, Forward> DelayedLoggerImpl<Data, IgnoredData, Message, Forward>
    where IgnoredData: 'static,
        Forward: Fn(&Data, Message)
{
    pub(crate) fn new(silent_duration: u64, forward: Forward) -> Self {
        Self {
            baseline: Instant::now() - Duration::from_micros(10),
            id_generator: CachePadded(AtomicU64::new(1)),
            all_spans: CachePadded(papaya::HashMap::default()),
            ignored_spans: CachePadded(papaya::HashMap::default()),
            thread_state: ThreadLocal::new(),
            silent_duration: silent_duration,
            forward: forward
        }
    }

    fn is_ignored(id: NonZeroU64) -> bool {
        id.get() & IGNORED_SPAN_TAG != 0
    }

    fn get_span<'a>(span_map: &'a SpanMap<Data, Message>, id: NonZeroU64) -> &'a SpanData<Data, Message> {
        span_map.get(&id).unwrap_or_else(|| panic!("invalid or closed span id {}", id))
    }

    fn get_ignored_span(&self, id: NonZeroU64) -> &'static IgnoredData {
        self.ignored_spans.0.pin().get(&id).copied().unwrap_or_else(|| panic!("invalid span id {}", id))
    }

    pub(crate) fn current_span(&self) -> Option<NonZeroU64> {
        self.thread_state().borrow().span_stack.last().copied()
    }

    fn thread_state(&self) -> &RefCell<ThreadState> {
        &self.thread_state.get_or(|| CachePadded(RefCell::new(ThreadState::default()))).0
    }

    fn next_id(&self) -> NonZeroU64 {
        let mut thread_state = self.thread_state().borrow_mut();
        if thread_state.reserved_ids.is_empty() {
            let start = self.id_generator.0.fetch_add(ID_BLOCK_LEN, SeqCst);
            thread_state.reserved_ids = start..(start + ID_BLOCK_LEN);
        }
        NonZeroU64::new(thread_state.reserved_ids.next().unwrap() << 1).unwrap()
    }

    pub(crate) fn with_span<Accept, Output>(&self, id: NonZeroU64, accept: Accept) -> Output
        where Accept: FnOnce(SpanView<Data, IgnoredData>) -> Output
    {
        if Self::is_ignored(id) {
            accept(SpanView::Ignored(self.get_ignored_span(id)))
        } else {
            let span_map = self.all_spans.0.pin();
            accept(SpanView::Tracked(&Self::get_span(&span_map, id).data))
        }
    }

    ///
    /// Returns the time since the span was last entered, or zero for ignored spans.
    ///
    pub(crate) fn running_time(&self, id: NonZeroU64) -> Duration {
        if Self::is_ignored(id) {
            return Duration::ZERO;
        }
        let span_map = self.all_spans.0.pin();
        let entered_time = Self::get_span(&span_map, id).entered_time.load(SeqCst);
        Duration::from_micros((self.baseline.elapsed().as_micros() as u64).saturating_sub(entered_time))
    }

    ///
    /// Creates a new span with the given parent. Use [`DelayedLoggerImpl::current_span()`]
    /// as parent for contextual spans.
    ///
    pub(crate) fn create_span<CreateData>(&self, parent_id: Option<NonZeroU64>, create_data: CreateData) -> NonZeroU64
        where CreateData: FnOnce(ParentData<Data>) -> NewSpan<Data, IgnoredData>
    {
        let span_map = self.all_spans.0.pin();
        let parent = parent_id.filter(|id| !Self::is_ignored(*id)).map(|id| Self::get_span(&span_map, id));
        let parent_data = match (parent_id, parent) {
            (None, _) => ParentData::Root,
            (Some(_), Some(parent)) => ParentData::Tracked(&parent.data),
            (Some(_), None) => ParentData::Ignored
        };
        match create_data(parent_data) {
            NewSpan::Ignored(payload) => {
                const { assert!(std::mem::align_of::<IgnoredData>() > IGNORED_SPAN_TAG as usize) };
                let id = NonZeroU64::new((payload as *const IgnoredData).addr() as u64 | IGNORED_SPAN_TAG).unwrap();
                self.ignored_spans.0.pin().get_or_insert(id, payload);
                id
            },
            NewSpan::Tracked(data) => {
                assert!(parent_id.is_none() || parent.is_some(), "child spans of ignored spans must be ignored");
                if let Some(parent) = parent {
                    // the child keeps the parent alive
                    parent.ref_counter.fetch_add(1, SeqCst);
                }
                let id = self.next_id();
                span_map.insert(id, SpanData::new(id, parent.map(|parent| parent.id), data));
                id
            }
        }
    }

    pub(crate) fn clone_span(&self, id: NonZeroU64) {
        if !Self::is_ignored(id) {
            let span_map = self.all_spans.0.pin();
            Self::get_span(&span_map, id).ref_counter.fetch_add(1, SeqCst);
        }
    }

    ///
    /// Releases the given span id. Returns `true` if this was the last id referencing the span,
    /// which is then closed. Note that its data might still be kept alive by child spans.
    ///
    pub(crate) fn delete_span(&self, id: NonZeroU64) -> bool {
        if Self::is_ignored(id) {
            return true;
        }
        let span_map = self.all_spans.0.pin();
        let mut current = Self::get_span(&span_map, id);
        let mut is_closed = false;
        // removing a span releases its reference to its parent
        while current.ref_counter.fetch_sub(1, SeqCst) == 1 {
            is_closed = true;
            span_map.remove(&current.id);
            if let Some(parent) = current.parent(&span_map) {
                current = parent;
            } else {
                break;
            }
        }
        return is_closed;
    }

    ///
    /// Forwards or queues the message created by `message`. If it is clear that the message
    /// will never be forwarded (e.g. because the span has been denied permission), `message`
    /// is not called at all.
    ///
    pub(crate) fn send_message<MakeMessage>(&self, span_id: NonZeroU64, message: MakeMessage)
        where MakeMessage: FnOnce(&Data) -> Message
    {
        if !Self::is_ignored(span_id) {
            let span_map = self.all_spans.0.pin();
            let span = Self::get_span(&span_map, span_id);
            span.send_message(message, &span_map, self.baseline, &self.forward, self.silent_duration);
        }
    }

    pub(crate) fn enter(&self, span_id: NonZeroU64) {
        if Self::is_ignored(span_id) {
            // check the id here, so that the span stack only contains valid ids
            self.get_ignored_span(span_id);
            self.thread_state().borrow_mut().span_stack.push(span_id);
        } else {
            let span_map = self.all_spans.0.pin();
            let span = Self::get_span(&span_map, span_id);
            self.thread_state().borrow_mut().span_stack.push(span_id);
            span.enter(self.baseline);
        }
    }

    pub(crate) fn exit(&self, span_id: NonZeroU64) {
        if Self::is_ignored(span_id) {
            self.thread_state().borrow_mut().span_stack.pop();
        } else {
            let span_map = self.all_spans.0.pin();
            let span = Self::get_span(&span_map, span_id);
            self.thread_state().borrow_mut().span_stack.pop();
            span.exit(&span_map);
        }
    }
}

#[cfg(test)]
use std::thread::{sleep, spawn};

#[cfg(test)]
type TestLogger<Forward> = DelayedLoggerImpl<String, u64, String, Forward>;

#[cfg(test)]
fn test_logger<'a>(silent_duration: u64, log: &'a Mutex<Vec<String>>) -> TestLogger<impl 'a + Fn(&String, String)> {
    DelayedLoggerImpl::new(silent_duration, move |_: &String, m: String| log.lock().unwrap().push(m))
}

#[cfg(test)]
fn tracked<Forward: Fn(&String, String)>(logger: &TestLogger<Forward>, parent: Option<NonZeroU64>, name: &str) -> NonZeroU64 {
    logger.create_span(parent, |_| NewSpan::Tracked(name.to_owned()))
}

#[cfg(test)]
fn contextual<Forward: Fn(&String, String)>(logger: &TestLogger<Forward>, name: &str) -> NonZeroU64 {
    tracked(logger, logger.current_span(), name)
}

#[cfg(test)]
fn message<Forward: Fn(&String, String)>(logger: &TestLogger<Forward>, span: NonZeroU64, message: &str) {
    logger.send_message(span, |_| message.to_owned())
}

#[test]
fn test_spans() {
    let log = Mutex::new(Vec::new());
    let logger = test_logger(0, &log);

    let a = contextual(&logger, "a");
    logger.enter(a);
    message(&logger, a, "enter a");

    let b = contextual(&logger, "b");
    logger.enter(b);
    message(&logger, b, "enter b");

    message(&logger, b, "exit b");
    logger.exit(b);
    logger.delete_span(b);

    message(&logger, a, "exit a");
    logger.exit(a);
    logger.delete_span(a);

    assert_eq!(0, logger.all_spans.0.len());
    drop(logger);
    let log = log.into_inner().unwrap();
    assert_eq!(vec!["enter a".to_owned(), "enter b".to_owned(), "exit b".to_owned(), "exit a".to_owned()], log);
}

#[test]
fn test_concurrent_spans() {
    let log = Mutex::new(Vec::new());
    let logger = test_logger(0, &log);

    let a = contextual(&logger, "a");
    logger.enter(a);
    message(&logger, a, "enter a");

    let b = contextual(&logger, "b");
    logger.enter(b);
    message(&logger, b, "enter b");

    let c = tracked(&logger, Some(a), "c");
    logger.enter(c);
    message(&logger, c, "enter c");

    message(&logger, b, "exit b");
    logger.exit(b);
    logger.delete_span(b);

    message(&logger, c, "exit c");
    logger.exit(c);
    logger.delete_span(c);

    let d = contextual(&logger, "d");
    logger.enter(d);
    message(&logger, d, "enter d");

    message(&logger, d, "exit d");
    logger.exit(d);
    logger.delete_span(d);

    message(&logger, a, "exit a");
    logger.exit(a);
    logger.delete_span(a);

    assert_eq!(0, logger.all_spans.0.len());
    drop(logger);
    let log = log.into_inner().unwrap();
    assert_eq!(vec!["enter a".to_owned(), "enter b".to_owned(), "exit b".to_owned(), "enter d".to_owned(), "exit d".to_owned(), "exit a".to_owned()], log);
}

#[test]
// relies on timing, which is way off in miri
#[cfg_attr(miri, ignore)]
fn test_skip_short_spans() {
    let log = Mutex::new(Vec::new());
    let logger = test_logger(1000, &log);

    let a = contextual(&logger, "a");
    logger.enter(a);
    message(&logger, a, "enter a");

    let b = contextual(&logger, "b");
    logger.enter(b);
    message(&logger, b, "enter b");

    sleep(Duration::from_micros(100));

    message(&logger, b, "exit b");
    logger.exit(b);
    logger.delete_span(b);

    message(&logger, a, "exit a");
    logger.exit(a);
    logger.delete_span(a);

    assert_eq!(0, logger.all_spans.0.len());
    drop(logger);
    let log = log.into_inner().unwrap();
    assert_eq!(Vec::<String>::new(), log);


    let log = Mutex::new(Vec::new());
    let logger = test_logger(1000, &log);

    let a = contextual(&logger, "a");
    logger.enter(a);
    message(&logger, a, "enter a");

    let b = contextual(&logger, "b");
    logger.enter(b);
    message(&logger, b, "enter b");

    message(&logger, b, "exit b");
    logger.exit(b);
    logger.delete_span(b);

    sleep(Duration::from_micros(2000));

    message(&logger, a, "exit a");
    logger.exit(a);
    logger.delete_span(a);

    assert_eq!(0, logger.all_spans.0.len());
    drop(logger);
    let log = log.into_inner().unwrap();
    assert_eq!(vec!["enter a".to_owned(), "exit a".to_owned()], log);
}

#[test]
// relies on timing, which is way off in miri
#[cfg_attr(miri, ignore)]
fn test_log_first_long_span() {
    let log = Mutex::new(Vec::new());
    let logger = test_logger(1000, &log);

    let a = contextual(&logger, "a");
    logger.enter(a);
    message(&logger, a, "enter a");

    let b = contextual(&logger, "b");
    logger.enter(b);
    message(&logger, b, "enter b");

    let c = tracked(&logger, Some(a), "c");
    logger.enter(c);
    message(&logger, c, "enter c");

    message(&logger, b, "exit b");
    logger.exit(b);
    logger.delete_span(b);

    sleep(Duration::from_micros(2000));

    message(&logger, c, "exit c");
    logger.exit(c);
    logger.delete_span(c);

    let d = contextual(&logger, "d");
    logger.enter(d);
    message(&logger, d, "enter d");

    message(&logger, d, "exit d");
    logger.exit(d);
    logger.delete_span(d);

    message(&logger, a, "exit a");
    logger.exit(a);
    logger.delete_span(a);

    assert_eq!(0, logger.all_spans.0.len());
    drop(logger);
    let log = log.into_inner().unwrap();
    assert_eq!(vec!["enter a".to_owned(), "enter c".to_owned(), "exit c".to_owned(), "exit a".to_owned()], log);
}

#[test]
fn test_span_tree_not_execution_stack() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let logger = Arc::new(DelayedLoggerImpl::<String, u64, String, _>::new(1000, move |_: &String, m: String| log.lock().unwrap().push(m)));
    let barrier = Arc::new(Barrier::new(2));

    let a = logger.create_span(None, |_| NewSpan::Tracked("/a".to_owned()));
    logger.enter(a);
    let logger_copy = logger.clone();
    let barrier_copy = barrier.clone();
    let helper_thread = spawn(move || {
        let b = logger_copy.create_span(Some(a), |_| NewSpan::Tracked("/a/b".to_owned()));
        logger_copy.enter(b);
        logger_copy.with_span(logger_copy.current_span().unwrap(), |data| assert!(matches!(data, SpanView::Tracked(data) if data == "/a/b")));
        logger_copy.exit(b);
        logger_copy.delete_span(b);
        barrier_copy.wait();
        barrier_copy.wait();
        let c = logger_copy.create_span(logger_copy.current_span(), |parent| NewSpan::Tracked(format!("{}/c", match parent {
            ParentData::Tracked(parent) => parent.as_str(),
            _ => ""
        })));
        logger_copy.enter(c);
        logger_copy.with_span(logger_copy.current_span().unwrap(), |data| assert!(matches!(data, SpanView::Tracked(data) if data == "/c")));
        logger_copy.exit(c);
        logger_copy.delete_span(c);
    });
    barrier.wait();
    logger.exit(a);
    logger.delete_span(a);
    barrier.wait();
    helper_thread.join().unwrap();
    assert_eq!(0, logger.all_spans.0.len());
}

#[test]
fn test_child_outlives_parent() {
    let log = Mutex::new(Vec::new());
    let logger = test_logger(0, &log);

    let a = contextual(&logger, "a");
    logger.enter(a);
    let b = tracked(&logger, Some(a), "b");
    logger.exit(a);
    // b keeps a alive
    assert!(!logger.delete_span(a));
    assert_eq!(2, logger.all_spans.0.len());

    logger.with_span(b, |data| assert!(matches!(data, SpanView::Tracked(data) if data == "b")));
    let b_clone = b;
    logger.clone_span(b_clone);
    assert!(!logger.delete_span(b));
    assert_eq!(2, logger.all_spans.0.len());
    assert!(logger.delete_span(b_clone));
    assert_eq!(0, logger.all_spans.0.len());
}

#[test]
fn test_ignored_spans() {
    static IGNORED: u64 = 0;
    let log = Mutex::new(Vec::new());
    let logger = test_logger(0, &log);

    let a = contextual(&logger, "a");
    logger.enter(a);
    message(&logger, a, "enter a");

    let b = logger.create_span(logger.current_span(), |parent| {
        assert!(matches!(parent, ParentData::Tracked(parent) if parent == "a"));
        NewSpan::Ignored(&IGNORED)
    });
    logger.enter(b);
    logger.with_span(logger.current_span().unwrap(), |data| assert!(matches!(data, SpanView::Ignored(data) if std::ptr::eq(data, &IGNORED))));
    let c = logger.create_span(logger.current_span(), |parent| {
        assert!(matches!(parent, ParentData::Ignored));
        NewSpan::Ignored(&IGNORED)
    });
    logger.clone_span(c);
    logger.enter(c);
    logger.send_message(c, |_| panic!("message of ignored span was created"));
    logger.exit(c);
    logger.delete_span(c);
    logger.delete_span(c);
    logger.exit(b);
    logger.delete_span(b);

    message(&logger, a, "exit a");
    logger.exit(a);
    logger.delete_span(a);

    assert_eq!(0, logger.all_spans.0.len());
    drop(logger);
    let log = log.into_inner().unwrap();
    assert_eq!(vec!["enter a".to_owned(), "exit a".to_owned()], log);
}

#[test]
fn test_no_message_creation_when_denied() {
    let log = Mutex::new(Vec::new());
    let logger = test_logger(0, &log);

    let a = contextual(&logger, "a");
    logger.enter(a);
    message(&logger, a, "enter a");
    let b = contextual(&logger, "b");
    logger.enter(b);
    message(&logger, b, "enter b");
    // c cannot get permission, since b has it
    let c = tracked(&logger, Some(a), "c");
    logger.enter(c);
    message(&logger, c, "enter c");
    logger.send_message(c, |_| panic!("message of denied span was created"));
    logger.exit(c);
    logger.delete_span(c);
    logger.exit(b);
    logger.delete_span(b);
    logger.exit(a);
    logger.delete_span(a);

    assert_eq!(0, logger.all_spans.0.len());
    drop(logger);
    let log = log.into_inner().unwrap();
    assert_eq!(vec!["enter a".to_owned(), "enter b".to_owned()], log);
}

#[cfg(test)]
fn assert_panics<Output>(f: impl FnOnce() -> Output) {
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).is_err());
}

#[test]
fn test_invalid_ids() {
    static IGNORED: u64 = 0;
    let log = Mutex::new(Vec::new());
    let logger = test_logger(0, &log);

    let a = contextual(&logger, "a");
    let ignored = logger.create_span(Some(a), |_| NewSpan::Ignored(&IGNORED));
    let closed = contextual(&logger, "closed");
    assert!(logger.delete_span(closed));
    let reused = contextual(&logger, "reused");

    static NOT_REGISTERED: u64 = 0;
    let invalid_ids = [
        closed, 
        NonZeroU64::new(reused.get() + 2).unwrap(),
        NonZeroU64::new(ignored.get() + 2).unwrap(),
        NonZeroU64::new(1 << 32).unwrap(),
        NonZeroU64::new(u64::MAX).unwrap(),
        NonZeroU64::new(u64::MAX - 1).unwrap(),
        NonZeroU64::new((&NOT_REGISTERED as *const u64).addr() as u64 | IGNORED_SPAN_TAG).unwrap(),
    ];
    for id in invalid_ids {
        assert_panics(|| logger.with_span(id, |_| ()));
        assert_panics(|| logger.enter(id));
        if id.get() & IGNORED_SPAN_TAG != 0 {
            // these are not checked, since nothing is dereferenced for ignored spans
            continue;
        }
        assert_panics(|| logger.running_time(id));
        assert_panics(|| logger.create_span(Some(id), |_| NewSpan::Tracked("child".to_owned())));
        assert_panics(|| logger.clone_span(id));
        assert_panics(|| logger.delete_span(id));
        assert_panics(|| logger.send_message(id, |_| "message".to_owned()));
        assert_panics(|| logger.exit(id));
    }
    assert_eq!(None, logger.current_span());

    logger.with_span(reused, |data| assert!(matches!(data, SpanView::Tracked(data) if data == "reused")));
    logger.with_span(ignored, |data| assert!(matches!(data, SpanView::Ignored(data) if std::ptr::eq(data, &IGNORED))));
    assert!(logger.delete_span(reused));
    assert!(logger.delete_span(a));
    assert_eq!(0, logger.all_spans.0.len());
}

#[test]
fn test_many_threads() {
    #[cfg(miri)]
    const THREADS: usize = 4;
    #[cfg(not(miri))]
    const THREADS: usize = 16;
    #[cfg(miri)]
    const ITERATIONS: usize = 20;
    #[cfg(not(miri))]
    const ITERATIONS: usize = 2000;

    static IGNORED: [u64; 3] = [0, 1, 2];
    let log = Mutex::new(Vec::new());
    let logger = test_logger(0, &log);
    let root = contextual(&logger, "root");
    logger.enter(root);
    std::thread::scope(|s| {
        for t in 0..THREADS {
            let logger = &logger;
            s.spawn(move || for i in 0..ITERATIONS {
                let a = tracked(logger, Some(root), "a");
                logger.enter(a);
                let b = contextual(logger, "b");
                logger.clone_span(b);
                logger.enter(b);
                message(logger, b, "message");
                let c = logger.create_span(logger.current_span(), |_| NewSpan::Ignored(&IGNORED[(t + i) % 3]));
                logger.enter(c);
                logger.exit(c);
                logger.delete_span(c);
                logger.exit(b);
                assert!(!logger.delete_span(b));
                // close `a` before `b`, while `b` still needs it
                logger.exit(a);
                assert!(!logger.delete_span(a));
                assert!(logger.delete_span(b));
            });
        }
    });
    logger.exit(root);
    assert!(logger.delete_span(root));
    assert_eq!(0, logger.all_spans.0.len());
}
