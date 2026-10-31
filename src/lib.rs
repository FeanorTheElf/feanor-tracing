#![doc = include_str!("../Readme.md")]

use tracing::{Id, Level, Event, span, Metadata, Subscriber};
use tracing::subscriber::Interest;
use tracing::field::{Field, Visit};
use std::ops::RangeInclusive;
use std::fmt::{Display, Write};
use std::sync::atomic::AtomicUsize;
use std::time::Duration;
use core::*;

#[cfg(feature = "print_colored")]
use owo_colors::{AnsiColors, OwoColorize, Stream};

mod core;

///
/// The palette of colors that is cycled through to distinguish second-level
/// (i.e. depth-1) spans within a single line of output.
///
#[cfg(feature = "print_colored")]
const SPAN_COLORS: [AnsiColors; 6] = [
    AnsiColors::BrightBlue,
    AnsiColors::BrightCyan,
    AnsiColors::BrightMagenta,
    AnsiColors::Yellow,
    AnsiColors::BrightGreen,
    AnsiColors::Red,
];

#[cfg(not(feature = "print_colored"))]
#[derive(Copy, Clone)]
struct AnsiColors;

///
/// Wraps `message` in the ANSI escape codes for the given color, if any.
///
/// This only emits escape codes when the output stream actually supports them; 
/// in particular it respects whether stdout is a terminal as well as the
/// `NO_COLOR` and `CLICOLOR` environment variables.
///
fn colorize(color: Option<AnsiColors>, message: String) -> String {
    match color {
        #[cfg(feature = "print_colored")]
        Some(index) => {
            message
                .if_supports_color(Stream::Stdout, |text| text.color(index))
                .to_string()
        },
        #[cfg(not(feature = "print_colored"))]
        Some(_) => unreachable!(),
        None => message
    }
}

fn print_to_stdout(message: &str) {
    // use print! here to interact with io capture during tests
    print!("{}", message);
    std::io::Write::flush(&mut std::io::stdout()).unwrap()
}

///
/// A message that is queued by a span; it is only formatted once it is clear that
/// it will be printed.
///
enum Message {
    Enter,
    EnterTruncated,
    Exit(Duration),
    Event(String)
}

fn print_message(span: &SpanData, message: Message) {
    let text = match message {
        Message::Enter => colorize(span.color, span.describe()),
        Message::EnterTruncated => colorize(span.color, format!("{}...", span.describe())),
        Message::Exit(running_time) if span.depth == 0 => format!("done({} us)\n", running_time.as_micros()),
        Message::Exit(running_time) => colorize(span.color, format!("done({} us)", running_time.as_micros())),
        Message::Event(text) => colorize(span.color, text)
    };
    print_to_stdout(&text)
}

///
/// Data of spans of depth at most `max_depth`; deeper spans are not tracked at all.
///
struct SpanData {
    /// the recorded fields; the span name is taken from `metadata`
    fields: Option<String>,
    depth: usize,
    metadata: &'static Metadata<'static>,
    color: Option<AnsiColors>,
    /// counter used to assign distinct colors to children; only set for
    /// top-level spans
    #[allow(unused)]
    color_counter: Option<AtomicUsize>
}

impl SpanData {

    fn describe(&self) -> String {
        if let Some(fields) = &self.fields {
            format!("{}({})", self.metadata.name(), fields)
        } else {
            format!("{}.", self.metadata.name())
        }
    }

    #[cfg(feature = "print_colored")]
    fn color_for_child(&self) -> Option<AnsiColors> {
        if let Some(color_counter) = &self.color_counter {
            debug_assert_eq!(0, self.depth);
            Some(SPAN_COLORS[color_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst) % SPAN_COLORS.len()])
        } else {
            self.color
        }
    }

    #[cfg(not(feature = "print_colored"))]
    fn color_for_child(&self) -> Option<AnsiColors> {
        None
    }
}

///
/// A [`tracing::Subscriber`] that prints the events deemed most important to get a
/// high-level overview of the executed algorithm to stdout.
///
/// Features:
///  - Messages are only printed after the algorithm has spent a certain time in the
///    current span, thus not printing anything for short spans
///  - Messages are printed from the same threads as they are generated, which makes
///    it compatible with IO capturing during tests
///  - for each root span, all logged sub-spans and events are written in a single line,
///    keeping the output compact.
///  - to make the single line easier to read, every second-level span (and its
///    sub-tree) is printed in a different color, cycling through a fixed palette.
///    Colors are only emitted when the feature `print_colored` is active and the output
///    stream supports them; Furthermore, they can be disabled via the the `NO_COLOR`
///    and `CLICOLOR` environment variables.
///
/// Limitations:
///  - Contrary to what [`tracing`] allows in general, a span must not be entered from
///    multiple threads concurrently; doing so will cause a panic. Instead, create a
///    separate child span for each thread, e.g. via `span!(parent: &outer_span, ...)`.
///    This only concerns spans within depth `max_depth`, but since that depends on the
///    calling context, it is best to always follow this pattern.
///
pub struct DelayedLogger {
    base: DelayedLoggerImpl<SpanData, Metadata<'static>, Message, fn(&SpanData, Message)>,
    levels: RangeInclusive<Level>,
    max_depth: usize
}

impl DelayedLogger {

    ///
    /// Creates a new [`DelayedLogger`], which logs only events and spans
    ///  - whose [`Level`] is within `levels`
    ///  - which are part of a span that will be running for at least
    ///    `silent_period` microseconds; any message queued at a time when this
    ///    is not yet decided will be stored and possibly printed later
    ///  - are part of a span of depth at most `max_depth` in the span tree
    ///
    pub fn new(max_depth: usize, silent_period: u64, levels: RangeInclusive<Level>) -> Self {
        Self {
            base: DelayedLoggerImpl::new(silent_period, print_message),
            levels: levels,
            max_depth: max_depth
        }
    }
    
    ///
    /// Creates a new [`DelayedLogger`] using [`DelayedLogger::new()`] and sets it as
    /// the global default. This will panic if a global default has already been set.
    ///
    pub fn init(max_depth: usize, silent_period: u64, levels: RangeInclusive<Level>) {
        tracing::subscriber::set_global_default(Self::new(max_depth, silent_period, levels)).unwrap()
    }

    ///
    /// Creates a new [`DelayedLogger`] using a test configuration and sets it as
    /// the global default, unless a global default is already set.
    ///
    pub fn init_test() {
        _ = tracing::subscriber::set_global_default(Self::new(4, 100000, Level::ERROR..=Level::TRACE));
    }
}

#[derive(Clone)]
struct FieldRecorder {
    message: Option<String>,
    fields: Option<String>
}

impl FieldRecorder {

    fn new() -> Self {
        Self { message: None, fields: None }
    }

    fn to_string(self) -> String {
        match (self.message, self.fields) {
            (Some(mut message), Some(fields)) => {
                write!(&mut message, "({})", fields).unwrap();
                message
            },
            (Some(mut message), None) => {
                write!(&mut message, ".").unwrap();
                message
            },
            (None, Some(fields)) => {
                format!("({})", fields)
            },
            (None, None) => ".".to_owned()
        }
    }
}

impl Display for FieldRecorder {

    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(message) = &self.message {
            write!(f, "{}", message)?;
        }
        if let Some(fields) = &self.fields {
            write!(f, "({})", fields)
        } else {
            write!(f, ".")
        }
    }
}

impl Visit for FieldRecorder {

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = Some(format!("{:?}", value));
        } else {
            if let Some(fields) = &mut self.fields {
                write!(fields, ", {}={:?}", field.name(), value).unwrap();
            } else {
                self.fields = Some(format!("{}={:?}", field.name(), value));
            }
        }
    }
}

impl Subscriber for DelayedLogger {

    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
        if self.levels.contains(metadata.level()) {
            Interest::always()
        } else {
            Interest::never()
        }
    }

    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        self.levels.contains(metadata.level())
    }

    fn current_span(&self) -> tracing_core::span::Current {
        if let Some(span) = self.base.current_span() {
            let metadata = self.base.with_span(span, |span| match span {
                SpanView::Tracked(data) => data.metadata,
                SpanView::Ignored(metadata) => metadata
            });
            tracing_core::span::Current::new(Id::from_non_zero_u64(span), metadata)
        } else {
            tracing_core::span::Current::none()
        }
    }

    fn new_span(&self, span: &span::Attributes<'_>) -> Id {
        let parent = if let Some(parent_id) = span.parent() {
            Some(parent_id.into_non_zero_u64())
        } else if span.is_contextual() {
            self.base.current_span()
        } else {
            None
        };
        let id = self.base.create_span(parent, |parent| {
            let (depth, color) = match parent {
                ParentData::Root => (0, None),
                ParentData::Tracked(parent_data) if parent_data.depth < self.max_depth => (parent_data.depth + 1, parent_data.color_for_child()),
                // spans deeper than `max_depth` never print anything, so don't track them
                ParentData::Tracked(_) | ParentData::Ignored => return NewSpan::Ignored(span.metadata())
            };
            let mut fields = FieldRecorder::new();
            span.record(&mut fields);
            NewSpan::Tracked(SpanData {
                color_counter: if depth == 0 { Some(AtomicUsize::new(0)) } else { None },
                fields: fields.fields,
                metadata: span.metadata(),
                depth: depth,
                color: color
            })
        });
        return Id::from_non_zero_u64(id);
    }

    fn record(&self, _span: &Id, _values: &span::Record<'_>) {
        unimplemented!()
    }

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {
        // we only care about parent spans currently
    }

    fn event(&self, event: &Event<'_>) {
        if self.levels.contains(event.metadata().level()) {
            if let Some(span) = self.base.current_span() {
                let is_within_depth = self.base.with_span(span, |span| matches!(span, SpanView::Tracked(data) if data.depth < self.max_depth));
                if is_within_depth {
                    self.base.send_message(span, |_| {
                        let mut fields = FieldRecorder::new();
                        event.record(&mut fields);
                        Message::Event(fields.to_string())
                    });
                }
            } else {
                let mut fields = FieldRecorder::new();
                event.record(&mut fields);
                print_to_stdout(&fields.to_string())
            }
        }
    }

    fn enter(&self, span: &Id) {
        let span = span.into_non_zero_u64();
        self.base.enter(span);
        self.base.send_message(span, |data| if data.depth < self.max_depth {
            Message::Enter
        } else {
            Message::EnterTruncated
        });
    }

    fn exit(&self, span: &Id) {
        let span = span.into_non_zero_u64();
        self.base.send_message(span, |_| Message::Exit(self.base.running_time(span)));
        self.base.exit(span)
    }

    fn clone_span(&self, id: &Id) -> Id {
        self.base.clone_span(id.into_non_zero_u64());
        return id.clone();
    }

    fn try_close(&self, id: Id) -> bool {
        self.base.delete_span(id.into_non_zero_u64())
    }
}

#[cfg(test)]
#[tracing::instrument(skip_all, level = "trace")]
fn test_recursion(depth: usize) -> usize {
    tracing::event!(Level::INFO, depth);
    assert!(!tracing::Span::current().is_none());
    if depth == 0 {
        0
    } else {
        test_recursion(depth - 1) + test_recursion(depth - 1)
    }
}

#[test]
fn test_subscriber() {
    let subscriber = DelayedLogger::new(2, 0, Level::ERROR..=Level::TRACE);
    tracing::subscriber::with_default(subscriber, || {
        let span = tracing::span!(Level::INFO, "root", value = 1);
        span.in_scope(|| {
            test_recursion(5);
            let current = tracing::Span::current();
            let dispatch = tracing::dispatcher::get_default(|dispatch| dispatch.clone());
            std::thread::scope(|s| {
                s.spawn(|| tracing::dispatcher::with_default(&dispatch, ||
                    tracing::span!(parent: &current, Level::INFO, "thread").in_scope(|| test_recursion(5))
                ));
            });
            let explicit_root = tracing::span!(parent: None, Level::INFO, "explicit_root");
            explicit_root.in_scope(|| test_recursion(1));
        });
    });
}
