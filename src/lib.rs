#![feature(unboxed_closures)]
#![feature(fn_traits)]

#![doc = include_str!("../Readme.md")]

use tracing::{Id, Level, Event, span, Metadata, Subscriber};
use tracing::subscriber::Interest;
use tracing::field::{Field, Visit};
use std::ops::RangeInclusive;
use std::fmt::{Display, Write};
use std::sync::atomic::AtomicUsize;
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

struct PrintToStdout;

impl FnOnce<(String,)> for PrintToStdout {
    type Output = ();

    extern "rust-call" fn call_once(self, args: (String,)) {
        self.call(args)
    }
}

impl<'a> FnMut<(String,)> for PrintToStdout {
    extern "rust-call" fn call_mut(&mut self, args: (String,)) {
        self.call(args)
    }
}

impl<'a> Fn<(String,)> for PrintToStdout {
    extern "rust-call" fn call(&self, args: (String,)) {
        // use print! here to interact with io capture during tests
        print!("{}", args.0);
        std::io::Write::flush(&mut std::io::stdout()).unwrap()
    }
}

struct SpanData {
    desc: FieldRecorder,
    depth: usize,
    metadata: &'static Metadata<'static>,
    color: Option<AnsiColors>,
    /// counter used to assign distinct colors to children; only set for
    /// top-level spans
    #[allow(unused)]
    color_counter: Option<AtomicUsize>
}

impl SpanData {

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
pub struct DelayedLogger {
    base: DelayedLoggerImpl<SpanData, PrintToStdout>,
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
            base: DelayedLoggerImpl::new(silent_period, PrintToStdout),
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
        let mut result = tracing_core::span::Current::none();
        if let Some(span) = self.base.span_stack().borrow().last().copied() {
            self.base.span_data(span, |data, _, _| result = 
                tracing_core::span::Current::new(Id::from_non_zero_u64(span), data.metadata)
            );
        }
        return result;
    }

    fn new_span(&self, span: &span::Attributes<'_>) -> Id {
        let mut fields = FieldRecorder::new();
        span.record(&mut fields);
        fields.message = Some(span.metadata().name().to_owned());

        let id = if let Some(parent_id) = span.parent() {
            let mut depth = 0;
            let mut color = None;
            self.base.span_data(parent_id.into_non_zero_u64(), |data, _, _| {
                depth = data.depth + 1;
                color = data.color_for_child();
            });
            self.base.create_span_with_parent(SpanData {
                color_counter: if depth == 0 { Some(AtomicUsize::new(0)) } else { None },
                desc: fields,
                metadata: span.metadata(),
                depth: depth,
                color: color
            }, Some(parent_id.into_non_zero_u64()))
        } else {
            self.base.create_span(|parent| {
                let (depth, color) = if let Some((parent_data, _)) = parent {
                    (parent_data.depth + 1, parent_data.color_for_child())
                } else {
                    (0, None)
                };
                SpanData {
                    color_counter: if depth == 0 { Some(AtomicUsize::new(0)) } else { None },
                    desc: fields,
                    metadata: span.metadata(),
                    depth: depth,
                    color: color
                }
            })
        };

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
            let mut is_within_depth = false;
            let mut color = None;
            if let Some(span) = self.base.span_stack().borrow().last().copied() {
                self.base.span_data(span, |data, _, _| if data.depth < self.max_depth {
                    is_within_depth = true;
                    color = data.color;
                });
                if is_within_depth {
                    let mut fields = FieldRecorder::new();
                    event.record(&mut fields);
                    self.base.send_message(colorize(color, fields.to_string()), span);
                }
            } else {
                let mut fields = FieldRecorder::new();
                event.record(&mut fields);
                PrintToStdout(fields.to_string())
            }
        }
    }

    fn enter(&self, span: &Id) {
        self.base.enter(span.into_non_zero_u64());
        let mut message = None;
        self.base.span_data(span.into_non_zero_u64(), |data, _, _| if data.depth < self.max_depth {
            message = Some(colorize(data.color, data.desc.clone().to_string()));
        } else if data.depth == self.max_depth {
            message = Some(colorize(data.color, format!("{}...", data.desc)));
        });
        if let Some(message) = message {
            self.base.send_message(message, span.into_non_zero_u64());
        }
    }

    fn exit(&self, span: &Id) {
        let mut message = None;
        self.base.span_data(span.into_non_zero_u64(), |data, _, running_time| if data.depth == 0 {
            message = Some(format!("done({} us)\n", running_time.as_micros()));
        } else if data.depth <= self.max_depth {
            message = Some(colorize(data.color, format!("done({} us)", running_time.as_micros())));
        });
        if let Some(message) = message {
            self.base.send_message(message, span.into_non_zero_u64());
        }
        self.base.exit(span.into_non_zero_u64())
    }

    fn clone_span(&self, id: &Id) -> Id {
        self.base.clone_span(id.into_non_zero_u64());
        return id.clone();
    }

    fn try_close(&self, id: Id) -> bool {
        self.base.delete_span(id.into_non_zero_u64())
    }
}
