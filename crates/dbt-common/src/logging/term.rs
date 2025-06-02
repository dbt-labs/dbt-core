use std::fmt;
use std::fmt::Display;
use std::ops::Sub;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::RwLock;
use std::time::Duration;
use std::time::Instant;

use counter::Counter;
use dashmap::DashMap;
use indicatif::MultiProgress;
use indicatif::ProgressBar;
use indicatif::ProgressState;
use indicatif::ProgressStyle;
use itertools::Itertools as _;
use unicode_segmentation::UnicodeSegmentation as _;

use crate::logging::events::StatEvent;
use crate::logging::events::TermEvent;

const SLOW_CONTEXT_THRESHOLD: Duration = Duration::from_secs(10);

const BORDERLINE_CONTEXT_THRESHOLD: Duration = Duration::from_secs(5);

/// Checks if the given log record is a terminal control-only record. Can be
/// used to short-circuit log delegation to child loggers.
pub fn is_term_control_only(record: &log::Record) -> bool {
    matches!(
        record
            .key_values()
            .get(log::kv::Key::from("_TERM_ONLY_")),
         Some(value) if value.to_bool() == Some(true)
    )
}

pub enum ProgressStyleType {
    Spinner,
    ArrowBar,
    ArrowBarWithCounters,
}

impl ProgressStyleType {
    pub fn get_style(&self) -> ProgressStyle {
        match self {
            ProgressStyleType::Spinner => ProgressStyle::with_template(
                "{prefix:.cyan.bold} {spinner:.gray} [{elapsed}] {counters} {context}",
            )
            .expect("Progress style template is valid")
            // .tick_strings(&["|", "/", "—", "\\"]),
            .tick_strings(&[
                "[>         ]",
                "[=>        ]",
                "[==>       ]",
                "[===>      ]",
                "[====>     ]",
                "[=====>    ]",
                "[======>   ]",
                "[=======>  ]",
                "[========> ]",
                "[=========>]",
                "[ =========]",
                "[  ========]",
                "[   =======]",
                "[    ======]",
                "[     =====]",
                "[      ====]",
                "[       ===]",
                "[        ==]",
                "[         =]",
                "[          ]",
            ]),
            ProgressStyleType::ArrowBar => {
                ProgressStyle::with_template(if console::Term::stdout().size().1 > 80 {
                    "{prefix:>.cyan.bold} [{bar:20}] {pos}/{len} {wide_msg}"
                } else {
                    "{prefix:>.cyan.bold} [{bar:20}] {pos}/{len}"
                })
                .expect("template is valid")
                .progress_chars("=> ")
            }
            ProgressStyleType::ArrowBarWithCounters => ProgressStyle::with_template(
                "{prefix:>.cyan.bold} [{bar:20}] {pos}/{len} {counters}",
            )
            .expect("template is valid")
            .progress_chars("=> "),
        }
    }

    pub fn get_context_line_style(&self) -> ProgressStyle {
        ProgressStyle::with_template("   {context}").expect("Progress style template is valid")
    }

    pub fn needs_context_line(&self) -> bool {
        matches!(self, ProgressStyleType::ArrowBarWithCounters)
    }
}

pub struct FancyLogger {
    // Multiplexer for active(currently on-screen) progress bars and spinners.
    controller: MultiProgress,
    // Map from UID to active spinner.
    spinners: Arc<DashMap<String, ContextualProgressBar>>,
    // Map from UID to active progress bar.
    bars: Arc<DashMap<String, ContextualProgressBar>>,

    // List of managed child loggers that also prints to stdout/stderr.
    children: Vec<Box<dyn log::Log>>,

    shutdown: Arc<(Mutex<bool>, Condvar)>,
    ticker: Option<std::thread::JoinHandle<()>>,
}

impl FancyLogger {
    /// Creates a new `FancyLogger`.
    ///
    /// `child_loggers` is a vector of child loggers that will be managed by
    /// this logger. These should include all loggers that potentially print to
    /// stdout or stderr (and ideally nothing else, since each child logger will
    /// incur a slight performance overhead).
    pub fn new(child_loggers: Vec<Box<dyn log::Log>>) -> Self {
        let shutdown = Arc::new((Mutex::new(false), Condvar::new()));

        FancyLogger {
            controller: MultiProgress::new(),
            spinners: Arc::new(DashMap::new()),
            bars: Arc::new(DashMap::new()),
            children: child_loggers,
            shutdown,
            ticker: None,
        }
    }

    /// Starts the ticker thread that will periodically tick all active spinners
    pub fn start_ticker(&mut self) {
        if self.ticker.is_some() {
            // Already started
            return;
        }

        let shutdown = Arc::clone(&self.shutdown);
        let spinners = Arc::clone(&self.spinners);
        let bars = Arc::clone(&self.bars);
        self.ticker = Some(std::thread::spawn(move || {
            loop {
                // Wait for the next tick
                let (lock, cvar) = &*shutdown;
                let Ok(lock) = lock.lock() else {
                    // Lock poisoned so we stop bothering
                    return;
                };

                let shut_down_flag = cvar.wait_timeout(lock, Duration::from_millis(100));
                if let Ok((flag, _)) = shut_down_flag {
                    if *flag {
                        // Shutdown requested
                        break;
                    }
                }

                spinners.iter().for_each(|item| {
                    item.tick();
                });
                bars.iter().for_each(|item| {
                    item.tick();
                });
            }
        }));
    }

    /// Starts a new spinner with the given prefix and UID.
    ///
    /// `uid` is a unique identifier for the spinner -- since we can't return a
    /// handle to the new progress bar to the caller, it is the caller's
    /// responsibility to ensure that the `uid` is globally unique. `prefix` is
    /// the text that will be displayed before the spinner, if not provided, the
    /// `uid` will be used as the prefix.
    ///
    /// Spinners can track in-progress tasks with context items, which can be
    /// updated as the task progresses. In-progress tasks will be rendered
    /// alongside the spinner.
    pub fn start_new_spinner(&self, uid: &str, prefix: Option<&str>) {
        if self.spinners.contains_key(uid) {
            // Should probably be an error
            return;
        }

        let spinner = ContextualProgressBar::new_spinner(
            prefix.map_or_else(|| uid.to_string(), |p| p.to_string()),
        );
        spinner.progress_bars().for_each(|pb| {
            self.controller.add(pb.clone());
        });
        self.spinners.insert(uid.to_string(), spinner);
    }

    /// Starts a new plain bar with the given prefix, total, and UID.
    ///
    /// `total` is the total number of items to process, and `uid` is a unique
    /// identifier for the bar -- since we can't return a handle to the new
    /// progress bar to the caller, it is the caller's responsibility to ensure
    /// that the `uid` is globally unique. `prefix` is the text that will be
    /// displayed before the bar, if not provided, the `uid` will be used as the
    /// prefix.
    ///
    /// Plain bars do not render in-progress tasks. If context items are added
    /// to a plain bar, they will be ignored.
    pub fn start_new_plain_bar(&self, uid: &str, total: u64, prefix: Option<&str>) {
        if self.bars.contains_key(uid) {
            // Should probably be an error
            return;
        }

        let bar = ContextualProgressBar::new_plain_bar(
            total,
            prefix.map_or_else(|| uid.to_string(), |p| p.to_string()),
        );
        bar.progress_bars().for_each(|pb| {
            self.controller.add(pb.clone());
        });
        self.bars.insert(uid.to_string(), bar);
    }

    /// Starts a new bar with the given prefix, total, and UID.
    ///
    /// `total` is the total number of items to process, and `uid` is a unique
    /// identifier for the bar -- since we can't return a handle to the new
    /// progress bar to the caller, it is the caller's responsibility to ensure
    /// that the `uid` is globally unique. `prefix` is the text that will be
    /// displayed before the bar, if not provided, the `uid` will be used as the
    /// prefix.
    ///
    /// Bars can track in-progress tasks with context items, which can be added
    /// and finished as the task progresses. In-progress tasks will be rendered
    /// alongside the bar.
    pub fn start_new_bar(&self, uid: &str, total: u64, prefix: Option<&str>) {
        if self.bars.contains_key(uid) {
            // Should probably be an error
            return;
        }

        let bar = ContextualProgressBar::new_bar(
            total,
            prefix.map_or_else(|| uid.to_string(), |p| p.to_string()),
        );
        bar.progress_bars().for_each(|pb| {
            self.controller.add(pb.clone());
        });
        self.bars.insert(uid.to_string(), bar);
    }

    /// Increments the bar with the given UID by the specified number of steps.
    pub fn inc_bar(&self, uid: &str, inc: u64) {
        if let Some(bar) = self.bars.get(uid) {
            bar.inc(inc);
        }
    }

    /// Remove the spinner with the given UID
    pub fn remove_spinner(&self, uid: &str) {
        if let Some((_name, spinner)) = self.spinners.remove(uid) {
            spinner.finish_and_clear();
            spinner.progress_bars().for_each(|pb| {
                self.controller.remove(pb);
            });
        }
    }

    /// Removes the bar with the given UID. The bar will be finished and cleared
    /// regardless of its current state.
    pub fn remove_bar(&self, uid: &str) {
        if let Some((_name, bar)) = self.bars.remove(uid) {
            bar.finish_and_clear();
            bar.progress_bars().for_each(|pb| {
                self.controller.remove(pb);
            });
        }
    }
}

impl FancyLogger {
    fn process_event(&self, record: &log::Record) {
        match TermEvent::from_record(record) {
            Some(TermEvent::StartSpinner { prefix, uid }) => {
                self.start_new_spinner(&uid, prefix.as_deref());
            }
            Some(TermEvent::StartBar { prefix, total, uid }) => {
                self.start_new_bar(&uid, total, prefix.as_deref());
            }
            Some(TermEvent::StartPlainBar { prefix, total, uid }) => {
                self.start_new_plain_bar(&uid, total, prefix.as_deref());
            }
            Some(TermEvent::AddBarContextItem { uid, item }) => {
                if let Some(bar) = self.bars.get(&uid) {
                    bar.push(&item);
                }
            }
            Some(TermEvent::FinishBarContextItem { uid, item }) => {
                if let Some(bar) = self.bars.get(&uid) {
                    bar.delete(&item);
                    bar.inc(1);
                    bar.maybe_update_stats(record);
                }
            }
            Some(TermEvent::AddSpinnerContextItem { uid, item }) => {
                if let Some(spinner) = self.spinners.get(&uid) {
                    spinner.push(&item);
                }
            }
            Some(TermEvent::FinishSpinnerContextItem { uid, item }) => {
                if let Some(spinner) = self.spinners.get(&uid) {
                    spinner.delete(&item);
                    spinner.maybe_update_stats(record);
                }
            }
            Some(TermEvent::IncBar { uid, inc }) => {
                self.inc_bar(&uid, inc);
            }
            Some(TermEvent::RemoveSpinner { uid }) => {
                self.remove_spinner(&uid);
            }
            Some(TermEvent::RemoveBar { uid }) => {
                self.remove_bar(&uid);
            }
            _ => {}
        }
    }
}

impl Default for FancyLogger {
    fn default() -> Self {
        Self::new(vec![])
    }
}

impl fmt::Debug for FancyLogger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PbLogger").finish_non_exhaustive()
    }
}

impl Drop for FancyLogger {
    fn drop(&mut self) {
        // Best-effort attempt to shut down cleanly. If anything goes wrong we
        // just give up quietly.
        let (lock, cvar) = &*self.shutdown;
        let Ok(mut shutdown) = lock.lock() else {
            // Lock poisoned, so we can't proceed
            return;
        };

        *shutdown = true;
        cvar.notify_all();

        // Wait for the ticker thread to finish
        if let Some(ticker) = self.ticker.take() {
            let _ = ticker.join();
        }
    }
}

impl log::Log for FancyLogger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() >= log::Level::Info
            || self.children.iter().any(|child| child.enabled(metadata))
    }

    fn log(&self, record: &log::Record) {
        self.process_event(record);

        if is_term_control_only(record) {
            // This is a terminal control-only record, so no need to pass it to
            // children.
            return;
        }

        for logger in &self.children {
            if logger.enabled(record.metadata()) {
                self.controller.suspend(|| {
                    logger.log(record);
                });
            }
        }
    }

    fn flush(&self) {
        for logger in &self.children {
            self.controller.suspend(|| {
                logger.flush();
            });
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ContextItem {
    name: String,
    start_time: Instant,
}

impl ContextItem {
    pub fn new(name: String) -> Self {
        Self {
            name,
            start_time: Instant::now(),
        }
    }
}

impl Display for ContextItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let elapsed = self.start_time.elapsed();
        let color = console::Style::new();
        let color = if elapsed > SLOW_CONTEXT_THRESHOLD {
            color.red().bold()
        } else if elapsed > BORDERLINE_CONTEXT_THRESHOLD {
            color.yellow().bold()
        } else {
            color
        };
        write!(
            f,
            "{} ({})",
            color.apply_to(self.name.as_str()),
            color.apply_to(format_duration_short(self.start_time.elapsed()))
        )
    }
}

impl AsRef<str> for ContextItem {
    fn as_ref(&self) -> &str {
        self.name.as_str()
    }
}

/// A progress bar that can display contextual information on in-progress tasks.
#[derive(Clone)]
struct ContextualProgressBar {
    main_bar: ProgressBar,
    context_bar: Option<ProgressBar>,
    counters: Arc<RwLock<Counter<String>>>,
    items: Arc<RwLock<Vec<ContextItem>>>,
}

impl ContextualProgressBar {
    pub fn new_plain_bar(total: u64, prefix: String) -> Self {
        let progress = ProgressBar::hidden().with_prefix(prefix);
        progress.set_style(ProgressStyleType::ArrowBar.get_style());
        progress.set_length(total);

        Self {
            main_bar: progress,
            context_bar: None,
            counters: Arc::new(RwLock::new(Counter::new())),
            items: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub fn new_bar(total: u64, prefix: String) -> Self {
        let progress = ProgressBar::hidden().with_prefix(prefix);
        progress.set_length(total);

        Self::init(progress, ProgressStyleType::ArrowBarWithCounters)
    }

    pub fn new_spinner(prefix: String) -> Self {
        let progress = ProgressBar::hidden().with_prefix(prefix);

        Self::init(progress, ProgressStyleType::Spinner)
    }

    fn init(main_bar: ProgressBar, style_type: ProgressStyleType) -> Self {
        let new_self = Self {
            main_bar,
            items: Arc::new(RwLock::new(Vec::new())),
            counters: Arc::new(RwLock::new(Counter::with_capacity(5))),
            context_bar: if style_type.needs_context_line() {
                Some(ProgressBar::hidden())
            } else {
                None
            },
        };

        let term_width = console::Term::stdout().size().1;
        let self_clone = new_self.clone();
        let style = style_type.get_style();
        let style = if style_type.needs_context_line() {
            style
        } else {
            style.with_key(
                "context",
                move |_state: &'_ ProgressState, writer: &'_ mut dyn fmt::Write| {
                    self_clone.format_context_msg(writer, term_width.sub(6) as usize)
                },
            )
        };
        let self_clone = new_self.clone();
        let style = style.with_key(
            "counters",
            move |_state: &'_ ProgressState, writer: &'_ mut dyn fmt::Write| {
                self_clone.format_counters(writer)
            },
        );

        new_self.main_bar.set_style(style);

        let self_clone = new_self.clone();
        if let Some(bar) = new_self.context_bar.as_ref() {
            bar.set_style(style_type.get_context_line_style().with_key(
                "context",
                move |_state: &'_ ProgressState, writer: &'_ mut dyn fmt::Write| {
                    self_clone.format_context_msg(writer, term_width.sub(6) as usize)
                },
            ));
        }

        new_self
    }

    fn format_counters(&self, writer: &'_ mut dyn fmt::Write) {
        let Ok(counters) = self.counters.read() else {
            // debug!("Poisoned 'counters' read lock in console");
            let _ = writer.write_str("<N/A>");
            return;
        };

        if let Ok(items) = self.items.read() {
            if !items.is_empty() {
                let _ = write!(writer, "in-progress: {}/", items.len());
            }
        }

        let mut iter = counters.iter().sorted_by(|a, b| a.0.cmp(b.0));
        let mut entry = iter.next();

        loop {
            let Some((item, count)) = entry else {
                break;
            };

            if write!(writer, "{}: {}", item, count).is_err() {
                // If we can't write to the writer, just stop
                break;
            }

            entry = iter.next();
            if entry.is_some() && write!(writer, "/").is_err() {
                break;
            }
        }
    }

    fn format_context_msg(&self, writer: &'_ mut dyn fmt::Write, max_len: usize) {
        match self.items.read() {
            Ok(items) => {
                let fullmsg = items.iter().join(", ");
                let graphemes = fullmsg.graphemes(true).collect::<Vec<&str>>();
                let shortmsg = if graphemes.len() < max_len {
                    fullmsg
                } else {
                    graphemes
                        .into_iter()
                        .take(max_len - 3)
                        .chain(std::iter::once("..."))
                        .collect::<String>()
                };
                match writer.write_str(shortmsg.as_str()) {
                    Ok(_) => (),
                    Err(_) => {
                        // debug!("Failed to write context message");
                    }
                }
                // items.iter().take(5).for_each(|item| {
                //     let _ = writeln!(writer);
                //     let _ = write!(writer, "   {}", item);
                // });
            }
            Err(_) => {
                // debug!("Poisoned 'context_slots' read lock in console");
                let _ = writer.write_str("<N/A>");
            }
        }
    }

    fn maybe_update_stats(&self, record: &log::Record) {
        if let Some(StatEvent::Counter { name, step }) = StatEvent::from_record(record) {
            self.inc_counter(&name, step);
        }
    }
}

impl ContextualProgressBar {
    pub fn progress_bars(&self) -> impl Iterator<Item = &ProgressBar> {
        std::iter::once(&self.main_bar).chain(self.context_bar.iter())
    }

    /// Increments the progress bar by the specified number of steps.
    pub fn inc(&self, inc: u64) {
        self.main_bar.inc(inc);
    }

    /// Increments the counter for the given item by the specified step.
    pub fn inc_counter(&self, item: &str, step: i64) {
        let _ = self.counters.write().map(|mut counters| {
            if let Some(count) = counters.get_mut(item) {
                let new_count = (*count as i64 + step).max(0) as usize;
                *count = new_count;
            } else {
                counters.insert(item.to_string(), step.max(0) as usize);
            }
        });
    }

    /// Finishes the progress bar and clears its context items.
    pub fn finish_and_clear(&self) {
        self.main_bar.finish_and_clear();
        if let Some(bar) = &self.context_bar {
            bar.finish_and_clear();
        }
    }

    pub fn tick(&self) {
        self.main_bar.tick();
        if let Some(bar) = &self.context_bar {
            bar.tick();
        }
    }

    pub fn push(&self, item: &str) {
        match self.items.write() {
            Ok(mut slots) => {
                slots.push(ContextItem::new(item.to_string()));
            }
            Err(_) => {
                // debug!("Poisoned 'context_slots' write lock in console");
            }
        }
    }

    pub fn delete(&self, item: &str) {
        match self.items.write() {
            Ok(mut slots) => {
                if let Some(pos) = slots.iter().position(|x| x.as_ref() == item) {
                    slots.remove(pos);
                }
            }
            Err(_) => {
                // debug!("Poisoned 'context_slots' write lock in console");
            }
        }
    }
}

fn format_duration_short(duration: Duration) -> String {
    let duration = duration.as_secs_f64();
    if duration > 60.0 {
        format!("{}m {:.0}s", duration as u32 / 60, duration % 60.0)
    } else {
        format!("{:.1}s", duration)
    }
}

/// A guard that will send a terminal event when dropped.
pub struct ProgressBarGuard {
    invocation_id: u128,
    event: Option<TermEvent>,
}

impl ProgressBarGuard {
    pub fn new(invocation_id: u128, event: TermEvent) -> Self {
        Self {
            invocation_id,
            event: Some(event),
        }
    }

    pub fn noop() -> Self {
        Self {
            invocation_id: 0,
            event: None,
        }
    }
}

impl Drop for ProgressBarGuard {
    fn drop(&mut self) {
        let Some(event) = self.event.take() else {
            // No event to send, so just return
            return;
        };
        // Send the specific terminal event to the logger.
        _log!(
            log::Level::Info,
            _INVOCATION_ID_=self.invocation_id,
            _TERM_ONLY_=true,
            _TERM_EVENT_:serde=event;
            ""
        );
    }
}
