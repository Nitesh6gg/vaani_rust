//! Tells whether this machine is too busy to take on another call.
//!
//! Past a CPU level the media tasks of the calls already up start missing their 20ms ticks, which the
//! callers hear as jitter. The guard measures the whole machine's CPU (Asterisk shares it, and a starved
//! Asterisk is as bad as a starved Vaani) and, like LiveKit's agent workers, only stops *new* work while
//! it is over the limit: calls in progress are never touched.

use crate::metrics;
use anyhow::{Result, bail};
use std::{
    collections::VecDeque,
    sync::atomic::{AtomicBool, Ordering::Relaxed},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

/// Samples averaged: the load is the last 5 seconds' CPU.
const WINDOW: usize = 5;
/// Samples below the resume level before new work is taken again.
const CALM_SAMPLES: usize = 10;

/// Cumulative busy and total CPU time of the machine, in clock ticks.
type Reader = Box<dyn Fn() -> Result<(u64, u64)> + Send + Sync>;

pub struct Guard {
    threshold: f64,
    resume: f64,
    over: AtomicBool,
    read: Reader,
    interval: Duration,
}

/// The sampling state: one second's CPU use in, whether the machine is overloaded out.
#[derive(Default)]
struct Tracker {
    samples: VecDeque<f64>,
    calm: usize,
    over: bool,
}

impl Tracker {
    /// Takes one second's CPU use (0..1). Overloaded starts when the average of the last 5 samples reaches
    /// the threshold; it ends only after 10 consecutive samples whose average is below the resume level,
    /// so it can't flap. Returns the average.
    fn observe(&mut self, use_: f64, threshold: f64, resume: f64) -> f64 {
        self.samples.push_back(use_);
        if self.samples.len() > WINDOW {
            self.samples.pop_front();
        }
        let avg = self.samples.iter().sum::<f64>() / self.samples.len() as f64;
        if !self.over {
            if self.samples.len() == WINDOW && avg >= threshold {
                self.over = true;
                self.calm = 0;
                tracing::warn!(
                    event = "load.overloaded",
                    cpu_percent = (avg * 100.0) as u32,
                    limit_percent = (threshold * 100.0) as u32,
                    "cpu over the limit: refusing new calls"
                );
            }
        } else if avg < resume {
            self.calm += 1;
            if self.calm >= CALM_SAMPLES {
                self.over = false;
                tracing::info!(
                    event = "load.recovered",
                    cpu_percent = (avg * 100.0) as u32,
                    "cpu back under the limit: accepting calls again"
                );
            }
        } else {
            self.calm = 0;
        }
        avg
    }
}

impl Guard {
    /// A guard that turns overloaded at `threshold_percent` CPU (a 5-second average) and calm again after
    /// 10 seconds below `resume_percent`. `None` when `threshold_percent` is 0: the guard is off.
    pub fn new(threshold_percent: u32, resume_percent: u32) -> Option<Self> {
        (threshold_percent > 0).then(|| Self {
            threshold: f64::from(threshold_percent) / 100.0,
            resume: f64::from(resume_percent) / 100.0,
            over: AtomicBool::new(false),
            read: Box::new(read_proc_stat),
            interval: Duration::from_secs(1),
        })
    }

    /// Whether new calls should be refused right now.
    pub fn overloaded(&self) -> bool {
        self.over.load(Relaxed)
    }

    /// Samples the machine's CPU every second until `cancel`.
    pub async fn run(&self, cancel: CancellationToken) {
        let Ok((mut prev_busy, mut prev_total)) = (self.read)() else {
            tracing::warn!(event = "load.unavailable", "cpu load guard disabled: cannot read the machine's CPU (/proc/stat)");
            return;
        };
        tracing::info!(
            event = "load.enabled",
            refuse_new_calls_at_percent = (self.threshold * 100.0) as u32,
            accept_again_below_percent = (self.resume * 100.0) as u32,
            "cpu load guard on"
        );
        let mut tick = tokio::time::interval(self.interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        tick.tick().await;
        let mut tracker = Tracker::default();
        loop {
            tokio::select! { _ = cancel.cancelled() => return, _ = tick.tick() => {} }
            let Ok((busy, total)) = (self.read)() else { continue };
            if total <= prev_total {
                continue;
            }
            let avg = tracker.observe((busy - prev_busy) as f64 / (total - prev_total) as f64, self.threshold, self.resume);
            (prev_busy, prev_total) = (busy, total);
            self.over.store(tracker.over, Relaxed);
            metrics::CPU_LOAD_PERMILLE.store((avg * 1000.0) as u64, Relaxed);
            metrics::OVERLOADED.store(tracker.over, Relaxed);
        }
    }
}

/// The machine's cumulative CPU time from /proc/stat: busy (everything but idle and iowait) and total.
fn read_proc_stat() -> Result<(u64, u64)> {
    let stat = std::fs::read_to_string("/proc/stat")?;
    parse_stat(stat.lines().next().unwrap_or_default())
}

/// Parses the aggregate "cpu  user nice system idle iowait irq softirq steal ..." line. Guest time is
/// already inside user and nice.
fn parse_stat(line: &str) -> Result<(u64, u64)> {
    let f: Vec<&str> = line.split_whitespace().collect();
    if f.len() < 5 || f[0] != "cpu" {
        bail!("unexpected /proc/stat line {line:?}");
    }
    let mut v = [0u64; 8];
    for (slot, field) in v.iter_mut().zip(&f[1..]) {
        *slot = field.parse()?;
    }
    let total: u64 = v.iter().sum();
    Ok((total - (v[3] + v[4]), total))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn feed(t: &mut Tracker, n: usize, use_: f64) {
        for _ in 0..n {
            t.observe(use_, 0.9, 0.8);
        }
    }

    #[test]
    fn overloaded_at_the_threshold_average_after_a_full_window() {
        let mut t = Tracker::default();
        feed(&mut t, 4, 1.0);
        assert!(!t.over, "not before a full 5-second window");
        feed(&mut t, 1, 1.0);
        assert!(t.over);
    }

    #[test]
    fn a_spike_is_not_overload() {
        let mut t = Tracker::default();
        feed(&mut t, 10, 0.5);
        feed(&mut t, 1, 1.0); // one busy second in a 5-second average of ~0.6
        assert!(!t.over);
    }

    #[test]
    fn stays_overloaded_until_calm_for_10_seconds() {
        let mut t = Tracker::default();
        feed(&mut t, 5, 0.95);
        assert!(t.over);
        feed(&mut t, 20, 0.85);
        assert!(t.over, "between resume and threshold it stays refusing");
        feed(&mut t, 9, 0.5); // the average is under 80% from the first calm second on
        assert!(t.over, "9 calm seconds are not enough");
        feed(&mut t, 1, 0.5);
        assert!(!t.over);
    }

    #[test]
    fn load_coming_back_restarts_the_calm_count() {
        let mut t = Tracker::default();
        feed(&mut t, 5, 0.95);
        feed(&mut t, 8, 0.5); // calm, 7 seconds counted
        assert!(t.over);
        feed(&mut t, 5, 0.9); // busy again: the count starts over
        feed(&mut t, 10, 0.5);
        assert!(t.over, "10 seconds after the window drained are not yet 10 calm ones");
        feed(&mut t, 1, 0.5);
        assert!(!t.over);
    }

    #[test]
    fn off_is_none() {
        assert!(Guard::new(0, 0).is_none());
    }

    #[test]
    fn parses_proc_stat() {
        let (busy, total) = parse_stat("cpu  100 10 50 800 20 5 15 0 0 0").unwrap();
        assert_eq!((busy, total), (180, 1000), "user+nice+system+irq+softirq+steal; idle and iowait are not busy");
        assert!(parse_stat("cpu0 1 2 3 4 5").is_err());
        assert!(parse_stat("cpu  1 2 x 4 5").is_err());
        if let Ok((busy, total)) = read_proc_stat() {
            assert!(total > 0 && busy <= total, "the real machine");
        }
    }

    #[tokio::test]
    async fn run_samples_the_counters() {
        let mut g = Guard::new(90, 80).unwrap();
        let counters = std::sync::Arc::new(std::sync::Mutex::new((0u64, 0u64)));
        let c = counters.clone();
        g.read = Box::new(move || {
            let mut c = c.lock().unwrap();
            *c = (c.0 + 98, c.1 + 100); // 98% busy every "second"
            Ok(*c)
        });
        g.interval = Duration::from_millis(10);
        let g = std::sync::Arc::new(g);
        let cancel = CancellationToken::new();
        let task = tokio::spawn({
            let (g, cancel) = (g.clone(), cancel.clone());
            async move { g.run(cancel).await }
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while !g.overloaded() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("overloaded after 5 busy samples");
        cancel.cancel();
        task.await.unwrap();
    }
}
