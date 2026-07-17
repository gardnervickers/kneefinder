use std::{
    collections::BTreeMap,
    env,
    error::Error,
    io::{BufRead, BufReader, BufWriter, Write},
    process::{Child, ChildStdin, ChildStdout, Command, Stdio},
    time::{SystemTime, UNIX_EPOCH},
};

use kneefinder::protocol::{
    AdapterMessage, ArgumentValue, ControllerMessage, OperationId, OperationStatus,
    PROTOCOL_VERSION, PhaseId, RunId, ScheduledOperation,
};
use kneefinder::stats::{OperationVariant, StatsReport, summarize_results};

const WORKERS: usize = 4;
const READ_SERVICE_MS: u64 = 10;
const WRITE_SERVICE_MS: u64 = 20;
const PHASE_DURATION_NS: u64 = 1_000_000_000;

pub fn run() -> Result<(), Box<dyn Error>> {
    let executable = env::current_exe()?;
    let mut adapter = AdapterProcess::spawn(&executable)?;
    adapter.send(&ControllerMessage::Initialize {
        protocol_version: PROTOCOL_VERSION,
        run_id: RunId(1),
        config: serde_json::json!({
            "workers": WORKERS,
            "read_service_ms": READ_SERVICE_MS,
            "write_service_ms": WRITE_SERVICE_MS,
            "queue_capacity": 4096
        }),
    })?;
    match adapter.receive()? {
        AdapterMessage::Ready { operations, .. }
            if operations
                .iter()
                .map(|operation| operation.name.as_str())
                .eq(["read", "write"]) =>
        {
            eprintln!("adapter advertised operations: read=9, write=1");
        }
        AdapterMessage::Ready { operations, .. } => {
            return Err(format!("unexpected advertised operations: {operations:?}").into());
        }
        message => return Err(format!("expected adapter ready, got {message:?}").into()),
    }

    let average_read_ms = READ_SERVICE_MS as f64 * 1.25;
    let average_write_ms = WRITE_SERVICE_MS as f64 * 1.25;
    let average_service_ms = (9.0 * average_read_ms + average_write_ms) / 10.0;
    let expected_knee = WORKERS as f64 * 1_000.0 / average_service_ms;
    eprintln!("e2e topology: controller -> external adapter with internal queue service");
    eprintln!(
        "target: {WORKERS} workers; 90/10 operations and 3/1 argument variants; theoretical knee {expected_knee:.0} req/s\n"
    );

    let rates = [100.0, 200.0, 250.0, 290.0, 325.0, 425.0, 550.0];
    let mut rows = Vec::new();
    for (index, rate) in rates.into_iter().enumerate() {
        eprint!("measuring {rate:.0} req/s... ");
        io_flush_stderr()?;
        let row = run_phase(&mut adapter, PhaseId(index as u64 + 1), rate)?;
        eprintln!("p95 {:.1}ms", row.p95_ms);
        rows.push(row);
    }

    adapter.send(&ControllerMessage::Shutdown)?;
    adapter.wait()?;
    print_rows(&rows, expected_knee);
    print_variant_stats(&rows);
    Ok(())
}

fn run_phase(
    adapter: &mut AdapterProcess,
    phase_id: PhaseId,
    offered_per_second: f64,
) -> Result<DemoRow, Box<dyn Error>> {
    let operation_count = (offered_per_second * PHASE_DURATION_NS as f64 / 1e9).floor() as u64;
    let phase_start_unix_ns = unix_now_ns().saturating_add(100_000_000);
    let operations = (0..operation_count)
        .map(|index| {
            let (operation, arguments) = variant_for_index(index);
            ScheduledOperation {
                id: OperationId(index),
                operation: operation.into(),
                start_offset_ns: (index as f64 * 1e9 / offered_per_second).round() as u64,
                arguments,
            }
        })
        .collect();

    adapter.send(&ControllerMessage::Schedule {
        phase_id,
        phase_start_unix_ns,
        operations,
    })?;
    let results = match adapter.receive()? {
        AdapterMessage::Results {
            phase_id: result_phase,
            operations,
        } if result_phase == phase_id => operations,
        message => return Err(format!("unexpected adapter response: {message:?}").into()),
    };

    let stats = summarize_results(&results)?;

    let completed_in_window = results
        .iter()
        .filter(|result| {
            matches!(result.status, OperationStatus::Ok)
                && result
                    .actual_start_offset_ns
                    .saturating_add(result.client_latency_ns)
                    <= PHASE_DURATION_NS
        })
        .count();

    Ok(DemoRow {
        offered_per_second,
        goodput_per_second: completed_in_window as f64 * 1e9 / PHASE_DURATION_NS as f64,
        p50_ms: ns_to_ms(stats.overall.client_latency_ns.p50),
        p95_ms: ns_to_ms(stats.overall.client_latency_ns.p95),
        dispatch_p99_ms: ns_to_ms(stats.overall.dispatch_lag_ns.p99),
        stats,
    })
}

/// One interleaved 40-operation cycle: read(0)=27, read(1)=9,
/// write(small)=3, write(large)=1.
fn variant_for_index(index: u64) -> (&'static str, BTreeMap<String, ArgumentValue>) {
    match index % 40 {
        39 => (
            "write",
            BTreeMap::from([("value".into(), ArgumentValue::String("large".into()))]),
        ),
        9 | 19 | 29 => (
            "write",
            BTreeMap::from([("value".into(), ArgumentValue::String("small".into()))]),
        ),
        3 | 7 | 11 | 15 | 23 | 27 | 31 | 35 | 37 => (
            "read",
            BTreeMap::from([("key".into(), ArgumentValue::Integer(1))]),
        ),
        _ => (
            "read",
            BTreeMap::from([("key".into(), ArgumentValue::Integer(0))]),
        ),
    }
}

fn print_rows(rows: &[DemoRow], expected_knee: f64) {
    let maximum_p95 = rows.iter().map(|row| row.p95_ms).fold(0.0_f64, f64::max);
    println!("\nexpected knee: {expected_knee:.0} req/s");
    println!(
        "{:<9} {:<9} {:<9} {:<9} {:<9} {:<12} latency",
        "offered", "goodput", "bad %", "p50 ms", "p95 ms", "dispatch p99"
    );
    for row in rows {
        let bar_width = if maximum_p95 == 0.0 {
            0
        } else {
            (row.p95_ms / maximum_p95 * 32.0).round() as usize
        };
        println!(
            "{:<9.0} {:<9.1} {:<9.2} {:<9.1} {:<9.1} {:<12.2} {}",
            row.offered_per_second,
            row.goodput_per_second,
            row.stats.overall.unsuccessful_rate() * 100.0,
            row.p50_ms,
            row.p95_ms,
            row.dispatch_p99_ms,
            "█".repeat(bar_width.max(1))
        );
    }
}

#[derive(Debug)]
struct DemoRow {
    offered_per_second: f64,
    goodput_per_second: f64,
    p50_ms: f64,
    p95_ms: f64,
    dispatch_p99_ms: f64,
    stats: StatsReport,
}

fn print_variant_stats(rows: &[DemoRow]) {
    println!("\nper operation variant:");
    println!(
        "{:<8} {:<24} {:<8} {:<8} {:<8} {:<9} {:<9} {:<9} {:<9}",
        "offered",
        "variant",
        "attempts",
        "ok",
        "errors",
        "timeouts",
        "p50 ms",
        "p95 ms",
        "p99 ms"
    );
    for row in rows {
        for variant in &row.stats.variants {
            println!(
                "{:<8.0} {:<24} {:<8} {:<8} {:<8} {:<9} {:<9.1} {:<9.1} {:<9.1}",
                row.offered_per_second,
                format_variant(&variant.variant),
                variant.stats.attempts,
                variant.stats.successful,
                variant.stats.failed,
                variant.stats.timed_out,
                ns_to_ms(variant.stats.client_latency_ns.p50),
                ns_to_ms(variant.stats.client_latency_ns.p95),
                ns_to_ms(variant.stats.client_latency_ns.p99),
            );
        }
    }
}

fn format_variant(variant: &OperationVariant) -> String {
    let arguments = variant
        .arguments
        .iter()
        .map(|(name, value)| {
            let value = match value {
                ArgumentValue::Integer(value) => value.to_string(),
                ArgumentValue::String(value) => value.clone(),
            };
            format!("{name}={value}")
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("{}({arguments})", variant.operation)
}

fn ns_to_ms(value: Option<u64>) -> f64 {
    value.unwrap_or_default() as f64 / 1e6
}

struct AdapterProcess {
    child: Child,
    input: BufWriter<ChildStdin>,
    output: BufReader<ChildStdout>,
}

impl AdapterProcess {
    fn spawn(executable: &std::path::Path) -> Result<Self, Box<dyn Error>> {
        let mut child = Command::new(executable)
            .arg("adapter")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;
        let input = BufWriter::new(child.stdin.take().ok_or("adapter stdin unavailable")?);
        let output = BufReader::new(child.stdout.take().ok_or("adapter stdout unavailable")?);
        Ok(Self {
            child,
            input,
            output,
        })
    }

    fn send(&mut self, message: &ControllerMessage) -> Result<(), Box<dyn Error>> {
        serde_json::to_writer(&mut self.input, message)?;
        self.input.write_all(b"\n")?;
        self.input.flush()?;
        Ok(())
    }

    fn receive(&mut self) -> Result<AdapterMessage, Box<dyn Error>> {
        let mut line = String::new();
        if self.output.read_line(&mut line)? == 0 {
            return Err("adapter closed stdout unexpectedly".into());
        }
        Ok(serde_json::from_str(&line)?)
    }

    fn wait(&mut self) -> Result<(), Box<dyn Error>> {
        let status = self.child.wait()?;
        if !status.success() {
            return Err(format!("adapter exited with {status}").into());
        }
        Ok(())
    }
}

impl Drop for AdapterProcess {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn unix_now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u64::MAX as u128) as u64
}

fn io_flush_stderr() -> Result<(), Box<dyn Error>> {
    std::io::stderr().flush()?;
    Ok(())
}
