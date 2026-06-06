//! Votor Timing Measurement Harness for Agave BankingStage
//!

use std::{
    collections::HashMap,
    num::NonZeroUsize,
    sync::{atomic::Ordering, Arc},
    time::{Duration, Instant},
};
    
use agave_banking_stage_ingress_types::{BankingPacketBatch, SchedulerPriorityFloor};
use crossbeam_channel::unbounded;
use solana_core::{
    banking_stage::BankingStage,
    banking_stage::transaction_scheduler::scheduler_controller::{
        SchedulerConfig,
    },
    banking_trace::{BankingTracer, Channels},
    validator::{BlockProductionMethod, SchedulerPacing},
};
use solana_entry::entry_or_marker::EntryOrMarker;
use solana_keypair::Keypair;
use solana_ledger::{
    blockstore::Blockstore,
    genesis_utils::{GenesisConfigInfo, create_genesis_config},
    get_tmp_ledger_path_auto_delete,
};
use solana_perf::packet::to_packet_batches;
use solana_poh::poh_recorder::create_test_recorder;
use solana_runtime::bank::Bank;
use solana_signer::Signer;
use solana_system_transaction as system_transaction;
use tokio::sync::mpsc;

// ─── Votor constants ──────────────────────────────────────────────────────

const DELTA_BLOCK:   Duration = Duration::from_millis(400);
const DELTA_TIMEOUT: Duration = Duration::from_millis(1_200);
const WARN_MS: u64 = 240; // 60 % of Δ_block

const N_ROUNDS:       usize = 10;
const TXS_PER_ROUND:  usize = 10;

// ─── /proc helpers ────────────────────────────────────────────────────────

/// (on_cpu_ns, runqueue_wait_ns) — field 1 is scheduling delay.
fn read_schedstat(tid: i32) -> Option<(u64, u64)> {
    let s = std::fs::read_to_string(
        format!("/proc/self/task/{tid}/schedstat")
    ).ok()?;
    let mut it = s.split_whitespace();
    Some((it.next()?.parse().ok()?, it.next()?.parse().ok()?))
}

/// (minor_faults, major_faults) from /proc/self/stat.
fn read_page_faults() -> (u64, u64) {
    let s = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    let f: Vec<&str> = s.split_whitespace().collect();
    (
        f.get(9).and_then(|v| v.parse().ok()).unwrap_or(0),
        f.get(11).and_then(|v| v.parse().ok()).unwrap_or(0),
    )
}

/// Kernel wait-channel: where the thread sleeps when off-CPU.
fn read_wchan(tid: i32) -> String {
    std::fs::read_to_string(format!("/proc/self/task/{tid}/wchan"))
        .unwrap_or_default()
        .trim()
        .to_string()
}

fn all_tids() -> Vec<i32> {
    let pid = std::process::id();
    std::fs::read_dir(format!("/proc/{pid}/task"))
        .into_iter()
        .flatten()
        .filter_map(|e| e.ok())
        .filter_map(|e| e.file_name().to_str()?.parse().ok())
        .collect()
}

fn snap_schedstat() -> HashMap<i32, (u64, u64)> {
    all_tids()
        .into_iter()
        .filter_map(|tid| read_schedstat(tid).map(|s| (tid, s)))
        .collect()
}

fn total_wait_delta(
    before: &HashMap<i32, (u64, u64)>,
    after:  &HashMap<i32, (u64, u64)>,
) -> u64 {
    after.iter().map(|(tid, (_, w_a))| {
        w_a.saturating_sub(before.get(tid).map(|(_, w)| *w).unwrap_or(0))
    }).sum()
}

// ─── Jitter cause ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cause {
    FirstTouch,     // round 0: demand-paging workers into RAM (expected)
    CpuScheduling,  // high sched_wait_ns → OS not giving workers CPU time
    IoStall,        // majflt > 0 → page from disk
    Normal,
}
impl Cause {
    fn label(self) -> &'static str {
        match self {
            Cause::FirstTouch    => "first_touch",
            Cause::CpuScheduling => "cpu_scheduling",
            Cause::IoStall       => "io_stall",
            Cause::Normal        => "normal",
        }
    }
}

fn classify(round: usize, sched_wait_ns: u64, minflt: u64, majflt: u64) -> Cause {
    if majflt > 0          { Cause::IoStall }
    else if round == 0 && minflt > 300 { Cause::FirstTouch }
    else if sched_wait_ns > 5_000_000  { Cause::CpuScheduling }
    else                   { Cause::Normal }
}

// ─── SLO tier ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tier { Ok, Warn, AtRisk, Missed }
impl Tier {
    fn from(d: Duration, committed: bool) -> Self {
        if !committed                        { Tier::Missed  }
        else if d.as_millis() > 400          { Tier::AtRisk  }
        else if d.as_millis() as u64 > WARN_MS { Tier::Warn }
        else                                 { Tier::Ok      }
    }
    fn label(self) -> &'static str {
        match self { Tier::Ok => "OK", Tier::Warn => "WARN",
                     Tier::AtRisk => "AT_RISK", Tier::Missed => "MISSED" }
    }
}

// ─── THE TEST ─────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn measure_banking_stage_against_votor_timing() {

    // ── 1. tokio-console subscriber ───────────────────────────────────────
    //
    // Opens gRPC on 127.0.0.1:6669 — attach with `tokio-console`.
    // You will see: wakeup_probe task + BankingStage manager (ep_poll).
    // You will NOT see: solBnkTxSched, solCoWorker00..03 — those are OS
    // threads, not tokio tasks. Their delay is in /proc schedstat below.
    console_subscriber::init();

    println!("\n[harness] tokio-console active → attach: tokio-console");
    println!("[harness] Votor: Δ_block={}ms Δ_timeout={}ms",
        DELTA_BLOCK.as_millis(), DELTA_TIMEOUT.as_millis());
    tokio::time::sleep(Duration::from_secs(3)).await;

    // ── 2. BankingStage setup ─────────────────────────────────────────────

    let GenesisConfigInfo { genesis_config, mint_keypair, .. } =
        create_genesis_config(1_000_000_000_000);
    let (bank, bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);

    let banking_tracer = BankingTracer::new_disabled();
    let Channels {
        non_vote_sender,
        non_vote_receiver,
        tpu_vote_sender,
        tpu_vote_receiver,
        gossip_vote_sender,
        gossip_vote_receiver,
    } = banking_tracer.create_channels();

    let ledger_path = get_tmp_ledger_path_auto_delete!();
    let blockstore = Arc::new(
        Blockstore::open(ledger_path.path()).expect("blockstore"),
    );
    let (exit, poh_recorder, _poh_controller, transaction_recorder, poh_service, entry_receiver) =
        create_test_recorder(bank.clone(), blockstore, None, None);

    let (replay_vote_sender, _replay_vote_receiver) = unbounded();

    // ── SchedulerConfig in a let-binding to avoid inline struct-literal
    //    parse ambiguity (which causes a misleading "wrong argument count" error).
    let scheduler_cfg = SchedulerConfig {
        scheduler_pacing: SchedulerPacing::Disabled,
    };

    let banking_stage = BankingStage::new_num_threads(
        BlockProductionMethod::CentralSchedulerGreedy,
        poh_recorder.clone(),
        transaction_recorder,
        non_vote_receiver,
        tpu_vote_receiver,
        gossip_vote_receiver,
        mpsc::channel(1).1,           // BankingControlMsg receiver
        NonZeroUsize::new(4).unwrap(),
        scheduler_cfg,
        None,                          // transaction_status_sender
        replay_vote_sender,
        None,                          // log_messages_bytes_limit
        bank_forks,
        None,                          // prioritization_fee_cache
        Arc::default(),                // filter_keys
        Arc::new(SchedulerPriorityFloor::new()),
    );

    // ── 3. tokio-console sentinel task ────────────────────────────────────
    //
    // Always-ready task. Its `Scheduled` column in tokio-console shows
    // how deep the Tokio run-queue is. The banking workers are OS threads
    // and will NEVER appear here — this is the intentional blind-spot
    // demonstration.
    let probe = tokio::spawn(async move {
        loop {
            let t0 = Instant::now();
            tokio::task::yield_now().await;
            let us = t0.elapsed().as_micros();
            if us > 5_000 {
                tracing::warn!(us, "wakeup_probe: {us}µs > 5ms — \
                    tokio run-queue saturated (banking workers not visible here; \
                    check /proc schedstat for OS scheduling delay)");
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    });

    tokio::time::sleep(Duration::from_millis(50)).await; // let workers start

    // ── 4. Measurement loop ───────────────────────────────────────────────

    struct RoundResult {
        idx:           usize,
        latency:       Duration,
        committed:     bool,
        sched_wait_ns: u64,
        minflt_delta:  u64,
        majflt_delta:  u64,
        cause:         Cause,
        tier:          Tier,
        wchan_sample:  Vec<String>,
    }

    let mut results: Vec<RoundResult> = Vec::with_capacity(N_ROUNDS);

    let mut senders: Vec<Keypair> = (0..N_ROUNDS * TXS_PER_ROUND).map(|_| Keypair::new()).collect();
    
    let funding_blockhash = bank.last_blockhash();
    let funding_txs: Vec<_> = senders.iter()
        .map(|sender| system_transaction::transfer(&mint_keypair, &sender.pubkey(), 100_000, funding_blockhash))
        .collect();
    
    let funding_batches = to_packet_batches(&funding_txs, funding_txs.len());
    non_vote_sender.send(BankingPacketBatch::new(funding_batches)).unwrap();

    while entry_receiver.try_recv().is_ok() {  }
    tokio::time::sleep(Duration::from_millis(100)).await;

    for i in 0..N_ROUNDS {
        let blockhash = bank.clone().last_blockhash();

        let round_senders = &senders[i * TXS_PER_ROUND .. (i + 1) * TXS_PER_ROUND];

        // Build real transactions.
        let recipients: Vec<Keypair> = (0..TXS_PER_ROUND).map(|_| Keypair::new()).collect();
        let txs: Vec<_> = round_senders.iter().zip(recipients.iter())
            .map(|(sender, rcpt)| system_transaction::transfer(
                sender, // Уникальный отправитель
                &rcpt.pubkey(), 
                1_000, 
                blockhash
            ))
            .collect();

        let packets = to_packet_batches(&txs, TXS_PER_ROUND);

        // ── pre-snapshot ──────────────────────────────────────────────────
        let sched_before = snap_schedstat();
        let (minflt_b, majflt_b) = read_page_faults();

        // wchan on round 0: banking workers should be in futex_wait_queue
        let wchan_sample: Vec<String> = all_tids().iter()
            .map(|&tid| format!("tid={tid} wchan={}", read_wchan(tid)))
            .filter(|s| s.contains("futex") || s.contains("ep_poll"))
            .take(4)
            .collect();

        // ── SEND — t=0 ────────────────────────────────────────────────────
        let t_send = Instant::now();

        // FIX: use BankingPacketBatch::new() — NOT (vec, None) tuple.
        non_vote_sender
            .send(BankingPacketBatch::new(packets))
            .expect("non_vote_sender closed");

        // ── POLL via entry_receiver ────────────────────────────────────────
        //
        // The banking stage records committed transactions to PoH → entries
        // appear in entry_receiver. This is the canonical completion signal
        // used by the internal tests (see test_banking_stage_entries_only_*).
        //
        // We use blocking try_recv with short sleeps so the async executor
        // can still run the wakeup_probe task concurrently.
        let deadline = Instant::now() + DELTA_TIMEOUT;
        let mut committed = false;

        while Instant::now() < deadline {
            match entry_receiver.try_recv() {
                Ok((_bank, (EntryOrMarker::Entry(entry), _tick)))
                    if !entry.transactions.is_empty() =>
                {
                    committed = true;
                    break;
                }
                _ => {}
            }
            // yield to tokio so the wakeup_probe task gets a turn
            tokio::task::yield_now().await;
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        let latency = t_send.elapsed();

        // ── post-snapshot ─────────────────────────────────────────────────
        let sched_after = snap_schedstat();
        let (minflt_a, majflt_a) = read_page_faults();

        let sched_wait_ns = total_wait_delta(&sched_before, &sched_after);
        let minflt_delta  = minflt_a.saturating_sub(minflt_b);
        let majflt_delta  = majflt_a.saturating_sub(majflt_b);

        let cause = classify(i, sched_wait_ns, minflt_delta, majflt_delta);
        let tier  = Tier::from(latency, committed);

        let remaining = DELTA_BLOCK.saturating_sub(latency);
        let budget_pct = latency.as_micros() as f64 /
                         DELTA_BLOCK.as_micros() as f64 * 100.0;

        match tier {
            Tier::Missed | Tier::AtRisk => tracing::warn!(
                round = i, latency_ms = latency.as_millis(), committed,
                sched_wait_ns, cause = cause.label(), tier = tier.label(),
                budget_pct, remaining_ms = remaining.as_millis(),
                "Votor SLO breach: banking={}ms remaining={}ms ({}% of Δ_block) cause={}",
                latency.as_millis(), remaining.as_millis(), budget_pct as u32, cause.label()
            ),
            _ => tracing::info!(
                round = i, latency_ms = latency.as_millis(), committed,
                cause = cause.label(), tier = tier.label(),
                "OK: banking={}ms remaining={}ms ({}% of Δ_block)",
                latency.as_millis(), remaining.as_millis(), budget_pct as u32
            ),
        }

        results.push(RoundResult {
            idx: i, latency, committed,
            sched_wait_ns, minflt_delta, majflt_delta,
            cause, tier, wchan_sample,
        });

        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // ── 5. Report ─────────────────────────────────────────────────────────

    let missed  = results.iter().filter(|r| r.tier == Tier::Missed).count();
    let at_risk = results.iter().filter(|r| r.tier == Tier::AtRisk).count();

    let steady: Vec<_> = results.iter().skip(1).collect();
    let avg_ms = steady.iter()
        .map(|r| r.latency.as_secs_f64() * 1000.0)
        .sum::<f64>() / steady.len().max(1) as f64;
    let max_ms = steady.iter()
        .map(|r| r.latency.as_secs_f64() * 1000.0)
        .fold(0.0f64, f64::max);

    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║  VOTOR HARNESS — BankingStage vs Alpenglow §2 Budget        ║");
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║  Δ_block={} ms  Δ_timeout={} ms                           ║",
        DELTA_BLOCK.as_millis(), DELTA_TIMEOUT.as_millis());
    println!("║  banking_latency ⊂ block_production ⊂ Votor_round          ║");
    println!("║  remaining_budget = Δ_block − banking_latency              ║");
    println!("╠════╦══════════╦════════════╦══════════╦═══════╦════════════╣");
    println!("║ #  ║ latency  ║ sched_wait ║ remaining║ tier  ║ cause      ║");
    println!("╠════╬══════════╬════════════╬══════════╬═══════╬════════════╣");
    for r in &results {
        let rem = DELTA_BLOCK.saturating_sub(r.latency);
        println!("║ {:>2} ║ {:>6.1} ms ║ {:>8} ns ║ {:>6.0} ms ║ {:<5} ║ {:<10} ║",
            r.idx,
            r.latency.as_secs_f64() * 1000.0,
            r.sched_wait_ns,
            rem.as_secs_f64() * 1000.0,
            r.tier.label(),
            r.cause.label(),
        );
    }
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║  Steady avg (rounds 1..N): {:>7.2} ms                       ║", avg_ms);
    println!("║  Steady max (rounds 1..N): {:>7.2} ms                       ║", max_ms);
    println!("║  Slots MISSED (>{} ms):  {:>3}                              ║",
        DELTA_TIMEOUT.as_millis(), missed);
    println!("║  Slots AT_RISK (>{} ms): {:>3}                              ║",
        DELTA_BLOCK.as_millis(), at_risk);
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║  JITTER BREAKDOWN                                           ║");
    println!("║  first_touch (round 0, demand paging): {:>3}                ║",
        results.iter().filter(|r| r.cause == Cause::FirstTouch).count());
    println!("║  cpu_scheduling (sched_wait>5ms):      {:>3}                ║",
        results.iter().filter(|r| r.cause == Cause::CpuScheduling).count());
    println!("║  io_stall (majflt>0):                  {:>3}                ║",
        results.iter().filter(|r| r.cause == Cause::IoStall).count());
    println!("║  normal:                               {:>3}                ║",
        results.iter().filter(|r| r.cause == Cause::Normal).count());
    println!("╠══════════════════════════════════════════════════════════════╣");
    println!("║  tokio-console shows:  wakeup_probe + manager (ep_poll)    ║");
    println!("║  tokio-console BLIND:  solBnkTxSched, solCoWorker00..03    ║");
    println!("║  OS worker scheduling: see sched_wait_ns column above      ║");
    if let Some(r0) = results.first() {
        for w in &r0.wchan_sample {
            println!("║  cold wchan: {:<50} ║", w);
        }
    }
    println!("╚══════════════════════════════════════════════════════════════╝\n");

    // ── 6. Cleanup ────────────────────────────────────────────────────────

    probe.abort();
    drop(non_vote_sender);
    drop(tpu_vote_sender);
    drop(gossip_vote_sender);
    drop(entry_receiver);
    exit.store(true, Ordering::Relaxed);

    tokio::task::spawn_blocking(move || {
        banking_stage.join().unwrap();
        poh_service.join().unwrap();
    })
    .await
    .expect("cleanup");

    // ── 7. Assertions ─────────────────────────────────────────────────────

    assert_eq!(
        missed, 0,
        "{missed} round(s) exceeded Δ_timeout ({}ms).\n\
         Diagnosis:\n\
         • cpu_scheduling: check sched_wait_ns + /proc/self/task/<tid>/schedstat\n\
         • io_stall:       check majflt_delta (page from disk)\n\
         • first_touch:    expected on round 0 only — demand paging, not a bug\n\
         • tokio blind:    banking workers are OS threads, not in tokio-console",
        DELTA_TIMEOUT.as_millis()
    );

    for r in results.iter().skip(1) {
        assert!(
            r.committed && r.latency < DELTA_BLOCK,
            "round {}: {:.1}ms > Δ_block {}ms — fast-finalization impossible (cause: {})",
            r.idx, r.latency.as_secs_f64() * 1000.0,
            DELTA_BLOCK.as_millis(), r.cause.label()
        );
    }
}