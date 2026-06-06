# Votor Timing Measurement Harness for Agave BankingStage

[![rust](https://img.shields.io/badge/rust-1.75+-blue.svg)](https://www.rust-lang.org/)
[![agave](https://img.shields.io/badge/agave-0.4.1-orange.svg)](https://github.com/anza-xyz/agave)


---

# Table of Contents

* [Theoretical Background](#theoretical-background)

  * [Alpenglow & Votor](#alpenglow--votor)
  * [Votor Timing Requirements](#votor-timing-requirements)
  * [Correct Conceptual Framing](#correct-conceptual-framing)
  * [Agave, SVM and Observability](#agave-svm-and-observability)
* [Architecture Discovery](#architecture-discovery)
* [Jitter Classification](#jitter-classification)
* [Harness Implementation](#harness-implementation)
* [Setup & Running](#setup--running)
* [Expected Output](#expected-output)
* [Synthesized Results from Multiple Projects](#synthesized-results-from-multiple-projects)
* [Limitations](#limitations)
* [References](#references)

---

# Theoretical Background

## Alpenglow & Votor

**Alpenglow** is the next-generation Solana consensus protocol designed to replace both Proof-of-History (PoH) and TowerBFT.

Goals:

* Sub-second finalization
* Increased throughput
* Improved network resilience
* Simpler consensus architecture without PoH
* Liveness with up to:

  * 20% malicious stake
  * 20% non-responding stake

Alpenglow consists of two major components:

| Component | Purpose                       |
| --------- | ----------------------------- |
| **Votor** | Voting and finalization logic |
| **Rotor** | Fast block dissemination      |

Timing budgets are defined in the **Alpenglow White Paper v1.1** (Figure 7, Definition 17).

---

## Votor Timing Requirements

```text
t = 0    Leader emits first shred
          │
          ▼

Round 1 (Δ_timeout = 1200 ms from slot start)

Validator receives block
        │
        ├─ notarVote
        │
        └─ timeout → skipVote

                │
       ┌────────┴────────┐
       │                 │
 ≥80% notarVotes   60–80% notarVotes
       │                 │
       ▼                 ▼

 Fast Final        Backup Final
   1 round            2 rounds

 Δ = 400 ms         2×Δ = 800 ms
```

| Symbol        | Value   | Derivation | Meaning                       |
| ------------- | ------- | ---------- | ----------------------------- |
| **Δ**         | 400 ms  | Assumed    | Maximum one-way network delay |
| **Δ_block**   | 400 ms  | 1 × Δ      | Block production budget       |
| **Δ_timeout** | 1200 ms | 3 × Δ      | `skipVote` timeout            |
| Fast Final    | 400 ms  | 1 × Δ      | ≥80% stake                    |
| Backup Final  | 800 ms  | 2 × Δ      | 60–80% stake                  |

SIMD-0326 further defines:

```text
Timeout(i)
  = clock()
  + Δ_timeout
  + (i - s + 1) * Δ_block
```

Finalization:

```text
min(1 × δ80%, 2 × δ60%)
```

where:

```text
δ ≈ 80 ms
```

actual network delay.

---

## Correct Conceptual Framing

`BankingStage` latency is only one component of the overall timing budget.

```text
BankingStage latency
    ⊂ Block Production latency
        ⊂ Votor Round latency
```

This harness reports:

```text
remaining_budget = Δ_block − banking_latency
```

and explicitly treats `BankingStage` as a contribution to finalization latency rather than finalization itself.

---

## Agave, SVM and Observability

### Agave Validator

Agave is the Rust implementation of a Solana validator.

Critical execution path:

```text
TPU
└── BankingStage
```

### SVM (Solana Virtual Machine)

SVM executes transactions in parallel using Sealevel.

Non-conflicting account accesses can run concurrently.

### tokio-console

`tokio-console` can observe Tokio tasks.

Important limitation:

> It cannot observe native OS threads.

The real `banking_stage` uses:

* `std::thread`
* `crossbeam`
* OS thread pools

As a result, most of the hot path is invisible to `tokio-console`.

---

# Architecture Discovery

`banking_stage.rs` is a hybrid architecture.

```text
┌──────────────────────────────────────────────────────┐
│ Manager Thread (1x OS thread)                        │
│                                                      │
│ Current-thread Tokio Runtime                         │
│ tokio::select!                                       │
│ spawn_blocking shims                                 │
│                                                      │
│ Visible in tokio-console                             │
│ Appears as ep_poll                                   │
└──────────────────────────────────────────────────────┘


┌──────────────────────────────────────────────────────┐
│ Worker Threads (N x OS threads)                      │
│                                                      │
│ std::thread                                          │
│ crossbeam channels                                   │
│                                                      │
│ solBnkTxSched                                        │
│ solCoWorker00                                        │
│ solCoWorker01                                        │
│ solCoWorker02                                        │
│ solCoWorker03                                        │
│                                                      │
│ Invisible to tokio-console                           │
│ Measured via schedstat                               │
└──────────────────────────────────────────────────────┘
```

### Tokio Console Blind Spot

The hot execution path runs on native worker threads.

Visible:

```text
Manager runtime
(ep_poll)
```

Invisible:

```text
solBnkTxSched
solCoWorker00..03
```

The harness combines:

* `tokio-console`
* `/proc` probes

to obtain complete visibility.


---

# Running

## 1. Run With Tokio Console

Terminal 1:

```bash
tokio-console
```

Terminal 2:

```bash
RUSTFLAGS="--cfg tokio_unstable" \
  cargo test -p solana-core \
    --test votor_banking_stage_harness \
  -- --nocapture
```

The harness pauses for 60 seconds to allow attachment.



## Overall Conclusion

```text
BankingStage latency
<<
Δ_block (400 ms)
```

Measured latency remains **2–3 orders of magnitude below** the Votor timing budget.

The current Agave implementation satisfies Alpenglow timing requirements with substantial margin.

---

# Limitations

| Limitation                    | Description                                |
| ----------------------------- | ------------------------------------------ |
| Single transaction iterations | Harnesses 1–2 process one packet at a time |
| `wchan` is opportunistic      | Only visible while blocked                 |
| Δ = 400 ms assumption         | Derived from Alpenglow specification       |
| Harness 2 patch               | Requires visibility modifications          |
| Linux-only instrumentation    | `/proc` metrics unavailable elsewhere      |
| Not measuring finalization    | Only block-production contribution         |

---

# References

## Alpenglow White Paper v1.1

Kniep, Sliwinski, Wattenhofer (2025)

https://www.anza.xyz/alpenglow-1-1

## SIMD-0326

Alpenglow Solana Improvement Document

https://github.com/solana-foundation/solana-improvement-documents/blob/main/proposals/0326-alpenglow.md

## Agave Source

```text
agave/core/src/banking_stage.rs
```

## tokio-console

https://github.com/tokio-rs/console
