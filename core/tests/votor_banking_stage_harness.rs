//! Votor Timing Measurement Harness for Agave BankingStage
//!
//! # Votor timing (Alpenglow §2, Definition 17)
//!
//! | Symbol          | Value   | Notes                                      |
//! |-----------------|---------|---------------------------------------------|
//! | `delta_block`   | 400 ms  | Block-production budget. Banking is graded  |
//! |                 |         | against this: **what banking is judged on** |
//! | `delta`         | 400 ms  | Max one-way network delay bound (§2 param.) |
//! | `delta_timeout` | 1200 ms | = 3 × delta. skipVote fires after this.     |
//! | fast-final      | 400 ms  | ≥ 80 % stake, 1 round                       |
//! | backup-final    | 800 ms  | 60–80 % stake, 2 rounds                     |
//!
//! Correct framing (see review §3):
//! ```
//! BankingStage latency ⊂ block-production latency ⊂ Votor round latency
//! remaining_budget = delta_block − banking_latency
//! ```
//! The harness reports `banking_latency` and `remaining_budget`; it does NOT
//! claim that `banking_latency = finalization_latency`.
//!
