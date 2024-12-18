use crate::{
    args::MineArgs,
    send_and_confirm::ComputeBudget,
    utils::{
        amount_u64_to_string, get_clock, get_config, get_updated_proof_with_authority, proof_pubkey,
    },
    Miner,
};
use colored::*;
use drillx::{
    equix::{self},
    Hash, Solution,
};
use ore_api::{
    consts::{BUS_ADDRESSES, BUS_COUNT, EPOCH_DURATION},
    state::{Bus, Config, Proof},
};
use ore_utils::AccountDeserialize;
use rand::Rng;
use rayon::prelude::*;
use solana_program::pubkey::Pubkey;
use solana_rpc_client::spinner;
use solana_sdk::signer::Signer;
use std::{collections::HashMap, sync::Arc, sync::RwLock, time::Instant};

impl Miner {
    pub async fn mine(&self, args: MineArgs) {
        // Open account, if needed.
        let signer = self.signer();
        self.open().await;

        // Check num threads
        self.check_num_cores(args.cores);

        // Start mining loop
        let mut last_hash_at = 0;

        let mut difficulty_stats: HashMap<u32, u32> = HashMap::new();
        let mut highest_difficulty = 0;
        loop {
            // Fetch proof
            let rpc_client_clone = Arc::clone(&self.rpc_client);
            let config = tokio::spawn(async move { get_config(&rpc_client_clone).await })
                .await
                .unwrap();
            let proof =
                get_updated_proof_with_authority(&self.rpc_client, signer.pubkey(), last_hash_at)
                    .await;

            let total_attempts: u32 = difficulty_stats.values().sum();

            for (diff, count) in &difficulty_stats {
                println!("| {:<20} | {:<10} |", diff, count);
            }
            println!("-----------------------------------------------");
            println!(
                "| {:<20} | {:>20} |\n\
                 | {:<20} | {:>20} |\n\
                 | {:<20} | {:>20} |",
                "Highest Difficulty",
                highest_difficulty,
                "Total Attempts",
                total_attempts,
                "Stake (ORE)",
                amount_u64_to_string(proof.balance),
            );
            println!("-----------------------------------------------");

            last_hash_at = proof.last_hash_at;

            // Calculate cutoff time
            let cutoff_time = self.get_cutoff(proof, args.buffer_time).await;

            // Run drillx
            let solution = Self::find_hash_par(
                proof,
                cutoff_time,
                args.cores,
                config.min_difficulty as u32,
                &mut difficulty_stats,
                &mut highest_difficulty,
            )
            .await;

            // Build instruction set
            let mut ixs = vec![ore_api::instruction::auth(proof_pubkey(signer.pubkey()))];
            let mut compute_budget = 500_000;
            if self.should_reset(config).await && rand::thread_rng().gen_range(0..100).eq(&0) {
                compute_budget += 100_000;
                ixs.push(ore_api::instruction::reset(signer.pubkey()));
            }

            // Build mine ix
            ixs.push(ore_api::instruction::mine(
                signer.pubkey(),
                signer.pubkey(),
                self.find_bus().await,
                solution,
            ));

            // Submit transaction
            self.send_and_confirm(&ixs, ComputeBudget::Fixed(compute_budget), false)
                .await
                .ok();
        }
    }

    async fn find_hash_par(
        proof: Proof,
        cutoff_time: u64,
        cores: u64,
        min_difficulty: u32,
        difficulty_stats: &mut HashMap<u32, u32>,
        highest_difficulty: &mut u32,
    ) -> Solution {
        // Dispatch job to each thread
        let progress_bar = Arc::new(spinner::new_progress_bar());
        let global_best_difficulty = Arc::new(RwLock::new(0u32));
        progress_bar.set_message("Mining...");
        let core_ids = core_affinity::get_core_ids().unwrap();

        let rt = tokio::runtime::Handle::current();
        let handles: Vec<_> = core_ids
            .into_par_iter()
            .map(|i| {
                let global_best_difficulty = Arc::clone(&global_best_difficulty);
                rt.spawn_blocking({
                    let proof = proof.clone();
                    let progress_bar = progress_bar.clone();
                    let mut memory = equix::SolverMemory::new();
                    move || {
                        // Return if core should not be used
                        if (i.id as u64).ge(&cores) {
                            return (0, 0, Hash::default());
                        }

                        // Pin to core
                        let _ = core_affinity::set_for_current(i);

                        // Start hashing
                        let timer = Instant::now();
                        let mut nonce = u64::MAX.saturating_div(cores).saturating_mul(i.id as u64);
                        let mut best_nonce = nonce;
                        let mut best_difficulty = 0;
                        let mut best_hash = Hash::default();
                        loop {
                            // Create hash
                            if let Ok(hx_array) = drillx::hash_with_memory(
                                &mut memory,
                                &proof.challenge,
                                &nonce.to_le_bytes(),
                            ) {
                                for hx in hx_array.into_iter() {
                                    let difficulty = hx.difficulty();
                                    if difficulty.gt(&best_difficulty) {
                                        best_nonce = nonce;
                                        best_difficulty = difficulty;
                                        best_hash = hx;
                                        // {{ edit_1 }}
                                        if best_difficulty
                                            .gt(&*global_best_difficulty.read().unwrap())
                                        {
                                            *global_best_difficulty.write().unwrap() =
                                                best_difficulty;
                                        }
                                        // {{ edit_1 }}
                                    }
                                }
                            }

                            // Exit if time has elapsed
                            if nonce % 100 == 0 {
                                let global_best_difficulty =
                                    *global_best_difficulty.read().unwrap();
                                if timer.elapsed().as_secs().ge(&cutoff_time) {
                                    if i.id == 0 {
                                        progress_bar.set_message(format!(
                                            "Mining... (difficulty {})",
                                            global_best_difficulty,
                                        ));
                                    }
                                    if global_best_difficulty.ge(&min_difficulty) {
                                        // Mine until min difficulty has been met
                                        break;
                                    }
                                } else if i.id == 0 {
                                    progress_bar.set_message(format!(
                                        "Mining... (difficulty {}, time {})",
                                        global_best_difficulty,
                                        format_duration(
                                            cutoff_time.saturating_sub(timer.elapsed().as_secs())
                                                as u32
                                        ),
                                    ));
                                }
                            }

                            // Increment nonce
                            nonce += 1;
                        }

                        // Return the best nonce
                        (best_nonce, best_difficulty, best_hash)
                    }
                })
            })
            .collect();

        // Join handles and return best nonce
        let joined = futures::future::join_all(handles).await;

        // 合并所有线程的结果，找到全局最佳结果
        let (best_nonce, best_difficulty, best_hash) = joined.into_iter().fold(
            (0, 0, Hash::default()),
            |(best_nonce, best_difficulty, best_hash), h| {
                if let Ok((nonce, difficulty, hash)) = h {
                    if difficulty > best_difficulty {
                        (nonce, difficulty, hash)
                    } else {
                        (best_nonce, best_difficulty, best_hash)
                    }
                } else {
                    (best_nonce, best_difficulty, best_hash)
                }
            },
        );

        *difficulty_stats.entry(best_difficulty).or_insert(0) += 1;
        if best_difficulty > *highest_difficulty {
            *highest_difficulty = best_difficulty;
        }

        // Update log
        println!(
            "Best hash: {} (difficulty {})",
            bs58::encode(best_hash.h).into_string(),
            best_difficulty
        );

        Solution::new(best_hash.d, best_nonce.to_le_bytes())
    }

    pub fn check_num_cores(&self, cores: u64) {
        let num_cores = num_cpus::get() as u64;
        if cores.gt(&num_cores) {
            println!(
                "{} Cannot exceeds available cores ({})",
                "WARNING".bold().yellow(),
                num_cores
            );
        }
    }

    async fn should_reset(&self, config: Config) -> bool {
        let clock = get_clock(&self.rpc_client).await;
        config
            .last_reset_at
            .saturating_add(EPOCH_DURATION)
            .saturating_sub(5) // Buffer
            .le(&clock.unix_timestamp)
    }

    async fn get_cutoff(&self, proof: Proof, buffer_time: u64) -> u64 {
        let clock = get_clock(&self.rpc_client).await;
        proof
            .last_hash_at
            .saturating_add(60)
            .saturating_sub(buffer_time as i64)
            .saturating_sub(clock.unix_timestamp)
            .max(0) as u64
    }

    async fn find_bus(&self) -> Pubkey {
        // Fetch the bus with the largest balance
        if let Ok(accounts) = self.rpc_client.get_multiple_accounts(&BUS_ADDRESSES).await {
            let mut top_bus_balance: u64 = 0;
            let mut top_bus = BUS_ADDRESSES[0];
            for account in accounts {
                if let Some(account) = account {
                    if let Ok(bus) = Bus::try_from_bytes(&account.data) {
                        if bus.rewards.gt(&top_bus_balance) {
                            top_bus_balance = bus.rewards;
                            top_bus = BUS_ADDRESSES[bus.id as usize];
                        }
                    }
                }
            }
            return top_bus;
        }

        // Otherwise return a random bus
        let i = rand::thread_rng().gen_range(0..BUS_COUNT);
        BUS_ADDRESSES[i]
    }
}

fn format_duration(seconds: u32) -> String {
    let minutes = seconds / 60;
    let remaining_seconds = seconds % 60;
    format!("{:02}:{:02}", minutes, remaining_seconds)
}
