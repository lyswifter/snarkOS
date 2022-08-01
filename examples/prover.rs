use snarkvm::dpc::{prelude::*, testnet2::Testnet2};

use rand::thread_rng;
use tokio::runtime;

use std::time::{Duration, Instant, SystemTime};

use std::sync::atomic::AtomicBool;

use std::task::Context;

pub struct Pprovers<N: Network> {
    pub address: Address<N>,
}

impl<N: Network> Pprovers<N> {
    pub async fn new() -> Pprovers<N> {
        let rng = &mut thread_rng();
        let private_key = PrivateKey::new(rng);
        let expected_address: Address<N> = private_key.into();

        let p = Pprovers { address: expected_address };
        p
    }

    pub async fn mine() {
        // let genesis_block = N::genesis_block();

        // let recipient = address;
        // let previous_ledger_root = genesis_block.previous_ledger_root();
        // let previous_block_hash = genesis_block.hash();
        // let block_height = genesis_block.height().saturating_add(1);

        // let block_timestamp = std::cmp::max(
        //     OffsetDateTime::now_utc().unix_timestamp(),
        //     genesis_block.timestamp().saturating_add(1),
        // );

        // let difficulty_target = Blocks::<N>::compute_difficulty_target(N::genesis_block().header(), block_timestamp, block_height);
        // let cumulative_weight = genesis_block
        //     .cumulative_weight()
        //     .saturating_add((u64::MAX / difficulty_target) as u128);

        // let mut coinbase_reward = Block::<N>::block_reward(block_height);
        // let mut transaction_fees = AleoAmount::ZERO;

        // // let mut transactions: Transaction<N>  =

        // // Craft a coinbase transaction, and append it to the list of transactions.
        // let (coinbase_transaction, coinbase_record) = Transaction::<N>::new_coinbase(recipient, coinbase_reward, false, &mut thread_rng()).unwrap();
        // // transactions.push(coinbase_transaction);

        // let block_templete = BlockTemplate::<N>::new(
        //     previous_block_hash,
        //     block_height,
        //     block_timestamp,
        //     difficulty_target,
        //     cumulative_weight,
        //     previous_ledger_root,
        //     coinbase_transaction,
        //     coinbase_record,
        // );

        let block = N::genesis_block();
        let expected_template = BlockTemplate::new(
            block.previous_block_hash(),
            block.height(),
            block.timestamp(),
            block.difficulty_target(),
            block.cumulative_weight(),
            block.previous_ledger_root(),
            block.transactions().clone(),
            block.to_coinbase_transaction().unwrap().to_records().next().unwrap(),
        );

        println!("{:?}", expected_template);

        let posw = N::posw();

        let atomic_false = AtomicBool::new(false);

        let ret = posw.mine(&expected_template, &atomic_false, &mut thread_rng());

        match ret {
            Ok(block) => {
                println!("mine new block at: {}", block.height());
            }
            Err(e) => {
                println!("e {}", e);
            }
        }
    }
}

async fn process() {
    let block = Testnet2::genesis_block();
    let expected_template = BlockTemplate::new(
        block.previous_block_hash(),
        block.height(),
        block.timestamp(),
        block.difficulty_target(),
        block.cumulative_weight(),
        block.previous_ledger_root(),
        block.transactions().clone(),
        block.to_coinbase_transaction().unwrap().to_records().next().unwrap(),
    );

    let posw = Testnet2::posw();
    let atomic_false = AtomicBool::new(false);
    let ret = posw.mine(&expected_template, &atomic_false, &mut thread_rng());
    match ret {
        Ok(block) => {
            println!("mine new block at: {}", block.height());
        }
        Err(e) => {
            println!("e {}", e);
        }
    }
}

pub fn main() {
    let (num_tokio_worker_threads, max_tokio_blocking_threads) = (num_cpus::get(), num_cpus::get());

    // Initialize the runtime configuration.
    let runtime = runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(8 * 1024 * 1024)
        .worker_threads(num_tokio_worker_threads)
        .max_blocking_threads(max_tokio_blocking_threads)
        .build()
        .unwrap();

    // let private_key = PrivateKey::from_str("APrivateKey1zkp8GeX2ezFYgsDyfNnTmiQAiAd8c3gxULnYGJKS8PRFDMa").unwrap();
    // let caller = Address::try_from(private_key)?;

    // let private_key = PrivateKey::new(rng);
    // let expected_address: Address<> = private_key.into();

    let now = Instant::now();
    let secs = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
    println!("timing - {:?}s", secs);

    runtime.block_on(async move {
        // for n in 1..100 {
        //     println!("loop {}", n);
        //     let private_key = PrivateKey::from_str("APrivateKey1zkp8GeX2ezFYgsDyfNnTmiQAiAd8c3gxULnYGJKS8PRFDMa").unwrap();
        //     let _genesis_block = generate::<Testnet3>(private_key).unwrap();
        // }

        for _ in 1..10 {
            tokio::spawn(async move {
                process().await;
            });
        }
    });

    println!("time to elapsed: {}", now.elapsed().as_secs());
}

// pub fn main() {
//     let (num_tokio_worker_threads, max_tokio_blocking_threads) = (num_cpus::get(), num_cpus::get());

//     // Initialize the runtime configuration.
//     let runtime = runtime::Builder::new_multi_thread()
//         .enable_all()
//         .thread_stack_size(8 * 1024 * 1024)
//         .worker_threads(num_tokio_worker_threads)
//         .max_blocking_threads(max_tokio_blocking_threads)
//         .build()
//         .unwrap();

//     let now = Instant::now();

//     let mut handles = Vec::with_capacity(10);
//     for _ in 0..10 {
//         handles.push(runtime.spawn(process()));
//     }

//     for handle in handles {
//         // The `spawn` method returns a `JoinHandle`. A `JoinHandle` is
//         // a future, so we can wait for it using `block_on`.
//         runtime.block_on(handle).unwrap();
//     }

//     println!("time to elapsed: {}", now.elapsed().as_secs());
// }
