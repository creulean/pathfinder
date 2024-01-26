use std::{collections::HashMap, num::NonZeroU32, time::Instant};

use anyhow::Context;
use pathfinder_common::{
    BlockNumber, ClassHash, ContractAddress, ContractNonce, ContractStateHash, StorageAddress,
    StorageValue,
};
use pathfinder_merkle_tree::{ContractsStorageTree, StorageCommitmentTree};
use pathfinder_storage::{BlockId, JournalMode, Storage};

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .compact()
        .init();

    let database_path = std::env::args().nth(1).unwrap();
    let storage = Storage::migrate(database_path.into(), JournalMode::WAL, 1)?
        .create_pool(NonZeroU32::new(10).unwrap())?;
    let mut db = storage
        .connection()
        .context("Opening database connection")?;

    let latest_block = {
        let tx = db.transaction().unwrap();
        let (latest_block, _) = tx.block_id(BlockId::Latest)?.unwrap();
        latest_block.get()
    };
    let from: u64 = std::env::args()
        .nth(2)
        .map(|s| str::parse(&s).unwrap())
        .unwrap();
    let to: u64 = std::env::args()
        .nth(3)
        .map(|s| str::parse(&s).unwrap())
        .unwrap();
    assert!(from <= latest_block);
    assert!(from > to);
    let from = BlockNumber::new_or_panic(from);
    let to = BlockNumber::new_or_panic(to);

    tracing::info!(%from, %to, "Testing state rollback");

    let started = Instant::now();

    let tx = db.transaction()?;
    let storage_updates = tx.reverse_storage_updates(from, to)?;
    let nonce_updates = tx.reverse_nonce_updates(from, to)?;
    let contract_updates = tx.reverse_contract_updates(from, to)?;

    let mut updates: HashMap<ContractAddress, ContractUpdate> = Default::default();

    for (contract_address, nonce_update) in nonce_updates {
        updates.entry(contract_address).or_default().nonce_update = nonce_update;
    }
    for (contract_address, storage_updates) in storage_updates {
        updates.entry(contract_address).or_default().storage_updates = storage_updates;
    }
    for (contract_address, class_hash_update) in contract_updates {
        updates
            .entry(contract_address)
            .or_default()
            .class_hash_update = class_hash_update.map_or(ClassUpdate::Deleted, |class_hash| {
            ClassUpdate::Reverted(class_hash)
        });
    }

    let updates_fetched = Instant::now();

    tracing::info!(
        num_updated_contracts = %updates.len(),
        "Applying reverse updates"
    );

    let old_global_tree = StorageCommitmentTree::load(&tx, to)
        .context("Loading old global storage tree for verification")?;
    let mut global_tree =
        StorageCommitmentTree::load(&tx, from).context("Loading global storage tree")?;

    for (contract_address, contract_update) in updates {
        let class_hash = match contract_update.class_hash_update {
            ClassUpdate::Deleted => {
                tracing::debug!(%contract_address, "Contract has been deleted");
                global_tree
                    .set(contract_address, ContractStateHash::ZERO)
                    .context("Removing contract from global state tree")?;
                continue;
            }
            ClassUpdate::Reverted(class_hash) => class_hash,
            ClassUpdate::None => {
                if contract_address == ContractAddress::ONE {
                    // system contracts have no class hash
                    ClassHash::ZERO
                } else {
                    tx.contract_class_hash(to.into(), contract_address)?
                        .unwrap()
                }
            }
        };

        let mut tree = ContractsStorageTree::load(&tx, contract_address, from)
            .context("Loading contract state")?;
        for (address, value) in contract_update.storage_updates {
            tree.set(address, value.unwrap_or(StorageValue::ZERO))
                .context("Updating contract state")?;
        }
        let (root, _, _) = tree.commit().context("Committing contract state")?;

        let nonce = match contract_update.nonce_update {
            Some(nonce) => nonce,
            None => tx
                .contract_nonce(contract_address, to.into())
                .context("Getting contract nonce")?
                .unwrap_or_default(),
        };

        let state_hash = pathfinder_merkle_tree::contract_state::calculate_contract_state_hash(
            class_hash, root, nonce,
        );

        // let expected_state_hash = old_global_tree
        //     .get(&contract_address)
        //     .context("Getting contract state")?
        //     .expect("Contract state should exist at this point");

        // tracing::debug!(%state_hash, %expected_state_hash, %contract_address, "Calculated contract state hash");
        // assert_eq!(state_hash, expected_state_hash);

        tracing::debug!(%contract_address, "Contract state rolled back");

        global_tree
            .set(contract_address, state_hash)
            .context("Updating global state tree")?;
    }

    let applied = Instant::now();
    tracing::info!("Applied reverse updates, committing global state tree");

    let (global_root, _, _) = global_tree
        .commit()
        .context("Committing global state tree")?;

    let (old_global_root, _, _) = old_global_tree
        .commit()
        .context("Committing old global state tree")?;

    assert_eq!(global_root, old_global_root);

    let fetching = updates_fetched - started;
    let applying = applied - updates_fetched;
    let committing = applied.elapsed();

    tracing::info!(
        from=%from,
        to=%to,
        ?fetching,
        ?applying,
        ?committing,
        total = ?started.elapsed(),
        "Finished state rollback"
    );

    Ok(())
}

#[derive(Default, Debug)]
struct ContractUpdate {
    storage_updates: Vec<(StorageAddress, Option<StorageValue>)>,
    nonce_update: Option<ContractNonce>,
    class_hash_update: ClassUpdate,
}

#[derive(Default, Debug)]
enum ClassUpdate {
    Reverted(ClassHash),
    Deleted,
    #[default]
    None,
}
