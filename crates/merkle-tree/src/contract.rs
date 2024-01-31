//! Contains the [StorageCommitmentTree] and [ContractsStorageTree] trees, which combined
//! store the total Starknet storage state.
//!
//! These are abstractions built-on the [Binary Merkle-Patricia Tree](MerkleTree).

use crate::{
    merkle_node::InternalNode,
    tree::{MerkleTree, Visit},
};
use anyhow::Context;
use bitvec::{prelude::Msb0, slice::BitSlice};
use pathfinder_common::hash::PedersenHash;
use pathfinder_common::trie::TrieNode;
use pathfinder_common::{
    BlockNumber, ContractAddress, ContractRoot, ContractStateHash, StorageAddress,
    StorageCommitment, StorageValue,
};
use pathfinder_crypto::Felt;
use pathfinder_storage::{Node, Transaction};
use std::collections::HashMap;
use std::ops::ControlFlow;

/// A [Patricia Merkle tree](MerkleTree) used to calculate commitments to a Starknet contract's storage.
///
/// It maps a contract's [storage addresses](StorageAddress) to their [values](StorageValue).
///
/// Tree data is persisted by a sqlite table 'tree_contracts'.
pub struct ContractsStorageTree<'tx> {
    tree: MerkleTree<PedersenHash, 251>,
    storage: ContractStorage<'tx>,
}

impl<'tx> ContractsStorageTree<'tx> {
    pub fn empty(tx: &'tx Transaction<'tx>, contract: ContractAddress) -> Self {
        let storage = ContractStorage {
            tx,
            block: None,
            contract,
        };
        let tree = MerkleTree::empty();

        Self { tree, storage }
    }

    pub fn load(
        tx: &'tx Transaction<'tx>,
        contract: ContractAddress,
        block: BlockNumber,
    ) -> anyhow::Result<Self> {
        let root = tx
            .contract_root_index(block, contract)
            .context("Querying contract root index")?;
        let Some(root) = root else {
            return Ok(Self::empty(tx, contract));
        };

        let storage = ContractStorage {
            tx,
            block: Some(block),
            contract,
        };
        let tree = MerkleTree::new(root);

        Ok(Self { tree, storage })
    }

    pub fn with_verify_hashes(mut self, verify_hashes: bool) -> Self {
        self.tree = self.tree.with_verify_hashes(verify_hashes);
        self
    }

    /// Generates a proof for `key`. See [`MerkleTree::get_proof`].
    pub fn get_proof(
        tx: &'tx Transaction<'tx>,
        contract: ContractAddress,
        block: BlockNumber,
        key: &BitSlice<u8, Msb0>,
    ) -> anyhow::Result<Vec<TrieNode>> {
        let root = tx
            .contract_root_index(block, contract)
            .context("Querying contract root index")?;

        let Some(root) = root else {
            return Ok(Vec::new());
        };

        let storage = ContractStorage {
            tx,
            block: Some(block),
            contract,
        };

        MerkleTree::<PedersenHash, 251>::get_proof(root, &storage, key)
    }

    pub fn set(&mut self, address: StorageAddress, value: StorageValue) -> anyhow::Result<()> {
        let key = address.view_bits().to_owned();
        self.tree.set(&self.storage, key, value.0)
    }

    /// Commits the changes and calculates the new node hashes. Returns the new commitment and
    /// any potentially newly created nodes.
    pub fn commit(self) -> anyhow::Result<(ContractRoot, HashMap<Felt, Node>)> {
        let update = self.tree.commit(&self.storage)?;
        let commitment = ContractRoot(update.root);
        Ok((commitment, update.nodes))
    }

    /// See [`MerkleTree::dfs`]
    pub fn dfs<B, F: FnMut(&InternalNode, &BitSlice<u8, Msb0>) -> ControlFlow<B, Visit>>(
        &mut self,
        f: &mut F,
    ) -> anyhow::Result<Option<B>> {
        self.tree.dfs(&self.storage, f)
    }
}

/// A [Patricia Merkle tree](MerkleTree) used to calculate commitments to all of Starknet's storage.
///
/// It maps each contract's [address](ContractAddress) to it's [state hash](ContractStateHash).
///
/// Tree data is persisted by a sqlite table 'tree_global'.
pub struct StorageCommitmentTree<'tx> {
    tree: MerkleTree<PedersenHash, 251>,
    storage: StorageTrieStorage<'tx>,
}

impl<'tx> StorageCommitmentTree<'tx> {
    pub fn empty(tx: &'tx Transaction<'tx>) -> Self {
        let storage = StorageTrieStorage { tx, block: None };
        let tree = MerkleTree::empty();

        Self { tree, storage }
    }

    pub fn load(tx: &'tx Transaction<'tx>, block: BlockNumber) -> anyhow::Result<Self> {
        let root = tx
            .storage_root_index(block)
            .context("Querying storage root index")?;
        let Some(root) = root else {
            return Ok(Self::empty(tx));
        };

        let storage = StorageTrieStorage {
            tx,
            block: Some(block),
        };

        let tree = MerkleTree::new(root);

        Ok(Self { tree, storage })
    }

    pub fn with_verify_hashes(mut self, verify_hashes: bool) -> Self {
        self.tree = self.tree.with_verify_hashes(verify_hashes);
        self
    }

    pub fn set(
        &mut self,
        address: ContractAddress,
        value: ContractStateHash,
    ) -> anyhow::Result<()> {
        let key = address.view_bits().to_owned();
        self.tree.set(&self.storage, key, value.0)
    }

    pub fn get(&self, address: &ContractAddress) -> anyhow::Result<Option<ContractStateHash>> {
        let key = address.view_bits().to_owned();
        let value = self.tree.get(&self.storage, key)?;
        Ok(value.map(ContractStateHash))
    }

    /// Commits the changes and calculates the new node hashes. Returns the new commitment and
    /// any potentially newly created nodes.
    pub fn commit(self) -> anyhow::Result<(StorageCommitment, HashMap<Felt, Node>)> {
        let update = self.tree.commit(&self.storage)?;
        let commitment = StorageCommitment(update.root);
        Ok((commitment, update.nodes))
    }

    /// Generates a proof for the given `key`. See [`MerkleTree::get_proof`].
    pub fn get_proof(
        tx: &'tx Transaction<'tx>,
        block: BlockNumber,
        address: &ContractAddress,
    ) -> anyhow::Result<Vec<TrieNode>> {
        let root = tx
            .storage_root_index(block)
            .context("Querying storage root index")?;

        let Some(root) = root else {
            return Ok(Vec::new());
        };

        let storage = StorageTrieStorage {
            tx,
            block: Some(block),
        };

        MerkleTree::<PedersenHash, 251>::get_proof(root, &storage, address.view_bits())
    }

    /// See [`MerkleTree::dfs`]
    pub fn dfs<B, F: FnMut(&InternalNode, &BitSlice<u8, Msb0>) -> ControlFlow<B, Visit>>(
        &mut self,
        f: &mut F,
    ) -> anyhow::Result<Option<B>> {
        self.tree.dfs(&self.storage, f)
    }
}

struct ContractStorage<'tx> {
    tx: &'tx Transaction<'tx>,
    block: Option<BlockNumber>,
    contract: ContractAddress,
}

impl crate::storage::Storage for ContractStorage<'_> {
    fn get(&self, index: u64) -> anyhow::Result<Option<pathfinder_storage::StoredNode>> {
        self.tx.contract_trie_node(index)
    }

    fn hash(&self, index: u64) -> anyhow::Result<Option<Felt>> {
        self.tx.contract_trie_node_hash(index)
    }

    fn leaf(&self, path: &BitSlice<u8, Msb0>) -> anyhow::Result<Option<Felt>> {
        assert!(path.len() == 251);

        let Some(block) = self.block else {
            return Ok(None);
        };

        let key =
            StorageAddress(Felt::from_bits(path).context("Mapping leaf path to storage address")?);

        let value = self
            .tx
            .storage_value(block.into(), self.contract, key)?
            .map(|x| x.0);

        Ok(value)
    }
}

struct StorageTrieStorage<'tx> {
    tx: &'tx Transaction<'tx>,
    block: Option<BlockNumber>,
}

impl crate::storage::Storage for StorageTrieStorage<'_> {
    fn get(&self, index: u64) -> anyhow::Result<Option<pathfinder_storage::StoredNode>> {
        self.tx.storage_trie_node(index)
    }

    fn hash(&self, index: u64) -> anyhow::Result<Option<Felt>> {
        self.tx.storage_trie_node_hash(index)
    }

    fn leaf(&self, path: &BitSlice<u8, Msb0>) -> anyhow::Result<Option<Felt>> {
        assert!(path.len() == 251);

        let Some(block) = self.block else {
            return Ok(None);
        };

        let contract = ContractAddress(
            Felt::from_bits(path).context("Mapping leaf path to contract address")?,
        );

        let value = self.tx.contract_state_hash(block, contract)?.map(|x| x.0);

        Ok(value)
    }
}
