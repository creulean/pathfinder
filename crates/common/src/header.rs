use crate::prelude::*;
use crate::BlockCommitmentSignature;
use fake::Dummy;

#[derive(Debug, Clone, PartialEq, Eq, Default, Dummy)]
pub struct BlockHeader {
    pub hash: BlockHash,
    pub parent_hash: BlockHash,
    pub number: BlockNumber,
    pub timestamp: BlockTimestamp,
    pub eth_l1_gas_price: GasPrice,
    pub strk_l1_gas_price: GasPrice,
    pub sequencer_address: SequencerAddress,
    pub starknet_version: StarknetVersion,
    pub class_commitment: ClassCommitment,
    pub event_commitment: EventCommitment,
    pub state_commitment: StateCommitment,
    pub storage_commitment: StorageCommitment,
    pub transaction_commitment: TransactionCommitment,
    pub transaction_count: usize,
    pub event_count: usize,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct SignedBlockHeader {
    pub header: BlockHeader,
    pub signature: BlockCommitmentSignature,
}

impl SignedBlockHeader {
    /// Returns true if the signature is correct for the block header.
    ///
    /// Note that this does not imply that a given state diff is correct.
    /// TODO: improve this documentation somehow.
    pub fn verify_signature(&self) -> bool {
        // TODO: implement this.
        true
    }
}

pub struct BlockHeaderBuilder(BlockHeader);

impl BlockHeader {
    /// Creates a [builder](BlockHeaderBuilder) with all fields initialized to default values.
    pub fn builder() -> BlockHeaderBuilder {
        BlockHeaderBuilder(BlockHeader::default())
    }

    /// Creates a [builder](BlockHeaderBuilder) with an incremented block number and parent hash set to this
    /// block's hash.
    pub fn child_builder(&self) -> BlockHeaderBuilder {
        BlockHeaderBuilder(BlockHeader::default())
            .with_number(self.number + 1)
            .with_parent_hash(self.hash)
    }

    /// Creates a [StateUpdate] with the block hash and state commitment fields initialized
    /// to match this header.
    pub fn init_state_update(&self) -> StateUpdate {
        StateUpdate::default()
            .with_block_hash(self.hash)
            .with_state_commitment(self.state_commitment)
    }

    pub fn verify_hash(&self) -> bool {
        todo!();
    }
}

impl BlockHeaderBuilder {
    pub fn with_number(mut self, number: BlockNumber) -> Self {
        self.0.number = number;
        self
    }

    pub fn with_parent_hash(mut self, parent_hash: BlockHash) -> Self {
        self.0.parent_hash = parent_hash;
        self
    }

    pub fn with_state_commitment(mut self, state_commmitment: StateCommitment) -> Self {
        self.0.state_commitment = state_commmitment;
        self
    }

    /// Sets the [StateCommitment] by calculating its value from the current [StorageCommitment] and [ClassCommitment].
    pub fn with_calculated_state_commitment(mut self) -> Self {
        self.0.state_commitment =
            StateCommitment::calculate(self.0.storage_commitment, self.0.class_commitment);
        self
    }

    pub fn with_timestamp(mut self, timestamp: BlockTimestamp) -> Self {
        self.0.timestamp = timestamp;
        self
    }

    pub fn with_eth_l1_gas_price(mut self, eth_l1_gas_price: GasPrice) -> Self {
        self.0.eth_l1_gas_price = eth_l1_gas_price;
        self
    }

    pub fn with_strk_l1_gas_price(mut self, strk_l1_gas_price: GasPrice) -> Self {
        self.0.strk_l1_gas_price = strk_l1_gas_price;
        self
    }

    pub fn with_sequencer_address(mut self, sequencer_address: SequencerAddress) -> Self {
        self.0.sequencer_address = sequencer_address;
        self
    }

    pub fn with_transaction_commitment(
        mut self,
        transaction_commitment: TransactionCommitment,
    ) -> Self {
        self.0.transaction_commitment = transaction_commitment;
        self
    }

    pub fn with_event_commitment(mut self, event_commitment: EventCommitment) -> Self {
        self.0.event_commitment = event_commitment;
        self
    }

    pub fn with_storage_commitment(mut self, storage_commitment: StorageCommitment) -> Self {
        self.0.storage_commitment = storage_commitment;
        self
    }

    pub fn with_class_commitment(mut self, class_commitment: ClassCommitment) -> Self {
        self.0.class_commitment = class_commitment;
        self
    }

    pub fn with_starknet_version(mut self, starknet_version: StarknetVersion) -> Self {
        self.0.starknet_version = starknet_version;
        self
    }

    pub fn with_transaction_count(mut self, transaction_count: usize) -> Self {
        self.0.transaction_count = transaction_count;
        self
    }

    pub fn with_event_count(mut self, event_count: usize) -> Self {
        self.0.event_count = event_count;
        self
    }

    pub fn finalize_with_hash(mut self, hash: BlockHash) -> BlockHeader {
        self.0.hash = hash;
        self.0
    }
}
