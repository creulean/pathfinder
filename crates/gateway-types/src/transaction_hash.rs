//! Calculate transaction hashes.

use crate::reply::transaction::{
    DeclareTransaction, DeclareTransactionV0V1, DeclareTransactionV2, DeclareTransactionV3,
    DeployAccountTransaction, DeployAccountTransactionV0V1, DeployAccountTransactionV3,
    DeployTransaction, InvokeTransaction, InvokeTransactionV0, InvokeTransactionV1,
    InvokeTransactionV3, L1HandlerTransaction, Transaction,
};
use pathfinder_common::{
    transaction::{DataAvailabilityMode, ResourceBound, ResourceBounds},
    CasmHash, ClassHash, ContractAddress, EntryPoint, Fee, PaymasterDataElem, Tip, TransactionHash,
    TransactionNonce, TransactionVersion,
};

use crate::class_hash::truncated_keccak;
use pathfinder_common::ChainId;
use pathfinder_crypto::{
    hash::{HashChain, PoseidonHasher},
    Felt,
};
use sha3::{Digest, Keccak256};

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum VerifyResult {
    Match,
    Mismatch(TransactionHash),
}

pub fn verify(txn: &Transaction, chain_id: ChainId) -> VerifyResult {
    let computed_hash = compute_transaction_hash(txn, chain_id);

    if computed_hash == txn.hash() {
        VerifyResult::Match
    } else {
        VerifyResult::Mismatch(computed_hash)
    }
}

/// Computes transaction hash according to the formulas from [starknet docs](https://docs.starknet.io/documentation/architecture_and_concepts/Blocks/transactions/).
///
/// ## Important
///
/// For __Invoke v0__, __Deploy__ and __L1 Handler__ there is a fallback hash calculation
/// algorithm used in case a hash mismatch is encountered and the fallback's result becomes
/// the ultimate result of the computation.
pub fn compute_transaction_hash(txn: &Transaction, chain_id: ChainId) -> TransactionHash {
    match txn {
        Transaction::Declare(DeclareTransaction::V0(txn)) => compute_declare_v0_hash(txn, chain_id),
        Transaction::Declare(DeclareTransaction::V1(txn)) => compute_declare_v1_hash(txn, chain_id),
        Transaction::Declare(DeclareTransaction::V2(txn)) => compute_declare_v2_hash(txn, chain_id),
        Transaction::Declare(DeclareTransaction::V3(txn)) => compute_declare_v3_hash(txn, chain_id),
        Transaction::Deploy(txn) => compute_deploy_hash(txn, chain_id),
        Transaction::DeployAccount(DeployAccountTransaction::V0V1(txn)) => {
            compute_deploy_account_v0v1_hash(txn, chain_id)
        }
        Transaction::DeployAccount(DeployAccountTransaction::V3(txn)) => {
            compute_deploy_account_v3_hash(txn, chain_id)
        }
        Transaction::Invoke(InvokeTransaction::V0(txn)) => compute_invoke_v0_hash(txn, chain_id),
        Transaction::Invoke(InvokeTransaction::V1(txn)) => compute_invoke_v1_hash(txn, chain_id),
        Transaction::Invoke(InvokeTransaction::V3(txn)) => compute_invoke_v3_hash(txn, chain_id),
        Transaction::L1Handler(txn) => compute_l1_handler_hash(txn, chain_id),
    }
}

/// Computes declare v0 transaction hash based on [this formula](https://docs.starknet.io/documentation/architecture_and_concepts/Blocks/transactions/#v0_hash_calculation_2):
/// ```text=
/// declare_v0_tx_hash = h("declare", version, sender_address,
///     0, h([]), max_fee, chain_id, class_hash)
/// ```
///
/// FIXME: SW should fix the formula in the docs
///
/// Where `h` is [Pedersen hash](https://docs.starknet.io/documentation/architecture_and_concepts/Hashing/hash-functions/#pedersen_hash)
fn compute_declare_v0_hash(txn: &DeclareTransactionV0V1, chain_id: ChainId) -> TransactionHash {
    compute_txn_hash(
        b"declare",
        TransactionVersion::ZERO,
        txn.sender_address,
        None,
        HashChain::default().finalize(), // Hash of an empty Felt list
        None,
        chain_id,
        txn.class_hash,
        None,
    )
}

/// Computes declare v1 transaction hash based on [this formula](https://docs.starknet.io/documentation/architecture_and_concepts/Blocks/transactions/#v1_hash_calculation_2):
/// ```text=
/// declare_v1_tx_hash = h("declare", version, sender_address,
///     0, h([class_hash]), max_fee, chain_id, nonce)
/// ```
///
/// FIXME: SW should fix the formula in the docs
///
/// Where `h` is [Pedersen hash](https://docs.starknet.io/documentation/architecture_and_concepts/Hashing/hash-functions/#pedersen_hash)
fn compute_declare_v1_hash(txn: &DeclareTransactionV0V1, chain_id: ChainId) -> TransactionHash {
    compute_txn_hash(
        b"declare",
        TransactionVersion::ONE,
        txn.sender_address,
        None,
        {
            let mut h = HashChain::default();
            h.update(txn.class_hash.0);
            h.finalize()
        },
        Some(txn.max_fee),
        chain_id,
        txn.nonce,
        None,
    )
}

/// Computes declare v2 transaction hash based on [this formula](https://docs.starknet.io/documentation/architecture_and_concepts/Blocks/transactions/#v2_hash_calculation):
/// ```text=
/// declare_v2_tx_hash = h("declare", version, sender_address,
///     0, h([class_hash]), max_fee, chain_id, nonce, compiled_class_hash)
/// ```
///
/// FIXME: SW should fix the formula in the docs
///
/// Where `h` is [Pedersen hash](https://docs.starknet.io/documentation/architecture_and_concepts/Hashing/hash-functions/#pedersen_hash)
fn compute_declare_v2_hash(txn: &DeclareTransactionV2, chain_id: ChainId) -> TransactionHash {
    compute_txn_hash(
        b"declare",
        TransactionVersion::TWO,
        txn.sender_address,
        None,
        {
            let mut h = HashChain::default();
            h.update(txn.class_hash.0);
            h.finalize()
        },
        Some(txn.max_fee),
        chain_id,
        txn.nonce,
        Some(txn.compiled_class_hash),
    )
}

fn compute_declare_v3_hash(txn: &DeclareTransactionV3, chain_id: ChainId) -> TransactionHash {
    let declare_specific_data = [
        txn.account_deployment_data
            .iter()
            .fold(PoseidonHasher::new(), |mut hh, e| {
                hh.write(e.0.into());
                hh
            })
            .finish()
            .into(),
        txn.class_hash.0,
        txn.compiled_class_hash.0,
    ];
    compute_v3_txn_hash(
        b"declare",
        TransactionVersion::THREE,
        txn.sender_address,
        chain_id,
        txn.nonce,
        &declare_specific_data,
        txn.tip,
        &txn.paymaster_data,
        txn.nonce_data_availability_mode.into(),
        txn.fee_data_availability_mode.into(),
        txn.resource_bounds.into(),
    )
}

/// Computes deploy transaction hash based on [this formula](https://docs.starknet.io/documentation/architecture_and_concepts/Blocks/transactions/#deploy_transaction):
/// ```text=
/// deploy_tx_hash = h(
///     "deploy", version, contract_address, sn_keccak("constructor"),
///     h(constructor_calldata), 0, chain_id)
/// ```
///
/// Where `h` is [Pedersen hash](https://docs.starknet.io/documentation/architecture_and_concepts/Hashing/hash-functions/#pedersen_hash), and `sn_keccak` is [Starknet Keccak](https://docs.starknet.io/documentation/architecture_and_concepts/Hashing/hash-functions/#Starknet-keccak)
fn compute_deploy_hash(txn: &DeployTransaction, chain_id: ChainId) -> TransactionHash {
    lazy_static::lazy_static!(
        static ref CONSTRUCTOR: EntryPoint = {
            let mut keccak = Keccak256::default();
            keccak.update(b"constructor");
            EntryPoint(truncated_keccak(<[u8; 32]>::from(keccak.finalize())))};
    );

    let constructor_params_hash = {
        let hh = txn.constructor_calldata.iter().fold(
            HashChain::default(),
            |mut hh, constructor_param| {
                hh.update(constructor_param.0);
                hh
            },
        );
        hh.finalize()
    };

    let h = compute_txn_hash(
        b"deploy",
        txn.version,
        txn.contract_address,
        Some(*CONSTRUCTOR),
        constructor_params_hash,
        None,
        chain_id,
        (),
        None,
    );

    if h == txn.transaction_hash {
        h
    } else {
        legacy_compute_txn_hash(
            b"deploy",
            txn.contract_address,
            Some(*CONSTRUCTOR),
            constructor_params_hash,
            chain_id,
            None,
        )
    }
}

/// Computes deploy account transaction hash based on [this formula](https://docs.starknet.io/documentation/architecture_and_concepts/Blocks/transactions/#deploy_account_hash_calculation):
/// ```text=
/// deploy_account_tx_hash = h(
///     "deploy_account", version, contract_address, 0,
///     h(class_hash, contract_address_salt, constructor_calldata),
///     max_fee, chain_id, nonce)
/// ```
///
/// FIXME: SW should fix the formula in the docs
///
/// Where `h` is [Pedersen hash](https://docs.starknet.io/documentation/architecture_and_concepts/Hashing/hash-functions/#pedersen_hash)
fn compute_deploy_account_v0v1_hash(
    txn: &DeployAccountTransactionV0V1,
    chain_id: ChainId,
) -> TransactionHash {
    compute_txn_hash(
        b"deploy_account",
        txn.version,
        txn.contract_address,
        None,
        {
            let mut hh = HashChain::default();
            hh.update(txn.class_hash.0);
            hh.update(txn.contract_address_salt.0);
            hh = txn
                .constructor_calldata
                .iter()
                .fold(hh, |mut hh, constructor_param| {
                    hh.update(constructor_param.0);
                    hh
                });
            hh.finalize()
        },
        Some(txn.max_fee),
        chain_id,
        txn.nonce,
        None,
    )
}

fn compute_deploy_account_v3_hash(
    txn: &DeployAccountTransactionV3,
    chain_id: ChainId,
) -> TransactionHash {
    let deploy_account_specific_data = [
        txn.constructor_calldata
            .iter()
            .fold(PoseidonHasher::new(), |mut hh, e| {
                hh.write(e.0.into());
                hh
            })
            .finish()
            .into(),
        txn.class_hash.0,
        txn.contract_address_salt.0,
    ];

    compute_v3_txn_hash(
        b"deploy_account",
        TransactionVersion::THREE,
        txn.sender_address,
        chain_id,
        txn.nonce,
        &deploy_account_specific_data,
        txn.tip,
        &txn.paymaster_data,
        txn.nonce_data_availability_mode.into(),
        txn.fee_data_availability_mode.into(),
        txn.resource_bounds.into(),
    )
}

/// Computes invoke v0 account transaction hash based on [this formula](https://docs.starknet.io/documentation/architecture_and_concepts/Blocks/transactions/#v0_hash_calculation):
/// ```text=
/// invoke_v0_tx_hash = h("invoke", version, sender_address,
///     entry_point_selector, h(calldata), max_fee, chain_id)
/// ```
///
/// FIXME: SW should fix the formula in the docs
///
/// Where `h` is [Pedersen hash](https://docs.starknet.io/documentation/architecture_and_concepts/Hashing/hash-functions/#pedersen_hash)
fn compute_invoke_v0_hash(txn: &InvokeTransactionV0, chain_id: ChainId) -> TransactionHash {
    let call_params_hash = {
        let mut hh = HashChain::default();
        hh = txn.calldata.iter().fold(hh, |mut hh, call_param| {
            hh.update(call_param.0);
            hh
        });
        hh.finalize()
    };

    let h = compute_txn_hash(
        b"invoke",
        TransactionVersion::ZERO,
        txn.sender_address,
        Some(txn.entry_point_selector),
        call_params_hash,
        Some(txn.max_fee),
        chain_id,
        (),
        None,
    );

    if h == txn.transaction_hash {
        h
    } else {
        legacy_compute_txn_hash(
            b"invoke",
            txn.sender_address,
            Some(txn.entry_point_selector),
            call_params_hash,
            chain_id,
            None,
        )
    }
}

/// Computes invoke v1 transaction hash based on [this formula](https://docs.starknet.io/documentation/architecture_and_concepts/Blocks/transactions/#v1_hash_calculation):
/// ```text=
/// invoke_v1_tx_hash = h("invoke", version, sender_address,
///     0, h(calldata), max_fee, chain_id, nonce)
/// ```
///
/// Where `h` is [Pedersen hash](https://docs.starknet.io/documentation/architecture_and_concepts/Hashing/hash-functions/#pedersen_hash)
fn compute_invoke_v1_hash(txn: &InvokeTransactionV1, chain_id: ChainId) -> TransactionHash {
    compute_txn_hash(
        b"invoke",
        TransactionVersion::ONE,
        txn.sender_address,
        None,
        {
            let mut hh = HashChain::default();
            hh = txn.calldata.iter().fold(hh, |mut hh, call_param| {
                hh.update(call_param.0);
                hh
            });
            hh.finalize()
        },
        Some(txn.max_fee),
        chain_id,
        txn.nonce,
        None,
    )
}

fn compute_invoke_v3_hash(txn: &InvokeTransactionV3, chain_id: ChainId) -> TransactionHash {
    let invoke_specific_data = [
        txn.account_deployment_data
            .iter()
            .fold(PoseidonHasher::new(), |mut hh, e| {
                hh.write(e.0.into());
                hh
            })
            .finish()
            .into(),
        txn.calldata
            .iter()
            .fold(PoseidonHasher::new(), |mut hh, e| {
                hh.write(e.0.into());
                hh
            })
            .finish()
            .into(),
    ];

    compute_v3_txn_hash(
        b"invoke",
        TransactionVersion::THREE,
        txn.sender_address,
        chain_id,
        txn.nonce,
        &invoke_specific_data,
        txn.tip,
        &txn.paymaster_data,
        txn.nonce_data_availability_mode.into(),
        txn.fee_data_availability_mode.into(),
        txn.resource_bounds.into(),
    )
}

/// Computes l1 handler transaction hash based on [this formula](https://docs.starknet.io/documentation/architecture_and_concepts/L1-L2_Communication/messaging-mechanism/#structure_and_hashing_l1-l2):
/// ```text=
/// l1_handler_tx_hash = h("l1_handler", version, contract_address,
///     entry_point_selector, h(calldata), 0, chain_id, nonce)
/// ```
///
/// FIXME: SW should fix the formula in the docs
///
/// Where `h` is [Pedersen hash](https://docs.starknet.io/documentation/architecture_and_concepts/Hashing/hash-functions/#pedersen_hash)
///
/// ## Important
///
/// Guarantees correct computation for Starknet **0.9.1** transactions onwards
fn compute_l1_handler_hash(txn: &L1HandlerTransaction, chain_id: ChainId) -> TransactionHash {
    let call_params_hash = {
        let mut hh = HashChain::default();
        hh = txn.calldata.iter().fold(hh, |mut hh, call_param| {
            hh.update(call_param.0);
            hh
        });
        hh.finalize()
    };

    let h = compute_txn_hash(
        b"l1_handler",
        txn.version,
        txn.contract_address,
        Some(txn.entry_point_selector),
        call_params_hash,
        None,
        chain_id,
        txn.nonce,
        None,
    );

    if h == txn.transaction_hash {
        h
    } else {
        // Starknet 0.7 L1 Handler transactions were
        // using a nonce.
        let h = legacy_compute_txn_hash(
            b"l1_handler",
            txn.contract_address,
            Some(txn.entry_point_selector),
            call_params_hash,
            chain_id,
            Some(txn.nonce.0),
        );
        if h == txn.transaction_hash {
            h
        } else {
            // Oldest L1 Handler transactions were actually Invokes
            // which later on were "renamed" to be the former,
            // yet the hashes remain, hence the prefix
            legacy_compute_txn_hash(
                b"invoke",
                txn.contract_address,
                Some(txn.entry_point_selector),
                call_params_hash,
                chain_id,
                None,
            )
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub enum NonceOrClassHash {
    Nonce(TransactionNonce),
    ClassHash(ClassHash),
    None,
}

impl From<TransactionNonce> for NonceOrClassHash {
    fn from(n: TransactionNonce) -> Self {
        Self::Nonce(n)
    }
}

impl From<ClassHash> for NonceOrClassHash {
    fn from(c: ClassHash) -> Self {
        Self::ClassHash(c)
    }
}

impl From<()> for NonceOrClassHash {
    fn from(_: ()) -> Self {
        Self::None
    }
}

/// _Generic_ compute transaction hash for older transactions (pre 0.8-ish)
fn legacy_compute_txn_hash(
    prefix: &[u8],
    address: ContractAddress,
    entry_point_selector: Option<EntryPoint>,
    list_hash: Felt,
    chain_id: ChainId,
    additional_data: Option<Felt>,
) -> TransactionHash {
    let mut h = HashChain::default();
    h.update(Felt::from_be_slice(prefix).expect("prefix is convertible"));
    h.update(*address.get());
    h.update(entry_point_selector.map(|e| e.0).unwrap_or(Felt::ZERO));
    h.update(list_hash);
    h.update(chain_id.0);
    if let Some(felt) = additional_data {
        h.update(felt);
    }

    TransactionHash(h.finalize())
}

/// _Generic_ compute transaction hash for v0-v2 transactions
#[allow(clippy::too_many_arguments)]
pub fn compute_txn_hash(
    prefix: &[u8],
    version: TransactionVersion,
    address: ContractAddress,
    entry_point_selector: Option<EntryPoint>,
    list_hash: Felt,
    max_fee: Option<Fee>,
    chain_id: ChainId,
    nonce_or_class_hash: impl Into<NonceOrClassHash>,
    compiled_class_hash: Option<CasmHash>,
) -> TransactionHash {
    let mut h = HashChain::default();
    h.update(Felt::from_be_slice(prefix).expect("prefix is convertible"));
    h.update(Felt::from_be_slice(version.0.as_be_bytes()).expect("version is convertible"));
    h.update(*address.get());
    h.update(entry_point_selector.map(|e| e.0).unwrap_or(Felt::ZERO));
    h.update(list_hash);
    h.update(max_fee.map(|e| e.0).unwrap_or(Felt::ZERO));
    h.update(chain_id.0);

    match nonce_or_class_hash.into() {
        NonceOrClassHash::Nonce(nonce) => h.update(nonce.0),
        NonceOrClassHash::ClassHash(class_hash) => h.update(class_hash.0),
        NonceOrClassHash::None => {}
    }

    if let Some(compiled_class_hash) = compiled_class_hash {
        h.update(compiled_class_hash.0);
    }

    TransactionHash(h.finalize())
}

const DA_AVAILABILITY_MODE_BITS: u8 = 32;

/// _Generic_ compute transaction hash for v3 transactions
#[allow(clippy::too_many_arguments)]
pub fn compute_v3_txn_hash(
    prefix: &[u8],
    version: TransactionVersion,
    sender_address: ContractAddress,
    chain_id: ChainId,
    nonce: TransactionNonce,
    tx_type_specific_data: &[Felt],
    tip: Tip,
    paymaster_data: &[PaymasterDataElem],
    nonce_data_availability_mode: DataAvailabilityMode,
    fee_data_availability_mode: DataAvailabilityMode,
    resource_bounds: ResourceBounds,
) -> TransactionHash {
    let fee_fields_hash = hash_fee_related_fields(&tip, &resource_bounds);
    let da_mode_concatenation = ((nonce_data_availability_mode as u64)
        << DA_AVAILABILITY_MODE_BITS)
        + fee_data_availability_mode as u64;

    let mut h: PoseidonHasher = PoseidonHasher::new();
    h.write(
        Felt::from_be_slice(prefix)
            .expect("prefix is convertible")
            .into(),
    );
    h.write(
        Felt::from_be_slice(version.0.as_be_bytes())
            .expect("version is convertible")
            .into(),
    );
    h.write((*sender_address.get()).into());
    h.write(fee_fields_hash.into());
    h.write(
        paymaster_data
            .iter()
            .fold(PoseidonHasher::new(), |mut hh, e| {
                hh.write(e.0.into());
                hh
            })
            .finish(),
    );
    h.write(chain_id.0.into());
    h.write(nonce.0.into());
    h.write(da_mode_concatenation.into());
    tx_type_specific_data
        .iter()
        .for_each(|e| h.write((*e).into()));

    TransactionHash(h.finish().into())
}

const MAX_AMOUNT_BITS: usize = 64;
const MAX_AMOUNT_BYTES: usize = MAX_AMOUNT_BITS / 8;
const MAX_PRICE_PER_UNIT_BITS: usize = 128;
const MAX_PRICE_PER_UNIT_BYTES: usize = MAX_PRICE_PER_UNIT_BITS / 8;
const RESOURCE_VALUE_OFFSET_BYTES: usize = MAX_AMOUNT_BYTES + MAX_PRICE_PER_UNIT_BYTES;
const L1_GAS_RESOURCE_NAME: &[u8] = b"L1_GAS";
const L2_GAS_RESOURCE_NAME: &[u8] = b"L2_GAS";

/// Calculates the hash of the fee related fields of a transaction.
///
/// - `tip`
/// - the resource bounds for L1 and L2
///   - concatenates the resource type, amount and max price per unit into a single felt
fn hash_fee_related_fields(tip: &Tip, resource_bounds: &ResourceBounds) -> Felt {
    let mut h = PoseidonHasher::new();
    h.write(tip.0.into());
    h.write(flattened_bounds(L1_GAS_RESOURCE_NAME, resource_bounds.l1_gas).into());
    h.write(flattened_bounds(L2_GAS_RESOURCE_NAME, resource_bounds.l2_gas).into());
    h.finish().into()
}

fn flattened_bounds(resource_name: &[u8], resource_bound: ResourceBound) -> Felt {
    let mut b: [u8; 32] = Default::default();
    b[(32 - MAX_PRICE_PER_UNIT_BYTES)..]
        .copy_from_slice(&resource_bound.max_price_per_unit.0.to_be_bytes());
    b[(32 - RESOURCE_VALUE_OFFSET_BYTES)..(32 - MAX_PRICE_PER_UNIT_BYTES)]
        .copy_from_slice(&resource_bound.max_amount.0.to_be_bytes());

    let padding_length = 8 - resource_name.len();
    b[padding_length..(32 - RESOURCE_VALUE_OFFSET_BYTES)].copy_from_slice(resource_name);

    Felt::from_be_bytes(b).expect("Resource names should fit within a felt")
}

#[cfg(test)]
mod tests {
    use super::compute_transaction_hash;
    use pathfinder_common::ChainId;
    use rstest::rstest;
    use starknet_gateway_test_fixtures::{v0_11_0, v0_13_0, v0_8_2, v0_9_0};

    #[derive(serde::Deserialize)]
    struct TxWrapper {
        transaction: crate::reply::transaction::Transaction,
    }

    #[rstest]
    #[test]
    // Declare
    #[case(v0_9_0::transaction::DECLARE)] // v0
    #[case(v0_11_0::transaction::declare::v1::BLOCK_463319)]
    #[case(v0_11_0::transaction::declare::v1::BLOCK_797215)]
    #[case(v0_11_0::transaction::declare::v2::BLOCK_797220)]
    #[case(v0_13_0::transaction::declare::v3::BLOCK_319709)]
    // Deploy
    #[case(v0_11_0::transaction::deploy::v0::GENESIS)]
    #[case(v0_9_0::transaction::DEPLOY)] // v0
    #[case(v0_11_0::transaction::deploy::v1::BLOCK_485004)]
    // Deploy account
    #[case(v0_11_0::transaction::deploy_account::v1::BLOCK_375919)]
    #[case(v0_11_0::transaction::deploy_account::v1::BLOCK_797K)]
    #[case(v0_13_0::transaction::deploy_account::v3::BLOCK_319693)]
    // Invoke
    #[case(v0_11_0::transaction::invoke::v0::GENESIS)]
    #[case(v0_8_2::transaction::INVOKE)]
    #[case(v0_9_0::transaction::INVOKE)]
    #[case(v0_11_0::transaction::invoke::v1::BLOCK_420K)]
    #[case(v0_11_0::transaction::invoke::v1::BLOCK_790K)]
    #[case(v0_13_0::transaction::invoke::v3::BLOCK_319106)]
    // L1 Handler
    #[case(v0_11_0::transaction::l1_handler::v0::BLOCK_1564)]
    #[case(v0_11_0::transaction::l1_handler::v0::BLOCK_272866)]
    #[case(v0_11_0::transaction::l1_handler::v0::BLOCK_790K)]
    fn computation(#[case] fixture: &str) {
        let txn = serde_json::from_str::<TxWrapper>(fixture)
            .unwrap()
            .transaction;

        let actual_hash = compute_transaction_hash(&txn, ChainId::GOERLI_TESTNET);
        assert_eq!(actual_hash, txn.hash());
    }

    mod verification {
        use super::TxWrapper;
        use crate::transaction_hash::{verify, VerifyResult};
        use pathfinder_common::ChainId;
        use starknet_gateway_test_fixtures::v0_11_0;

        // Historically L1 handler transactions were served as Invoke V0 because there was no explicit L1 transaction type.
        // Later on the gateway retroactively changed these to the new L1 handler transaction type.
        // In Starknet 0.7 nonces were introduced for L1 handlers.
        mod l1_handler {
            use super::*;

            // This is how L1 handler transactions were served later on.
            #[test]
            fn rewritten_l1_handler() {
                let block_854_idx_96 =
                    serde_json::from_str(v0_11_0::transaction::l1_handler::v0::BLOCK_854_IDX_96)
                        .unwrap();

                assert_eq!(
                    verify(&block_854_idx_96, ChainId::MAINNET,),
                    VerifyResult::Match
                );
            }

            // This is how L1 handler transactions were initially served.
            #[test]
            fn old_l1_handler_in_invoke_v0() {
                let block_854_idx_96 =
                    serde_json::from_str(v0_11_0::transaction::invoke::v0::BLOCK_854_IDX_96)
                        .unwrap();

                assert_eq!(
                    verify(&block_854_idx_96, ChainId::MAINNET,),
                    VerifyResult::Match
                );
            }
        }

        #[test]
        fn ok() {
            let txn = serde_json::from_str::<TxWrapper>(
                super::v0_11_0::transaction::declare::v2::BLOCK_797220,
            )
            .unwrap()
            .transaction;

            assert_eq!(verify(&txn, ChainId::GOERLI_TESTNET,), VerifyResult::Match);
        }

        #[test]
        fn failed() {
            let txn = serde_json::from_str::<TxWrapper>(
                super::v0_11_0::transaction::declare::v2::BLOCK_797220,
            )
            .unwrap()
            .transaction;
            // Wrong chain id to force failure
            assert!(matches!(
                verify(&txn, ChainId::MAINNET),
                VerifyResult::Mismatch(_)
            ))
        }
    }
}
