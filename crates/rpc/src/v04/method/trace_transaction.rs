use anyhow::Context;
use pathfinder_common::TransactionHash;
use pathfinder_executor::{ExecutionState, TransactionExecutionError};
use serde::{Deserialize, Serialize};
use starknet_gateway_client::GatewayApi;

use crate::executor::VERSIONS_LOWER_THAN_THIS_SHOULD_FALL_BACK_TO_FETCHING_TRACE_FROM_GATEWAY;
use crate::v04::v04_method::simulate_transactions::dto::map_gateway_trace;
use crate::{
    compose_executor_transaction,
    context::RpcContext,
    error::{ApplicationError, TraceError},
    executor::ExecutionStateError,
};

use super::simulate_transactions::dto::TransactionTrace;

#[derive(Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct TraceTransactionInput {
    pub transaction_hash: TransactionHash,
}

#[derive(Debug, Serialize, Eq, PartialEq)]
pub struct TraceTransactionOutput(pub TransactionTrace);

#[derive(Debug)]
pub enum TraceTransactionError {
    InvalidTxnHash,
    NoTraceAvailable(TraceError),
    Internal(anyhow::Error),
    Custom(anyhow::Error),
}

impl From<ExecutionStateError> for TraceTransactionError {
    fn from(value: ExecutionStateError) -> Self {
        match value {
            ExecutionStateError::BlockNotFound => Self::Custom(anyhow::anyhow!("Block not found")),
            ExecutionStateError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<TransactionExecutionError> for TraceTransactionError {
    fn from(value: TransactionExecutionError) -> Self {
        use TransactionExecutionError::*;
        match value {
            ExecutionError {
                transaction_index,
                error,
            } => Self::Custom(anyhow::anyhow!(
                "Execution error at transaction index {}: {}",
                transaction_index,
                error
            )),
            Internal(e) => Self::Internal(e),
            Custom(e) => Self::Custom(e),
        }
    }
}

impl From<anyhow::Error> for TraceTransactionError {
    fn from(e: anyhow::Error) -> Self {
        Self::Internal(e)
    }
}

impl From<TraceTransactionError> for ApplicationError {
    fn from(value: TraceTransactionError) -> Self {
        match value {
            TraceTransactionError::InvalidTxnHash => ApplicationError::InvalidTxnHash,
            TraceTransactionError::NoTraceAvailable(status) => {
                ApplicationError::NoTraceAvailable(status)
            }
            TraceTransactionError::Internal(e) => ApplicationError::Internal(e),
            TraceTransactionError::Custom(e) => ApplicationError::Custom(e),
        }
    }
}

pub async fn trace_transaction(
    context: RpcContext,
    input: TraceTransactionInput,
) -> Result<TraceTransactionOutput, TraceTransactionError> {
    #[allow(clippy::large_enum_variant)]
    enum LocalExecution {
        Success(TransactionTrace),
        Unsupported(starknet_gateway_types::reply::transaction::Transaction),
    }

    let span = tracing::Span::current();
    let local = tokio::task::spawn_blocking(move || {
        let _g = span.enter();

        let mut db = context
            .storage
            .connection()
            .context("Creating database connection")?;
        let db = db.transaction().context("Creating database transaction")?;

        // Find the transaction's block.
        let pending = context
            .pending_data
            .get(&db)
            .context("Querying pending data")?;

        let (header, transactions) = if let Some(pending_idx) = pending
            .block
            .transactions
            .iter()
            .position(|tx| tx.hash() == input.transaction_hash)
        {
            let header = pending.header();

            let starknet_version = header
                .starknet_version
                .parse_as_semver()
                .context("Parsing starknet version")?
                .unwrap_or(semver::Version::new(0, 0, 0));
            if starknet_version
                < VERSIONS_LOWER_THAN_THIS_SHOULD_FALL_BACK_TO_FETCHING_TRACE_FROM_GATEWAY
            {
                return Ok(LocalExecution::Unsupported(
                    pending.block.transactions[pending_idx].clone(),
                ));
            }

            (
                header,
                pending
                    .block
                    .transactions
                    .iter()
                    .take(pending_idx + 1)
                    .cloned()
                    .collect::<Vec<_>>(),
            )
        } else {
            let block_hash = db
                .transaction_block_hash(input.transaction_hash)?
                .ok_or(TraceTransactionError::InvalidTxnHash)?;

            let header = db
                .block_header(block_hash.into())
                .context("Fetching block header")?
                .context("Block header is missing")?;

            let starknet_version = header
                .starknet_version
                .parse_as_semver()
                .context("Parsing starknet version")?
                .unwrap_or(semver::Version::new(0, 0, 0));
            if starknet_version
                < VERSIONS_LOWER_THAN_THIS_SHOULD_FALL_BACK_TO_FETCHING_TRACE_FROM_GATEWAY
            {
                let transaction = db
                    .transaction(input.transaction_hash)
                    .context("Fetching transaction data")?
                    .context("Transaction data missing")?;

                return Ok(LocalExecution::Unsupported(transaction));
            }

            let transactions = db
                .transactions_for_block(header.number.into())
                .context("Fetching block transactions")?
                .context("Block transactions missing")?;

            let index = transactions
                .iter()
                .position(|tx| tx.hash() == input.transaction_hash)
                .context("Failed to find transaction in the batch")?;

            (
                header,
                transactions.into_iter().take(index + 1).collect::<Vec<_>>(),
            )
        };

        let state = ExecutionState::trace(&db, context.chain_id, header, None);

        let transactions = transactions
            .iter()
            .map(|transaction| compose_executor_transaction(transaction, &db))
            .collect::<Result<Vec<_>, _>>()?;

        pathfinder_executor::trace_one(state, transactions, input.transaction_hash, true, true)
            .map_err(TraceTransactionError::from)
            .map(|x| LocalExecution::Success(x.into()))
    })
    .await
    .context("trace_transaction: execution")??;

    let transaction = match local {
        LocalExecution::Success(trace) => return Ok(TraceTransactionOutput(trace)),
        LocalExecution::Unsupported(x) => x,
    };

    let trace = context
        .sequencer
        .transaction_trace(input.transaction_hash)
        .await
        .context("Proxying call to feeder gateway")?;

    let trace = map_gateway_trace(transaction, trace);

    Ok(TraceTransactionOutput(trace))
}

#[cfg(test)]
pub mod tests {
    use super::super::trace_block_transactions::tests::setup_multi_tx_trace_test;
    use super::*;

    #[tokio::test]
    async fn test_multiple_transactions() -> anyhow::Result<()> {
        let (context, _, traces) = setup_multi_tx_trace_test().await?;

        for trace in traces {
            let input = TraceTransactionInput {
                transaction_hash: trace.transaction_hash,
            };
            let output = trace_transaction(context.clone(), input).await.unwrap();
            let expected = TraceTransactionOutput(trace.trace_root);
            pretty_assertions_sorted::assert_eq!(output, expected);
        }

        Ok(())
    }
}
