mod estimate_fee;
pub(crate) mod estimate_message_fee;
mod get_events;
mod get_state_update;
pub(crate) mod simulate_transaction;

pub(super) use estimate_fee::estimate_fee;
pub(crate) use estimate_message_fee::estimate_message_fee;
pub(super) use get_events::get_events;
pub(super) use get_state_update::get_state_update;
pub(crate) use simulate_transaction::simulate_transaction;

pub(crate) mod common {
    use std::sync::Arc;

    use pathfinder_common::pending::PendingData;
    use pathfinder_common::{BlockId, BlockTimestamp, StateUpdate};

    use crate::{
        cairo::ext_py::{BlockHashNumberOrLatest, GasPriceSource, Handle},
        context::RpcContext,
    };

    pub async fn prepare_handle_and_block(
        context: &RpcContext,
        block_id: BlockId,
    ) -> Result<
        (
            &Handle,
            GasPriceSource,
            BlockHashNumberOrLatest,
            Option<BlockTimestamp>,
            Option<Arc<StateUpdate>>,
        ),
        anyhow::Error,
    > {
        let handle = context
            .call_handle
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Unsupported configuration"))?;

        // discussed during estimateFee work: when user is requesting using block_hash use the
        // gasPrice from the starknet_blocks::gas_price column, otherwise (tags) get the latest
        // eth_gasPrice.
        //
        // the fact that [`base_block_and_pending_for_call`] transforms pending cases to use
        // actual parent blocks by hash is an internal transformation we do for correctness,
        // unrelated to this consideration.
        let gas_price = if matches!(block_id, BlockId::Pending | BlockId::Latest) {
            let gas_price = match context.eth_gas_price.as_ref() {
                Some(cached) => cached.get().await,
                None => None,
            };

            let gas_price =
                gas_price.ok_or_else(|| anyhow::anyhow!("Current eth_gasPrice is unavailable"))?;

            GasPriceSource::Current(gas_price)
        } else {
            GasPriceSource::PastBlock
        };

        let (when, pending_timestamp, pending_update) =
            base_block_and_pending_for_call(block_id, &context.pending_data).await?;

        Ok((handle, gas_price, when, pending_timestamp, pending_update))
    }

    /// Transforms the request to call or estimate fee at some point in time to the type expected
    /// by [`crate::cairo::ext_py`] with the optional, latest pending data.
    pub async fn base_block_and_pending_for_call(
        at_block: BlockId,
        pending_data: &PendingData,
    ) -> Result<
        (
            BlockHashNumberOrLatest,
            Option<BlockTimestamp>,
            Option<Arc<StateUpdate>>,
        ),
        anyhow::Error,
    > {
        use crate::cairo::ext_py::Pending;

        match BlockHashNumberOrLatest::try_from(at_block) {
            Ok(when) => Ok((when, None, None)),
            Err(Pending) => {
                let block = pending_data.block_unchecked();
                let state_update = pending_data.state_update_unchecked();
                Ok((
                    BlockHashNumberOrLatest::Latest,
                    Some(block.header.timestamp),
                    Some(state_update),
                ))
            }
        }
    }
}
