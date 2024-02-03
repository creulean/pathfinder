#![allow(dead_code, unused_variables)]
use anyhow::Context;
use p2p::PeerData;
use pathfinder_common::{BlockHash, BlockNumber, SignedBlockHeader};
use pathfinder_storage::Storage;
use tokio::task::spawn_blocking;

type SignedHeaderResult = Result<PeerData<SignedBlockHeader>, HeaderSyncError>;

/// Describes a gap in the stored headers.
///
/// Both head and tail form part of the gap i.e. it is an inclusive range.
pub(super) struct HeaderGap {
    /// Freshest block height of the gap.
    pub head: BlockNumber,
    /// Hash of the gap's head block. Used to validate the header chain data received.
    pub head_hash: BlockHash,
    /// Oldest block height of the gap.
    pub tail: BlockNumber,
    /// Oldest block's parent's hash. Used to link any received data to the existing local
    /// chain data.
    pub tail_parent_hash: BlockHash,
}

/// Returns the first [HeaderGap] in headers, searching from the given block backwards.
pub(super) async fn next_gap(
    storage: Storage,
    head: BlockNumber,
    head_hash: BlockHash,
) -> anyhow::Result<Option<HeaderGap>> {
    spawn_blocking(move || {
        let mut db = storage
            .connection()
            .context("Creating database connection")?;
        let db = db.transaction().context("Creating database transaction")?;

        // It's possible for the head block to be the head of the gap. This can occur when
        // called with the L1 anchor which has not been synced yet.
        let head_exists = db
            .block_exists(head.into())
            .context("Checking if search head exists locally")?;
        let gap_head = if head_exists {
            // Find the next header that exists, but whose parent does not.
            let Some(gap_head) = db
                .next_ancestor_without_parent(head)
                .context("Querying head of gap")?
            else {
                // No headers are missing so no gap found.
                return Ok(None);
            };

            gap_head
        } else {
            // Start of search is already missing so it becomes the head of the gap.
            (head, head_hash)
        };

        let gap_tail = db
            .next_ancestor(gap_head.0)
            .context("Querying tail of gap")?
            // By this point we are certain there is a gap, so the tail automatically becomes genesis
            // if no actual tail block is found.
            .unwrap_or_default();

        Ok(Some(HeaderGap {
            head: gap_head.0,
            head_hash: gap_head.1,
            tail: gap_tail.0 + 1,
            tail_parent_hash: gap_tail.1,
        }))
    })
    .await
    .context("Joining blocking task")?
}

#[derive(Debug, thiserror::Error)]
pub(super) enum HeaderSyncError {
    #[error(transparent)]
    DatabaseError(#[from] anyhow::Error),
    #[error("Signature verification failed")]
    BadSignature(PeerData<SignedBlockHeader>),
    #[error("Block hash verification failed")]
    BadBlockHash(PeerData<SignedBlockHeader>),
    #[error("Discontinuity in header chain")]
    Discontinuity(PeerData<SignedBlockHeader>),
}

impl HeaderSyncError {
    pub fn peer_id_and_data(&self) -> Option<&PeerData<SignedBlockHeader>> {
        match self {
            HeaderSyncError::DatabaseError(_) => None,
            HeaderSyncError::BadSignature(x) => Some(x),
            HeaderSyncError::BadBlockHash(x) => Some(x),
            HeaderSyncError::Discontinuity(x) => Some(x),
        }
    }
}

/// Ensures the header block ID matches expectations.
///
/// Intended for use with [scan](futures::StreamExt::scan) which is why
/// its function signature is a bit strange.
pub(super) fn check_continuity(
    expected: &mut (BlockNumber, BlockHash, bool),
    input: PeerData<SignedBlockHeader>,
) -> impl futures::Future<Output = Option<SignedHeaderResult>> {
    if expected.2 {
        return std::future::ready(None);
    }

    let header = &input.data.header;
    let is_correct = header.number == expected.0 && header.hash == expected.1;

    // Update expectation.
    expected.0 = header.number;
    expected.1 = header.hash;

    let result = if is_correct {
        Some(Ok(input))
    } else {
        expected.2 = true;
        Some(Err(HeaderSyncError::Discontinuity(input)))
    };

    std::future::ready(result)
}

/// Verifies the block hash and signature.
pub(super) async fn verify(signed_header: PeerData<SignedBlockHeader>) -> SignedHeaderResult {
    tokio::task::spawn_blocking(move || {
        if !signed_header.data.verify_signature() {
            return Err(HeaderSyncError::BadSignature(signed_header));
        }

        if !signed_header.data.header.verify_hash() {
            return Err(HeaderSyncError::BadBlockHash(signed_header));
        }

        Ok(signed_header)
    })
    .await
    .expect("Task should not crash")
}

/// Writes the headers to storage.
pub(super) async fn persist(
    mut signed_headers: Vec<PeerData<SignedBlockHeader>>,
    storage: Storage,
) -> SignedHeaderResult {
    tokio::task::spawn_blocking(move || {
        let mut db = storage
            .connection()
            .context("Creating database connection")?;
        let tx = db.transaction().context("Creating database transaction")?;

        for SignedBlockHeader { header, signature } in signed_headers.iter().map(|x| &x.data) {
            tx.insert_block_header(header)
                .context("Persisting block header")?;
            tx.insert_signature(header.number, signature)
                .context("Persisting block signature")?;
        }

        tx.commit().context("Committing database transaction")?;

        Ok(signed_headers.pop().expect("Headers should not be empty"))
    })
    .await
    .expect("Task should not crash")
}
