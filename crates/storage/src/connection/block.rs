use anyhow::Context;
use pathfinder_common::{BlockHash, BlockHeader, BlockNumber, GasPrice, StarknetVersion};

use crate::{prelude::*, BlockId};

pub(super) fn insert_block_header(
    tx: &Transaction<'_>,
    header: &BlockHeader,
) -> anyhow::Result<()> {
    // Intern the starknet version
    let version_id = intern_starknet_version(tx, &header.starknet_version)
        .context("Interning starknet version")?;

    // Insert the header
    tx.inner().execute(
        r"INSERT INTO block_headers 
                   ( number,  hash,  storage_commitment,  timestamp,  eth_l1_gas_price,  strk_l1_gas_price,  sequencer_address,  version_id,  transaction_commitment,  event_commitment,  state_commitment,  class_commitment,  transaction_count,  event_count)
            VALUES (:number, :hash, :storage_commitment, :timestamp, :eth_l1_gas_price, :strk_l1_gas_price, :sequencer_address, :version_id, :transaction_commitment, :event_commitment, :state_commitment, :class_commitment, :transaction_count, :event_count)",
        named_params! {
            ":number": &header.number,
            ":hash": &header.hash,
            ":storage_commitment": &header.storage_commitment,
            ":timestamp": &header.timestamp,
            ":eth_l1_gas_price": &header.eth_l1_gas_price.to_be_bytes().as_slice(),
            ":strk_l1_gas_price": &header.strk_l1_gas_price.to_be_bytes().as_slice(),
            ":sequencer_address": &header.sequencer_address,
            ":version_id": &version_id,
            ":transaction_commitment": &header.transaction_commitment,
            ":event_commitment": &header.event_commitment,
            ":class_commitment": &header.class_commitment,
            ":transaction_count": &header.transaction_count.try_into_sql_int()?,
            ":event_count": &header.event_count.try_into_sql_int()?,
            ":state_commitment": &header.state_commitment,
        },
    ).context("Inserting block header")?;

    // This must occur after the header is inserted as this table references the header table.
    tx.inner()
        .execute(
            "INSERT INTO canonical_blocks(number, hash) values(?,?)",
            params![&header.number, &header.hash],
        )
        .context("Inserting into canonical_blocks table")?;

    Ok(())
}

pub(super) fn next_ancestor(
    tx: &Transaction<'_>,
    target: BlockNumber,
) -> anyhow::Result<Option<(BlockNumber, BlockHash)>> {
    tx.inner()
        .query_row(
            "SELECT number,hash FROM block_headers 
                WHERE number < ? 
                ORDER BY number DESC LIMIT 1",
            params![&target],
            |row| {
                let number = row.get_block_number(0)?;
                let hash = row.get_block_hash(1)?;
                Ok((number, hash))
            },
        )
        .optional()
        .map_err(|x| x.into())
}

pub(super) fn next_ancestor_without_parent(
    tx: &Transaction<'_>,
    target: BlockNumber,
) -> anyhow::Result<Option<(BlockNumber, BlockHash)>> {
    tx.inner()
        .query_row(
            "SELECT number,hash FROM block_headers t1 
                WHERE number <= ? AND 
                NOT EXISTS (SELECT * FROM block_headers t2 WHERE t1.number - 1 = t2.number) 
                ORDER BY number DESC LIMIT 1;",
            params![&target],
            |row| {
                let number = row.get_block_number(0)?;
                let hash = row.get_block_hash(1)?;
                Ok((number, hash))
            },
        )
        .optional()
        .map_err(|x| x.into())
}

fn intern_starknet_version(tx: &Transaction<'_>, version: &StarknetVersion) -> anyhow::Result<i64> {
    let id: Option<i64> = tx
        .inner()
        .query_row(
            "SELECT id FROM starknet_versions WHERE version = ?",
            params![version],
            |r| Ok(r.get_unwrap(0)),
        )
        .optional()
        .context("Querying for an existing starknet_version")?;

    let id = if let Some(id) = id {
        id
    } else {
        // sqlite "autoincrement" for integer primary keys works like this: we leave it out of
        // the insert, even though it's not null, it will get max(id)+1 assigned, which we can
        // read back with last_insert_rowid
        let rows = tx
            .inner()
            .execute(
                "INSERT INTO starknet_versions(version) VALUES (?)",
                params![version],
            )
            .context("Inserting unique starknet_version")?;

        anyhow::ensure!(rows == 1, "Unexpected number of rows inserted: {rows}");

        tx.inner().last_insert_rowid()
    };

    Ok(id)
}

pub(super) fn purge_block(tx: &Transaction<'_>, block: BlockNumber) -> anyhow::Result<()> {
    tx.inner()
        .execute(
            r"DELETE FROM starknet_transactions WHERE block_hash = (
                SELECT hash FROM canonical_blocks WHERE number = ?
            )",
            params![&block],
        )
        .context("Deleting transactions")?;

    tx.inner()
        .execute(
            "DELETE FROM canonical_blocks WHERE number = ?",
            params![&block],
        )
        .context("Deleting block from canonical_blocks table")?;

    tx.inner()
        .execute(
            "DELETE FROM block_headers WHERE number = ?",
            params![&block],
        )
        .context("Deleting block from block_headers table")?;

    tx.inner()
        .execute(
            "DELETE FROM contract_roots WHERE block_number = ?",
            params![&block],
        )
        .context("Deleting block from contract_roots table")?;

    tx.inner()
        .execute(
            "DELETE FROM class_commitment_leaves WHERE block_number = ?",
            params![&block],
        )
        .context("Deleting block from class_commitment_leaves table")?;

    tx.inner()
        .execute(
            "DELETE FROM contract_state_hashes WHERE block_number = ?",
            params![&block],
        )
        .context("Deleting block from contract_state_hashes table")?;

    tx.inner()
        .execute(
            "DELETE FROM class_roots WHERE block_number = ?",
            params![&block],
        )
        .context("Deleting block from class_roots table")?;

    tx.inner()
        .execute(
            "DELETE FROM storage_roots WHERE block_number = ?",
            params![&block],
        )
        .context("Deleting block from storage_roots table")?;

    Ok(())
}

pub(super) fn block_id(
    tx: &Transaction<'_>,
    block: BlockId,
) -> anyhow::Result<Option<(BlockNumber, BlockHash)>> {
    match block {
        BlockId::Latest => tx.inner().query_row(
            "SELECT number, hash FROM canonical_blocks ORDER BY number DESC LIMIT 1",
            [],
            |row| {
                let number = row.get_block_number(0)?;
                let hash = row.get_block_hash(1)?;

                Ok((number, hash))
            },
        ),
        BlockId::Number(number) => tx.inner().query_row(
            "SELECT hash FROM canonical_blocks WHERE number = ?",
            params![&number],
            |row| {
                let hash = row.get_block_hash(0)?;
                Ok((number, hash))
            },
        ),
        BlockId::Hash(hash) => tx.inner().query_row(
            "SELECT number FROM canonical_blocks WHERE hash = ?",
            params![&hash],
            |row| {
                let number = row.get_block_number(0)?;
                Ok((number, hash))
            },
        ),
    }
    .optional()
    .map_err(|e| e.into())
}

pub(super) fn block_exists(tx: &Transaction<'_>, block: BlockId) -> anyhow::Result<bool> {
    match block {
        BlockId::Latest => {
            let mut stmt = tx
                .inner()
                .prepare_cached("SELECT EXISTS(SELECT 1 FROM canonical_blocks)")?;
            stmt.query_row([], |row| row.get(0))
        }
        BlockId::Number(number) => {
            let mut stmt = tx
                .inner()
                .prepare_cached("SELECT EXISTS(SELECT 1 FROM canonical_blocks WHERE number = ?)")?;
            stmt.query_row(params![&number], |row| row.get(0))
        }
        BlockId::Hash(hash) => {
            let mut stmt = tx
                .inner()
                .prepare_cached("SELECT EXISTS(SELECT 1 FROM canonical_blocks WHERE hash = ?)")?;
            stmt.query_row(params![&hash], |row| row.get(0))
        }
    }
    .map_err(|e| e.into())
}

pub(super) fn block_header(
    tx: &Transaction<'_>,
    block: BlockId,
) -> anyhow::Result<Option<BlockHeader>> {
    // TODO: is LEFT JOIN reasonable? It's required because version ID can be null for non-existent versions.
    const BASE_SQL: &str = "SELECT * FROM block_headers LEFT JOIN starknet_versions ON block_headers.version_id = starknet_versions.id";
    let sql = match block {
        BlockId::Latest => format!("{BASE_SQL} ORDER BY number DESC LIMIT 1"),
        BlockId::Number(_) => format!("{BASE_SQL} WHERE number = ?"),
        BlockId::Hash(_) => format!("{BASE_SQL} WHERE hash = ?"),
    };

    let parse_row = |row: &rusqlite::Row<'_>| {
        let number = row.get_block_number("number")?;
        let hash = row.get_block_hash("hash")?;
        let storage_commitment = row.get_storage_commitment("storage_commitment")?;
        let timestamp = row.get_timestamp("timestamp")?;
        let eth_l1_gas_price = row.get_gas_price("eth_l1_gas_price")?;
        let strk_l1_gas_price = row
            .get_optional_gas_price("strk_l1_gas_price")?
            .unwrap_or(GasPrice::ZERO);
        let sequencer_address = row.get_sequencer_address("sequencer_address")?;
        let transaction_commitment = row.get_transaction_commitment("transaction_commitment")?;
        let event_commitment = row.get_event_commitment("event_commitment")?;
        let class_commitment = row.get_class_commitment("class_commitment")?;
        let starknet_version = row.get_starknet_version("version")?;
        let event_count: usize = row.get("event_count")?;
        let transaction_count: usize = row.get("transaction_count")?;
        let state_commitment = row.get_state_commitment("state_commitment")?;

        let header = BlockHeader {
            hash,
            number,
            timestamp,
            eth_l1_gas_price,
            strk_l1_gas_price,
            sequencer_address,
            class_commitment,
            event_commitment,
            state_commitment,
            storage_commitment,
            transaction_commitment,
            starknet_version,
            transaction_count,
            event_count,
            // TODO: store block hash in-line.
            // This gets filled in by a separate query, but really should get stored as a column in
            // order to support truncated history.
            parent_hash: BlockHash::default(),
        };

        Ok(header)
    };

    let mut stmt = tx
        .inner()
        .prepare_cached(&sql)
        .context("Preparing block header query")?;

    let header = match block {
        BlockId::Latest => stmt.query_row([], parse_row),
        BlockId::Number(number) => stmt.query_row(params![&number], parse_row),
        BlockId::Hash(hash) => stmt.query_row(params![&hash], parse_row),
    }
    .optional()
    .context("Querying for block header")?;

    let Some(mut header) = header else {
        return Ok(None);
    };

    // Fill in parent hash (unless we are at genesis in which case the current ZERO is correct).
    if header.number != BlockNumber::GENESIS {
        let parent_hash = tx
            .inner()
            .query_row(
                "SELECT hash FROM block_headers WHERE number = ?",
                params![&(header.number - 1)],
                |row| row.get_block_hash(0),
            )
            .context("Querying parent hash")?;

        header.parent_hash = parent_hash;
    }

    Ok(Some(header))
}

pub(super) fn block_is_l1_accepted(tx: &Transaction<'_>, block: BlockId) -> anyhow::Result<bool> {
    let Some(l1_l2) = tx.l1_l2_pointer().context("Querying L1-L2 pointer")? else {
        return Ok(false);
    };

    let Some((block_number, _)) = tx.block_id(block).context("Fetching block number")? else {
        return Ok(false);
    };

    Ok(block_number <= l1_l2)
}

#[cfg(test)]
mod tests {
    use pathfinder_common::macro_prelude::*;
    use pathfinder_common::prelude::*;

    use super::*;
    use crate::Connection;

    // Create test database filled with block headers.
    fn setup() -> (Connection, Vec<BlockHeader>) {
        let storage = crate::Storage::in_memory().unwrap();
        let mut connection = storage.connection().unwrap();
        let tx = connection.transaction().unwrap();

        // This intentionally does not use the builder so that we don't forget to test
        // any new fields that get added.
        //
        // Set unique values so we can be sure we are (de)serializing correctly.
        let storage_commitment = storage_commitment_bytes!(b"storage commitment genesis");
        let class_commitment = class_commitment_bytes!(b"class commitment genesis");

        let genesis = BlockHeader {
            hash: block_hash_bytes!(b"genesis hash"),
            parent_hash: BlockHash::ZERO,
            number: BlockNumber::GENESIS,
            timestamp: BlockTimestamp::new_or_panic(10),
            eth_l1_gas_price: GasPrice(32),
            strk_l1_gas_price: GasPrice(33),
            sequencer_address: sequencer_address_bytes!(b"sequencer address genesis"),
            starknet_version: StarknetVersion::default(),
            class_commitment,
            event_commitment: event_commitment_bytes!(b"event commitment genesis"),
            state_commitment: StateCommitment::calculate(storage_commitment, class_commitment),
            storage_commitment,
            transaction_commitment: transaction_commitment_bytes!(b"tx commitment genesis"),
            transaction_count: 37,
            event_count: 40,
        };
        let header1 = genesis
            .child_builder()
            .with_timestamp(BlockTimestamp::new_or_panic(12))
            .with_eth_l1_gas_price(GasPrice(34))
            .with_strk_l1_gas_price(GasPrice(35))
            .with_sequencer_address(sequencer_address_bytes!(b"sequencer address 1"))
            .with_event_commitment(event_commitment_bytes!(b"event commitment 1"))
            .with_class_commitment(class_commitment_bytes!(b"class commitment 1"))
            .with_storage_commitment(storage_commitment_bytes!(b"storage commitment 1"))
            .with_calculated_state_commitment()
            .with_transaction_commitment(transaction_commitment_bytes!(b"tx commitment 1"))
            .finalize_with_hash(block_hash_bytes!(b"block 1 hash"));

        let header2 = header1
            .child_builder()
            .with_eth_l1_gas_price(GasPrice(38))
            .with_strk_l1_gas_price(GasPrice(39))
            .with_timestamp(BlockTimestamp::new_or_panic(15))
            .with_sequencer_address(sequencer_address_bytes!(b"sequencer address 2"))
            .with_event_commitment(event_commitment_bytes!(b"event commitment 2"))
            .with_class_commitment(class_commitment_bytes!(b"class commitment 2"))
            .with_storage_commitment(storage_commitment_bytes!(b"storage commitment 2"))
            .with_calculated_state_commitment()
            .with_transaction_commitment(transaction_commitment_bytes!(b"tx commitment 2"))
            .finalize_with_hash(block_hash_bytes!(b"block 2 hash"));

        let headers = vec![genesis, header1, header2];
        for header in &headers {
            tx.insert_block_header(header).unwrap();
        }
        tx.commit().unwrap();

        (connection, headers)
    }

    #[test]
    fn get_latest() {
        let (mut connection, headers) = setup();
        let tx = connection.transaction().unwrap();

        let result = tx.block_header(BlockId::Latest).unwrap().unwrap();
        let expected = headers.last().unwrap();

        assert_eq!(&result, expected);
    }

    #[test]
    fn get_by_number() {
        let (mut connection, headers) = setup();
        let tx = connection.transaction().unwrap();

        for header in &headers {
            let result = tx.block_header(header.number.into()).unwrap().unwrap();
            assert_eq!(&result, header);
        }

        let past_head = headers.last().unwrap().number + 1;
        let result = tx.block_header(past_head.into()).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn get_by_hash() {
        let (mut connection, headers) = setup();
        let tx = connection.transaction().unwrap();

        for header in &headers {
            let result = tx.block_header(header.hash.into()).unwrap().unwrap();
            assert_eq!(&result, header);
        }

        let invalid = block_hash_bytes!(b"invalid block hash");
        let result = tx.block_header(invalid.into()).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn purge_block() {
        let (mut connection, headers) = setup();
        let tx = connection.transaction().unwrap();
        let latest = headers.last().unwrap();

        // Add a class to test that purging a block unsets its block number;
        let cairo_hash = class_hash!("0x1234");
        tx.insert_cairo_class(cairo_hash, &[]).unwrap();
        tx.insert_state_update(
            latest.number,
            &StateUpdate::default().with_declared_cairo_class(cairo_hash),
        )
        .unwrap();

        tx.purge_block(latest.number).unwrap();

        let exists = tx.block_exists(latest.number.into()).unwrap();
        assert!(!exists);

        let class_exists = tx
            .class_definition_at(latest.number.into(), ClassHash(cairo_hash.0))
            .unwrap();
        assert_eq!(class_exists, None);
    }

    #[test]
    fn block_id() {
        let (mut connection, headers) = setup();
        let tx = connection.transaction().unwrap();

        let target = headers.last().unwrap();
        let expected = Some((target.number, target.hash));

        let by_number = tx.block_id(target.number.into()).unwrap();
        assert_eq!(by_number, expected);

        let by_hash = tx.block_id(target.hash.into()).unwrap();
        assert_eq!(by_hash, expected);
    }

    #[test]
    fn block_is_l1_accepted() {
        let (mut connection, headers) = setup();
        let tx = connection.transaction().unwrap();

        // Mark the genesis header as L1 accepted.
        tx.update_l1_l2_pointer(Some(headers[0].number)).unwrap();

        let l1_by_hash = tx.block_is_l1_accepted(headers[0].hash.into()).unwrap();
        assert!(l1_by_hash);
        let l1_by_number = tx.block_is_l1_accepted(headers[0].number.into()).unwrap();
        assert!(l1_by_number);

        // The second block will therefore be L2 accepted.
        let l2_by_hash = tx.block_is_l1_accepted(headers[1].hash.into()).unwrap();
        assert!(!l2_by_hash);
        let l2_by_number = tx.block_is_l1_accepted(headers[1].number.into()).unwrap();
        assert!(!l2_by_number);
    }

    mod next_ancestor {
        use super::*;

        #[test]
        fn empty_chain_returns_none() {
            let storage = crate::Storage::in_memory().unwrap();
            let mut db = storage.connection().unwrap();
            let db = db.transaction().unwrap();

            let result = next_ancestor(&db, BlockNumber::GENESIS + 10).unwrap();
            assert!(result.is_none());

            let result = next_ancestor(&db, BlockNumber::GENESIS).unwrap();
            assert!(result.is_none());
        }

        #[test]
        fn father_exists() {
            let (mut connection, headers) = setup();
            let tx = connection.transaction().unwrap();

            let result = next_ancestor(&tx, headers[2].number + 1).unwrap().unwrap();
            let expected = (headers[2].number, headers[2].hash);
            assert_eq!(result, expected);
        }

        #[test]
        fn grandfather_exists() {
            let (mut connection, headers) = setup();
            let tx = connection.transaction().unwrap();

            let result = next_ancestor(&tx, headers[2].number + 2).unwrap().unwrap();
            let expected = (headers[2].number, headers[2].hash);
            assert_eq!(result, expected);
        }

        #[test]
        fn gap_in_chain() {
            let storage = crate::Storage::in_memory().unwrap();
            let mut db = storage.connection().unwrap();
            let db = db.transaction().unwrap();

            let genesis = BlockHeader::default();
            db.insert_block_header(&genesis).unwrap();

            let header_after_gap = genesis
                .child_builder()
                .finalize_with_hash(block_hash_bytes!(b"skipped"))
                .child_builder()
                .finalize_with_hash(block_hash_bytes!(b"expected"));

            db.insert_block_header(&header_after_gap).unwrap();

            let result = next_ancestor(&db, header_after_gap.number + 1)
                .unwrap()
                .unwrap();
            let expected = (header_after_gap.number, header_after_gap.hash);
            assert_eq!(result, expected);
        }
    }

    mod next_ancestor_without_parent {
        use super::*;

        #[test]
        fn empty_chain_returns_none() {
            let storage = crate::Storage::in_memory().unwrap();
            let mut db = storage.connection().unwrap();
            let db = db.transaction().unwrap();

            let result = next_ancestor_without_parent(&db, BlockNumber::GENESIS + 10).unwrap();
            assert!(result.is_none());

            let result = next_ancestor_without_parent(&db, BlockNumber::GENESIS).unwrap();
            assert!(result.is_none());
        }

        #[test]
        fn target_without_parent_returns_target() {
            let storage = crate::Storage::in_memory().unwrap();
            let mut db = storage.connection().unwrap();
            let db = db.transaction().unwrap();

            let genesis = BlockHeader::default();

            let header_after_gap = genesis
                .child_builder()
                .finalize_with_hash(block_hash_bytes!(b"skipped"))
                .child_builder()
                .finalize_with_hash(block_hash_bytes!(b"expected"));

            db.insert_block_header(&genesis).unwrap();
            db.insert_block_header(&header_after_gap).unwrap();

            let expected = (genesis.number, genesis.hash);
            let result = next_ancestor_without_parent(&db, genesis.number)
                .unwrap()
                .unwrap();
            assert_eq!(result, expected);

            let expected = (header_after_gap.number, header_after_gap.hash);
            let result = next_ancestor_without_parent(&db, header_after_gap.number)
                .unwrap()
                .unwrap();
            assert_eq!(result, expected);
        }

        #[test]
        fn missing_target_is_skipped() {
            let (mut connection, headers) = setup();
            let tx = connection.transaction().unwrap();

            let target = headers
                .last()
                .unwrap()
                .child_builder()
                .finalize_with_hash(block_hash_bytes!(b"target"));

            let expected = (headers[0].number, headers[0].hash);
            let result = next_ancestor_without_parent(&tx, target.number)
                .unwrap()
                .unwrap();
            assert_eq!(result, expected);
        }

        #[test]
        fn complete_chain_returns_genesis() {
            let (mut connection, headers) = setup();
            let tx = connection.transaction().unwrap();

            let result = next_ancestor_without_parent(&tx, BlockNumber::GENESIS)
                .unwrap()
                .unwrap();
            let expected = (headers[0].number, headers[0].hash);
            assert_eq!(result, expected);
        }

        #[test]
        fn incomplete_chain_returns_tail() {
            let (mut connection, headers) = setup();
            let tx = connection.transaction().unwrap();

            let tail = headers
                .last()
                .unwrap()
                .child_builder()
                .finalize_with_hash(block_hash_bytes!(b"skipped"))
                .child_builder()
                .finalize_with_hash(block_hash_bytes!(b"tail"));
            let target = tail
                .child_builder()
                .finalize_with_hash(block_hash_bytes!(b"target"));
            tx.insert_block_header(&tail).unwrap();
            tx.insert_block_header(&target).unwrap();

            let result = next_ancestor_without_parent(&tx, target.number)
                .unwrap()
                .unwrap();
            let expected = (tail.number, tail.hash);
            assert_eq!(result, expected);
        }
    }
}
