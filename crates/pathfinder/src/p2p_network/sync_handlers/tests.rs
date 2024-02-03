use p2p_proto::common::{Direction, Step};
use pathfinder_common::BlockNumber;
use rstest::rstest;

const I64_MAX: u64 = i64::MAX as u64;

#[rstest]
#[case(0, 1, Direction::Forward, Some(1))]
#[case(0, I64_MAX, Direction::Forward, Some(I64_MAX))]
#[case(1, I64_MAX, Direction::Forward, None)]
#[case(0, 1, Direction::Backward, None)]
#[case(1, 1, Direction::Backward, Some(0))]
#[case(I64_MAX, 1, Direction::Backward, Some(I64_MAX - 1))]
#[case(I64_MAX, I64_MAX, Direction::Backward, Some(0))]
#[case(I64_MAX, I64_MAX + 1, Direction::Backward, None)]
#[test]
fn get_next_block_number(
    #[case] start: u64,
    #[case] step: u64,
    #[case] direction: Direction,
    #[case] expected: Option<u64>,
) {
    assert_eq!(
        super::get_next_block_number(
            BlockNumber::new_or_panic(start),
            Step::from(Some(step)),
            direction
        ),
        expected.map(BlockNumber::new_or_panic)
    );
}

mod boundary_conditions {
    use super::I64_MAX;
    use crate::p2p_network::sync_handlers::{
        get_bodies, get_events, get_headers, get_receipts, get_transactions, MAX_COUNT_IN_TESTS,
    };
    use assert_matches::assert_matches;
    use fake::{Fake, Faker};
    use futures::channel::mpsc;
    use futures::StreamExt;
    use p2p_proto::block::{
        BlockBodiesRequest, BlockBodyMessage, BlockHeadersRequest, BlockHeadersResponse,
        BlockHeadersResponsePart,
    };
    use p2p_proto::common::{BlockNumberOrHash, Direction, Fin, Iteration};
    use p2p_proto::event::{EventsRequest, EventsResponseKind};
    use p2p_proto::receipt::{ReceiptsRequest, ReceiptsResponseKind};
    use p2p_proto::transaction::{TransactionsRequest, TransactionsResponseKind};
    use pathfinder_storage::fake::with_n_blocks;
    use pathfinder_storage::Storage;
    use rand::{thread_rng, Rng};
    use rstest::rstest;

    mod zero_limit_yields_fin_ok_invalid_start_yields_fin_unknown {
        use super::*;

        fn zero_limit() -> Iteration {
            Iteration {
                limit: 0,
                ..Faker.fake()
            }
        }

        fn invalid_start() -> Iteration {
            Iteration {
                start: BlockNumberOrHash::Number(
                    rand::thread_rng().gen_range(I64_MAX + 1..=u64::MAX),
                ),
                ..Faker.fake()
            }
        }

        macro_rules! define_test {
            ($name:ident, $uut_name:ident, $request:tt) => {
                #[rstest]
                #[case(zero_limit(), Fin::ok())]
                #[case(invalid_start(), Fin::unknown())]
                #[tokio::test]
                async fn $name(#[case] iteration: Iteration, #[case] fin: Fin) {
                    let storage = Storage::in_memory().unwrap();
                    let (tx, mut rx) = mpsc::channel(0);
                    let _jh = tokio::spawn($uut_name(storage, $request { iteration }, tx));
                    assert_eq!(rx.next().await.unwrap().into_fin(), Some(fin));
                }
            };
        }

        define_test!(headers, get_headers, BlockHeadersRequest);
        define_test!(bodies, get_bodies, BlockBodiesRequest);
        define_test!(transactions, get_transactions, TransactionsRequest);
        define_test!(receipts, get_receipts, ReceiptsRequest);
        define_test!(events, get_events, EventsRequest);
    }

    mod partially_successful_requests_end_with_additional_fin_unknown {
        use super::*;

        fn init_test<T>(
            direction: Direction,
        ) -> (Storage, Iteration, mpsc::Sender<T>, mpsc::Receiver<T>) {
            let storage: Storage = Storage::in_memory().unwrap();
            let _ = with_n_blocks(&storage, 1);
            let iteration = Iteration {
                start: BlockNumberOrHash::Number(0),
                // We want more than available, we don't care about the internal limit because
                // partial failure (`Fin::unknown()`) takes precedence over it (`Fin::too_much()`)
                limit: thread_rng().gen_range(2..=MAX_COUNT_IN_TESTS * 2),
                direction,
                ..Faker.fake()
            };
            let (tx, rx) = mpsc::channel::<T>(0);
            (storage, iteration, tx, rx)
        }

        #[rstest]
        #[tokio::test]
        async fn test_get_headers(
            #[values(Direction::Backward, Direction::Forward)] direction: Direction,
        ) {
            let (storage, iteration, tx, mut rx) = init_test(direction);
            let getter_fut = get_headers(storage, BlockHeadersRequest { iteration }, tx);

            let (_, ret) = tokio::join!(getter_fut, rx.next());

            let BlockHeadersResponse { parts } = ret.unwrap();
            assert_eq!(parts.len(), 4);
            assert_matches!(&parts[0], BlockHeadersResponsePart::Header(h) => assert_eq!(h.number, 0));
            assert_matches!(&parts[1], BlockHeadersResponsePart::Signatures(s) => assert_eq!(s.block.number, 0));
            assert_eq!(parts[2], BlockHeadersResponsePart::Fin(Fin::ok()));
            // Expect Fin::unknown() where the first unavailable item would be
            assert_eq!(parts[3], BlockHeadersResponsePart::Fin(Fin::unknown()));
        }

        #[rstest]
        #[tokio::test]
        async fn test_get_bodies(
            #[values(Direction::Backward, Direction::Forward)] direction: Direction,
        ) {
            let (storage, iteration, tx, mut rx) = init_test(direction);
            let _jh = tokio::spawn(get_bodies(storage, BlockBodiesRequest { iteration }, tx));
            rx.next().await.unwrap(); // Diff
            match rx.next().await.unwrap().body_message {
                // New classes in block
                BlockBodyMessage::Classes(_) => {
                    assert_eq!(
                        rx.next().await.unwrap().body_message,
                        BlockBodyMessage::Fin(Fin::ok())
                    );
                }
                // No new classes in block
                BlockBodyMessage::Fin(f) => assert_eq!(f, Fin::ok()),
                _ => panic!("unexpected message type"),
            }

            // Expect Fin::unknown() where the first unavailable item would be
            assert_eq!(
                rx.next().await.unwrap().body_message,
                BlockBodyMessage::Fin(Fin::unknown())
            );
        }

        macro_rules! define_test {
            ($name:ident, $uut_name:ident, $request:tt, $reply:tt) => {
                #[rstest]
                #[tokio::test]
                async fn $name(
                    #[values(Direction::Backward, Direction::Forward)] direction: Direction,
                ) {
                    let (storage, iteration, tx, mut rx) = init_test(direction);
                    let _jh = tokio::spawn($uut_name(storage, $request { iteration }, tx));
                    // Block data
                    rx.next().await.unwrap();
                    // Properly delimited with Fin::ok()
                    assert_eq!(rx.next().await.unwrap().kind, $reply::Fin(Fin::ok()));
                    // Expect Fin::unknown() where the first unavailable item would be
                    assert_eq!(rx.next().await.unwrap().kind, $reply::Fin(Fin::unknown()));
                }
            };
        }

        define_test!(
            test_get_transactions,
            get_transactions,
            TransactionsRequest,
            TransactionsResponseKind
        );
        define_test!(
            test_get_receipts,
            get_receipts,
            ReceiptsRequest,
            ReceiptsResponseKind
        );
        define_test!(
            test_get_events,
            get_events,
            EventsRequest,
            EventsResponseKind
        );
    }

    mod internally_limited_requests_end_with_additional_fin_too_much {
        use super::*;

        const NUM_BLOCKS_IN_STORAGE: u64 = MAX_COUNT_IN_TESTS;

        fn init_test<T>(
            direction: Direction,
        ) -> (Storage, Iteration, mpsc::Sender<T>, mpsc::Receiver<T>) {
            let storage = Storage::in_memory().unwrap();
            let _ = with_n_blocks(&storage, NUM_BLOCKS_IN_STORAGE as usize);
            let (tx, rx) = mpsc::channel::<T>(1);
            let start = match direction {
                Direction::Forward => BlockNumberOrHash::Number(0),
                Direction::Backward => BlockNumberOrHash::Number(NUM_BLOCKS_IN_STORAGE - 1),
            };
            let iteration = Iteration {
                start,
                // We want to trigger the internal limit
                limit: thread_rng().gen_range(NUM_BLOCKS_IN_STORAGE + 1..=u64::MAX),
                step: 1.into(),
                direction,
            };
            (storage, iteration, tx, rx)
        }

        #[rstest]
        #[tokio::test]
        async fn test_get_headers(
            #[values(Direction::Backward, Direction::Forward)] direction: Direction,
        ) {
            let (storage, iteration, tx, mut rx) = init_test(direction);
            get_headers(storage, BlockHeadersRequest { iteration }, tx.clone())
                .await
                .unwrap();

            let BlockHeadersResponse { parts } = rx.next().await.unwrap();
            assert_eq!(parts.len(), NUM_BLOCKS_IN_STORAGE as usize * 3 + 1);

            let chunked = parts.chunks_exact(3);
            let remainder = chunked.remainder();

            chunked.for_each(|chunk| {
                assert_matches!(&chunk[0], BlockHeadersResponsePart::Header(_));
                assert_matches!(&chunk[1], BlockHeadersResponsePart::Signatures(_));
                assert_eq!(chunk[2], BlockHeadersResponsePart::Fin(Fin::ok()));
            });
            // Expect Fin::too_much() if all requested items were found up to the internal limit
            assert_eq!(remainder, &[BlockHeadersResponsePart::Fin(Fin::too_much())]);
        }

        #[rstest]
        #[tokio::test]
        async fn test_get_bodies(
            #[values(Direction::Backward, Direction::Forward)] direction: Direction,
        ) {
            let (storage, iteration, tx, mut rx) = init_test(direction);
            let _jh = tokio::spawn(get_bodies(storage, BlockBodiesRequest { iteration }, tx));
            // 10 x [Diff, Classes*, Fin::ok()]
            for _ in 0..NUM_BLOCKS_IN_STORAGE {
                rx.next().await.unwrap(); // Diff
                match rx.next().await.unwrap().body_message {
                    // New classes in block
                    BlockBodyMessage::Classes(_) => {
                        assert_eq!(
                            rx.next().await.unwrap().body_message,
                            BlockBodyMessage::Fin(Fin::ok())
                        );
                    }
                    // No new classes in block
                    BlockBodyMessage::Fin(f) => {
                        assert_eq!(f, Fin::ok());
                    }
                    _ => panic!("unexpected message type"),
                }
            }
            // Expect Fin::too_much() where the first unavailable item would be
            assert_eq!(
                rx.next().await.unwrap().body_message,
                BlockBodyMessage::Fin(Fin::too_much())
            );
        }

        macro_rules! define_test {
            ($name:ident, $uut_name:ident, $request:tt, $reply:tt) => {
                #[rstest]
                #[tokio::test]
                async fn $name(
                    #[values(Direction::Backward, Direction::Forward)] direction: Direction,
                ) {
                    let (storage, iteration, tx, mut rx) = init_test(direction);
                    let _jh = tokio::spawn($uut_name(storage, $request { iteration }, tx));
                    for _ in 0..NUM_BLOCKS_IN_STORAGE {
                        rx.next().await.unwrap(); // Block data
                        rx.next().await.unwrap(); // Fin::ok()
                    }
                    // Expect Fin::too_much() where the first unavailable item would be
                    assert_eq!(rx.next().await.unwrap().kind, $reply::Fin(Fin::too_much()));
                }
            };
        }

        define_test!(
            test_get_transactions,
            get_transactions,
            TransactionsRequest,
            TransactionsResponseKind
        );
        define_test!(
            test_get_receipts,
            get_receipts,
            ReceiptsRequest,
            ReceiptsResponseKind
        );
        define_test!(
            test_get_events,
            get_events,
            EventsRequest,
            EventsResponseKind
        );
    }
}

/// Property tests, grouped to be immediately visible when executed
mod prop {
    use crate::p2p_network::client::types as simplified;
    use crate::p2p_network::sync_handlers;
    use crate::p2p_network::sync_handlers::def_into_dto;
    use futures::channel::mpsc;
    use futures::StreamExt;
    use p2p::client::types::{self as p2p_types, RawTransactionVariant, TryFromDto};
    use p2p_proto::block::{
        BlockBodiesRequest, BlockBodyMessage, BlockHeadersRequest, BlockHeadersResponse,
        BlockHeadersResponsePart,
    };
    use p2p_proto::common::{BlockId, BlockNumberOrHash, Error, Fin, Iteration};
    use p2p_proto::event::{EventsRequest, EventsResponseKind};
    use p2p_proto::receipt::{ReceiptsRequest, ReceiptsResponseKind};
    use p2p_proto::state::{Cairo0Class, Cairo1Class, Class};
    use p2p_proto::transaction::{TransactionsRequest, TransactionsResponseKind};
    use pathfinder_common::event::Event;
    use pathfinder_common::transaction::Transaction;
    use pathfinder_common::{
        BlockCommitmentSignature, BlockCommitmentSignatureElem, BlockHash, BlockNumber,
        TransactionHash,
    };
    use proptest::prelude::*;
    use std::collections::{BTreeSet, HashMap};
    use tokio::runtime::Runtime;

    #[macro_export]
    macro_rules! prop_assert_eq_sorted {
        ($left:expr, $right:expr) => {{
            let left = &$left;
            let right = &$right;
            let comparison_string = pretty_assertions_sorted::Comparison::new(
                &pretty_assertions_sorted::SortedDebug::new(left),
                &pretty_assertions_sorted::SortedDebug::new(right)
            ).to_string();
            proptest::prop_assert!(
                *left == *right,
                "assertion failed: `(left == right)`\n{comparison_string}\n");
        }};

        ($left:expr, $right:expr, $fmt:tt $($args:tt)*) => {{
            let left = &$left;
            let right = &$right;
            let comparison_string = pretty_assertions_sorted::Comparison::new(
                &pretty_assertions_sorted::SortedDebug::new(left),
                &pretty_assertions_sorted::SortedDebug::new(right)
            ).to_string();
            proptest::prop_assert!(
                *left == *right,
                concat!(
                    "assertion failed: `(left == right)`\n\
                    {}: ", $fmt),
                comparison_string $($args)*);
        }};
    }

    proptest! {
        #[test]
        fn get_headers((num_blocks, seed, start_block, limit, step, direction) in strategy::composite()) {
            // Fake storage with a given number of blocks
            let (storage, in_db) = fixtures::storage_with_seed(seed, num_blocks);
            // Compute the overlapping set between the db and the request
            // These are the headers that we expect to be read from the db
            let expected = overlapping::get(in_db, start_block, limit, step, num_blocks, direction)
                .into_iter().map(|(h, s, _, _, _, _)| (h.into(), s)).collect::<Vec<_>>();
            // Run the handler
            let request = BlockHeadersRequest { iteration: Iteration { start: BlockNumberOrHash::Number(start_block), limit, step, direction, } };
            // Reusing the runtime does not yield any performance gains
            let parts = Runtime::new().unwrap().block_on(async {
                let (tx, mut rx) = mpsc::channel(0);
                let getter_fut = sync_handlers::get_headers(storage, request, tx);

                // Waiting for both futures to run to completion is faster than spawning the getter
                // and awaiting the receiver (almost 1s for 100 iterations on Ryzen 3700X).
                // BTW, we cannot just await the getter and then the receiver
                // as there is backpressure (channel size 0) and we would deadlock.
                let (_, ret) = tokio::join!(getter_fut, rx.next());

                let BlockHeadersResponse { parts } = ret.unwrap();
                parts
            });
            // Empty reply in the test is only possible if the request does not overlap with storage
            // Invalid start and zero limit are tested in boundary_conditions::
            if expected.is_empty() {
                prop_assert_eq_sorted!(parts.len(), 1);
                prop_assert_eq_sorted!(parts[0].clone().into_fin().unwrap(), Fin::unknown());
            } else {
                // Group reply parts by block: [[hdr-0, fin-0], [hdr-1, fin-1], ...]
                let actual = parts.chunks_exact(3).map(|chunk| {
                    // Make sure block data is delimited
                    assert_eq!(chunk[2], BlockHeadersResponsePart::Fin(Fin::ok()));
                    // Extract the header
                    let h = p2p_types::BlockHeader::try_from(chunk[0].clone().into_header().unwrap()).unwrap();
                    // Extract the signature
                    let s = chunk[1].clone().into_signatures().unwrap();
                    assert_eq!(s.signatures.len(), 1);
                    let s = s.signatures.into_iter().next().unwrap();
                    let s = BlockCommitmentSignature {
                        r: BlockCommitmentSignatureElem(s.r),
                        s: BlockCommitmentSignatureElem(s.s),
                    };
                    (h, s)
                }).collect::<Vec<_>>();

                prop_assert_eq_sorted!(actual, expected);
            }
        }
    }

    proptest! {
        #[test]
        fn get_bodies((num_blocks, db_seed, start_block, limit, step, direction) in strategy::composite()) {
            use crate::p2p_network::sync_handlers::class_definition::{Cairo, Sierra};

            // Fake storage with a given number of blocks
            let (storage, in_db) = fixtures::storage_with_seed(db_seed, num_blocks);
            // Get the overlapping set between the db and the request
            let expected = overlapping::get(in_db, start_block, limit, step, num_blocks, direction);
            // Extract the expected state updates, definitions and classes from the overlapping set
            // in a form digestable for this test
            let expected = expected.into_iter()
                .map(|(header, _, _, state_update, cairo_defs, sierra_defs)|
                    (
                        // block number and hash
                        (header.number, header.hash),
                        (
                            // "simplified" state update, without an explicit list of declared and replaced classes
                            state_update.clone().into(),
                            // Cairo0 class definitions, parsed into p2p DTOs
                            cairo_defs.into_iter().map(|(_, d)| {
                                let def = serde_json::from_slice::<Cairo<'_>>(&d).unwrap();
                                def_into_dto::cairo(def)
                            }).collect(),
                            // Cairo1 (Sierra) class definitions, parsed into p2p DTOs
                            sierra_defs.into_iter().map(|(_, s, c)| {
                                let def = serde_json::from_slice::<Sierra<'_>>(&s).unwrap();
                                def_into_dto::sierra(def, c)
                            }).collect()
                        )
                    )
                ).collect::<HashMap<_, (p2p_types::StateUpdate, BTreeSet<Cairo0Class>, BTreeSet<Cairo1Class>)>>();
            // Run the handler
            let request = BlockBodiesRequest { iteration: Iteration { start: BlockNumberOrHash::Number(start_block), limit, step, direction, } };
            let replies = Runtime::new().unwrap().block_on(async {
                let (tx, rx) = mpsc::channel(0);
                let getter_fut = sync_handlers::get_bodies(storage, request, tx);
                let (_, replies) = tokio::join!(getter_fut, rx.collect::<Vec<_>>());
                replies
            });

            // Empty reply is only possible if the request does not overlap with storage
            // Invalid start and zero limit are tested in boundary_conditions::
            if expected.is_empty() {
                prop_assert_eq_sorted!(replies.len(), 1);
                prop_assert_eq_sorted!(replies[0].clone().into_fin().unwrap(), Fin::unknown());
            } else {
                // Collect replies into a set of (block_number, state_update, definitions)
                let mut actual = HashMap::new();
                let mut block_id = None;

                for reply in replies {
                    match reply.body_message {
                        BlockBodyMessage::Diff(d) => {
                            let BlockId { number, hash } = reply.id.unwrap();
                            block_id = Some((BlockNumber::new(number).unwrap(), BlockHash(hash.0)));

                            let state_update = p2p_types::StateUpdate::from(d);
                            actual.insert(block_id.unwrap(), (state_update, BTreeSet::new(), BTreeSet::new()));
                        },
                        BlockBodyMessage::Classes(c) => {
                            // Classes coming after a state diff should be for the same block
                            let entry = actual.get_mut(&block_id.expect("Classes follow Diff so current block id should be set")).unwrap();
                            c.classes.into_iter().for_each(|c| {
                                match c {
                                    Class::Cairo0(cairo) => entry.1.insert(cairo),
                                    Class::Cairo1(sierra) => entry.2.insert(sierra),
                                };
                            });
                        },
                        BlockBodyMessage::Fin(f) => {
                            match f.error {
                                // We either managed to fit the entire range or we hit the internal limit
                                None | Some(Error::TooMuch) => assert!(actual.contains_key(&block_id.unwrap())),
                                // Either the request yielded nothing or was only partially successful
                                Some(Error::Unknown) => {},
                                Some(_) => panic!("unexpected error"),
                            }
                        }
                        _ => unimplemented!(),
                    }
                }

                prop_assert_eq_sorted!(actual, expected);
            }
        }
    }

    mod workaround {
        use pathfinder_common::{TransactionNonce, TransactionVersion};
        use starknet_gateway_types::reply::transaction as gw;

        // Align with the deserialization workaround to avoid false negative mismatches
        pub fn for_legacy_l1_handlers(tx: gw::Transaction) -> gw::Transaction {
            match tx {
                gw::Transaction::Invoke(gw::InvokeTransaction::V0(tx))
                    if tx.entry_point_type == Some(gw::EntryPointType::L1Handler) =>
                {
                    gw::Transaction::L1Handler(gw::L1HandlerTransaction {
                        contract_address: tx.sender_address,
                        entry_point_selector: tx.entry_point_selector,
                        nonce: TransactionNonce::ZERO,
                        calldata: tx.calldata,
                        transaction_hash: tx.transaction_hash,
                        version: TransactionVersion::ZERO,
                    })
                }
                x => x,
            }
        }
    }

    proptest! {
        #[test]
        fn get_transactions((num_blocks, seed, start_block, limit, step, direction) in strategy::composite()) {
            // Fake storage with a given number of blocks
            let (storage, in_db) = fixtures::storage_with_seed(seed, num_blocks);
            // Compute the overlapping set between the db and the request
            // These are the transactions that we expect to be read from the db
            let expected = overlapping::get(in_db, start_block, limit, step, num_blocks, direction).into_iter()
                .map(|(h, _, tr, _, _, _)|
                    (
                        h.number,
                        h.hash,
                        tr.into_iter().map(|(t, _)| Transaction::from(workaround::for_legacy_l1_handlers(t)).variant.into()).collect::<Vec<_>>()
                    )
            ).collect::<Vec<_>>();
            // Run the handler
            let request = TransactionsRequest { iteration: Iteration { start: BlockNumberOrHash::Number(start_block), limit, step, direction, } };
            let replies = Runtime::new().unwrap().block_on(async {
                let (tx, rx) = mpsc::channel(0);
                let getter_fut = sync_handlers::get_transactions(storage, request, tx);
                let (_, replies) = tokio::join!(getter_fut, rx.collect::<Vec<_>>());
                replies
            });
            // Empty reply is only possible if the request does not overlap with storage
            // Invalid start and zero limit are tested in boundary_conditions::
            if expected.is_empty() {
                prop_assert_eq_sorted!(replies.len(), 1);
                prop_assert_eq_sorted!(replies[0].clone().into_fin().unwrap(), Fin::unknown());
            } else {
                // Group replies by block, it is assumed that transactions per block are small enough to fit under the 1MiB limit
                // This means that there are 2 replies per block: [[transactions-0, fin-0], [transactions-1, fin-1], ...]
                let actual = replies.chunks_exact(2).map(|replies | {
                    assert_eq!(replies[0].id, replies[1].id);
                    // Make sure block data is delimited
                    assert_eq!(replies[1].kind, TransactionsResponseKind::Fin(Fin::ok()));
                    // Extract transactions
                    let transactions = replies[0].kind.clone().into_transactions().unwrap().items;
                    let BlockId { number, hash } = replies[0].id.unwrap();
                    (
                        BlockNumber::new(number).unwrap(),
                        BlockHash(hash.0),
                        transactions.into_iter().map(|t| RawTransactionVariant::try_from_dto(t).unwrap()).collect::<Vec<_>>()
                    )
                }).collect::<Vec<_>>();

                prop_assert_eq_sorted!(actual, expected);
            }
        }
    }

    proptest! {
        #[test]
        fn get_receipts((num_blocks, seed, start_block, limit, step, direction) in strategy::composite()) {
            // Fake storage with a given number of blocks
            let (storage, in_db) = fixtures::storage_with_seed(seed, num_blocks);
            // Compute the overlapping set between the db and the request
            // These are the receipts that we expect to be read from the db
            let expected = overlapping::get(in_db, start_block, limit, step, num_blocks, direction).into_iter()
                .map(|(h, _, tr, _, _, _)|
                    (
                        h.number,
                        h.hash,
                        tr.into_iter().map(|(_, r)| r.into()).collect::<Vec<_>>()
                    )
            ).collect::<Vec<_>>();
            // Run the handler
            let request = ReceiptsRequest { iteration: Iteration { start: BlockNumberOrHash::Number(start_block), limit, step, direction, } };
            let replies = Runtime::new().unwrap().block_on(async {
                let (tx, rx) = mpsc::channel(0);
                let getter_fut = sync_handlers::get_receipts(storage, request, tx);
                let (_, replies) = tokio::join!(getter_fut, rx.collect::<Vec<_>>());
                replies
            });
            // Empty reply is only possible if the request does not overlap with storage
            // Invalid start and zero limit are tested in boundary_conditions::
            if expected.is_empty() {
                prop_assert_eq_sorted!(replies.len(), 1);
                prop_assert_eq_sorted!(replies[0].clone().into_fin().unwrap(), Fin::unknown());
            } else {
                // Group replies by block, it is assumed that receipts per block small enough to fit under the 1MiB limit
                // This means that there are 2 replies per block: [[receipts-0, fin-0], [receipts-1, fin-1], ...]
                let actual = replies.chunks_exact(2).map(|replies | {
                    assert_eq!(replies[0].id, replies[1].id);
                    // Make sure block data is delimited
                    assert_eq!(replies[1].kind, ReceiptsResponseKind::Fin(Fin::ok()));
                    // Extract receipts
                    let receipts = replies[0].kind.clone().into_receipts().unwrap().items;
                    let BlockId { number, hash } = replies[0].id.unwrap();
                    (
                        BlockNumber::new(number).unwrap(),
                        BlockHash(hash.0),
                        receipts.into_iter().map(|r| simplified::Receipt::try_from(r).unwrap()).collect::<Vec<_>>()
                    )
                }).collect::<Vec<_>>();

                prop_assert_eq_sorted!(actual, expected);
            }
        }
    }

    proptest! {
        #[test]
        fn get_events((num_blocks, seed, start_block, limit, step, direction) in strategy::composite()) {
            // Fake storage with a given number of blocks
            let (storage, in_db) = fixtures::storage_with_seed(seed, num_blocks);
            // Compute the overlapping set between the db and the request
            // These are the events that we expect to be read from the db
            // Extract tuples (block_number, block_hash, [events{txn#1}, events{txn#2}, ...])
            let expected = overlapping::get(in_db, start_block, limit, step, num_blocks, direction).into_iter()
                .map(|(h, _, tr, _, _, _)|{
                    let events = tr.into_iter().map(|(_, r)| (r.transaction_hash, r.events)).collect::<HashMap<_, Vec<_>>>();
                    (
                        h.number,
                        h.hash,
                        events
                    )}
            ).collect::<Vec<_>>();
            // Run the handler
            let request = EventsRequest { iteration: Iteration { start: BlockNumberOrHash::Number(start_block), limit, step, direction, } };
            let replies = Runtime::new().unwrap().block_on(async {
                let (tx, rx) = mpsc::channel(0);
                let getter_fut = sync_handlers::get_events(storage, request, tx);
                let (_, replies) = tokio::join!(getter_fut, rx.collect::<Vec<_>>());
                replies
            });
            // Empty reply is only possible if the request does not overlap with storage
            // Invalid start and zero limit are tested in boundary_conditions::
            if expected.is_empty() {
                prop_assert_eq_sorted!(replies.len(), 1);
                prop_assert_eq_sorted!(replies[0].clone().into_fin().unwrap(), Fin::unknown());
            } else {
                // Group replies by block, it is assumed that events per block small enough to fit under the 1MiB limit
                // This means that there are 2 replies per block: [[events-0, fin-0], [events-1, fin-1], ...]
                let actual = replies.chunks_exact(2).map(|replies | {
                    assert_eq!(replies[0].id, replies[1].id);
                    // Make sure block data is delimited
                    assert_eq!(replies[1].kind, EventsResponseKind::Fin(Fin::ok()));
                    let BlockId { number, hash } = replies[0].id.unwrap();
                    // Extract events
                    let mut events = HashMap::<_, Vec<_>>::new();
                    replies[0].kind.clone().into_events().unwrap().items.into_iter().for_each(|e| {
                        events.entry(TransactionHash(e.transaction_hash.0)).or_default().push(Event::try_from_dto(e).unwrap());
                    });
                    (
                        BlockNumber::new(number).unwrap(),
                        BlockHash(hash.0),
                        events
                    )
                }).collect::<Vec<_>>();

                prop_assert_eq_sorted!(actual, expected);
            }
        }
    }

    /// Fixtures for prop tests
    mod fixtures {
        use crate::p2p_network::sync_handlers::MAX_COUNT_IN_TESTS;
        use pathfinder_storage::fake::{with_n_blocks_and_rng, StorageInitializer};
        use pathfinder_storage::Storage;

        pub const MAX_NUM_BLOCKS: u64 = MAX_COUNT_IN_TESTS * 2;

        pub fn storage_with_seed(seed: u64, num_blocks: u64) -> (Storage, StorageInitializer) {
            use rand::SeedableRng;
            let storage = Storage::in_memory().unwrap();
            // Explicitly choose RNG to make sure seeded storage is always reproducible
            let mut rng = rand_chacha::ChaCha12Rng::seed_from_u64(seed);
            let initializer =
                with_n_blocks_and_rng(&storage, num_blocks.try_into().unwrap(), &mut rng);
            (storage, initializer)
        }
    }

    /// Find overlapping range between the DB and the request
    mod overlapping {
        use crate::p2p_network::sync_handlers::MAX_COUNT_IN_TESTS;
        use p2p_proto::common::{Direction, Step};
        use pathfinder_storage::fake::{StorageInitializer, StorageInitializerItem};

        pub fn get(
            from_db: StorageInitializer,
            start_block: u64,
            limit: u64,
            step: Step,
            num_blocks: u64,
            direction: Direction,
        ) -> StorageInitializer {
            match direction {
                Direction::Forward => forward(from_db, start_block, limit, step).collect(),
                Direction::Backward => {
                    backward(from_db, start_block, limit, step, num_blocks).collect()
                }
            }
        }

        fn forward(
            from_db: StorageInitializer,
            start_block: u64,
            limit: u64,
            step: Step,
        ) -> impl Iterator<Item = StorageInitializerItem> {
            from_db
                .into_iter()
                .skip(start_block.try_into().unwrap())
                .step_by(step.into_inner().try_into().unwrap())
                .take(std::cmp::min(limit, MAX_COUNT_IN_TESTS).try_into().unwrap())
        }

        fn backward(
            mut from_db: StorageInitializer,
            start_block: u64,
            limit: u64,
            step: Step,
            num_blocks: u64,
        ) -> impl Iterator<Item = StorageInitializerItem> {
            if start_block >= num_blocks {
                // The is no overlapping range but we want to keep the iterator type in this
                // branch type-consistent
                from_db.clear();
            }

            from_db
                .into_iter()
                .take((start_block + 1).try_into().unwrap())
                .rev()
                .step_by(step.into_inner().try_into().unwrap())
                .take(std::cmp::min(limit, MAX_COUNT_IN_TESTS).try_into().unwrap())
        }
    }

    /// Building blocks for the ultimate composite strategy used in all property tests
    mod strategy {
        use super::fixtures::MAX_NUM_BLOCKS;
        use crate::p2p_network::sync_handlers::tests::I64_MAX;
        use p2p_proto::common::{Direction, Step};
        use proptest::prelude::*;
        use std::ops::Range;

        prop_compose! {
            fn inside(range: Range<u64>)(x in range) -> u64 { x }
        }

        prop_compose! {
            fn outside_le(range: Range<u64>, max: u64)(x in range.end..=max) -> u64 { x }
        }

        fn rarely_outside_le(range: std::ops::Range<u64>, max: u64) -> BoxedStrategy<u64> {
            // Empty range will trigger a panic in rand::distributions::Uniform
            if range.is_empty() {
                return Just(range.start).boxed();
            }

            prop_oneof![
                // Occurrence 4:1
                4 => inside(range.clone()),
                1 => outside_le(range, max),
            ]
            .boxed()
        }

        fn rarely_outside(range: std::ops::Range<u64>) -> BoxedStrategy<u64> {
            rarely_outside_le(range, u64::MAX)
        }

        prop_compose! {
            pub fn composite()
                (num_blocks in 0..MAX_NUM_BLOCKS)
                (
                    num_blocks in Just(num_blocks),
                    storage_seed in any::<u64>(),
                    // out of range (> i64::MAX) start values are tested in `empty_reply::`
                    start in rarely_outside_le(0..num_blocks, I64_MAX),
                    // limit of 0 is tested in `empty_reply::`
                    limit in rarely_outside(1..num_blocks),
                    // step is always >= 1
                    step in rarely_outside(1..num_blocks / 4),
                    direction in prop_oneof![Just(Direction::Forward), Just(Direction::Backward)],
                ) -> (u64, u64, u64, u64, Step, Direction) {
                (num_blocks, storage_seed, start, limit, step.into(), direction)
            }
        }
    }
}

mod classes {
    use crate::p2p_network::sync_handlers::classes;
    use fake::{Fake, Faker};

    #[test]
    fn getter_error_yields_error() {
        let mut responses = vec![];
        assert!(classes(
            Faker.fake(),
            Faker.fake(),
            vec![Faker.fake()],
            &mut responses,
            |_, _| anyhow::bail!("getter failed"),
        )
        .is_err());
        assert!(responses.is_empty());
    }
}
