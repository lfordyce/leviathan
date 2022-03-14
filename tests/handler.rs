use futures::{stream, StreamExt};
use lazy_static::lazy_static;
use leviathan::engine::domain::{AccountSnapshot, TransactionEvent, TransactionType};
use leviathan::engine::ledger::{Account, InMemoryLedger};
use leviathan::listener::handler::DispatcherHandler;
use leviathan::listener::update::UpdateWithCx;
use leviathan::TransactionDispatcher;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokio::{
    sync::{mpsc, Mutex},
    time::Duration,
};

#[tokio::test]
async fn test_updates_from_transaction_dispatcher() {
    #[derive(Eq, PartialEq, Debug)]
    struct Update {
        locked: bool,
        held: Decimal,
        total: Decimal,
        available: Decimal,
    }

    lazy_static! {
        static ref SEQ1: Mutex<Vec<Update>> = Mutex::new(Vec::new());
        static ref SEQ2: Mutex<Vec<Update>> = Mutex::new(Vec::new());
        static ref SEQ3: Mutex<Vec<Update>> = Mutex::new(Vec::new());
    }

    let dispatcher = TransactionDispatcher::<InMemoryLedger<Account>, _>::new(
        |snapshot: Vec<AccountSnapshot>| async move {
            for data in snapshot {
                match data {
                    AccountSnapshot {
                        client_id: 1,
                        available,
                        held,
                        total,
                        locked,
                    } => {
                        SEQ1.lock().await.push(Update {
                            locked,
                            held,
                            total,
                            available,
                        });
                    }
                    AccountSnapshot {
                        client_id: 2,
                        available,
                        held,
                        total,
                        locked,
                    } => {
                        SEQ2.lock().await.push(Update {
                            locked,
                            held,
                            total,
                            available,
                        });
                    }
                    AccountSnapshot {
                        client_id: 3,
                        available,
                        held,
                        total,
                        locked,
                    } => {
                        SEQ3.lock().await.push(Update {
                            locked,
                            held,
                            total,
                            available,
                        });
                    }
                    _ => unreachable!(),
                }
            }
        },
    );

    let updates = stream::iter(
        vec![
            TransactionEvent {
                client_id: 1,
                tx_id: 1,
                transaction_type: TransactionType::Deposit,
                amount: Some(dec!(55467.44)),
            },
            TransactionEvent {
                client_id: 1,
                tx_id: 2,
                transaction_type: TransactionType::Deposit,
                amount: Some(dec!(547.44)),
            },
            TransactionEvent {
                client_id: 3,
                tx_id: 4,
                transaction_type: TransactionType::Deposit,
                amount: Some(dec!(5577.6)),
            },
            TransactionEvent {
                client_id: 2,
                tx_id: 3,
                transaction_type: TransactionType::Deposit,
                amount: Some(dec!(2344)),
            },
            TransactionEvent {
                client_id: 3,
                tx_id: 7,
                transaction_type: TransactionType::Withdrawal,
                amount: Some(dec!(334.756)),
            },
            TransactionEvent {
                client_id: 1,
                tx_id: 9,
                transaction_type: TransactionType::Withdrawal,
                amount: Some(dec!(752.56)),
            },
            TransactionEvent {
                client_id: 1,
                tx_id: 9,
                transaction_type: TransactionType::Dispute,
                amount: None,
            },
            TransactionEvent {
                client_id: 3,
                tx_id: 11,
                transaction_type: TransactionType::Deposit,
                amount: Some(dec!(4446.23)),
            },
            TransactionEvent {
                client_id: 3,
                tx_id: 13,
                transaction_type: TransactionType::Withdrawal,
                amount: Some(dec!(45.768)),
            },
            TransactionEvent {
                client_id: 3,
                tx_id: 13,
                transaction_type: TransactionType::Dispute,
                amount: None,
            },
            TransactionEvent {
                client_id: 1,
                tx_id: 15,
                transaction_type: TransactionType::Deposit,
                amount: Some(dec!(6759.754)),
            },
            TransactionEvent {
                client_id: 3,
                tx_id: 13,
                transaction_type: TransactionType::Resolve,
                amount: None,
            },
            TransactionEvent {
                client_id: 3,
                tx_id: 17,
                transaction_type: TransactionType::Withdrawal,
                amount: Some(dec!(657.43)),
            },
            TransactionEvent {
                client_id: 3,
                tx_id: 17,
                transaction_type: TransactionType::Dispute,
                amount: None,
            },
            TransactionEvent {
                client_id: 2,
                tx_id: 18,
                transaction_type: TransactionType::Deposit,
                amount: Some(dec!(4346.43)),
            },
            TransactionEvent {
                client_id: 1,
                tx_id: 19,
                transaction_type: TransactionType::Withdrawal,
                amount: Some(dec!(456)),
            },
            TransactionEvent {
                client_id: 3,
                tx_id: 17,
                transaction_type: TransactionType::Chargeback,
                amount: None,
            },
            TransactionEvent {
                client_id: 1,
                tx_id: 20,
                transaction_type: TransactionType::Withdrawal,
                amount: Some(dec!(111)),
            },
        ]
        .into_iter()
        .map(|update| UpdateWithCx { update })
        .collect::<Vec<UpdateWithCx<TransactionEvent>>>(),
    );

    let (tx, rx) = mpsc::unbounded_channel();

    updates
        .for_each(move |update| {
            let tx = tx.clone();
            async move {
                if tx.send(update).is_err() {
                    panic!("tx.send(update) failed");
                }
            }
        })
        .await;

    dispatcher.handle(rx).await;
    // Wait until our futures to be finished.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        *SEQ1.lock().await,
        vec![Update {
            locked: false,
            held: dec!(752.56),
            total: dec!(61455.074),
            available: dec!(60702.514),
        }]
    );
    assert_eq!(
        *SEQ2.lock().await,
        vec![Update {
            locked: false,
            held: dec!(0),
            total: dec!(6690.43),
            available: dec!(6690.43),
        }]
    );
    assert_eq!(
        *SEQ3.lock().await,
        vec![Update {
            locked: true,
            held: dec!(0.00),
            total: dec!(8328.446),
            available: dec!(8328.446),
        }]
    );
}
