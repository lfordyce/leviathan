use futures::{stream, StreamExt};
use lazy_static::lazy_static;
use leviathan::engine::domain::{AccountSnapshot, TransactionEvent, TransactionType};
use leviathan::engine::ledger::{Account, InMemoryLedger};
use leviathan::listener::handler::DispatcherHandler;
use leviathan::listener::update::UpdateWithCx;
use leviathan::{TransactionDispatcher, TransactionDispatcherHandler};
use rust_decimal_macros::dec;
use tokio::{
    sync::{mpsc, Mutex},
    time::Duration,
};

#[tokio::test]
async fn test_updates_from_transaction_dispatcher() {
    let dispatcher = TransactionDispatcher::<InMemoryLedger<Account>, _>::new(
        |snapshot: Vec<AccountSnapshot>| async move {
            for data in snapshot {
                println!("{:?}", data);
            }
        },
    );

    let updates = stream::iter(
        vec![
            TransactionEvent {
                client_id: 1,
                tx_id: 1,
                transaction_type: TransactionType::Deposit,
                amount: Some(dec!(50.0001)),
            },
            TransactionEvent {
                client_id: 1,
                tx_id: 2,
                transaction_type: TransactionType::Deposit,
                amount: Some(dec!(50.0001)),
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
}
