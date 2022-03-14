pub mod engine;
pub mod error_handler;
pub mod listener;

use std::future::Future;
use std::{fmt::Debug, sync::Arc};

use futures::{future::BoxFuture, FutureExt, StreamExt};
use tokio::io;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::engine::domain::AccountSnapshot;
use crate::{
    engine::{
        domain::TransactionEvent,
        ledger::{Account, Aggregate, InMemoryLedger, Ledger},
    },
    error_handler::LoggingErrorHandler,
    listener::{
        handler::{Dispatcher, DispatcherHandler, DispatcherHandlerRx},
        update::UpdateWithCx,
        UpdateListener,
    },
};

pub trait SnapshotHandler {
    fn handle(self: Arc<Self>, snapshot: Vec<AccountSnapshot>) -> BoxFuture<'static, ()>
    where
        AccountSnapshot: Send + 'static;
}

impl<F, Fut> SnapshotHandler for F
where
    F: Fn(Vec<AccountSnapshot>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    fn handle(self: Arc<Self>, snapshot: Vec<AccountSnapshot>) -> BoxFuture<'static, ()>
    where
        AccountSnapshot: Send + 'static,
    {
        Box::pin(async move { self(snapshot).await })
    }
}

pub struct TransactionDispatcher<L, H> {
    ledger: Arc<L>,
    handler: Arc<H>,
}

impl<A, H> TransactionDispatcher<InMemoryLedger<A>, H>
where
    A: Aggregate + Clone + Send + Sync + 'static,
    H: SnapshotHandler + Send + Sync + 'static,
{
    pub fn new(handler: H) -> Self {
        Self {
            ledger: InMemoryLedger::new(),
            handler: Arc::new(handler),
        }
    }
}

impl<H> DispatcherHandler<TransactionEvent> for TransactionDispatcher<InMemoryLedger<Account>, H>
where
    H: SnapshotHandler + Send + Sync + 'static,
{
    fn handle(self, updates: DispatcherHandlerRx<TransactionEvent>) -> BoxFuture<'static, ()>
    where
        UpdateWithCx<TransactionEvent>: Send + 'static,
    {
        let this = Arc::new(self);
        let other = Arc::clone(&this);
        UnboundedReceiverStream::new(updates)
            .for_each(move |cx| {
                let ledger = Arc::clone(&this.ledger);
                async move {
                    if (Arc::clone(&ledger)
                        .process_transaction(cx.update.client_id, cx.update.tx_id, cx.update)
                        .await)
                        .is_err()
                    {
                        eprintln!("failed to process event")
                    }
                }
            })
            .then(move |()| {
                let this = Arc::clone(&other);
                async move {
                    if let Ok(snapshot) = Arc::clone(&this.ledger).all_snapshots().await {
                        Arc::clone(&this.handler).handle(snapshot).await;
                    }
                }
            })
            .boxed()
    }
}

pub async fn pipeline<'a, L, ListenerErr, H, Fut>(listener: L, handler: H)
where
    L: UpdateListener<ListenerErr> + Send + 'a,
    ListenerErr: Debug,
    H: Fn(Vec<AccountSnapshot>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    Dispatcher::new()
        .messages_handler(TransactionDispatcher::new(handler))
        .dispatch_with_listener(
            listener,
            LoggingErrorHandler::with_custom_text("An error from the update listener"),
        )
        .await;
}

/// Handler function that writes the account snapshots as a CSV to stdout.
pub async fn to_std_out(snapshot: Vec<AccountSnapshot>) {
    let mut wri = csv_async::AsyncWriterBuilder::new()
        .has_headers(true)
        .create_serializer(io::stdout());

    for data in snapshot {
        wri.serialize(&data).await.unwrap();
    }
}
