pub mod engine;
pub mod error_handler;
pub mod listener;

use crate::engine::domain::TransactionEvent;
use crate::engine::ledger::{Account, Aggregate, InMemoryLedger, Ledger};
use crate::error_handler::LoggingErrorHandler;
use crate::listener::handler::{Dispatcher, DispatcherHandler, DispatcherHandlerRx};
use crate::listener::update::UpdateWithCx;
use crate::listener::UpdateListener;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use lazy_static::lazy_static;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{fmt::Debug, sync::Arc};
use tokio::io;
use tokio_stream::wrappers::UnboundedReceiverStream;

lazy_static! {
    static ref MESSAGE_COUNT: AtomicU64 = AtomicU64::new(0);
}

pub struct TransactionDispatcher<L> {
    ledger: Arc<L>,
}

impl<A> Default for TransactionDispatcher<InMemoryLedger<A>>
where
    A: Aggregate + Clone + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<A> TransactionDispatcher<InMemoryLedger<A>>
where
    A: Aggregate + Clone + Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self {
            ledger: InMemoryLedger::new(),
        }
    }
}

impl DispatcherHandler<TransactionEvent> for TransactionDispatcher<InMemoryLedger<Account>> {
    fn handle(self, updates: DispatcherHandlerRx<TransactionEvent>) -> BoxFuture<'static, ()>
    where
        UpdateWithCx<TransactionEvent>: Send + 'static,
    {
        let this = Arc::new(self);
        UnboundedReceiverStream::new(updates)
            .for_each(move |cx| {
                let this = Arc::clone(&this);
                async move {
                    if let Ok(account_id) = Arc::clone(&this.ledger)
                        .process_transaction(cx.update.client_id, cx.update.tx_id, cx.update)
                        .await
                    {
                        if let Ok(snapshot) = Arc::clone(&this.ledger).snapshot(account_id).await {
                            // unfortunate hack since updates are being streamed we need to
                            // disable CSV headers after the first message
                            let previous = MESSAGE_COUNT.fetch_add(1, Ordering::Relaxed);
                            let mut wri = if previous == 0 {
                                csv_async::AsyncWriterBuilder::new()
                                    .has_headers(true)
                                    .create_serializer(io::stdout())
                            } else {
                                csv_async::AsyncWriterBuilder::new()
                                    .has_headers(false)
                                    .create_serializer(io::stdout())
                            };
                            wri.serialize(&snapshot).await.unwrap();
                        }
                    }
                }
            })
            .boxed()
    }
}

pub async fn pipeline<'a, L, ListenerE>(listener: L)
where
    L: UpdateListener<ListenerE> + Send + 'a,
    ListenerE: Debug,
{
    Dispatcher::new()
        .messages_handler(TransactionDispatcher::new())
        .dispatch_with_listener(
            listener,
            LoggingErrorHandler::with_custom_text("An error from the update listener"),
        )
        .await;
}
