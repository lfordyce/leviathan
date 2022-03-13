pub mod engine;
pub mod error_handler;
pub mod listener;

use crate::engine::domain::TransactionEvent;
use crate::engine::ledger::{Account, Aggregate, InMemoryLedger, Ledger};
use crate::error_handler::{LoggingErrorHandler, OnError};
use crate::listener::handler::{Dispatcher, DispatcherHandler, DispatcherHandlerRx};
use crate::listener::update::UpdateWithCx;
use crate::listener::UpdateListener;
use futures::future::BoxFuture;
use futures::{FutureExt, StreamExt};
use std::borrow::Borrow;
use std::{fmt::Debug, future::Future, sync::Arc};
use tokio::io;
use tokio_stream::wrappers::UnboundedReceiverStream;

pub struct TransactionDispatcher<L> {
    ledger: Arc<L>,
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

// impl<A> DispatcherHandler<TransactionEvent> for TransactionDispatcher<InMemoryLedger<A>>
// where
//     A: Aggregate + Clone + Send + Sync + 'static,
//     <A as Aggregate>::EventData: Clone,
// {
//     fn handle(self, updates: DispatcherHandlerRx<TransactionEvent>) -> BoxFuture<'static, ()>
//     where
//         UpdateWithCx<TransactionEvent>: Send + 'static,
//     {
//         let this = Arc::new(self);
//         UnboundedReceiverStream::new(updates)
//             .for_each(move |cx| {
//                 let this = Arc::clone(&this);
//                 async move {
//                     let _ = this
//                         .ledger
//                         .process_transaction(cx.update.client_id, cx.update.tx_id, cx.update)
//                         .await;
//                 }
//             })
//             .boxed()
//     }
// }

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
                            let mut wri = csv_async::AsyncSerializer::from_writer(io::stdout());
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

// pub async fn pipeline<'a, H, Fut, E, L, ListenerE>(handler: H, listener: L)
// where
//     H: Fn(UpdateWithCx<TransactionEvent>) -> Fut + Send + Sync + 'static,
//     Fut: Future<Output = Result<(), E>> + Send + 'static,
//     L: UpdateListener<ListenerE> + Send + 'a,
//     ListenerE: Debug,
//     Result<(), E>: OnError<E>,
//     E: Debug + Send,
// {
//     let handler = Arc::new(handler);
//
//     Dispatcher::new()
//         .messages_handler(|rx: DispatcherHandlerRx<TransactionEvent>| {
//             UnboundedReceiverStream::new(rx).for_each(move |message| {
//                 let handler = Arc::clone(&handler);
//
//                 async move {
//                     handler(message).await.log_on_error().await;
//                 }
//             })
//         })
//         .dispatch_with_listener(
//             listener,
//             LoggingErrorHandler::with_custom_text("An error from the update listener"),
//         )
//         .await;
// }
