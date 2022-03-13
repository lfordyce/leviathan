use crate::engine::domain::TransactionEvent;
use crate::error_handler::ErrorHandler;
use crate::listener::update::UpdateWithCx;
use crate::listener::UpdateListener;
use futures::future::BoxFuture;
use futures::{stream::FuturesUnordered, StreamExt};
use std::future::Future;
use std::{fmt::Debug, sync::Arc};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::{sync::mpsc, task::JoinHandle, time::timeout};

pub type DispatcherHandlerRx<Upd> = UnboundedReceiver<UpdateWithCx<Upd>>;

type Tx<Upd> = Option<mpsc::UnboundedSender<UpdateWithCx<Upd>>>;

pub trait DispatcherHandler<Upd> {
    fn handle(self, updates: DispatcherHandlerRx<Upd>) -> BoxFuture<'static, ()>
    where
        UpdateWithCx<Upd>: Send + 'static;
}

impl<Upd, F, Fut> DispatcherHandler<Upd> for F
where
    F: FnOnce(DispatcherHandlerRx<Upd>) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    fn handle(self, updates: DispatcherHandlerRx<Upd>) -> BoxFuture<'static, ()>
    where
        UpdateWithCx<Upd>: Send + 'static,
    {
        Box::pin(async move { self(updates).await })
    }
}

pub struct Dispatcher {
    messages_queue: Tx<TransactionEvent>,
    running_handlers: FuturesUnordered<JoinHandle<()>>,
}

impl Dispatcher {
    /// Constructs a new dispatcher with the specified `requester`.
    pub fn new() -> Self {
        Self {
            messages_queue: None,
            running_handlers: FuturesUnordered::new(),
        }
    }

    fn new_tx<H, Upd>(&mut self, h: H) -> Tx<Upd>
    where
        H: DispatcherHandler<Upd> + Send + 'static,
        Upd: Send + 'static,
    {
        let (tx, rx) = mpsc::unbounded_channel();
        let join_handle = tokio::spawn(h.handle(rx));

        self.running_handlers.push(join_handle);

        Some(tx)
    }

    pub fn messages_handler<H>(mut self, h: H) -> Self
    where
        H: DispatcherHandler<TransactionEvent> + 'static + Send,
    {
        self.messages_queue = self.new_tx(h);
        self
    }

    pub async fn dispatch_with_listener<'a, UListener, ListenerE, Eh>(
        &'a mut self,
        mut update_listener: UListener,
        update_listener_error_handler: Arc<Eh>,
    ) where
        UListener: UpdateListener<ListenerE> + 'a,
        Eh: ErrorHandler<ListenerE> + 'a,
        ListenerE: Debug,
    {
        {
            let stream = update_listener.as_stream();
            tokio::pin!(stream);
            while let Some(upd) = stream.next().await {
                self.process_update(upd, &update_listener_error_handler)
                    .await;
            }
        }
        self.wait_for_handlers().await;
    }

    async fn process_update<ListenerE, Eh>(
        &self,
        update: Result<TransactionEvent, ListenerE>,
        update_listener_error_handler: &Arc<Eh>,
    ) where
        Eh: ErrorHandler<ListenerE>,
        ListenerE: Debug,
    {
        {
            // println!("Dispatcher received an update");
            let update = match update {
                Ok(update) => update,
                Err(error) => {
                    Arc::clone(update_listener_error_handler)
                        .handle_error(error)
                        .await;
                    return;
                }
            };

            send(&self.messages_queue, update)
        }
    }

    async fn wait_for_handlers(&mut self) {
        self.messages_queue.take();
        self.running_handlers.by_ref().for_each(|_| async {}).await;
    }
}

fn send<Upd>(tx: &Tx<Upd>, update: Upd)
where
    Upd: Debug,
{
    if let Some(tx) = tx {
        if let Err(error) = tx.send(UpdateWithCx { update }) {
            eprintln!(
                "The RX part of the channel is closed, but an update is received.\nError:{}\n",
                error
            );
        }
    }
}
