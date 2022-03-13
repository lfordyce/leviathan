use futures::future::BoxFuture;
use std::{convert::Infallible, fmt::Debug, future::Future, sync::Arc};

/// An asynchronous handler of an error.
///
/// See [the module-level documentation for the design
/// overview](crate::dispatching).
pub trait ErrorHandler<E> {
    fn handle_error(self: Arc<Self>, error: E) -> BoxFuture<'static, ()>;
}

impl<E, F, Fut> ErrorHandler<E> for F
where
    F: Fn(E) -> Fut + Send + Sync + 'static,
    E: Send + 'static,
    Fut: Future<Output = ()> + Send,
{
    fn handle_error(self: Arc<Self>, error: E) -> BoxFuture<'static, ()> {
        Box::pin(async move { self(error).await })
    }
}

pub trait OnError<E> {
    fn on_error<'a, Eh>(self, eh: Arc<Eh>) -> BoxFuture<'a, ()>
    where
        Self: 'a,
        Eh: ErrorHandler<E> + Send + Sync,
        Arc<Eh>: 'a;

    /// A shortcut for `.on_error(LoggingErrorHandler::new())`.
    fn log_on_error<'a>(self) -> BoxFuture<'a, ()>
    where
        Self: Sized + 'a,
        E: Debug,
    {
        self.on_error(LoggingErrorHandler::new())
    }
}

impl<T, E> OnError<E> for Result<T, E>
where
    T: Send,
    E: Send,
{
    fn on_error<'a, Eh>(self, eh: Arc<Eh>) -> BoxFuture<'a, ()>
    where
        Self: 'a,
        Eh: ErrorHandler<E> + Send + Sync,
        Arc<Eh>: 'a,
    {
        Box::pin(async move {
            if let Err(error) = self {
                eh.handle_error(error).await;
            }
        })
    }
}

pub struct LoggingErrorHandler {
    text: String,
}

impl LoggingErrorHandler {
    /// Creates `LoggingErrorHandler` with a meta text before a log.
    ///
    /// The logs will be printed in this format: `{text}: {:?}`.
    pub fn with_custom_text<T>(text: T) -> Arc<Self>
    where
        T: Into<String>,
    {
        Arc::new(Self { text: text.into() })
    }

    /// A shortcut for
    /// `LoggingErrorHandler::with_custom_text("Error".to_owned())`.
    pub fn new() -> Arc<Self> {
        Self::with_custom_text("Error".to_owned())
    }
}

impl<E> ErrorHandler<E> for LoggingErrorHandler
where
    E: Debug,
{
    fn handle_error(self: Arc<Self>, error: E) -> BoxFuture<'static, ()> {
        println!("{text}: {:?}", error, text = self.text);
        Box::pin(async {})
    }
}
