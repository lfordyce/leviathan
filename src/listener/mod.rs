pub mod handler;
pub mod update;

use crate::engine::domain::TransactionEvent;
use futures::Stream;
use std::path::Path;
use tokio::fs::File;
use tokio::io;

pub trait UpdateListener<E>: for<'a> AsUpdateStream<'a, E> {}

pub trait AsUpdateStream<'a, E> {
    type Stream: Stream<Item = Result<TransactionEvent, E>> + Send + 'a;

    /// Creates the update [`Stream`].
    ///
    /// [`Stream`]: AsUpdateStream::Stream
    fn as_stream(&'a mut self) -> Self::Stream;
}

pub struct StatefulListener<St, Assf> {
    /// The state of the listener.
    pub state: St,

    /// The function used as [`AsUpdateStream::as_stream`].
    ///
    /// Must implement `for<'a> FnMut(&'a mut St) -> impl Stream + 'a`.
    pub stream: Assf,
}

impl<St, Assf> StatefulListener<St, Assf> {
    pub fn new(state: St, stream: Assf) -> Self {
        Self { state, stream }
    }
}

impl<'a, St, Assf, Strm, E> AsUpdateStream<'a, E> for StatefulListener<St, Assf>
where
    (St, Strm): 'a,
    Strm: Send,
    Assf: FnMut(&'a mut St) -> Strm,
    Strm: Stream<Item = Result<TransactionEvent, E>>,
{
    type Stream = Strm;

    fn as_stream(&'a mut self) -> Self::Stream {
        (self.stream)(&mut self.state)
    }
}

impl<St, Assf, E> UpdateListener<E> for StatefulListener<St, Assf> where
    Self: for<'a> AsUpdateStream<'a, E>
{
}

pub async fn polling<T>(filename: T) -> impl UpdateListener<csv_async::Error>
where
    T: AsRef<Path>,
{
    let resource = File::open(filename).await.unwrap();

    struct State<R: io::AsyncRead + Unpin + Send> {
        reader: csv_async::AsyncDeserializer<R>,
    }

    fn stream<T>(
        st: &mut State<T>,
    ) -> impl Stream<Item = Result<TransactionEvent, csv_async::Error>> + Send + '_
    where
        T: io::AsyncRead + Unpin + Send,
    {
        st.reader.deserialize::<TransactionEvent>()
    }

    let state = State {
        reader: csv_async::AsyncReaderBuilder::new()
            .flexible(true)
            .trim(csv_async::Trim::All)
            .create_deserializer(resource),
    };

    StatefulListener::new(state, stream)
}
