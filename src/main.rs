use futures::{SinkExt, StreamExt};
use leviathan::listener::UpdateListener;
use leviathan::{engine::domain::TransactionEvent, listener::polling, pipeline};
use std::env;
use std::error::Error;
use tokio::fs::File;
use tokio::io;
use tokio_util::codec::{BytesCodec, FramedRead, FramedWrite, LinesCodec};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let path = match env::args().nth(1) {
        Some(arg) => arg,
        None => return Err(From::from("expected 1 argument, but got none")),
    };

    // let listener = polling(path).await;
    pipeline(polling(path).await).await;
    // test_listener(listener).await;

    // let mut wri = csv_async::AsyncSerializer::from_writer(io::stdout());
    //
    // let mut rdr = csv_async::AsyncReaderBuilder::new()
    //     .flexible(true)
    //     .trim(csv_async::Trim::All)
    //     .create_deserializer(File::open(path).await?);
    //
    // let mut records = rdr.deserialize::<TransactionEvent>();
    //
    // while let Some(record) = records.next().await {
    //     let mut record: TransactionEvent = record?;
    //     wri.serialize(&record).await?;
    // }

    // let file = File::open(path).await.unwrap();
    // let mut lines = FramedRead::new(file, LinesCodec::new());
    // let mut stdout = FramedWrite::new(io::stdout(), LinesCodec::new());
    // while let Some(result) = lines.next().await {
    //     match result {
    //         Ok(line) => {
    //             println!("LINE: {:?}", line);
    //             let mut rdr = csv::Reader::from_reader(line.as_bytes());
    //             for result in rdr.deserialize::<Transaction>() {
    //                 match result {
    //                     Ok(transaction) => {
    //                         let line = format!("LOG: read a line: {:?}\n", transaction);
    //                         let _ = stdout.send(line).await;
    //                     }
    //                     Err(error) => {
    //                         eprintln!("failed to deserialize record, {error}");
    //                     }
    //                 }
    //             }
    //         }
    //         Err(error) => {
    //             eprintln!("failed to read line, {error}");
    //         }
    //     }
    // }
    Ok(())
}

pub async fn test_listener<'a, UListener>(mut update_listener: UListener)
where
    UListener: UpdateListener<csv_async::Error> + 'a,
{
    let mut wri = csv_async::AsyncSerializer::from_writer(io::stdout());

    let stream = update_listener.as_stream();
    tokio::pin!(stream);
    while let Some(item) = stream.next().await {
        let mut record = item.unwrap();

        wri.serialize(&record).await.unwrap();
    }
}
