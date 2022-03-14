use std::{env, error::Error};

use leviathan::{listener::polling, pipeline};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let path = match env::args().nth(1) {
        Some(arg) => arg,
        None => return Err(From::from("expected 1 argument, but got none")),
    };

    pipeline(polling(path).await).await;
    Ok(())
}
