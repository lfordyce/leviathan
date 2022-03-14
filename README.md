# Leviathan
Simple toy payments engine that reads a series of transactions from a CSV, updates client accounts, handles disputes and chargebacks, and then outputs the state of accounts as a CSV.

## Installation
```shell
git clone git@github.com:lfordyce/leviathan.git
```

## Execution
```shell
cargo run -- transactions.csv > accounts.csv
```
- Alternatively run in release mode:
```shell
cargo run --release -- transactions.csv >accounts.csv
```

## Error Handling
- When an illegal action occurs, for example a transaction attempting to withdrawal more funds than available, the transaction will not be applied to the account and errors will output to `stderr`.
- To capture account balances and errors separately, run the following:
```shell
cargo run -- 2> error.log 1> accounts.csv
```

## Testing
- Run unit tests
```shell
cargo test
```

## Highlights
 - **Generic and Modular.** Functional design along with the [Rust] typesystem, Leviathan could be configured to listen on a TCP Stream for transaction events. (_currently setup to only read events from a csv file_)
 - **Functional reactive design.** Utilizing the [Tokio] runtime, the Leviathan engine asynchronously streams in transaction events to update an internal account ledger.
 
[Rust]: https://www.rust-lang.org/
[Tokio]: https://tokio.rs/