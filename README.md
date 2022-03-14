# Leviathan
Simple toy payments engine that reads a series of transactions from a CSV, updates client accounts, handles disputes and chargebacks, and then outputs the state of clients accounts as a CSV.

## Installation
```shell
git clone git@github.com:lfordyce/leviathan.git
```

## Execution
```shell
cargo run -- transactions.csv > accounts.csv
```