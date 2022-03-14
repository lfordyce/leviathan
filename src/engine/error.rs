use rust_decimal::Decimal;
use thiserror::Error;

/// Error enum.
#[derive(Debug, Error)]
pub enum LedgerError {
    #[error("Transaction occurred for locked account. Transaction: `{0}` was ignored")]
    LockedAccount(u32),
    #[error("Failed to lookup transaction with ID: `{0}`")]
    TransactionNotFound(u32),
    #[error("The account does not have sufficient funds. Available {available:?}, Transaction amount {amount:?}")]
    InsufficientFunds { available: Decimal, amount: Decimal },
    #[error("Transaction: `{0}` is disputed")]
    DisputedTransaction(u32),
    #[error("Transaction ID: `{0}` is lower than previously recorded")]
    SuspiciousTransaction(u32),
    #[error("Associated Transaction `{0}` is missing an amount when one is expected")]
    MissingAmount(u32),
}
