use crate::engine::domain::{AccountSnapshot, Balance, TransactionEvent, TransactionType};
use crate::engine::error::LedgerError;
use futures::future::BoxFuture;
use rust_decimal::Decimal;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::Mutex;

pub trait Aggregate {
    type Error;
    type ID: Send + Sync + Clone + PartialEq + PartialOrd + Hash + Eq;
    type TxID: Send + Sync + Clone + PartialEq + PartialOrd + Hash + Eq;
    type EventData: Send + Sync;
    type Snapshot: Send + Sync;
    fn new(id: Self::TxID, tx_data: Self::EventData) -> Self;
    fn apply_tx(&mut self, tx_id: Self::TxID, tx_data: Self::EventData) -> Result<(), Self::Error>;
    fn snapshot(&self, client_id: Self::ID) -> Self::Snapshot;
}

pub trait Ledger<A> {
    type Error;

    fn process_transaction(
        self: Arc<Self>,
        id: <A as Aggregate>::ID,
        tx_id: <A as Aggregate>::TxID,
        transaction: <A as Aggregate>::EventData,
    ) -> BoxFuture<'static, Result<<A as Aggregate>::ID, Self::Error>>
    where
        A: Aggregate + Send + Sync + 'static,
        <A as Aggregate>::TxID: Clone,
        <A as Aggregate>::EventData: Clone,
        <A as Aggregate>::Error: std::fmt::Display;

    fn snapshot(
        self: Arc<Self>,
        id: <A as Aggregate>::ID,
    ) -> BoxFuture<'static, Result<<A as Aggregate>::Snapshot, Self::Error>>
    where
        A: Aggregate + Send + Sync + 'static,
        <A as Aggregate>::ID: Clone;
}

pub struct InMemoryLedger<A>
where
    A: Aggregate + Clone + Send + Sync + 'static,
{
    view: Mutex<HashMap<<A as Aggregate>::ID, A>>,
}

impl<A> InMemoryLedger<A>
where
    A: Aggregate + Clone + Send + Sync + 'static,
{
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            view: Mutex::new(HashMap::new()),
        })
    }
}

impl<A> Ledger<A> for InMemoryLedger<A>
where
    A: Aggregate + Clone + Send + Sync + 'static,
{
    type Error = ();

    fn process_transaction(
        self: Arc<Self>,
        id: <A as Aggregate>::ID,
        tx_id: <A as Aggregate>::TxID,
        transaction: <A as Aggregate>::EventData,
    ) -> BoxFuture<'static, Result<<A as Aggregate>::ID, Self::Error>>
    where
        A: Aggregate + Send + Sync + 'static,
        <A as Aggregate>::TxID: Clone,
        <A as Aggregate>::EventData: Clone,
        <A as Aggregate>::Error: std::fmt::Display,
    {
        Box::pin(async move {
            self.view
                .lock()
                .await
                .entry(id.clone())
                .and_modify(|account| {
                    if let Err(err) = account.apply_tx(tx_id.clone(), transaction.clone()) {
                        eprintln!("Error processing transaction {err}");
                    }
                })
                .or_insert_with(|| <A as Aggregate>::new(tx_id, transaction));
            Ok(id)
        })
    }

    fn snapshot(
        self: Arc<Self>,
        id: <A as Aggregate>::ID,
    ) -> BoxFuture<'static, Result<<A as Aggregate>::Snapshot, Self::Error>>
    where
        A: Aggregate + Send + Sync + 'static,
        <A as Aggregate>::ID: Clone,
    {
        Box::pin(async move {
            match self.view.lock().await.get(&id) {
                Some(view) => Ok(view.snapshot(id)),
                None => Err(()),
            }
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Account {
    balance: Balance,
    transactions: HashMap<u32, TransactionEvent>,
    disputed_transactions: HashSet<u32>,
    previous_tx_id: u32,
    locked: bool,
}

impl Account {
    fn record_tx(&mut self, tx_id: u32, tx_data: TransactionEvent) {
        self.transactions.insert(tx_id, tx_data);
        self.previous_tx_id = tx_id;
    }

    fn get_tx(&self, tx_id: u32) -> Result<&TransactionEvent, LedgerError> {
        self.transactions
            .get(&tx_id)
            .ok_or(LedgerError::TransactionNotFound(tx_id))
    }

    fn check_tx_id(&self, tx_id: u32) -> Result<(), LedgerError> {
        if self.previous_tx_id < tx_id {
            Ok(())
        } else {
            Err(LedgerError::SuspiciousTransaction(tx_id))
        }
    }

    fn check_available_amount(&self, tx_amount: Decimal) -> Result<(), LedgerError> {
        if self.balance.available >= tx_amount {
            Ok(())
        } else {
            Err(LedgerError::InsufficientFunds {
                available: self.balance.available,
                amount: tx_amount,
            })
        }
    }

    fn check_disputed_transaction(&self, tx_id: u32, expected: bool) -> Result<(), LedgerError> {
        if self.disputed_transactions.contains(&tx_id) != expected {
            Err(LedgerError::DisputedTransaction(tx_id))
        } else {
            Ok(())
        }
    }

    fn locked_account(&self, tx_id: u32) -> Result<(), LedgerError> {
        if self.locked {
            Err(LedgerError::LockedAccount(tx_id))
        } else {
            Ok(())
        }
    }
}

impl Aggregate for Account {
    type Error = LedgerError;
    type ID = u16;
    type TxID = u32;
    type EventData = TransactionEvent;
    type Snapshot = AccountSnapshot;

    fn new(id: Self::TxID, tx_data: Self::EventData) -> Self {
        let balance = match tx_data.transaction_type {
            TransactionType::Deposit => Balance::new(tx_data.amount.unwrap()),
            _ => Balance::default(),
        };
        let mut account = Account {
            balance,
            transactions: HashMap::new(),
            disputed_transactions: HashSet::new(),
            previous_tx_id: id,
            locked: false,
        };
        account.record_tx(id, tx_data);
        account
    }

    fn apply_tx(&mut self, tx_id: Self::TxID, tx_data: Self::EventData) -> Result<(), Self::Error> {
        self.locked_account(tx_id)?;

        match tx_data.transaction_type {
            TransactionType::Deposit => {
                self.check_tx_id(tx_id)?;
                // TODO handle optional
                self.balance.available += tx_data.amount.unwrap();
                self.record_tx(tx_id, tx_data);
            }
            TransactionType::Withdrawal => {
                self.check_tx_id(tx_id)?;
                // TODO handle optional
                self.check_available_amount(tx_data.amount.unwrap())?;
                self.balance.available -= tx_data.amount.unwrap();
                self.record_tx(tx_id, tx_data);
            }
            TransactionType::Dispute => {
                self.check_disputed_transaction(tx_id, false)?;
                if let Some(disputed_amount) = self.get_tx(tx_id)?.amount {
                    self.check_available_amount(disputed_amount)?;
                    self.balance.available -= disputed_amount;
                    self.balance.held += disputed_amount;
                    self.disputed_transactions.insert(tx_id);
                }
            }
            TransactionType::Resolve => {
                self.check_disputed_transaction(tx_id, true)?;
                if let Some(disputed_amount) = self.get_tx(tx_id)?.amount {
                    if self.balance.held >= disputed_amount {
                        self.balance.held -= disputed_amount;
                        self.balance.available += disputed_amount;
                        self.disputed_transactions.remove(&tx_id);
                    }
                }
            }
            TransactionType::Chargeback => {
                self.check_disputed_transaction(tx_id, true)?;
                if let Some(disputed_amount) = self.get_tx(tx_id)?.amount {
                    if self.balance.held >= disputed_amount {
                        self.balance.held -= disputed_amount;
                        self.locked = true;
                        self.disputed_transactions.remove(&tx_id);
                    }
                }
            }
        }
        Ok(())
    }

    fn snapshot(&self, id: Self::ID) -> Self::Snapshot {
        AccountSnapshot {
            client_id: id,
            available: self.balance.available,
            held: self.balance.held,
            total: self.balance.available + self.balance.held,
            locked: self.locked,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_initial_deposit() {
        let tx_event = TransactionEvent {
            client_id: 1,
            tx_id: 1,
            transaction_type: TransactionType::Deposit,
            amount: Some(dec!(12.3456)),
        };

        let account = Account::new(1, tx_event.clone());
        let mut expected = Account {
            balance: Balance {
                available: dec!(12.3456),
                held: Decimal::default(),
            },
            transactions: HashMap::new(),
            disputed_transactions: HashSet::new(),
            previous_tx_id: 1,
            locked: false,
        };
        expected.record_tx(1, tx_event);
        assert_eq!(account, expected);
    }
}
