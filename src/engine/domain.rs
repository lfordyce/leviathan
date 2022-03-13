use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Transaction type enum
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
pub struct TransactionEvent {
    /// Client ID
    #[serde(rename = "client")]
    pub client_id: u16,
    /// Transaction ID
    #[serde(rename = "tx")]
    pub tx_id: u32,
    /// Transaction type ( deposit, withdrawal, dispute, etc.)
    #[serde(rename = "type")]
    pub transaction_type: TransactionType,
    /// Transaction amount, if withdrawal or deposit type
    pub amount: Option<Decimal>,
}

/// Balance for the account
#[derive(Debug, Default, PartialEq, Clone)]
pub struct Balance {
    /// The total funds that are available. This should be equal to the total - held amounts
    pub available: Decimal,
    /// The total funds that are held for dispute. This should be equal to total - available amounts
    pub held: Decimal,
}

impl Balance {
    pub fn new(available: Decimal) -> Self {
        Balance {
            available,
            held: Decimal::default(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Clone)]
pub struct AccountSnapshot {
    /// Client ID
    #[serde(rename = "client")]
    pub client_id: u16,
    pub available: Decimal,
    pub held: Decimal,
    pub total: Decimal,
    pub locked: bool,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_decode_encode() {
        let data = r#"
type,client,tx,amount
deposit,1,1,56
chargeback,1,2
withdrawal,1,1,12
deposit,3,86,34
withdrawal,3,96,344
chargeback,3,96
dispute,3,96"#;
        let mut rdr = csv::ReaderBuilder::new()
            .flexible(true)
            .trim(csv::Trim::All)
            .from_reader(data.as_bytes());
        for result in rdr.deserialize() {
            let record: TransactionEvent = result.unwrap();
            println!("{:?}", record);
        }
    }
}
