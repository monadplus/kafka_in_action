use itertools::Itertools;
use std::str::FromStr;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("Invalid arguments: {0}")]
    InvalidArguments(String),
}

#[derive(Debug, Clone, derive_more::Display)]
#[display(fmt = "{host}:{port}")]
pub struct Broker {
    pub host: String,
    pub port: u16,
}

impl FromStr for Broker {
    type Err = AppError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.split_once(':')
            .and_then(|(host, port)| {
                let host = host.to_string();
                port.parse::<u16>().map(|port| Broker { host, port }).ok()
            })
            .ok_or_else(|| {
                AppError::InvalidArguments(
                    r#"Expected "<host>:<port>(,<host>:<port>)*""#.to_string(),
                )
            })
    }
}

pub fn brokers_to_str(brokers: impl IntoIterator<Item = Broker>) -> String {
    brokers
        .into_iter()
        .map(|broker| broker.to_string())
        .join(",")
}

#[derive(Debug, Clone, derive_more::Display, derive_more::FromStr)]
pub struct TopicName(pub String);
