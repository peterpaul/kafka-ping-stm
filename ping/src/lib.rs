use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct CorrelationId {
    uuid: Uuid,
}

impl Default for CorrelationId {
    fn default() -> Self {
        Self {
            uuid: Uuid::new_v4(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Ping {
    pub correlation_id: CorrelationId,
}

impl Ping {
    pub fn new() -> Self {
        Self {
            correlation_id: CorrelationId::default(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Pong {
    pub correlation_id: CorrelationId,
}

impl Pong {
    pub fn new(ping: &Ping) -> Self {
        Self {
            correlation_id: ping.correlation_id.clone(),
        }
    }
}
