use kafka_ping_stm::{Ping, Pong};

use kafka::consumer::{Consumer, FetchOffset};
use kafka::producer::{Producer, Record, RequiredAcks};
use log::{debug, info};
use oblivious_state_machine::{
    state::{DeliveryStatus, State, StateTypes, Transition},
    state_machine::{TimeBoundStateMachineResult, TimeBoundStateMachineRunner},
};
use std::collections::VecDeque;
use std::time::Duration;
use tokio::{select, time};
use uuid::Uuid;

#[derive(Debug)]
enum IncomingMessage {
    SendPing(Ping),
    ReceivePong(Pong),
}

#[derive(Debug)]
struct Types;
impl StateTypes for Types {
    type In = IncomingMessage;
    type Out = ();
    type Err = String;
}

struct SendingPing {
    sent_ping: Option<Ping>,
    producer: Producer,
}

impl SendingPing {
    fn new() -> Self {
        let producer = Producer::from_hosts(vec!["localhost:9092".to_owned()])
            .with_ack_timeout(Duration::from_secs(1))
            .with_required_acks(RequiredAcks::One)
            .create()
            .unwrap();

        Self {
            sent_ping: None,
            producer,
        }
    }

    fn send_ping(&mut self, ping: Ping) {
        info!("Send Ping: {:?}", ping);
        let ping_message = serde_json::to_string_pretty(&ping).expect("json serialization failed");
        let ping_record = Record::from_value("ping", ping_message);
        self.producer
            .send(&ping_record)
            .expect("failed to send message");
        self.sent_ping = Some(ping);
    }
}

impl State<Types> for SendingPing {
    fn desc(&self) -> String {
        "Sending Ping".to_owned()
    }

    fn deliver(
        &mut self,
        message: IncomingMessage,
    ) -> DeliveryStatus<IncomingMessage, <Types as StateTypes>::Err> {
        match message {
            IncomingMessage::SendPing(ping) => {
                self.send_ping(ping);
                DeliveryStatus::Delivered
            }
            _ => DeliveryStatus::Unexpected(message),
        }
    }

    fn advance(&self) -> Result<Transition<Types>, <Types as StateTypes>::Err> {
        let next = match &self.sent_ping {
            Some(ping) => Transition::Next(Box::new(ListeningForPong::new(ping.clone()))),
            None => Transition::Same,
        };
        Ok(next)
    }
}

#[derive(Debug)]
struct ListeningForPong {
    sent_ping: Ping,
    received_pong: Option<Pong>,
}

impl ListeningForPong {
    fn new(sent_ping: Ping) -> Self {
        Self {
            sent_ping,
            received_pong: None,
        }
    }

    fn receive_pong(&mut self, pong: Pong) {
        info!("Received Pong: {:?}", pong);
        self.received_pong = Some(pong);
    }
}

impl State<Types> for ListeningForPong {
    fn desc(&self) -> String {
        "Waiting for Pong".to_owned()
    }

    fn deliver(
        &mut self,
        message: IncomingMessage,
    ) -> DeliveryStatus<IncomingMessage, <Types as StateTypes>::Err> {
        match message {
            IncomingMessage::ReceivePong(ref pong) => {
                if pong.envelope.correlation_id == self.sent_ping.envelope.correlation_id {
                    self.receive_pong(pong.clone());
                    DeliveryStatus::Delivered
                } else {
                    DeliveryStatus::Unexpected(message)
                }
            }
            _ => DeliveryStatus::Unexpected(message),
        }
    }

    fn advance(&self) -> Result<Transition<Types>, <Types as StateTypes>::Err> {
        let next = match &self.received_pong {
            Some(_pong) => Transition::Terminal,
            None => Transition::Same,
        };
        Ok(next)
    }
}

#[tokio::main]
async fn main() {
    pretty_env_logger::init();

    let address = Uuid::new_v4();
    info!("My address: {}", address);

    let mut consumer = Consumer::from_hosts(vec!["localhost:9092".to_owned()])
        .with_topic("pong".to_owned())
        .with_fallback_offset(FetchOffset::Earliest)
        .with_group("my_consumer_group".to_owned())
        .create()
        .expect("invalid consumer config");

    let mut feed = VecDeque::from([IncomingMessage::SendPing(Ping::new(address))]);
    let state: Box<dyn State<Types> + Send> = Box::new(SendingPing::new());

    let mut feeding_interval = time::interval(Duration::from_millis(100));
    feeding_interval.tick().await;

    let mut state_machine_runner =
        TimeBoundStateMachineRunner::new("Ping".to_owned(), state, Duration::from_secs(5));

    let (_outgoing, mut result) = state_machine_runner.run();

    let res: TimeBoundStateMachineResult<Types> = loop {
        for msg_result in consumer.poll().unwrap().iter() {
            for msg in msg_result.messages() {
                // let _key: &str = std::str::from_utf8(msg.key).unwrap();
                let pong: Pong =
                    serde_json::from_slice(msg.value).expect("failed to deser JSON to Pong");
                if pong.envelope.is_directed_at(address) {
                    feed.push_back(IncomingMessage::ReceivePong(pong));
                } else {
                    debug!("Dropped: {:?}", pong);
                }
            }
        }
        select! {
            res = &mut result => {
                break res.expect("Result from State Machine must be communicated");
            }
            _ = feeding_interval.tick() => {
                // feed a message if present.
                if let Some(msg) = feed.pop_front() {
                    let _ = state_machine_runner.deliver(msg);
                }
            }
        }
    };

    let _result = res.unwrap_or_else(|_| panic!("State machine did not complete in time"));
}
