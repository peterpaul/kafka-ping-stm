use kafka::consumer::{Consumer, FetchOffset};
use kafka::producer::{Producer, Record, RequiredAcks};
use log::debug;
use oblivious_state_machine::{
    state::{DeliveryStatus, State, StateTypes, Transition},
    state_machine::{TimeBoundStateMachineResult, TimeBoundStateMachineRunner},
};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::time::Duration;
use tokio::{select, time};

#[derive(Debug)]
enum IncomingMessage {
    SendPing,
    Pong(Pong),
}

#[derive(Debug)]
struct Types;
impl StateTypes for Types {
    type In = IncomingMessage;
    type Out = ();
    type Err = String;
}

#[derive(Serialize, Deserialize, Debug)]
struct Ping {
    id: u32,
}

impl Ping {
    fn new() -> Self {
        Self { id: 42 }
    }
}

struct SendingPing {
    ping_sent: bool,
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
            ping_sent: false,
	    producer,
        }
    }

    fn send_ping(&mut self) {
        debug!("Send Ping");
        let ping = Ping::new();
        let ping_message = serde_json::to_string_pretty(&ping).expect("json serialization failed");
        let ping_record = Record::from_value("ping", ping_message);
        self.producer
            .send(&ping_record)
            .expect("failed to send message");
        self.ping_sent = true;
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
            IncomingMessage::SendPing => {
                self.send_ping();
                DeliveryStatus::Delivered
            }
            _ => DeliveryStatus::Unexpected(message),
        }
    }

    fn advance(&self) -> Result<Transition<Types>, <Types as StateTypes>::Err> {
        let next = if self.ping_sent {
            Transition::Next(Box::new(ListeningForPong::new()))
        } else {
            Transition::Same
        };
        Ok(next)
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Pong {
    id: u32,
}

#[derive(Debug)]
struct ListeningForPong {
    pong_received: bool,
}

impl ListeningForPong {
    fn new() -> Self {
        Self {
            pong_received: false,
        }
    }

    fn receive_pong(&mut self, pong: Pong) {
        debug!("Received Pong: {}", pong.id);
        self.pong_received = true;
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
            IncomingMessage::Pong(pong) => {
                self.receive_pong(pong);
                DeliveryStatus::Delivered
            }
            _ => DeliveryStatus::Unexpected(message),
        }
    }

    fn advance(&self) -> Result<Transition<Types>, <Types as StateTypes>::Err> {
        let next = if self.pong_received {
            Transition::Terminal
        } else {
            Transition::Same
        };
        Ok(next)
    }
}

#[tokio::main]
async fn main() {
    pretty_env_logger::init();

    let mut consumer = Consumer::from_hosts(vec!["localhost:9092".to_owned()])
        .with_topic("pong".to_owned())
        .with_fallback_offset(FetchOffset::Earliest)
        .with_group("my_consumer_group".to_owned())
        .create()
        .expect("invalid consumer config");

    let mut feed = VecDeque::from([IncomingMessage::SendPing]);
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
                feed.push_front(IncomingMessage::Pong(pong));
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
