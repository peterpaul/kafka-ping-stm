use kafka_ping_stm::{Ping, Pong};

use kafka::consumer::{Consumer, FetchOffset};
use kafka::producer::{Producer, Record, RequiredAcks};
use log::info;
use oblivious_state_machine::{
    state::{DeliveryStatus, State, StateTypes, Transition},
    state_machine::{TimeBoundStateMachineResult, TimeBoundStateMachineRunner},
};
use std::collections::VecDeque;
use std::time::Duration;
use tokio::{select, time};

#[derive(Debug)]
enum IncomingMessage {
    ReceivePing(Ping),
    SendPong(Pong),
}

#[derive(Debug)]
struct Types;
impl StateTypes for Types {
    type In = IncomingMessage;
    type Out = ();
    type Err = String;
}

#[derive(Debug)]
struct ListeningForPing {
    received_ping: Option<Ping>,
}

impl ListeningForPing {
    fn new() -> Self {
        Self {
            received_ping: None,
        }
    }

    fn receive_ping(&mut self, ping: Ping) {
        info!("Received Ping: {:?}", ping);
        self.received_ping = Some(ping);
    }
}

impl State<Types> for ListeningForPing {
    fn desc(&self) -> String {
        "Waiting for Ping".to_owned()
    }

    fn deliver(
        &mut self,
        message: IncomingMessage,
    ) -> DeliveryStatus<IncomingMessage, <Types as StateTypes>::Err> {
        match message {
            IncomingMessage::ReceivePing(ping) => {
                self.receive_ping(ping);
                DeliveryStatus::Delivered
            }
            _ => DeliveryStatus::Unexpected(message),
        }
    }

    fn advance(&self) -> Result<Transition<Types>, <Types as StateTypes>::Err> {
        let next = match &self.received_ping {
            Some(ping) => Transition::Next(Box::new(SendingPong::new(ping.clone()))),
            None => Transition::Same,
        };
        Ok(next)
    }
}

struct SendingPong {
    received_ping: Ping,
    sent_pong: Option<Pong>,
    producer: Producer,
}

impl SendingPong {
    fn new(received_ping: Ping) -> Self {
        let producer = Producer::from_hosts(vec!["localhost:9092".to_owned()])
            .with_ack_timeout(Duration::from_secs(1))
            .with_required_acks(RequiredAcks::One)
            .create()
            .unwrap();

        Self {
            received_ping,
            sent_pong: None,
            producer,
        }
    }

    fn send_pong(&mut self, pong: Pong) {
        info!("Send Pong: {:?}", pong);
        let pong_message = serde_json::to_string_pretty(&pong).expect("json serialization failed");
        let pong_record = Record::from_value("pong", pong_message);
        self.producer
            .send(&pong_record)
            .expect("failed to send message");
        self.sent_pong = Some(pong);
    }
}

impl State<Types> for SendingPong {
    fn desc(&self) -> String {
        "Sending Pong".to_owned()
    }

    fn deliver(
        &mut self,
        message: IncomingMessage,
    ) -> DeliveryStatus<IncomingMessage, <Types as StateTypes>::Err> {
        match message {
            IncomingMessage::SendPong(pong) => {
                self.send_pong(pong);
                DeliveryStatus::Delivered
            }
            _ => DeliveryStatus::Unexpected(message),
        }
    }

    fn advance(&self) -> Result<Transition<Types>, <Types as StateTypes>::Err> {
        let next = match &self.sent_pong {
            Some(_pong) => Transition::Terminal,
            None => Transition::Same,
        };
        Ok(next)
    }
}

async fn create_and_run_stm(ping: Ping) {
    let state: Box<dyn State<Types> + Send> = Box::new(ListeningForPing::new());

    let mut feed = VecDeque::from([]);
    feed.push_back(IncomingMessage::ReceivePing(ping.clone()));
    feed.push_back(IncomingMessage::SendPong(Pong::new(&ping)));

    let mut feeding_interval = time::interval(Duration::from_millis(100));
    feeding_interval.tick().await;

    let mut state_machine_runner =
        TimeBoundStateMachineRunner::new("Ping".to_owned(), state, Duration::from_secs(30));

    let (_outgoing, mut result) = state_machine_runner.run();

    let res: TimeBoundStateMachineResult<Types> = loop {
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

#[tokio::main]
async fn main() {
    pretty_env_logger::init();

    let mut consumer = Consumer::from_hosts(vec!["localhost:9092".to_owned()])
        .with_topic("ping".to_owned())
        .with_fallback_offset(FetchOffset::Earliest)
        .with_group("my_consumer_group".to_owned())
        .create()
        .expect("invalid consumer config");

    loop {
        for msg_result in consumer.poll().unwrap().iter() {
            for msg in msg_result.messages() {
                // parse json message, ideally this is done inside the tokio task
                let ping: Ping =
                    serde_json::from_slice(msg.value).expect("failed to deser JSON to Ping");
                // spawn a separate tokio task which runs the oblivious STM
                tokio::spawn(async move { create_and_run_stm(ping).await });
            }
            consumer.consume_messageset(msg_result).unwrap();
        }
        consumer.commit_consumed().unwrap();
    }
}
