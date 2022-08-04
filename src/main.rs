use log::debug;
use oblivious_state_machine::{
    state::{DeliveryStatus, State, StateTypes, Transition},
    state_machine::{TimeBoundStateMachineResult, TimeBoundStateMachineRunner},
};
use std::collections::VecDeque;
use std::time::Duration;
use tokio::{select, time};

#[derive(Debug)]
enum IncomingMessage {
    SendPing,
    Pong,
}

#[derive(Debug)]
struct Types;
impl StateTypes for Types {
    type In = IncomingMessage;
    type Out = ();
    type Err = String;
}

#[derive(Debug)]
struct SendingPing {
    ping_sent: bool,
}

impl SendingPing {
    fn new() -> Self {
        Self { ping_sent: false }
    }

    fn send_ping(&mut self) {
        debug!("Send Ping");
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

    fn receive_pong(&mut self) {
        self.pong_received = true;
        debug!("Received Pong");
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
            IncomingMessage::Pong => {
                self.receive_pong();
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

    let mut feed = VecDeque::from([IncomingMessage::SendPing, IncomingMessage::Pong]);
    let state: Box<dyn State<Types> + Send> = Box::new(SendingPing::new());

    let mut feeding_interval = time::interval(Duration::from_millis(100));
    feeding_interval.tick().await;

    let mut state_machine_runner =
        TimeBoundStateMachineRunner::new("Ping".to_owned(), state, Duration::from_secs(5));

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
