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
    Ping,
    SendPong,
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
    ping_received: bool,
}

impl ListeningForPing {
    fn new() -> Self {
        Self {
            ping_received: false,
        }
    }

    fn receive_ping(&mut self) {
        self.ping_received = true;
        debug!("Received Ping");
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
            IncomingMessage::Ping => {
                self.receive_ping();
                DeliveryStatus::Delivered
            }
            _ => DeliveryStatus::Unexpected(message),
        }
    }

    fn advance(&self) -> Result<Transition<Types>, <Types as StateTypes>::Err> {
        let next = if self.ping_received {
            Transition::Next(Box::new(SendingPong::new()))
        } else {
            Transition::Same
        };
        Ok(next)
    }
}

#[derive(Debug)]
struct SendingPong {
    pong_sent: bool,
}

impl SendingPong {
    fn new() -> Self {
        Self { pong_sent: false }
    }

    fn send_pong(&mut self) {
        debug!("Send Pong");
        self.pong_sent = true;
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
            IncomingMessage::SendPong => {
                self.send_pong();
                DeliveryStatus::Delivered
            }
            _ => DeliveryStatus::Unexpected(message),
        }
    }

    fn advance(&self) -> Result<Transition<Types>, <Types as StateTypes>::Err> {
        let next = if self.pong_sent {
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

    let mut feed = VecDeque::from([IncomingMessage::Ping, IncomingMessage::SendPong]);
    let state: Box<dyn State<Types> + Send> = Box::new(ListeningForPing::new());

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
