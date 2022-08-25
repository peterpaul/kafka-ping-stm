use kafka_ping_stm::{Ping, Pong};

use kafka::consumer::{Consumer, FetchOffset};
use kafka::producer::{Producer, Record, RequiredAcks};
use log::{debug, info};
use oblivious_state_machine::{
    state::{DeliveryStatus, State, StateTypes, Transition},
    state_machine::{TimeBoundStateMachineResult, TimeBoundStateMachineRunner},
};
use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::oneshot;
use uuid::Uuid;

#[derive(Debug)]
enum InboundMessage {
    PingReceived(Ping),
    PongSent(Pong),
}

#[derive(Debug)]
struct Types;
impl StateTypes for Types {
    type In = InboundMessage;
    type Out = Pong;
    type Err = String;
}

#[derive(Debug)]
struct ListeningForPing {
    my_address: Uuid,
    received_ping: Option<Ping>,
}

impl ListeningForPing {
    fn new(my_address: Uuid) -> Self {
        Self {
            my_address,
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
        message: InboundMessage,
    ) -> DeliveryStatus<InboundMessage, <Types as StateTypes>::Err> {
        match message {
            InboundMessage::PingReceived(ping) => {
                self.receive_ping(ping);
                DeliveryStatus::Delivered
            }
            _ => DeliveryStatus::Unexpected(message),
        }
    }

    fn advance(&self) -> Result<Transition<Types>, <Types as StateTypes>::Err> {
        let next = match &self.received_ping {
            Some(ping) => {
                Transition::Next(Box::new(SendingPong::new(self.my_address, ping.clone())))
            }
            None => Transition::Same,
        };
        Ok(next)
    }
}

struct SendingPong {
    my_address: Uuid,
    received_ping: Ping,
    sent_pong: Option<Pong>,
}

impl SendingPong {
    fn new(my_address: Uuid, received_ping: Ping) -> Self {
        Self {
            my_address,
            received_ping,
            sent_pong: None,
        }
    }

    fn get_pong(&self) -> Pong {
        Pong::new(&self.received_ping, self.my_address)
    }
}

impl State<Types> for SendingPong {
    fn desc(&self) -> String {
        "Sending Pong".to_owned()
    }

    fn initialize(&self) -> Vec<Pong> {
        vec![self.get_pong()]
    }

    fn deliver(
        &mut self,
        message: InboundMessage,
    ) -> DeliveryStatus<InboundMessage, <Types as StateTypes>::Err> {
        match message {
            InboundMessage::PongSent(pong) => {
                self.sent_pong = Some(pong);
                DeliveryStatus::Delivered
            }
            _ => DeliveryStatus::Unexpected(message),
        }
    }

    fn advance(&self) -> Result<Transition<Types>, <Types as StateTypes>::Err> {
        let next = match &self.sent_pong {
            Some(_pong) => Transition::Next(Box::new(SentPong::new())),
            None => Transition::Same,
        };
        Ok(next)
    }
}

struct SentPong;

impl SentPong {
    fn new() -> Self {
        SentPong
    }
}

impl State<Types> for SentPong {
    fn desc(&self) -> String {
        "Sent Pong".to_owned()
    }

    fn deliver(
        &mut self,
        message: <Types as StateTypes>::In,
    ) -> DeliveryStatus<<Types as StateTypes>::In, <Types as StateTypes>::Err> {
        DeliveryStatus::Unexpected(message)
    }

    fn advance(&self) -> Result<Transition<Types>, <Types as StateTypes>::Err> {
        Ok(Transition::Terminal)
    }
}

fn send_pong(
    producer: &mut Producer,
    pong: Pong,
    context: Arc<Mutex<Context>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    info!("Send Pong: {:?}", pong);
    let pong_message = serde_json::to_string_pretty(&pong)?;
    let pong_record = Record::from_value("pong", pong_message);
    producer.send(&pong_record)?;
    let ctx = context.lock().unwrap();
    ctx.deliver(
        &pong.envelope.correlation_id,
        InboundMessage::PongSent(pong.clone()),
    )
    .unwrap();
    Ok(())
}

struct Context {
    stm_map: HashMap<Uuid, TimeBoundStateMachineRunner<Types>>,
}

impl Context {
    fn new() -> Self {
        Self {
            stm_map: HashMap::new(),
        }
    }

    fn create_stm(&mut self, correlation_id: Uuid, state: Box<dyn State<Types> + Send>) {
        self.stm_map.insert(
            correlation_id,
            TimeBoundStateMachineRunner::new("Ping".to_owned(), state, Duration::from_secs(30)),
        );
    }

    fn run(
        &mut self,
        correlation_id: &Uuid,
    ) -> Option<(
        UnboundedReceiver<Vec<Pong>>,
        oneshot::Receiver<TimeBoundStateMachineResult<Types>>,
    )> {
        self.stm_map.get_mut(correlation_id).map(|stm| stm.run())
    }

    fn deliver(
        &self,
        correlation_id: &Uuid,
        message: InboundMessage,
    ) -> Result<(), InboundMessage> {
        if let Some(stm) = self.stm_map.get(correlation_id) {
            stm.deliver(message)
        } else {
            Err(message)
        }
    }

    fn remove(&mut self, correlation_id: &Uuid) {
        self.stm_map.remove(correlation_id);
    }
}

async fn create_and_run_stm(
    ping: Ping,
    address: Uuid,
    sender: mpsc::Sender<Pong>,
    context: Arc<Mutex<Context>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let state: Box<dyn State<Types> + Send> = Box::new(ListeningForPing::new(address));
    let (mut outgoing, mut result) = {
        let mut ctx = context.lock().unwrap();

        ctx.create_stm(ping.envelope.correlation_id, state);

        let (outgoing, result) = ctx.run(&ping.envelope.correlation_id).unwrap();

        ctx.deliver(
            &ping.envelope.correlation_id,
            InboundMessage::PingReceived(ping.clone()),
        )
        .unwrap();

        (outgoing, result)
    };

    // TODO: Proper error handling
    let res: TimeBoundStateMachineResult<Types> = loop {
        select! {
            outgoing_messages = outgoing.recv() => {
                if let Some(messages) = outgoing_messages {
                    for pong in messages {
                        sender.send(pong)?;
                    }
                }
            }
            res = &mut result => {
                break res?;
            }
        }
    };

    let _result =
        res.unwrap_or_else(|err| panic!("State machine did not complete in time: {}", err));

    //
    let mut ctx = context.lock().unwrap();
    ctx.remove(&ping.envelope.correlation_id);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    pretty_env_logger::init();

    let address = Uuid::new_v4();
    info!("My address: {}", address);

    let mut consumer = Consumer::from_hosts(vec!["localhost:9092".to_owned()])
        .with_topic("ping".to_owned())
        .with_fallback_offset(FetchOffset::Earliest)
        .with_group("my_consumer_group".to_owned())
        .create()?;

    let mut producer = Producer::from_hosts(vec!["localhost:9092".to_owned()])
        .with_ack_timeout(Duration::from_secs(1))
        .with_required_acks(RequiredAcks::One)
        .create()?;

    let (pong_tx, pong_rx) = mpsc::channel::<Pong>();

    let context = Arc::new(Mutex::new(Context::new()));

    // Create outbound message sender thread
    {
        let sender_context = (&context).clone();

        let _sender_thread = std::thread::spawn(move || {
            while let Ok(pong) = pong_rx.recv() {
                send_pong(&mut producer, pong, (&sender_context).clone()).unwrap();
            }
            info!("Terminating sender_thread");
        });
    }

    loop {
        for msg_result in consumer.poll()?.iter() {
            for msg in msg_result.messages() {
                // parse json message, ideally this is done inside the tokio task
                let ping: Ping =
                    serde_json::from_slice(msg.value).expect("failed to deser JSON to Ping");
                if ping.envelope.is_directed_at(address) {
                    // spawn a separate tokio task which runs the oblivious STM
                    let tx = pong_tx.clone();
                    let context = (&context).clone();
                    tokio::spawn(
                        async move { create_and_run_stm(ping, address, tx, context).await },
                    );
                } else {
                    debug!("Dropped: {:?}", ping);
                }
            }
            // This could be problematic, if the ping was directed at a specific node and there are multiple pong nodes running.
            consumer.consume_messageset(msg_result)?;
        }
        consumer.commit_consumed()?;
    }
}
