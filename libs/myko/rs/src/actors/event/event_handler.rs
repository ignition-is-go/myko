use super::common::ProcessEventData;
use crate::{
    actors::{
        event::{common::PersistEvent, event_manager::EventManagerMsg},
        kafka::{
            common::KafkaSharedConfig,
            consumer::{KafkaConsumer, KafkaConsumerArgs},
            producer::{KafkaProducer, KafkaProducerArgs, KafkaProducerMsg, ProduceEventData},
        },
        query::{common::ProcessUpdateData, query_manager::QueryManagerMsg},
    },
    parsers::item::MykoItemParser,
    prelude::AnyItem,
    server::MykoServerCtx,
};
use log::{debug, error, trace};
use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort};
use std::{collections::BTreeMap, sync::Arc};

pub struct EventHandler;

pub struct EventHandlerState {
    entity_name: Arc<str>,
    /// Direct reference to EventManager for init callbacks
    event_manager: ActorRef<EventManagerMsg>,
    /// Direct reference to QueryManager for update routing (bypasses Server bottleneck)
    query_manager: ActorRef<QueryManagerMsg>,
    ctx: Arc<MykoServerCtx>,
    kafka_producer: Option<ActorRef<KafkaProducerMsg>>,
    parser: Arc<dyn MykoItemParser>,
    store: BTreeMap<Arc<str>, Arc<dyn AnyItem>>,
}

pub enum EventHandlerMessage {
    ProcessEvent(ProcessEventData), // bool for persist
    /// Initialize the handler. When kafka_config is None, runs in-memory only and signals caught up immediately.
    Init(Option<KafkaSharedConfig>),
    PersisterCaughtUp,
    GetState(RpcReplyPort<BTreeMap<Arc<str>, Arc<dyn AnyItem>>>),

    // Internal query messages for RelationshipManager
    /// Query items by field value (field name is JSON camelCase)
    QueryByField {
        field: String,
        value: String,
        reply: RpcReplyPort<Vec<serde_json::Value>>,
    },
    /// Query items where an array field contains a specific value
    QueryArrayContains {
        field: String,
        value: String,
        reply: RpcReplyPort<Vec<serde_json::Value>>,
    },
    /// Get items by their IDs
    GetByIds {
        ids: Vec<Arc<str>>,
        reply: RpcReplyPort<Vec<serde_json::Value>>,
    },
    /// Get all items as JSON values
    GetAllItems(RpcReplyPort<Vec<serde_json::Value>>),
}

pub struct EventHandlerArgs {
    pub entity_name: Arc<str>,
    /// Direct reference to EventManager for init callbacks
    pub event_manager: ActorRef<EventManagerMsg>,
    /// Direct reference to QueryManager for update routing
    pub query_manager: ActorRef<QueryManagerMsg>,
    pub ctx: Arc<MykoServerCtx>,
    pub parser: Arc<dyn MykoItemParser>,
}

impl Actor for EventHandler {
    type Msg = EventHandlerMessage;

    type State = EventHandlerState;

    type Arguments = EventHandlerArgs;

    async fn pre_start(
        &self,
        _myself: ractor::ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ractor::ActorProcessingErr> {
        let EventHandlerArgs {
            entity_name,
            event_manager,
            query_manager,
            ctx,
            parser,
        } = args;

        debug!("Creating Repo: {}", entity_name);

        Ok(EventHandlerState {
            entity_name,
            event_manager,
            query_manager,
            ctx,
            parser,
            kafka_producer: None,
            store: BTreeMap::new(),
        })
    }

    async fn handle(
        &self,
        myself: ractor::ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            EventHandlerMessage::Init(conf) => {
                let entity_name = state.entity_name.clone();
                debug!("{}: Init", entity_name);

                match conf {
                    Some(conf) => {
                        // With Kafka: spawn consumer and producer
                        Actor::spawn(
                            None,
                            KafkaConsumer,
                            KafkaConsumerArgs {
                                topic: entity_name.clone(),
                                shared_conf: conf.clone(),
                                repo_ref: myself.clone(),
                                ctx: state.ctx.clone(),
                            },
                        )
                        .await?;

                        let (producer_ref, _producer_handle) = Actor::spawn(
                            None,
                            KafkaProducer,
                            KafkaProducerArgs {
                                shared_conf: conf,
                                topic: entity_name.clone(),
                            },
                        )
                        .await?;

                        state.kafka_producer = Some(producer_ref);
                    }
                    None => {
                        // In-memory mode: signal caught up immediately
                        debug!("{}: In-memory mode, signaling caught up", entity_name);
                        myself.send_message(EventHandlerMessage::PersisterCaughtUp)?;
                    }
                }

                Ok(())
            }
            EventHandlerMessage::PersisterCaughtUp => {
                debug!(
                    "{} Init Complete: {} entities",
                    state.entity_name,
                    state.store.len()
                );
                // Direct send to EventManager (bypasses Server actor)
                match state.event_manager.send_message(
                    EventManagerMsg::RepoInitComplete(state.entity_name.clone()),
                ) {
                    Ok(_) => Ok(()),
                    Err(err) => {
                        error!("Failed to notify repo manager: {}", err);
                        Err(ActorProcessingErr::from(String::from(
                            "failed to notify repo manager of topic caught up",
                        )))
                    }
                }
            }
            EventHandlerMessage::ProcessEvent(data) => {
                let change_type = data.event.change_type();

                // Use pre-parsed item if available (local events), otherwise parse from JSON
                let item = match data.parsed_item {
                    Some(item) => item,
                    None => {
                        let item_json = data.event.item_json();
                        match state.parser.parse(item_json.clone()) {
                            Ok(item) => item,
                            Err(err) => {
                                error!(
                                    "{}: Failed to parse item: {} \n {:?}",
                                    state.entity_name, err, item_json
                                );
                                return Ok(());
                            }
                        }
                    }
                };

                if let Some(kafka_producer) = &state.kafka_producer
                    && let PersistEvent::Persist = data.persist
                {
                    let mut event = data.event.clone();
                    event.source_id = Some(state.ctx.host_id.to_string());

                    let produce_res = kafka_producer.send_message(KafkaProducerMsg::ProduceEvent(
                        ProduceEventData {
                            event,
                            key: item.id().clone(),
                        },
                    ));

                    match produce_res {
                        Ok(_) => (),
                        Err(err) => {
                            error!("Failed to produce event: {}", err);
                        }
                    }
                }

                match change_type {
                    crate::event::MEventType::DEL => {
                        trace!(
                            "{}: Processing DEL for item {}",
                            state.entity_name,
                            item.id()
                        );
                        state.store.remove(&item.id());

                        // Direct send to QueryManager (bypasses Server actor)
                        if let Err(err) = state.query_manager.send_message(
                            QueryManagerMsg::ProcessUpdate(
                                ProcessUpdateData::Del(item.id().clone()),
                                state.entity_name.clone(),
                            ),
                        ) {
                            error!("Failed to send message to query manager: {}", err);
                        };
                    }
                    crate::event::MEventType::SET => {
                        state.store.insert(item.id(), item.clone());

                        // Direct send to QueryManager (bypasses Server actor)
                        if let Err(err) = state.query_manager.send_message(
                            QueryManagerMsg::ProcessUpdate(
                                ProcessUpdateData::Set(item.clone()),
                                state.entity_name.clone(),
                            ),
                        ) {
                            error!("Failed to send message to query manager: {}", err);
                        };
                    }
                }

                Ok(())
            }
            EventHandlerMessage::GetState(reply) => {
                if let Err(err) = reply.send(state.store.clone()) {
                    error!("Unable to reply with store state: {}", err);
                };
                Ok(())
            }
            EventHandlerMessage::QueryByField { field, value, reply } => {
                let results: Vec<serde_json::Value> = state
                    .store
                    .values()
                    .filter_map(|item| {
                        let json = item.to_value();
                        // Check if the field matches the value
                        if let Some(field_value) = json.get(&field) {
                            // Compare as string (handles both string and numeric values)
                            let matches = match field_value {
                                serde_json::Value::String(s) => s == &value,
                                serde_json::Value::Number(n) => n.to_string() == value,
                                _ => field_value.to_string().trim_matches('"') == value,
                            };
                            if matches {
                                return Some(json);
                            }
                        }
                        None
                    })
                    .collect();

                if let Err(err) = reply.send(results) {
                    error!("Unable to reply with query results: {}", err);
                }
                Ok(())
            }
            EventHandlerMessage::QueryArrayContains { field, value, reply } => {
                let results: Vec<serde_json::Value> = state
                    .store
                    .values()
                    .filter_map(|item| {
                        let json = item.to_value();
                        // Check if the array field contains the value
                        if let Some(serde_json::Value::Array(arr)) = json.get(&field) {
                            let contains = arr.iter().any(|v| match v {
                                serde_json::Value::String(s) => s == &value,
                                serde_json::Value::Number(n) => n.to_string() == value,
                                _ => v.to_string().trim_matches('"') == value,
                            });
                            if contains {
                                return Some(json);
                            }
                        }
                        None
                    })
                    .collect();

                if let Err(err) = reply.send(results) {
                    error!("Unable to reply with query results: {}", err);
                }
                Ok(())
            }
            EventHandlerMessage::GetByIds { ids, reply } => {
                let results: Vec<serde_json::Value> = ids
                    .iter()
                    .filter_map(|id| state.store.get(id).map(|item| item.to_value()))
                    .collect();

                if let Err(err) = reply.send(results) {
                    error!("Unable to reply with items: {}", err);
                }
                Ok(())
            }
            EventHandlerMessage::GetAllItems(reply) => {
                let results: Vec<serde_json::Value> =
                    state.store.values().map(|item| item.to_value()).collect();

                if let Err(err) = reply.send(results) {
                    error!("Unable to reply with all items: {}", err);
                }
                Ok(())
            }
        }
    }
}
