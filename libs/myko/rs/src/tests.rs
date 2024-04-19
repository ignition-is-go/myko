#[cfg(test)]

mod tests {

    use macros::Eventable;
    use myko_wasm::event::{MEvent, MEventType};
    use myko_wasm::item::Eventable;
    use myko_wasm::query::{Query, QueryResponse};
    use tokio::sync::mpsc::Sender;

    use crate::kafka::KafkaClient;
    use crate::{
        module::Module,
        repo::{Repo, RepoStruct},
        utils::matches,
    };

    use partially::Partial;
    use serde::{Deserialize, Serialize};
    use serde_json::Value;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[derive(Clone, Serialize, Partial, Deserialize, PartialEq, Debug)]
    #[partially(derive(Clone, Serialize, Deserialize, Default))]
    struct Demo {
        id: String,
        hash: String,
    }

    #[derive(Clone, Partial, Serialize, Deserialize, PartialEq, Debug, Eventable)]
    #[partially(derive(Clone, Serialize, Deserialize, Default))]
    struct Auto {
        id: String,
        hash: String,
    }

    #[test]
    fn it_checks_equality() {
        let item = Auto {
            id: "1".to_string(),
            hash: "1".to_string(),
        };

        let item2 = Auto {
            id: "2".to_string(),
            hash: "2".to_string(),
        };

        let item3 = Auto {
            id: "1".to_string(),
            hash: "1".to_string(),
        };

        assert_eq!(item, item3);
        assert_ne!(item, item2);
    }

    #[test]
    fn it_checks_partial_equality() {
        let item = Auto {
            id: "1".to_string(),
            hash: "1".to_string(),
        };

        let item2 = Auto {
            id: "2".to_string(),
            hash: "2".to_string(),
        };

        let query = PartialAuto {
            id: Some("1".to_string()),
            ..Default::default()
        };

        let query2 = PartialAuto {
            id: Some("2".to_string()),
            ..Default::default()
        };

        assert!(matches(&item, &query));
        assert!(matches(&item2, &query2));
        assert!(!matches(&item2, &query));
        assert!(!matches(&item, &query2));
    }

    #[tokio::test]
    async fn it_makes_a_repo() {
        let mut repo = RepoStruct::<Auto, PartialAuto>::new();

        let item = Auto {
            id: "1".to_string(),
            hash: "1".to_string(),
        };

        let item2 = Auto {
            id: "2".to_string(),
            hash: "2".to_string(),
        };

        let num1s = Arc::new(std::sync::Mutex::new(0));
        let num2s = Arc::new(std::sync::Mutex::new(0));

        let mut rx1 = repo.watch(PartialAuto {
            id: Some("1".to_string()),
            hash: None,
            ..Default::default()
        });

        let mut rx2 = repo.watch(PartialAuto {
            id: Some("2".to_string()),
            hash: None,
            ..Default::default()
        });

        tokio::spawn(async move {
            while let Some(items) = rx1.recv().await {
                // increment the number of 1s
                *num1s.lock().unwrap() += 1;
                // check that the list is num1s long
                assert_eq!(items.len(), *num1s.lock().unwrap());
            }
        });

        tokio::spawn(async move {
            while let Some(items) = rx2.recv().await {
                // increment the number of 2s
                *num2s.lock().unwrap() += 1;
                // check that the list is num2s long
                assert_eq!(items.len(), *num2s.lock().unwrap());
            }
        });

        repo.process(MEvent::from_item(&item2, MEventType::SET, "2".to_string()))
            .await
            .unwrap();

        repo.process(MEvent::from_item(&item, MEventType::SET, "2".to_string()))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn it_makes_a_module() {
        let _module: AutoModule = AutoModule::new();

        impl Eventable<Demo, PartialDemo> for Demo {
            type T = PartialDemo;

            fn id(&self) -> String {
                self.id.clone()
            }

            fn hash(&self) -> String {
                self.hash.clone()
            }

            fn entity_name(&self) -> String {
                "Demo".to_string()
            }
        }

        struct DemoModule {
            repo: Arc<Mutex<RepoStruct<Demo, PartialDemo>>>,
            kafka: Option<KafkaClient>,
        }

        #[async_trait::async_trait]
        impl Module for DemoModule {
            fn new() -> Self {
                DemoModule {
                    repo: Arc::new(Mutex::new(RepoStruct::new())),
                    kafka: None,
                }
            }

            async fn start_kafka(&mut self, brokers: &[&str], from_kafka_tx: Sender<MEvent>) {
                let k = KafkaClient::new(brokers.join(",").as_str(), "Demo").await;

                k.consume_events(from_kafka_tx).await;

                self.kafka = Some(k);
            }

            fn entity_name(&self) -> String {
                "Demo".to_string()
            }

            async fn process_event(&mut self, event: MEvent, persist: bool) {
                if event.item_type() != "Demo" {
                    return;
                }

                println!("Processing event in {}", "Demo");

                if persist {
                    match self.kafka {
                        Some(ref k) => {
                            k.append_event(&event).await;
                        }
                        None => (),
                    }
                }

                match self.repo.lock().await.process(event.clone()).await {
                    Ok(_) => (),
                    Err(e) => println!("Failed to process event: {}, {:?}", e, event),
                }
            }

            async fn handle_query(
                &mut self,
                query: myko_wasm::query::Query,
            ) -> Option<tokio::sync::mpsc::Receiver<QueryResponse>> {
                match query {
                    Query::WatchId(query) => {
                        if query.item_type != "Auto" {
                            return None;
                        }
                        let (tx, rx) = tokio::sync::mpsc::channel::<QueryResponse>(1);

                        let query_filter = PartialDemo {
                            id: Some(query.item_id),
                            ..Default::default()
                        };

                        let mut qrx = self.repo.lock().await.watch(query_filter);

                        tokio::spawn(async move {
                            while let Some(items) = qrx.recv().await {
                                let values = items
                                    .iter()
                                    .map(|x| serde_json::to_value(x))
                                    .filter_map(Result::ok)
                                    .collect::<Vec<Value>>();

                                let response = QueryResponse::new(query.tx.clone(), values);
                                match tx.send(response).await {
                                    Ok(_) => (),
                                    Err(e) => println!("Failed to send response: {}", e),
                                }
                            }
                        });

                        return Some(rx);
                    }
                    Query::Watch(query) => {
                        if query.item_type != "Auto" {
                            return None;
                        }
                        let (tx, rx) = tokio::sync::mpsc::channel::<QueryResponse>(1);

                        let filter_query =
                            serde_json::from_str::<PartialDemo>(query.query.as_str());

                        let safe_filter_query = match filter_query {
                            Ok(fq) => fq,
                            Err(e) => {
                                println!("Failed to parse query: {}", e);
                                return None;
                            }
                        };

                        let mut qrx = self.repo.lock().await.watch(safe_filter_query);

                        tokio::spawn(async move {
                            while let Some(items) = qrx.recv().await {
                                let values = items
                                    .iter()
                                    .map(|x| serde_json::to_value(x))
                                    .filter_map(Result::ok)
                                    .collect::<Vec<Value>>();

                                let response = QueryResponse::new(query.tx.clone(), values);
                                match tx.send(response).await {
                                    Ok(_) => (),
                                    Err(e) => println!("Failed to send response: {}", e),
                                }
                            }
                        });

                        return Some(rx);
                    }
                }
            }
        }
    }
}
