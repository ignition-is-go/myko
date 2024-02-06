#[cfg(test)]

mod tests {

    use crate::{
        event::{self, MEvent},
        item::Eventable,
        module::Module,
        query::{AllQueries, QueryResponse},
        repo::{self, Repo, RepoStruct},
        utils::matches,
    };
    use macros::Eventable;
    use partially::Partial;
    use serde::{Deserialize, Serialize};
    use serde_json::Value;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[derive(Clone, Serialize, Partial, Deserialize, PartialEq, Eq, Debug, Eventable)]
    #[partially(derive(Clone, Serialize, Deserialize))]
    struct Demo {
        id: String,
        hash: String,
    }

    #[derive(Clone, Serialize, Partial, Deserialize, PartialEq, Eq, Debug, Eventable)]
    #[partially(derive(Clone, Serialize, Deserialize))]
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
            hash: None,
        };

        let query2 = PartialAuto {
            id: Some("2".to_string()),
            hash: None,
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

        repo.watch(
            Arc::new(move |items: Vec<Auto>| {
                let mut val = num1s.lock().unwrap();
                assert!(items.len() == *val);
                *val = *val + 1;
            }),
            PartialAuto {
                id: Some("1".to_string()),
                hash: None,
            },
        );

        repo.watch(
            Arc::new(move |items| {
                let mut val = num2s.lock().unwrap();
                assert!(items.len() == *val);
                *val = *val + 1;
            }),
            PartialAuto {
                id: Some("2".to_string()),
                hash: None,
            },
        );

        repo.process(event::MEvent::from_item(
            item2.clone(),
            event::MEventType::SET,
            "2".to_string(),
        ))
        .await
        .unwrap();

        repo.process(event::MEvent::from_item(
            item.clone(),
            event::MEventType::SET,
            "2".to_string(),
        ))
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn it_makes_a_module() {
        let module: AutoModule = AutoModule::new();

        let (tx, rx) = tokio::sync::broadcast::channel::<MEvent>(1);

        module.start(rx).await;

        struct DemoModule {
            repo: Arc<Mutex<repo::RepoStruct<Demo, PartialDemo>>>,
        }

        impl Module for DemoModule {
            fn new() -> Self {
                DemoModule {
                    repo: Arc::new(Mutex::new(repo::RepoStruct::new())),
                }
            }

            async fn handle_query(
                &mut self,
                query: crate::query::AllQueries,
            ) -> Option<std::sync::mpsc::Receiver<QueryResponse>> {
                match query {
                    AllQueries::WatchId(query) => {
                        if query.item_type != "Auto" {
                            return None;
                        }
                        let (tx, rx) = std::sync::mpsc::channel::<QueryResponse>();
                        let func = Arc::new(move |items: Vec<Demo>| {
                            let values = items
                                .iter()
                                .map(|x| serde_json::to_value(x))
                                .filter_map(Result::ok)
                                .collect::<Vec<Value>>();
                            let response = QueryResponse::new(query.tx.clone(), values);
                            match tx.send(response) {
                                Ok(_) => (),
                                Err(e) => println!("Failed to send response: {}", e),
                            }
                        });

                        let query = PartialDemo {
                            id: Some(query.item_id),
                            hash: None,
                        };

                        self.repo.lock().await.watch(func, query);

                        return Some(rx);
                    }
                    AllQueries::Watch(query) => {
                        if query.item_type != "Auto" {
                            return None;
                        }
                        let (tx, rx) = std::sync::mpsc::channel::<QueryResponse>();
                        let func = Arc::new(move |items: Vec<Demo>| {
                            let values = items
                                .iter()
                                .map(|x| serde_json::to_value(x))
                                .filter_map(Result::ok)
                                .collect::<Vec<Value>>();
                            let response = QueryResponse::new(query.tx.clone(), values);
                            match tx.send(response) {
                                Ok(_) => (),
                                Err(e) => println!("Failed to send response: {}", e),
                            }
                        });

                        let query = serde_json::from_value::<PartialDemo>(query.query).unwrap();

                        self.repo.lock().await.watch(func, query);

                        return Some(rx);
                    }
                }
            }

            async fn start(&self, events: tokio::sync::broadcast::Receiver<MEvent>) {
                self.repo.lock().await.listen(events).await;
            }
        }
    }
}
