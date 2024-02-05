#[cfg(test)]

mod tests {

    use std::cell::Cell;

    use serde::{Deserialize, Serialize};

    use crate::{
        event,
        item::{matches, Eventable},
        repo::Repo,
    };
    use partially::Partial;

    #[derive(Clone, Serialize, Partial, Deserialize, PartialEq, Eq, Debug)]
    #[partially(derive(Clone, Serialize, Deserialize))]
    struct Demo {
        id: String,
        hash: String,
    }

    impl Eventable<Demo, PartialDemo> for Demo {
        type T = PartialDemo;

        fn id(&self) -> String {
            self.id.clone()
        }

        fn hash(&self) -> String {
            self.hash.clone()
        }
    }

    #[test]
    fn it_checks_equality() {
        let item = Demo {
            id: "1".to_string(),
            hash: "1".to_string(),
        };

        let item2 = Demo {
            id: "2".to_string(),
            hash: "2".to_string(),
        };

        let item3 = Demo {
            id: "1".to_string(),
            hash: "1".to_string(),
        };

        assert_eq!(item, item3);
        assert_ne!(item, item2);
    }

    #[test]
    fn it_checks_partial_equality() {
        let item = Demo {
            id: "1".to_string(),
            hash: "1".to_string(),
        };

        let item2 = Demo {
            id: "2".to_string(),
            hash: "2".to_string(),
        };

        let query = PartialDemo {
            id: Some("1".to_string()),
            hash: None,
        };

        let query2 = PartialDemo {
            id: Some("2".to_string()),
            hash: None,
        };

        assert!(matches(&item, &query));
        assert!(matches(&item2, &query2));
        assert!(!matches(&item2, &query));
        assert!(!matches(&item, &query2));
    }

    #[test]
    fn it_makes_a_repo() {
        let mut repo = Repo::<Demo, PartialDemo>::new();

        let item = Demo {
            id: "1".to_string(),
            hash: "1".to_string(),
        };

        let item2 = Demo {
            id: "2".to_string(),
            hash: "2".to_string(),
        };

        let num1s = Cell::new(0);
        let num2s = Cell::new(0);

        repo.watch(
            Box::new(move |items: Vec<Demo>| {
                assert!(items.len() == num1s.get());
                num1s.set(num1s.get() + 1);
            }),
            PartialDemo {
                id: Some("1".to_string()),
                hash: None,
            },
        );

        repo.watch(
            Box::new(move |items| {
                assert!(items.len() == num2s.get());
                num2s.set(num2s.get() + 1);
            }),
            PartialDemo {
                id: Some("2".to_string()),
                hash: None,
            },
        );

        repo.process(event::MEvent::from_item(
            item2.clone(),
            event::MEventType::SET,
            "2".to_string(),
        ))
        .unwrap();

        repo.process(event::MEvent::from_item(
            item.clone(),
            event::MEventType::SET,
            "2".to_string(),
        ))
        .unwrap();
    }
}
