// lib.rs
extern crate proc_macro;

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(Eventable)]
pub fn eventable_impl(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let name = &ast.ident;

    let module_name = format_ident!("{}Module", name);
    let name_str = name.to_string();

    let partial_name = format_ident!("Partial{}", name);

    let gen = quote! {
        impl Eventable<#name, #partial_name> for #name {
            type T = #partial_name;

            fn id(&self) -> String {
                self.id.clone()
            }

            fn hash(&self) -> String {
                self.hash.clone()
            }
        }

        struct #module_name {
            repo: Arc<Mutex<repo::RepoStruct<#name, #partial_name>>>,
        }

        impl Module for #module_name {
            fn new() -> Self {
                #module_name {
                    repo: Arc::new(Mutex::new(repo::RepoStruct::new())),
                }
            }

            async fn handle_query(
                &mut self,
                query: crate::query::AllQueries,
            ) -> Option<std::sync::mpsc::Receiver<QueryResponse>> {
                match query {
                    AllQueries::WatchId(query) => {
                        if query.item_type != #name_str {
                            return None;
                        }
                        let (tx, rx) = std::sync::mpsc::channel::<QueryResponse>();
                        let func = Arc::new(move |items: Vec<#name>| {
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

                        let query = #partial_name {
                            id: Some(query.item_id),
                            hash: None,
                        };

                        self.repo.lock().await.watch(func, query);

                        return Some(rx);
                    },
                      AllQueries::Watch(query) => {
                        if query.item_type != #name_str {
                            return None;
                        }
                        let (tx, rx) = std::sync::mpsc::channel::<QueryResponse>();
                        let func = Arc::new(move |items: Vec<#name>| {
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

                        let query = serde_json::from_value::<#partial_name>(query.query).unwrap();

                        self.repo.lock().await.watch(func, query);

                        return Some(rx);
                    }
                }
            }

            async fn start(&self, events: tokio::sync::broadcast::Receiver<MEvent>) {
                self.repo.lock().await.listen(events).await;
            }
        }

    };

    gen.into()
}
