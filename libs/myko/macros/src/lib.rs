// lib.rs
extern crate proc_macro;

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(Empty)]
pub fn empty_impl(input: TokenStream) -> TokenStream {
    let ast = parse_macro_input!(input as DeriveInput);
    let name = &ast.ident;

    let gen = quote! {

        impl Empty for #name {
            fn empty(&self) -> bool {
                false
            }
        }

    };

    gen.into()
}

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
            repo: Arc<Mutex<RepoStruct<#name, #partial_name>>>,
        }

        #[async_trait::async_trait]
        impl Module for #module_name {
            fn new() -> Self {
                #module_name {
                    repo: Arc::new(Mutex::new(RepoStruct::new())),
                }
            }

            async fn handle_query(
                &mut self,
                query: myko_wasm::query::Query,
            ) -> Option<std::sync::mpsc::Receiver<QueryResponse>> {
                match query {
                        Query::WatchId(query) => {
                        if query.item_type != #name_str {
                            return None;
                        }
                        let (tx, rx) = std::sync::mpsc::channel::<QueryResponse>();

                        let query_filter = #partial_name {
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
                                match tx.send(response) {
                                    Ok(_) => (),
                                    Err(e) => println!("Failed to send response: {}", e),
                                }
                            }
                        });

                        return Some(rx);
                    }
                    Query::Watch(query) => {
                        if query.item_type != #name_str {
                            return None;
                        }
                        let (tx, rx) = std::sync::mpsc::channel::<QueryResponse>();

                        let filter_query =
                            serde_json::from_value::<#partial_name>(query.query).unwrap();

                        let mut qrx = self.repo.lock().await.watch(filter_query);

                        tokio::spawn(async move {
                            while let Some(items) = qrx.recv().await {
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
                            }
                        });

                        return Some(rx);
                    }
                }
            }

            async fn start(&self, events: tokio::sync::broadcast::Receiver<myko_wasm::event::MEvent>) {
                self.repo.lock().await.listen(events).await;
            }
        }

    };

    gen.into()
}
