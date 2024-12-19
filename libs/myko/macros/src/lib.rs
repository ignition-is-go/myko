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

            fn entity_name(&self) -> String {
                #name_str.to_string()
            }
        }

        pub struct #module_name {
            repo: Arc<Mutex<RepoStruct<#name, #partial_name>>>,
            kafka: Option<KafkaClient>,
        }

        #[async_trait::async_trait]
        impl Module for #module_name {
            fn new() -> Self {
                #module_name {
                    repo: Arc::new(Mutex::new(RepoStruct::new())),
                    kafka: None,
                }
            }

          async fn start_kafka(&mut self, brokers: &[&str], from_kafka_tx: tokio::sync::mpsc::Sender<myko_wasm::event::MEvent>) {
                let k = KafkaClient::new(brokers.join(",").as_str(), #name_str).await;

                k.consume_events(from_kafka_tx).await;

                self.kafka = Some(k);
            }

            fn entity_name(&self) -> String {
                #name_str.to_string()
            }

            async fn process_event(&mut self, event:  myko_wasm::event::MEvent, persist: bool)  {
                if event.item_type() != #name_str {
                    return;
                }
                println!("Processing event in {}", #name_str);

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
                        if query.item_type != #name_str {
                            return None;
                        }
                        let (tx, rx) = tokio::sync::mpsc::channel::<QueryResponse>(1);

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
                                match tx.send(response).await {
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
                        let (tx, rx) = tokio::sync::mpsc::channel::<QueryResponse>(1);

                        let filter_query =
                            serde_json::from_str::<#partial_name>(query.query.as_str());

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

    };

    gen.into()
}

use syn::{ItemStruct, Path};

#[proc_macro_attribute]
pub fn myko_query(attr: TokenStream, input: TokenStream) -> TokenStream {
    // Parse the single argument (e.g., `File`) from the attribute
    let query_item_type: Path = parse_macro_input!(attr as Path);

    // Parse the input struct
    let input_struct = parse_macro_input!(input as ItemStruct);
    let struct_name = &input_struct.ident;

    // Generate the implementation
    let expanded = quote! {
        #input_struct

        impl MykoQuery<#query_item_type> for #struct_name {
            fn watch(&self, client: MykoClient) -> impl tokio_stream::Stream<Item = Vec<#query_item_type>> {
                let mut query_obj = serde_json::to_value(self).unwrap();

                query_obj
                    .as_object_mut()
                    .unwrap()
                    .insert("tx".to_string(), uuid::Uuid::new_v4().to_string().into());

                let query = WrappedQuery {
                    query: query_obj,
                    query_id: stringify!(#struct_name).to_string(),
                    query_item_type: stringify!(#query_item_type).to_string(),
                };

                client.watch_query(query)
            }
        }
    };

    // Return the generated code
    TokenStream::from(expanded)
}

#[proc_macro_attribute]
pub fn myko_report(attr: TokenStream, input: TokenStream) -> TokenStream {
    let report_item_type: Path = parse_macro_input!(attr as Path);

    // Parse the input struct
    let input_struct = parse_macro_input!(input as ItemStruct);
    let struct_name = &input_struct.ident;

    // Generate the implementation
    let expanded = quote! {
        #input_struct

        impl MykoReport<#report_item_type> for #struct_name {
            fn watch(&self, client: MykoClient) -> impl tokio_stream::Stream<Item = #report_item_type> {
                let mut report_obj = serde_json::to_value(self).unwrap();

                report_obj
                    .as_object_mut()
                    .unwrap()
                    .insert("tx".to_string(), uuid::Uuid::new_v4().to_string().into());

                let report = WrappedReport {
                    report: report_obj,
                    report_id: stringify!(#struct_name).to_string(),
                };

                client.watch_report::<#struct_name, #report_item_type>(report)
            }
        }
    };

    // Return the generated code
    TokenStream::from(expanded)
}
