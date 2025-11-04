#[derive(Clone)]
pub struct KafkaSharedConfig {
    pub bootstrap_servers: &'static [&'static str],
}
