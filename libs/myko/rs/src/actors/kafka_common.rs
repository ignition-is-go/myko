#[derive(Clone, Copy)]
pub struct KafkaSharedConfig {
    pub bootstrap_servers: &'static [&'static str],
}
