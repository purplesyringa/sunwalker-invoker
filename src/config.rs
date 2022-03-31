use serde::Deserialize;

#[derive(Deserialize)]
pub struct Config {
    pub invoker: InvokerConfig,
    pub image: ImageConfig,
    pub environment: EnvironmentConfig,
    pub conductor: ConductorConfig,
    pub cache: CacheConfig,
}

#[derive(Deserialize)]
pub struct InvokerConfig {
    pub name: String,
}

#[derive(Deserialize)]
pub struct ImageConfig {
    pub path: String,
    pub config: String,
}

#[derive(Deserialize)]
pub struct EnvironmentConfig {
    pub cpu_cores: Vec<u64>,
    pub ephemeral_disk_space: Space,
    pub ephemeral_inodes: u64,
}

#[derive(Deserialize)]
pub struct ConductorConfig {
    pub address: String,
}

#[derive(Deserialize)]
pub struct CacheConfig {
    pub problems: String,
}

#[derive(Clone, Deserialize)]
#[serde(untagged)]
pub enum Space {
    Text(String),
    Number(u64),
}

impl Into<u64> for Space {
    fn into(self) -> u64 {
        match self {
            Space::Text(s) => parse_size::parse_size(s).unwrap(),
            Space::Number(n) => n,
        }
    }
}
