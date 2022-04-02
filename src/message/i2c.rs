use serde::Serialize;

#[derive(Debug, Serialize)]
pub enum Message {
    Handshake(Handshake),
    UpdateMode(UpdateMode),
    NotifyCompilationStatus(NotifyCompilationStatus),
    NotifyTestStatus(NotifyTestStatus),
}

#[derive(Debug, Serialize)]
pub struct Handshake {
    pub invoker_name: String,
}

#[derive(Debug, Serialize)]
pub struct UpdateMode {
    pub added_cores: Vec<u64>,
    pub removed_cores: Vec<u64>,
    pub designated_ram: u64,
}

#[derive(Debug, Serialize)]
pub struct NotifyCompilationStatus {
    pub submission_id: String,
    pub success: bool,
    pub log: String,
}

#[derive(Debug, Serialize)]
pub struct NotifyTestStatus {
    pub submission_id: String,
    pub test: u64,
    pub verdict: String,
}
