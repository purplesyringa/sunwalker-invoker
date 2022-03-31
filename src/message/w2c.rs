use serde::Serialize;

#[derive(Debug, Serialize)]
pub enum Message {
    Handshake(Handshake),
    UpdateMode(UpdateMode),
    PushCompilationStatus(PushCompilationStatus),
    PushTestStatus(PushTestStatus),
}

#[derive(Debug, Serialize)]
pub struct Handshake {
    pub worker_name: String,
}

#[derive(Debug, Serialize)]
pub struct UpdateMode {
    pub added_cores: Vec<u64>,
    pub removed_cores: Vec<u64>,
    pub designated_ram: u64,
}

#[derive(Debug, Serialize)]
pub struct PushCompilationStatus {
    pub submission_id: String,
    pub success: bool,
    pub status: String,
}

#[derive(Debug, Serialize)]
pub struct PushTestStatus {
    pub submission_id: String,
    pub test: u64,
    pub verdict: String,
}
