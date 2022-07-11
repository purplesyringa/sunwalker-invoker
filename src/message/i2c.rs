use crate::{errors, problem::verdict::TestJudgementResult};
use serde::Serialize;

#[derive(Debug, Serialize)]
pub enum Message {
    Handshake(Handshake),
    UpdateMode(UpdateMode),
    NotifyCompilationStatus(NotifyCompilationStatus),
    NotifyTestStatus(NotifyTestStatus),
    NotifySubmissionError(NotifySubmissionError),
    RequestFile(RequestFile),
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
    pub result: Result<String, errors::Error>,
}

#[derive(Debug, Serialize)]
pub struct NotifyTestStatus {
    pub submission_id: String,
    pub test: u64,
    pub judgement_result: TestJudgementResult,
}

#[derive(Debug, Serialize)]
pub struct NotifySubmissionError {
    pub submission_id: String,
    pub error: errors::Error,
}

#[derive(Debug, Serialize)]
pub struct RequestFile {
    pub request_id: u64,
    pub hash: String,
}
