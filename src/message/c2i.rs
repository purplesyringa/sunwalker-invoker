use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
pub enum Message {
    AddSubmission(AddSubmission),
    PushToJudgementQueue(PushToJudgementQueue),
    CancelJudgementOnTests(CancelJudgementOnTests),
    FinalizeSubmission(FinalizeSubmission),
}

#[derive(Debug, Deserialize)]
pub struct AddSubmission {
    pub compilation_core: u64,
    pub submission_id: String,
    pub problem_id: String,
    pub files: HashMap<String, Vec<u8>>,
    pub language: String,
}

#[derive(Debug, Deserialize)]
pub struct PushToJudgementQueue {
    pub core: u64,
    pub submission_id: String,
    pub tests: Vec<u64>,
}

#[derive(Debug, Deserialize)]
pub struct CancelJudgementOnTests {
    pub submission_id: String,
    pub failed_tests: Vec<u64>,
}

#[derive(Debug, Deserialize)]
pub struct FinalizeSubmission {
    pub submission_id: String,
}
