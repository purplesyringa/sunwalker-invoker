use anyhow::Result;

#[multiprocessing::main]
fn main() -> Result<()> {
    sunwalker_invoker::init::main()
}
