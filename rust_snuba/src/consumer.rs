use clap::Parser;
use log;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    storage: String,

    #[arg(long)]
    settings_path: String,
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let args = Args::parse();
    log::warn!(
        "Starting consumer for {} with settings at {}",
        args.storage,
        args.settings_path,
    );
}
