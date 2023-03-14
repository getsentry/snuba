use clap::Parser;
use log;

mod settings;

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
    log::info!(
        "Starting consumer for {} with settings at {}",
        args.storage,
        args.settings_path,
    );
    let settings = settings::Settings::load_from_json(&args.settings_path).unwrap();
    log::info!("Loaded settings: {:?}", settings);
}
