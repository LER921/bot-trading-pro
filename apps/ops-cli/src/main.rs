use anyhow::Result;
use clap::{Parser, Subcommand};
use domain::Symbol;

#[derive(Debug, Parser)]
#[command(name = "ops-cli", about = "Operational commands scaffold for the trading bot")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Status,
    Pause,
    Resume,
    Reduced,
    RiskOff,
    CancelAll { symbol: Option<Symbol> },
    Flatten { symbol: Symbol },
    Shutdown,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    println!("ops-cli scaffold command accepted: {:?}", cli.command);
    Ok(())
}
