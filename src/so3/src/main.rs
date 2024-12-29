#![forbid(unsafe_code)]

use clap::Parser;

use crate::cli::{CliArgs, CliSubcommands};

mod cli;

#[tokio::main]
async fn main() {
    let cli = CliArgs::parse();
    handle_cli(cli);
    
    println!("Hello, world!");
}

fn handle_cli(cli: CliArgs) {
    match &cli.command {
        Some(CliSubcommands::Serve { url }) => {
            if !url.is_empty() {
                println!("Provided url: {}", url);
            } else {
                println!("Url not provided...");
            }
        }
        None => {}
    }
}
