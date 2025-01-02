use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(version, about, long_about = None)]
pub struct CliArgs {
    #[command(subcommand)]
    pub command: Option<CliSubcommands>,
}

#[derive(Subcommand)]
pub enum CliSubcommands {
    /// Start serving
    Serve {
        /// url to serve
        #[arg(short, long)]
        url: String,
    },
}
