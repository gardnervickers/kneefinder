use std::process::ExitCode;

use clap::Parser;
use kneefinder::{
    engine::Engine,
    frontends::{
        Frontend,
        cli::{Cli, CliFrontend},
    },
};

fn main() -> ExitCode {
    let engine = Engine::new();
    let frontend = CliFrontend::new(Cli::parse());
    if let Err(error) = frontend.run(engine.handle()) {
        eprintln!("error: {error}");
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}
