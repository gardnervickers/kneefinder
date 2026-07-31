use std::process::ExitCode;

use clap::Parser;
use kneefinder::{
    artifact::ArtifactExitStatus,
    engine::Engine,
    frontends::cli::{Cli, CliFrontend},
};

fn main() -> ExitCode {
    let engine = Engine::new();
    let frontend = CliFrontend::new(Cli::parse());
    match frontend.run_with_status(engine.handle()) {
        Ok(ArtifactExitStatus::Completed) => ExitCode::SUCCESS,
        Ok(status) => ExitCode::from(status.code()),
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}
