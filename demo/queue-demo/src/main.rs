mod adapter;
mod e2e;

use std::{env, error::Error};

fn main() -> Result<(), Box<dyn Error>> {
    let mut arguments = env::args().skip(1);
    let command = arguments.next().unwrap_or_else(|| "e2e".into());

    match command.as_str() {
        "adapter" => adapter::run(),
        "e2e" => e2e::run(),
        "help" | "--help" | "-h" => {
            print_help();
            Ok(())
        }
        unknown => Err(format!("unknown command {unknown:?}; run with --help").into()),
    }
}

fn print_help() {
    println!(
        "kneefinder queue demo\n\n\
         Usage:\n  \
           kneefinder-queue-demo e2e\n  \
           kneefinder-queue-demo adapter"
    );
}
