mod adapter;
mod e2e;

use std::{env, error::Error};

fn main() -> Result<(), Box<dyn Error>> {
    let mut arguments = env::args().skip(1);
    let command = arguments.next().unwrap_or_else(|| "e2e".into());

    match command.as_str() {
        "adapter" => adapter::run(),
        "adapter-tcp" => {
            let address = arguments
                .next()
                .ok_or("adapter-tcp requires a listen address")?;
            adapter::run_tcp(&address)
        }
        "e2e" => e2e::run(),
        "e2e-tcp" => e2e::run_tcp_multi_client(),
        "e2e-tcp-web" => {
            #[cfg(feature = "web")]
            {
                let bind = arguments
                    .next()
                    .unwrap_or_else(|| "127.0.0.1:8080".into())
                    .parse()?;
                e2e::run_tcp_multi_client_web(bind)
            }
            #[cfg(not(feature = "web"))]
            {
                Err("e2e-tcp-web requires --features web".into())
            }
        }
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
           kneefinder-queue-demo e2e-tcp\n  \
           kneefinder-queue-demo e2e-tcp-web [HOST:PORT]\n  \
           kneefinder-queue-demo adapter\n  \
           kneefinder-queue-demo adapter-tcp HOST:PORT"
    );
}
