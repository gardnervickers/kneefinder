mod adapter;
mod e2e;

use std::{
    env,
    error::Error,
    net::{SocketAddr, TcpStream},
    time::Duration,
};

#[cfg(any(feature = "web", test))]
use std::io;

#[cfg(any(feature = "web", test))]
use kneefinder::config::{AgentEndpointConfig, AgentTransportConfig};

fn main() -> Result<(), Box<dyn Error>> {
    let mut arguments = env::args().skip(1);
    let command = arguments.next().unwrap_or_else(|| "e2e".into());

    match command.as_str() {
        "adapter" => adapter::run(),
        "adapter-hang" => adapter::run_hanging(),
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
        "e2e-tcp-web-external" => {
            #[cfg(feature = "web")]
            {
                let bind = arguments
                    .next()
                    .ok_or("e2e-tcp-web-external requires a web listen address")?
                    .parse()?;
                let endpoints = arguments
                    .map(|endpoint| parse_agent_endpoint(&endpoint))
                    .collect::<Result<Vec<_>, _>>()?;
                if endpoints.len() < 2 {
                    return Err("e2e-tcp-web-external requires at least two agent endpoints".into());
                }
                e2e::run_tcp_multi_client_web_external(bind, endpoints)
            }
            #[cfg(not(feature = "web"))]
            {
                Err("e2e-tcp-web-external requires --features web".into())
            }
        }
        "healthcheck" => {
            let address = arguments
                .next()
                .ok_or("healthcheck requires a TCP address")?
                .parse::<SocketAddr>()?;
            TcpStream::connect_timeout(&address, Duration::from_secs(1))?;
            Ok(())
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
           kneefinder-queue-demo e2e-tcp-web-external HOST:PORT ID=tcp://HOST:PORT...\n  \
           kneefinder-queue-demo adapter\n  \
           kneefinder-queue-demo adapter-tcp HOST:PORT\n  \
           kneefinder-queue-demo healthcheck HOST:PORT"
    );
}

#[cfg(any(feature = "web", test))]
fn parse_agent_endpoint(value: &str) -> Result<AgentEndpointConfig, io::Error> {
    let invalid = || {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("agent endpoint {value:?} must be ID=tcp://HOST:PORT"),
        )
    };
    let (id, endpoint) = value.split_once('=').ok_or_else(invalid)?;
    if id.is_empty()
        || !id
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
    {
        return Err(invalid());
    }
    let address = endpoint.strip_prefix("tcp://").ok_or_else(invalid)?;
    let (host, port) = address.rsplit_once(':').ok_or_else(invalid)?;
    if host.is_empty()
        || address.chars().any(char::is_whitespace)
        || port.parse::<u16>().ok().filter(|port| *port > 0).is_none()
    {
        return Err(invalid());
    }
    Ok(AgentEndpointConfig {
        id: id.into(),
        transport: AgentTransportConfig::Tcp {
            address: address.into(),
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_external_agent_endpoints() {
        assert_eq!(
            parse_agent_endpoint("agent-a=tcp://agent-a:9000").unwrap(),
            AgentEndpointConfig {
                id: "agent-a".into(),
                transport: AgentTransportConfig::Tcp {
                    address: "agent-a:9000".into(),
                },
            }
        );
        assert!(parse_agent_endpoint("agent a=tcp://agent-a:9000").is_err());
        assert!(parse_agent_endpoint("agent-a=http://agent-a:9000").is_err());
        assert!(parse_agent_endpoint("agent-a=tcp://agent-a:0").is_err());
    }
}
