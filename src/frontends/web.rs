//! Browser GUI and versioned HTTP/WebSocket control API.

use std::{
    collections::BTreeMap,
    fmt,
    net::SocketAddr,
    sync::{Arc, Mutex},
    thread,
};

use axum::{
    Json, Router,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{any, get, post},
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc};

use crate::{
    engine::{EngineCommand, EngineError, EngineEvent, EngineHandle, RunSnapshot},
    protocol::{PhaseId, RunId},
    stats::PhaseReport,
};

use super::Frontend;

pub const WEB_API_VERSION: u16 = 1;
const EVENT_BUFFER: usize = 256;
const CLIENT_BUFFER: usize = 128;
const MAX_PHASES_PER_RUN: usize = 4_096;

const INDEX_HTML: &str = include_str!("web/index.html");
const APP_CSS: &str = include_str!("web/app.css");
const APP_JS: &str = include_str!("web/app.js");

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApiSnapshot {
    pub api_version: u16,
    pub runs: Vec<RunSnapshot>,
    pub results: Vec<RunResults>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunResults {
    pub run_id: RunId,
    pub phases: Vec<PhaseObservation>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PhaseObservation {
    pub phase_id: PhaseId,
    pub report: PhaseReport,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    Command {
        request_id: String,
        command: EngineCommand,
    },
    Ping {
        nonce: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    Snapshot {
        snapshot: ApiSnapshot,
    },
    Event {
        event: EngineEvent,
    },
    CommandAccepted {
        request_id: String,
        snapshot: RunSnapshot,
    },
    CommandRejected {
        request_id: Option<String>,
        code: String,
        message: String,
    },
    ResyncRequired {
        missed_events: u64,
        snapshot: ApiSnapshot,
    },
    Pong {
        nonce: u64,
    },
}

#[derive(Clone)]
struct WebState {
    engine: EngineHandle,
    events: broadcast::Sender<EngineEvent>,
    results: Arc<Mutex<ResultStore>>,
    loopback_binding: bool,
}

impl WebState {
    fn snapshot(&self) -> ApiSnapshot {
        ApiSnapshot {
            api_version: WEB_API_VERSION,
            runs: self.engine.snapshots(),
            results: self
                .results
                .lock()
                .expect("web result mutex poisoned")
                .snapshot(),
        }
    }
}

#[derive(Default)]
struct ResultStore {
    phases: BTreeMap<RunId, Vec<PhaseObservation>>,
}

impl ResultStore {
    fn observe(&mut self, event: &EngineEvent) {
        let EngineEvent::PhaseStats {
            run_id,
            phase_id,
            report,
        } = event
        else {
            return;
        };
        let phases = self.phases.entry(*run_id).or_default();
        let observation = PhaseObservation {
            phase_id: *phase_id,
            report: report.clone(),
        };
        if let Some(existing) = phases
            .iter_mut()
            .find(|existing| existing.phase_id == *phase_id)
        {
            *existing = observation;
        } else {
            phases.push(observation);
            if phases.len() > MAX_PHASES_PER_RUN {
                phases.remove(0);
            }
        }
    }

    fn snapshot(&self) -> Vec<RunResults> {
        self.phases
            .iter()
            .map(|(run_id, phases)| RunResults {
                run_id: *run_id,
                phases: phases.clone(),
            })
            .collect()
    }
}

pub struct WebFrontend {
    bind: SocketAddr,
    allow_remote: bool,
}

impl WebFrontend {
    pub fn new(bind: SocketAddr, allow_remote: bool) -> Self {
        Self { bind, allow_remote }
    }

    fn validate_binding(&self) -> Result<(), WebFrontendError> {
        if !self.bind.ip().is_loopback() && !self.allow_remote {
            return Err(WebFrontendError::RemoteBindingDenied(self.bind));
        }
        Ok(())
    }

    async fn serve(self, engine: EngineHandle) -> Result<(), WebFrontendError> {
        self.validate_binding()?;
        let state = web_state(engine, self.bind.ip().is_loopback())?;
        let app = router(state);
        let listener = tokio::net::TcpListener::bind(self.bind).await?;
        eprintln!("kneefinder web UI: http://{}", listener.local_addr()?);
        axum::serve(listener, app).await?;
        Ok(())
    }
}

impl Frontend for WebFrontend {
    type Error = WebFrontendError;

    fn run(self, engine: EngineHandle) -> Result<(), Self::Error> {
        self.validate_binding()?;
        tokio::runtime::Runtime::new()?.block_on(self.serve(engine))
    }
}

fn web_state(engine: EngineHandle, loopback_binding: bool) -> Result<WebState, WebFrontendError> {
    let subscription = engine.subscribe();
    let (events, _) = broadcast::channel(EVENT_BUFFER);
    let results = Arc::new(Mutex::new(ResultStore::default()));
    let bridge_events = events.clone();
    let bridge_results = Arc::clone(&results);
    thread::Builder::new()
        .name("kneefinder-web-events".into())
        .spawn(move || {
            while let Ok(event) = subscription.recv() {
                bridge_results
                    .lock()
                    .expect("web result mutex poisoned")
                    .observe(&event);
                let _ = bridge_events.send(event);
            }
        })?;
    Ok(WebState {
        engine,
        events,
        results,
        loopback_binding,
    })
}

fn router(state: WebState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/app.css", get(styles))
        .route("/app.js", get(script))
        .route("/healthz", get(health))
        .route("/api/v1/snapshot", get(snapshot))
        .route("/api/v1/commands", post(command))
        .route("/api/v1/ws", any(websocket_upgrade))
        .with_state(state)
}

async fn index() -> Response {
    static_asset("text/html; charset=utf-8", INDEX_HTML, true)
}

async fn styles() -> Response {
    static_asset("text/css; charset=utf-8", APP_CSS, false)
}

async fn script() -> Response {
    static_asset("text/javascript; charset=utf-8", APP_JS, false)
}

fn static_asset(content_type: &'static str, body: &'static str, html: bool) -> Response {
    let mut response = body.into_response();
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, content_type.parse().unwrap());
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        "no-cache, no-store, must-revalidate".parse().unwrap(),
    );
    if html {
        response.headers_mut().insert(
            header::CONTENT_SECURITY_POLICY,
            "default-src 'self'; connect-src 'self' ws: wss:; img-src 'self' data:; style-src 'self'; script-src 'self'; base-uri 'none'; frame-ancestors 'none'"
                .parse()
                .unwrap(),
        );
    }
    response
}

async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "ok",
        "api_version": WEB_API_VERSION,
    }))
}

async fn snapshot(State(state): State<WebState>) -> Json<ApiSnapshot> {
    Json(state.snapshot())
}

async fn command(
    State(state): State<WebState>,
    headers: HeaderMap,
    Json(command): Json<EngineCommand>,
) -> Response {
    if !origin_allowed(&headers, state.loopback_binding) {
        return forbidden_origin();
    }
    match state.engine.execute(command) {
        Ok(snapshot) => (StatusCode::OK, Json(snapshot)).into_response(),
        Err(error) => engine_error_response(None, error),
    }
}

async fn websocket_upgrade(
    upgrade: WebSocketUpgrade,
    State(state): State<WebState>,
    headers: HeaderMap,
) -> Response {
    if !origin_allowed(&headers, state.loopback_binding) {
        return forbidden_origin();
    }
    upgrade.on_upgrade(move |socket| websocket(socket, state))
}

fn origin_allowed(headers: &HeaderMap, loopback_binding: bool) -> bool {
    let Some(origin) = headers.get(header::ORIGIN) else {
        // Non-browser API clients are not required to manufacture an Origin.
        return true;
    };
    let (Ok(origin), Some(host)) = (
        origin.to_str(),
        headers
            .get(header::HOST)
            .and_then(|host| host.to_str().ok()),
    ) else {
        return false;
    };
    if loopback_binding && !is_loopback_host(host) {
        return false;
    }
    origin == format!("http://{host}") || origin == format!("https://{host}")
}

fn is_loopback_host(host: &str) -> bool {
    host == "localhost"
        || host.starts_with("localhost:")
        || host == "[::1]"
        || host.starts_with("[::1]:")
        || host
            .split_once(':')
            .map_or(host, |(address, _)| address)
            .parse::<std::net::Ipv4Addr>()
            .is_ok_and(|address| address.is_loopback())
}

fn forbidden_origin() -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(ServerMessage::CommandRejected {
            request_id: None,
            code: "origin_rejected".into(),
            message: "request Origin does not match the kneefinder server".into(),
        }),
    )
        .into_response()
}

async fn websocket(socket: WebSocket, state: WebState) {
    let mut events = state.events.subscribe();
    let (mut sender, mut receiver) = socket.split();
    let (outgoing, mut messages) = mpsc::channel::<Message>(CLIENT_BUFFER);
    let writer = tokio::spawn(async move {
        while let Some(message) = messages.recv().await {
            if sender.send(message).await.is_err() {
                break;
            }
        }
    });

    if queue_message(
        &outgoing,
        &ServerMessage::Snapshot {
            snapshot: state.snapshot(),
        },
    )
    .await
    .is_err()
    {
        writer.abort();
        return;
    }

    loop {
        tokio::select! {
            incoming = receiver.next() => {
                let Some(Ok(message)) = incoming else { break };
                match message {
                    Message::Text(text) => {
                        let reply = match serde_json::from_str::<ClientMessage>(&text) {
                            Ok(ClientMessage::Command { request_id, command }) => {
                                match state.engine.execute(command) {
                                    Ok(snapshot) => ServerMessage::CommandAccepted {
                                        request_id,
                                        snapshot,
                                    },
                                    Err(error) => ServerMessage::CommandRejected {
                                        request_id: Some(request_id),
                                        code: engine_error_code(&error).into(),
                                        message: error.to_string(),
                                    },
                                }
                            }
                            Ok(ClientMessage::Ping { nonce }) => ServerMessage::Pong { nonce },
                            Err(error) => ServerMessage::CommandRejected {
                                request_id: None,
                                code: "invalid_message".into(),
                                message: error.to_string(),
                            },
                        };
                        if queue_message(&outgoing, &reply).await.is_err() {
                            break;
                        }
                    }
                    Message::Close(_) => break,
                    Message::Ping(payload) => {
                        if outgoing.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    _ => {}
                }
            }
            event = events.recv() => {
                let message = match event {
                    Ok(event) => ServerMessage::Event { event },
                    Err(broadcast::error::RecvError::Lagged(missed_events)) => {
                        ServerMessage::ResyncRequired {
                            missed_events,
                            snapshot: state.snapshot(),
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                };
                if queue_message(&outgoing, &message).await.is_err() {
                    break;
                }
            }
        }
    }

    drop(outgoing);
    let _ = writer.await;
}

async fn queue_message(
    outgoing: &mpsc::Sender<Message>,
    message: &ServerMessage,
) -> Result<(), ()> {
    let encoded = serde_json::to_string(message).map_err(|_| ())?;
    outgoing
        .send(Message::Text(encoded.into()))
        .await
        .map_err(|_| ())
}

fn engine_error_response(request_id: Option<String>, error: EngineError) -> Response {
    let status = match error {
        EngineError::RunNotFound(_) => StatusCode::NOT_FOUND,
        EngineError::ConfigurationLocked(_) | EngineError::InvalidTransition { .. } => {
            StatusCode::CONFLICT
        }
        EngineError::RunIdExhausted | EngineError::RevisionExhausted(_) => {
            StatusCode::INTERNAL_SERVER_ERROR
        }
    };
    let message = ServerMessage::CommandRejected {
        request_id,
        code: engine_error_code(&error).into(),
        message: error.to_string(),
    };
    (status, Json(message)).into_response()
}

fn engine_error_code(error: &EngineError) -> &'static str {
    match error {
        EngineError::RunNotFound(_) => "run_not_found",
        EngineError::ConfigurationLocked(_) => "configuration_locked",
        EngineError::InvalidTransition { .. } => "invalid_transition",
        EngineError::RunIdExhausted => "run_id_exhausted",
        EngineError::RevisionExhausted(_) => "revision_exhausted",
    }
}

#[derive(Debug)]
pub enum WebFrontendError {
    RemoteBindingDenied(SocketAddr),
    Io(std::io::Error),
}

impl fmt::Display for WebFrontendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RemoteBindingDenied(address) => write!(
                formatter,
                "refusing unauthenticated non-loopback bind {address}; pass --allow-remote to acknowledge the risk"
            ),
            Self::Io(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for WebFrontendError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::RemoteBindingDenied(_) => None,
        }
    }
}

impl From<std::io::Error> for WebFrontendError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::{PhaseReport, summarize_results};

    #[test]
    fn remote_binding_requires_an_explicit_opt_in() {
        let frontend = WebFrontend::new("0.0.0.0:8080".parse().unwrap(), false);
        assert!(matches!(
            frontend.validate_binding(),
            Err(WebFrontendError::RemoteBindingDenied(_))
        ));
        assert!(
            WebFrontend::new("127.0.0.1:8080".parse().unwrap(), false)
                .validate_binding()
                .is_ok()
        );
    }

    #[test]
    fn result_store_replaces_a_repeated_phase() {
        let mut store = ResultStore::default();
        for goodput_rate in [90.0, 95.0] {
            store.observe(&EngineEvent::PhaseStats {
                run_id: RunId(1),
                phase_id: PhaseId(2),
                report: PhaseReport {
                    offered_rate: 100.0,
                    goodput_rate,
                    elapsed_ns: 1_000_000_000,
                    stats: summarize_results(&[]).unwrap(),
                },
            });
        }

        let snapshot = store.snapshot();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].phases.len(), 1);
        assert_eq!(snapshot[0].phases[0].report.goodput_rate, 95.0);
    }

    #[test]
    fn websocket_protocol_is_tagged_and_versioned() {
        let message = ServerMessage::Snapshot {
            snapshot: ApiSnapshot {
                api_version: WEB_API_VERSION,
                runs: Vec::new(),
                results: Vec::new(),
            },
        };
        let json = serde_json::to_string(&message).unwrap();
        assert!(json.contains(r#""type":"snapshot""#));
        assert!(json.contains(r#""api_version":1"#));
    }

    #[test]
    fn browser_origins_must_match_the_server_host() {
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, "127.0.0.1:8080".parse().unwrap());
        headers.insert(header::ORIGIN, "http://127.0.0.1:8080".parse().unwrap());
        assert!(origin_allowed(&headers, true));

        headers.insert(header::ORIGIN, "https://attacker.example".parse().unwrap());
        assert!(!origin_allowed(&headers, true));

        headers.remove(header::ORIGIN);
        assert!(origin_allowed(&headers, true));
    }
}
