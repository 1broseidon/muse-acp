//! The client half of the Muse Session Protocol.
//!
//! `muse serve` speaks newline-delimited JSON-RPC 2.0 on its stdin and stdout,
//! and the process that owns those pipes is its only client. This module is
//! that client and nothing more: it numbers requests, matches their answers,
//! and hands everything the host says unprompted to whoever reads the event
//! channel. It knows no method beyond the two server requests, which it has to
//! acknowledge itself (see [`Event::Request`]).

use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::Path;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, oneshot};

/// Something the host said that was not an answer to one of our requests.
#[derive(Debug)]
pub enum Event {
    Notification {
        method: String,
        params: Value,
    },
    /// A server-initiated request (`approval/request`, `userInput/request`).
    /// Its JSON-RPC response is only a presentation receipt; the decision
    /// travels later as a command of its own. The receipt has already been
    /// sent by the time this is read, so a slow reader cannot stall the host.
    Request {
        method: String,
        params: Value,
    },
}

/// An error answer from the host, or the host having gone away.
#[derive(Clone, Debug, PartialEq)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    pub data: Option<Value>,
}

impl RpcError {
    fn host_gone() -> Self {
        Self {
            code: -32603,
            message: "muse serve has exited".to_string(),
            data: None,
        }
    }

    /// The stable `error.data.kind` MSP puts on every error, when present.
    pub fn kind(&self) -> Option<&str> {
        self.data.as_ref()?.get("kind")?.as_str()
    }
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind() {
            Some(kind) => write!(f, "{} ({kind})", self.message),
            None => f.write_str(&self.message),
        }
    }
}

type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<Result<Value, RpcError>>>>>;

/// A connection to one `muse serve` host.
pub struct Msp {
    outgoing: mpsc::UnboundedSender<String>,
    pending: Pending,
    next_id: AtomicU64,
}

impl Msp {
    /// Speaks MSP over a pair of byte streams. Production passes the child's
    /// pipes; tests pass an in-memory duplex with a scripted host behind it.
    pub fn connect(
        reader: impl AsyncRead + Unpin + Send + 'static,
        writer: impl AsyncWrite + Unpin + Send + 'static,
    ) -> (Self, mpsc::UnboundedReceiver<Event>) {
        let (outgoing, lines) = mpsc::unbounded_channel();
        let (events, received) = mpsc::unbounded_channel();
        let pending: Pending = Arc::default();
        tokio::spawn(write_lines(writer, lines));
        // The reader holds only a weak handle for receipts, so dropping the
        // connection closes the host's stdin even while the reader still runs.
        tokio::spawn(read_lines(
            reader,
            pending.clone(),
            outgoing.downgrade(),
            events,
        ));
        let msp = Self {
            outgoing,
            pending,
            next_id: AtomicU64::new(1),
        };
        (msp, received)
    }

    pub async fn request(&self, method: &str, params: Value) -> Result<Value, RpcError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (answer, answered) = oneshot::channel();
        lock(&self.pending).insert(id, answer);
        let line = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        if self.outgoing.send(line.to_string()).is_err() {
            lock(&self.pending).remove(&id);
            return Err(RpcError::host_gone());
        }
        answered
            .await
            .unwrap_or_else(|_| Err(RpcError::host_gone()))
    }

    pub fn notify(&self, method: &str, params: Option<Value>) {
        let mut line = json!({"jsonrpc": "2.0", "method": method});
        if let Some(params) = params {
            line["params"] = params;
        }
        let _ = self.outgoing.send(line.to_string());
    }
}

/// Starts `muse serve` with the given arguments.
///
/// `META_API_KEY` is removed from the child's environment because Muse
/// prefers it over the account login, and this adapter exists to run on the
/// person's subscription. A key stored with `muse auth set` is unaffected.
/// stderr is inherited so the host's own diagnostics reach whoever runs us.
pub fn spawn(
    muse: &Path,
    serve_args: &[String],
) -> std::io::Result<(Child, Msp, mpsc::UnboundedReceiver<Event>)> {
    let mut child = Command::new(muse)
        .arg("serve")
        .args(serve_args)
        .env_remove("META_API_KEY")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()?;
    let stdin = child.stdin.take().expect("stdin was piped");
    let stdout = child.stdout.take().expect("stdout was piped");
    let (msp, events) = Msp::connect(stdout, stdin);
    Ok((child, msp, events))
}

/// A fresh MSP idempotency handle. Every command carries one, and the host
/// derives a turn's id from the `turn/start` that created it.
pub(crate) fn command_id() -> String {
    uuid::Uuid::now_v7().to_string()
}

async fn write_lines(
    mut writer: impl AsyncWrite + Unpin,
    mut lines: mpsc::UnboundedReceiver<String>,
) {
    while let Some(line) = lines.recv().await {
        let written = async {
            writer.write_all(line.as_bytes()).await?;
            writer.write_all(b"\n").await?;
            writer.flush().await
        };
        if written.await.is_err() {
            break;
        }
    }
    // Dropping the writer closes the host's stdin, which is how it is told
    // to shut down.
}

async fn read_lines(
    reader: impl AsyncRead + Unpin,
    pending: Pending,
    receipts: mpsc::WeakUnboundedSender<String>,
    events: mpsc::UnboundedSender<Event>,
) {
    let mut lines = BufReader::new(reader).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let Ok(message) = serde_json::from_str::<Value>(&line) else {
            eprintln!("muse-acp: unreadable line from muse serve: {line}");
            continue;
        };
        let method = message
            .get("method")
            .and_then(Value::as_str)
            .map(str::to_string);
        let id = message.get("id").cloned().filter(|id| !id.is_null());
        let params = message.get("params").cloned().unwrap_or(Value::Null);
        match (method, id) {
            (Some(method), Some(id)) => {
                let receipt = json!({"jsonrpc": "2.0", "id": id, "result": {}});
                if let Some(outgoing) = receipts.upgrade() {
                    let _ = outgoing.send(receipt.to_string());
                }
                let _ = events.send(Event::Request { method, params });
            }
            (Some(method), None) => {
                let _ = events.send(Event::Notification { method, params });
            }
            (None, Some(id)) => {
                let Some(answer) = id.as_u64().and_then(|id| lock(&pending).remove(&id)) else {
                    continue;
                };
                let _ = answer.send(match message.get("error") {
                    Some(error) => Err(RpcError {
                        code: error.get("code").and_then(Value::as_i64).unwrap_or(-32603),
                        message: error
                            .get("message")
                            .and_then(Value::as_str)
                            .unwrap_or("muse serve refused the request")
                            .to_string(),
                        data: error.get("data").cloned(),
                    }),
                    None => Ok(message.get("result").cloned().unwrap_or(Value::Null)),
                });
            }
            (None, None) => {}
        }
    }
    // The host is gone: every caller still waiting hears so, and the event
    // channel closes when `events` drops here.
    for (_, answer) in lock(&pending).drain() {
        let _ = answer.send(Err(RpcError::host_gone()));
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
