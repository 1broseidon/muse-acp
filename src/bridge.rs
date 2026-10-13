//! The ACP agent: one ACP connection in front of one `muse serve` host.
//!
//! ACP asks and waits; MSP acknowledges and then streams. A `session/prompt`
//! is therefore answered only when Muse's `turn/completed` for that turn
//! arrives on the event stream, and everything Muse says in between is
//! translated into `session/update`s as it comes. An ACP session id is the
//! MSP session id, so a client's saved checkpoint reopens the same Muse
//! conversation with `session/load` or `session/resume`.

use crate::msp::{Event, Msp, RpcError, command_id};
use crate::translate::{self, EFFORT_CONFIG, MODEL_CONFIG, Model, Transcript};
use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::schema::v1::{
    AgentCapabilities, AuthMethod, AuthMethodTerminal, AuthenticateRequest, AuthenticateResponse,
    CancelNotification, ConfigOptionUpdate, ContentBlock, CurrentModeUpdate, Error, Implementation,
    InitializeRequest, InitializeResponse, LoadSessionRequest, LoadSessionResponse,
    McpCapabilities, McpServer, NewSessionRequest, NewSessionResponse, PromptCapabilities,
    PromptRequest, PromptResponse, RequestPermissionOutcome, RequestPermissionRequest,
    ResumeSessionRequest, ResumeSessionResponse, SessionAdditionalDirectoriesCapabilities,
    SessionCapabilities, SessionConfigOption, SessionModeState, SessionNotification,
    SessionResumeCapabilities, SessionUpdate, SetSessionConfigOptionRequest,
    SetSessionConfigOptionResponse, SetSessionModeRequest, SetSessionModeResponse, StopReason,
    Usage,
};
use agent_client_protocol::{
    Agent, Client, ConnectTo, ConnectionTo, JsonRpcRequest, JsonRpcResponse, Responder,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use tokio::sync::{mpsc, oneshot};

/// The auth method id a client runs as `muse-acp login`.
pub const LOGIN_METHOD: &str = "muse-login";

/// Runs the agent until the client hangs up or the host exits.
pub async fn serve(
    transport: impl ConnectTo<Agent> + 'static,
    msp: Msp,
    events: mpsc::UnboundedReceiver<Event>,
) -> Result<(), Error> {
    let bridge = Arc::new(Bridge {
        msp,
        granted: Mutex::default(),
        sessions: Mutex::default(),
    });
    let router = bridge.clone();
    Agent
        .builder()
        .name("muse-acp")
        .on_receive_request(
            {
                let bridge = bridge.clone();
                move |request: InitializeRequest, responder: Responder<InitializeResponse>, _cx| {
                    let bridge = bridge.clone();
                    async move { responder.respond_with_result(bridge.initialize(request).await) }
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            async |_: AuthenticateRequest, responder: Responder<AuthenticateResponse>, _cx| {
                // Login happens in a terminal (`muse-acp login`), and Muse
                // reads the stored credential itself; there is nothing to do
                // here but say so.
                responder.respond(AuthenticateResponse::new())
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let bridge = bridge.clone();
                move |request: NewSessionRequest, responder: Responder<NewSessionResponse>, _cx| {
                    let bridge = bridge.clone();
                    async move { responder.respond_with_result(bridge.new_session(request).await) }
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let bridge = bridge.clone();
                move |request: LoadSessionRequest, responder: Responder<LoadSessionResponse>, cx: ConnectionTo<Client>| {
                    let bridge = bridge.clone();
                    async move { responder.respond_with_result(bridge.load_session(request, &cx).await) }
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let bridge = bridge.clone();
                move |request: ResumeSessionRequest, responder: Responder<ResumeSessionResponse>, _cx| {
                    let bridge = bridge.clone();
                    async move { responder.respond_with_result(bridge.resume_session(request).await) }
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let bridge = bridge.clone();
                move |request: PromptRequest, responder: Responder<PromptResponse>, cx: ConnectionTo<Client>| {
                    let bridge = bridge.clone();
                    // A turn outlives this callback; answering it from a task
                    // keeps the connection reading cancels and steering lines
                    // while Muse works.
                    async move { cx.spawn(async move { responder.respond_with_result(bridge.prompt(request).await) }) }
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let bridge = bridge.clone();
                move |request: SteerRequest, responder: Responder<SteerResponse>, _cx| {
                    let bridge = bridge.clone();
                    async move { responder.respond_with_result(bridge.steer(request).await) }
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let bridge = bridge.clone();
                move |request: SetSessionModeRequest, responder: Responder<SetSessionModeResponse>, _cx| {
                    let bridge = bridge.clone();
                    async move { responder.respond_with_result(bridge.set_mode(request).await) }
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_request(
            {
                let bridge = bridge.clone();
                move |request: SetSessionConfigOptionRequest, responder: Responder<SetSessionConfigOptionResponse>, _cx| {
                    let bridge = bridge.clone();
                    async move { responder.respond_with_result(bridge.set_config(request).await) }
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .on_receive_notification(
            {
                let bridge = bridge.clone();
                move |notification: CancelNotification, _cx| {
                    let bridge = bridge.clone();
                    async move {
                        bridge.cancel(&notification.session_id.0).await;
                        Ok(())
                    }
                }
            },
            agent_client_protocol::on_receive_notification!(),
        )
        .connect_with(transport, async move |cx: ConnectionTo<Client>| {
            tokio::select! {
                () = cx.incoming_closed() => {}
                () = router.route(cx.clone(), events) => {}
            }
            Ok(())
        })
        .await
}

/// The steering extension's request: a line for the turn already running.
/// `_meta.steering.idleBehavior` is accepted and ignored, because this agent
/// only ever answers an idle session with `promptRequired`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonRpcRequest)]
#[request(method = "_session/steering", response = SteerResponse)]
#[serde(rename_all = "camelCase")]
struct SteerRequest {
    session_id: String,
    prompt: Vec<ContentBlock>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonRpcResponse)]
struct SteerResponse {
    outcome: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

struct Bridge {
    msp: Msp,
    /// The MSP capabilities the host granted at `initialize`.
    granted: Mutex<Vec<String>>,
    sessions: Mutex<HashMap<String, Session>>,
}

/// What a session was opened with, kept so Muse can be handed it again when
/// the session has to be reopened.
#[derive(Clone, Default)]
struct Opening {
    cwd: PathBuf,
    /// The workspace roots: the session's directory, then its extra folders.
    roots: Vec<String>,
    /// MSP's `config` for the session: the client's MCP servers.
    config: Option<Value>,
}

#[derive(Default)]
struct Session {
    opening: Opening,
    /// Whether the next turn must carry the workspace roots. A resumed
    /// session cannot be given them any earlier: `session/resume` takes none.
    roots_pending: bool,
    mode: String,
    model: Option<String>,
    effort: Option<String>,
    catalog: Vec<Model>,
    transcript: Transcript,
    /// The turn Muse is running, from `turn/started` to `turn/completed`.
    running: Option<String>,
    /// Prompts waiting on their turn, by turn id.
    waiting: HashMap<String, Waiter>,
    /// Approvals still open, by id, as last presented; and the stages already
    /// put to the client, so a re-issued request is not asked twice.
    approvals: HashMap<String, Value>,
    asked: HashSet<(String, i64)>,
}

struct Waiter {
    done: oneshot::Sender<Ended>,
    usage: Option<Usage>,
}

/// How a turn ended, as the prompt waiting on it needs to know.
enum Ended {
    Completed(Option<Usage>),
    Cancelled,
    Failed { kind: String, message: String },
}

impl Bridge {
    async fn initialize(&self, request: InitializeRequest) -> Result<InitializeResponse, Error> {
        let client = json!({
            // MSP wants a machine identifier here: lowercase, digits, underscores.
            "clientInfo": {"name": "muse_acp", "version": env!("CARGO_PKG_VERSION")},
            // An ACP client has no dialog for Muse's own questions; declining
            // them makes Muse settle without asking rather than wait forever.
            "capabilities": {"requestedCapabilities": ["sessionMcp"], "userInputDialogs": false},
        });
        let host = self
            .msp
            .request("initialize", client)
            .await
            .map_err(refused)?;
        self.msp.notify("initialized", None);
        *lock(&self.granted) = host
            .get("grantedCapabilities")
            .and_then(Value::as_array)
            .map(|names| {
                names
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();

        let login = AuthMethodTerminal::new(LOGIN_METHOD, "Log in with Meta")
            .description(
                "Runs `muse login`: approve the code it shows in your browser.".to_string(),
            )
            .args(vec!["login".to_string()]);
        let mut meta = serde_json::Map::new();
        meta.insert("steering".to_string(), json!({"supported": true}));
        let version = request.protocol_version.min(ProtocolVersion::V1);
        Ok(
            InitializeResponse::new(version)
                .agent_capabilities(
                    AgentCapabilities::new()
                        .load_session(true)
                        .prompt_capabilities(
                            PromptCapabilities::new().image(true).embedded_context(true),
                        )
                        .mcp_capabilities(McpCapabilities::new().http(true))
                        .session_capabilities(
                            SessionCapabilities::new()
                                .resume(SessionResumeCapabilities::new())
                                .additional_directories(
                                    SessionAdditionalDirectoriesCapabilities::new(),
                                ),
                        ),
                )
                .auth_methods(vec![AuthMethod::Terminal(login)])
                .agent_info(
                    Implementation::new("muse-acp", env!("CARGO_PKG_VERSION"))
                        .title("Muse Code".to_string()),
                )
                .meta(meta),
        )
    }

    async fn new_session(&self, request: NewSessionRequest) -> Result<NewSessionResponse, Error> {
        let opening = self.opening(
            request.cwd,
            &request.additional_directories,
            &request.mcp_servers,
        );
        let mut params = json!({"commandId": command_id(), "workspaceRoots": opening.roots});
        if let Some(config) = &opening.config {
            params["config"] = config.clone();
        }
        let started = self
            .msp
            .request("session/start", params)
            .await
            .map_err(refused)?;
        let session = started.get("session").unwrap_or(&Value::Null);
        let (id, modes, options) = self.adopt(session, opening, false).await?;
        Ok(NewSessionResponse::new(id)
            .modes(modes)
            .config_options(options))
    }

    /// Reopens a stored conversation and replays it to the client before
    /// answering, as `session/load` promises.
    async fn load_session(
        &self,
        request: LoadSessionRequest,
        cx: &ConnectionTo<Client>,
    ) -> Result<LoadSessionResponse, Error> {
        let opening = self.opening(
            request.cwd,
            &request.additional_directories,
            &request.mcp_servers,
        );
        let resumed = self
            .resume(&request.session_id.0, opening.config.clone(), true)
            .await?;
        let items = resumed.pointer("/history/items").and_then(Value::as_array);
        for item in items.map(Vec::as_slice).unwrap_or_default() {
            for update in translate::replay(item) {
                send(cx, &request.session_id.0, update);
            }
        }
        let session = resumed.get("session").unwrap_or(&Value::Null);
        let (_, modes, options) = self.adopt(session, opening, true).await?;
        Ok(LoadSessionResponse::new()
            .modes(modes)
            .config_options(options))
    }

    /// Reopens a stored conversation without replaying it: the client keeps
    /// its own transcript.
    async fn resume_session(
        &self,
        request: ResumeSessionRequest,
    ) -> Result<ResumeSessionResponse, Error> {
        let opening = self.opening(
            request.cwd,
            &request.additional_directories,
            &request.mcp_servers,
        );
        let resumed = self
            .resume(&request.session_id.0, opening.config.clone(), false)
            .await?;
        let session = resumed.get("session").unwrap_or(&Value::Null);
        let (_, modes, options) = self.adopt(session, opening, true).await?;
        Ok(ResumeSessionResponse::new()
            .modes(modes)
            .config_options(options))
    }

    async fn resume(
        &self,
        session_id: &str,
        config: Option<Value>,
        with_history: bool,
    ) -> Result<Value, Error> {
        let mut params = json!({
            "commandId": command_id(),
            "sessionId": session_id,
            "excludeItems": !with_history,
        });
        if with_history {
            params["history"] = json!("inline");
        }
        if let Some(config) = config {
            params["config"] = config;
        }
        self.msp
            .request("session/resume", params)
            .await
            .map_err(refused)
    }

    /// Records a session Muse just opened and answers with its pickers.
    async fn adopt(
        &self,
        session: &Value,
        opening: Opening,
        roots_pending: bool,
    ) -> Result<(String, SessionModeState, Vec<SessionConfigOption>), Error> {
        let id = session
            .get("sessionId")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                Error::internal_error().data(json!("muse serve opened a session without an id"))
            })?
            .to_string();
        // The pickers are a convenience; a host that cannot list its models
        // still runs the conversation on its default.
        let catalog = match self
            .msp
            .request("model/list", json!({"sessionId": id}))
            .await
        {
            Ok(list) => translate::models(&list),
            Err(error) => {
                eprintln!("muse-acp: model/list failed: {error}");
                Vec::new()
            }
        };
        let mode = session
            .pointer("/approvalMode/mode")
            .and_then(Value::as_str)
            .unwrap_or("onRequest")
            .to_string();
        let model = session
            .get("modelId")
            .and_then(Value::as_str)
            .map(str::to_string);
        let running = session
            .get("activeTurnId")
            .and_then(Value::as_str)
            .map(str::to_string);
        let modes = translate::modes(&mode);
        let options = translate::config_options(&catalog, model.as_deref(), None);
        lock(&self.sessions).insert(
            id.clone(),
            Session {
                opening,
                roots_pending,
                mode,
                model,
                catalog,
                running,
                ..Session::default()
            },
        );
        Ok((id, modes, options))
    }

    /// A session's directory, folders and MCP servers, as MSP takes them.
    fn opening(&self, cwd: PathBuf, additional: &[PathBuf], servers: &[McpServer]) -> Opening {
        let roots = workspace_roots(&cwd, additional);
        let config = if servers.is_empty() {
            None
        } else if !lock(&self.granted).iter().any(|name| name == "sessionMcp") {
            eprintln!(
                "muse-acp: this muse serve does not take MCP servers per session; ignoring them"
            );
            None
        } else {
            let servers: serde_json::Map<String, Value> =
                servers.iter().filter_map(translate::mcp_server).collect();
            Some(json!({"mcpServers": servers}))
        };
        Opening { cwd, roots, config }
    }

    async fn prompt(&self, request: PromptRequest) -> Result<PromptResponse, Error> {
        let session_id = request.session_id.0.to_string();
        let (opening, roots_pending) = {
            let mut sessions = lock(&self.sessions);
            let session = sessions
                .get_mut(&session_id)
                .ok_or_else(|| unknown_session(&session_id))?;
            (
                session.opening.clone(),
                std::mem::take(&mut session.roots_pending),
            )
        };
        let input = translate::turn_input(&request.prompt, &opening.cwd);
        let mut command = command_id();
        let (done, ended) = oneshot::channel();
        self.wait_for(&session_id, &command, done);
        let mut params = json!({
            "commandId": command,
            "sessionId": session_id,
            "input": input,
            "ifBusy": "queue",
        });
        if roots_pending {
            params["workspaceRoots"] = json!(opening.roots);
        }
        let mut started = self.msp.request("turn/start", params.clone()).await;
        // Muse unloads a session that sits idle; the conversation is still on
        // disk, so it is reopened with what it was opened with and the turn
        // asked for again. The new command id keeps any record Muse kept of
        // the refused one from answering for it.
        if matches!(&started, Err(error) if error.kind() == Some("sessionNotLoaded")) {
            let retry = command_id();
            self.rekey(&session_id, &command, &retry);
            command = retry;
            params["commandId"] = json!(command);
            params["workspaceRoots"] = json!(opening.roots);
            if let Err(error) = self.resume(&session_id, opening.config, false).await {
                self.forget_waiter(&session_id, &command);
                return Err(error);
            }
            started = self.msp.request("turn/start", params).await;
        }
        match started {
            Ok(result) => {
                let turn = result
                    .get("turnId")
                    .and_then(Value::as_str)
                    .unwrap_or(&command);
                if turn != command {
                    self.rekey(&session_id, &command, turn);
                }
            }
            Err(error) => {
                self.forget_waiter(&session_id, &command);
                return Err(refused(error));
            }
        }
        match ended.await {
            Ok(Ended::Completed(usage)) => {
                Ok(PromptResponse::new(StopReason::EndTurn).usage(usage))
            }
            Ok(Ended::Cancelled) => Ok(PromptResponse::new(StopReason::Cancelled)),
            Ok(Ended::Failed { kind, .. }) if kind == "stepLimit" => {
                Ok(PromptResponse::new(StopReason::MaxTurnRequests))
            }
            Ok(Ended::Failed { kind, message }) if kind == "authRequired" => {
                Err(Error::auth_required().data(json!(message)))
            }
            Ok(Ended::Failed { message, .. }) => Err(Error::new(-32603, message)),
            Err(_) => Err(Error::new(-32603, "muse serve has exited")),
        }
    }

    fn wait_for(&self, session_id: &str, turn: &str, done: oneshot::Sender<Ended>) {
        if let Some(session) = lock(&self.sessions).get_mut(session_id) {
            session
                .waiting
                .insert(turn.to_string(), Waiter { done, usage: None });
        }
    }

    fn rekey(&self, session_id: &str, from: &str, to: &str) {
        if let Some(session) = lock(&self.sessions).get_mut(session_id)
            && let Some(waiter) = session.waiting.remove(from)
        {
            session.waiting.insert(to.to_string(), waiter);
        }
    }

    fn forget_waiter(&self, session_id: &str, turn: &str) {
        if let Some(session) = lock(&self.sessions).get_mut(session_id) {
            session.waiting.remove(turn);
        }
    }

    /// Puts a line into the turn Muse is running. MSP names the turn it means
    /// (`expectedTurnId`), so a line meant for one turn can never land in the
    /// next; a turn that ended first leaves the line for a prompt of its own.
    async fn steer(&self, request: SteerRequest) -> Result<SteerResponse, Error> {
        let (running, cwd) = {
            let sessions = lock(&self.sessions);
            let session = sessions
                .get(&request.session_id)
                .ok_or_else(|| unknown_session(&request.session_id))?;
            (session.running.clone(), session.opening.cwd.clone())
        };
        let prompt_required = SteerResponse {
            outcome: "promptRequired".to_string(),
            reason: Some("noRunningTurn".to_string()),
        };
        let Some(turn) = running else {
            return Ok(prompt_required);
        };
        let params = json!({
            "commandId": command_id(),
            "sessionId": request.session_id,
            "expectedTurnId": turn,
            "input": translate::turn_input(&request.prompt, &cwd),
        });
        match self.msp.request("turn/steer", params).await {
            Ok(_) => Ok(SteerResponse {
                outcome: "injected".to_string(),
                reason: None,
            }),
            Err(_) => Ok(prompt_required),
        }
    }

    async fn cancel(&self, session_id: &str) {
        let running = lock(&self.sessions)
            .get(session_id)
            .and_then(|session| session.running.clone());
        let mut params = json!({"commandId": command_id(), "sessionId": session_id});
        if let Some(turn) = running {
            params["turnId"] = json!(turn);
        }
        if let Err(error) = self.msp.request("turn/interrupt", params).await {
            eprintln!("muse-acp: turn/interrupt failed: {error}");
        }
    }

    async fn set_mode(
        &self,
        request: SetSessionModeRequest,
    ) -> Result<SetSessionModeResponse, Error> {
        let session_id = request.session_id.0.to_string();
        let mode = request.mode_id.0.to_string();
        let params = json!({"commandId": command_id(), "sessionId": session_id, "mode": mode});
        self.msp
            .request("session/setApprovalMode", params)
            .await
            .map_err(refused)?;
        if let Some(session) = lock(&self.sessions).get_mut(&session_id) {
            session.mode = mode;
        }
        Ok(SetSessionModeResponse::new())
    }

    async fn set_config(
        &self,
        request: SetSessionConfigOptionRequest,
    ) -> Result<SetSessionConfigOptionResponse, Error> {
        let session_id = request.session_id.0.to_string();
        let value = request
            .value
            .as_value_id()
            .map(|value| value.0.to_string())
            .ok_or_else(|| Error::invalid_params().data(json!("expected a choice")))?;
        match request.config_id.0.as_ref() {
            MODEL_CONFIG => {
                let model = lock(&self.sessions)
                    .get(&session_id)
                    .and_then(|session| {
                        session
                            .catalog
                            .iter()
                            .find(|model| model.id == value)
                            .cloned()
                    })
                    .ok_or_else(|| {
                        Error::invalid_params().data(json!(format!("unknown model {value}")))
                    })?;
                let params = json!({
                    "commandId": command_id(),
                    "sessionId": session_id,
                    "model": translate::model_selection(&model),
                });
                self.msp
                    .request("session/setModel", params)
                    .await
                    .map_err(refused)?;
                if let Some(session) = lock(&self.sessions).get_mut(&session_id) {
                    session.model = Some(value);
                }
            }
            EFFORT_CONFIG => {
                let params = json!({"commandId": command_id(), "sessionId": session_id, "reasoningEffort": value});
                self.msp
                    .request("session/setReasoningEffort", params)
                    .await
                    .map_err(refused)?;
                if let Some(session) = lock(&self.sessions).get_mut(&session_id) {
                    session.effort = Some(value);
                }
            }
            other => {
                return Err(Error::invalid_params().data(json!(format!("unknown setting {other}"))));
            }
        }
        Ok(SetSessionConfigOptionResponse::new(
            self.options_of(&session_id),
        ))
    }

    fn options_of(&self, session_id: &str) -> Vec<SessionConfigOption> {
        lock(&self.sessions)
            .get(session_id)
            .map(|session| {
                translate::config_options(
                    &session.catalog,
                    session.model.as_deref(),
                    session.effort.as_deref(),
                )
            })
            .unwrap_or_default()
    }

    /// Reads everything the host says on its own until it exits.
    async fn route(
        self: Arc<Self>,
        cx: ConnectionTo<Client>,
        mut events: mpsc::UnboundedReceiver<Event>,
    ) {
        while let Some(event) = events.recv().await {
            match event {
                Event::Notification { method, params } => self.notice(&cx, &method, &params),
                Event::Request { method, params } if method == "approval/request" => {
                    self.present(&cx, params);
                }
                Event::Request { method, .. } => {
                    eprintln!(
                        "muse-acp: muse serve asked {method}, which an ACP client cannot answer"
                    );
                }
            }
        }
        // The host is gone, and so is every turn it was running.
        for session in lock(&self.sessions).values_mut() {
            for (_, waiter) in session.waiting.drain() {
                let _ = waiter.done.send(Ended::Failed {
                    kind: "hostExited".to_string(),
                    message: "muse serve has exited".to_string(),
                });
            }
        }
    }

    fn notice(self: &Arc<Self>, cx: &ConnectionTo<Client>, method: &str, params: &Value) {
        let Some(session_id) = params.get("sessionId").and_then(Value::as_str) else {
            return;
        };
        let text = |name: &str| params.get(name).and_then(Value::as_str).map(str::to_string);
        let mut updates = Vec::new();
        {
            let mut sessions = lock(&self.sessions);
            let Some(session) = sessions.get_mut(session_id) else {
                return;
            };
            match method {
                "item/started" | "item/updated" | "item/completed" => {
                    if let Some(item) = params.get("item") {
                        updates = session.transcript.item(item);
                    }
                }
                "item/delta" => {
                    let field = text("field").unwrap_or_else(|| "text".to_string());
                    if let (Some(item), Some(delta)) = (text("itemId"), text("delta")) {
                        updates = session.transcript.delta(&item, &field, &delta);
                    }
                }
                "turn/started" => session.running = text("turnId"),
                "turn/completed" => {
                    let turn = text("turnId").unwrap_or_default();
                    if session.running.as_deref() == Some(turn.as_str()) {
                        session.running = None;
                    }
                    if let Some(waiter) = session.waiting.remove(&turn) {
                        let _ = waiter.done.send(ended(params, waiter.usage));
                    }
                }
                "session/tokenUsage" => {
                    if let Some(waiter) =
                        text("turnId").and_then(|turn| session.waiting.get_mut(&turn))
                    {
                        waiter.usage = translate::add_usage(waiter.usage.take(), params);
                    }
                }
                "session/todoListChanged" => {
                    updates.push(SessionUpdate::Plan(translate::plan(params)))
                }
                "session/modelChanged" => {
                    session.model = text("modelId");
                    let options = translate::config_options(
                        &session.catalog,
                        session.model.as_deref(),
                        session.effort.as_deref(),
                    );
                    updates.push(SessionUpdate::ConfigOptionUpdate(ConfigOptionUpdate::new(
                        options,
                    )));
                }
                "session/reasoningEffortChanged" => {
                    session.effort = text("reasoningEffort");
                    let options = translate::config_options(
                        &session.catalog,
                        session.model.as_deref(),
                        session.effort.as_deref(),
                    );
                    updates.push(SessionUpdate::ConfigOptionUpdate(ConfigOptionUpdate::new(
                        options,
                    )));
                }
                "session/approvalModeChanged" => {
                    if let Some(mode) = text("mode") {
                        session.mode = mode.clone();
                        updates.push(SessionUpdate::CurrentModeUpdate(CurrentModeUpdate::new(
                            mode,
                        )));
                    }
                }
                "approval/updated" => {
                    // A later stage of the same approval: what changed rides
                    // on top of what was first presented.
                    let approval = text("approvalId").unwrap_or_default();
                    if let Some(first) = session.approvals.get(&approval).cloned() {
                        let mut merged = first;
                        for (key, value) in params.as_object().into_iter().flatten() {
                            merged[key.as_str()] = value.clone();
                        }
                        drop(sessions);
                        self.present(cx, merged);
                        return;
                    }
                }
                "approval/resolved" => {
                    if let Some(approval) = text("approvalId") {
                        session.approvals.remove(&approval);
                    }
                }
                "session/closed" => {
                    // An idle unload keeps the conversation on disk; the next
                    // prompt reopens it. Only a running turn has to hear now.
                    for (_, waiter) in session.waiting.drain() {
                        let _ = waiter.done.send(Ended::Failed {
                            kind: "sessionClosed".to_string(),
                            message: "Muse closed the session".to_string(),
                        });
                    }
                    session.running = None;
                }
                _ => {}
            }
        }
        for update in updates {
            send(cx, session_id, update);
        }
    }

    /// Puts one approval stage to the client and decides it with the answer.
    fn present(self: &Arc<Self>, cx: &ConnectionTo<Client>, params: Value) {
        let field = |name: &str| {
            params
                .get(name)
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string()
        };
        let (session_id, approval) = (field("sessionId"), field("approvalId"));
        let stage = params
            .pointer("/currentRequirementId/sourceIndex")
            .and_then(Value::as_i64)
            .unwrap_or(0);
        {
            let mut sessions = lock(&self.sessions);
            let Some(session) = sessions.get_mut(&session_id) else {
                return;
            };
            if !session.asked.insert((approval.clone(), stage)) {
                return;
            }
            session.approvals.insert(approval.clone(), params.clone());
        }
        let bridge = self.clone();
        let cx = cx.clone();
        tokio::spawn(async move {
            let (call, options) = translate::permission(&params);
            let request = RequestPermissionRequest::new(session_id.clone(), call, options);
            let choice = match cx.send_request(request).block_task().await {
                Ok(answer) => match answer.outcome {
                    RequestPermissionOutcome::Selected(selected) => {
                        Some(selected.option_id.0.to_string())
                    }
                    _ => translate::refusal(&params),
                },
                Err(_) => translate::refusal(&params),
            };
            let Some(choice) = choice else {
                return;
            };
            let decision = json!({
                "commandId": command_id(),
                "sessionId": session_id,
                "approvalId": approval,
                "choiceId": choice,
                "requirementId": params.get("currentRequirementId").cloned().unwrap_or(Value::Null),
            });
            // Muse may have settled it already (its own timeout, another
            // client); that refusal is the outcome, not an error.
            if let Err(error) = bridge.msp.request("approval/decide", decision).await {
                eprintln!("muse-acp: approval/decide: {error}");
            }
        });
    }
}

fn ended(params: &Value, usage: Option<Usage>) -> Ended {
    match params.get("terminal").and_then(Value::as_str) {
        Some("completed") => {
            let reported = params
                .get("usage")
                .and_then(|usage| translate::add_usage(None, &json!({"usage": usage})));
            Ended::Completed(usage.or(reported))
        }
        Some("cancelled") => Ended::Cancelled,
        _ => {
            let error = params.get("error");
            let kind = error
                .and_then(|error| error.get("kind"))
                .and_then(Value::as_str)
                .unwrap_or_default();
            let message = error
                .and_then(|error| error.get("message"))
                .or_else(|| params.get("reason"))
                .and_then(Value::as_str)
                .unwrap_or("The turn failed.");
            Ended::Failed {
                kind: kind.to_string(),
                message: message.to_string(),
            }
        }
    }
}

/// The session's directory first, then the extra folders, as MSP wants them:
/// absolute, canonical, existing. A path that cannot be canonicalized is
/// passed as given, and Muse says what is wrong with it.
fn workspace_roots(cwd: &Path, additional: &[PathBuf]) -> Vec<String> {
    std::iter::once(cwd)
        .chain(additional.iter().map(PathBuf::as_path))
        .map(|path| std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf()))
        .map(|path| path.display().to_string())
        .collect()
}

fn send(cx: &ConnectionTo<Client>, session_id: &str, update: SessionUpdate) {
    if let Err(error) =
        cx.send_notification(SessionNotification::new(session_id.to_string(), update))
    {
        eprintln!("muse-acp: could not send a session update: {error:?}");
    }
}

/// An MSP refusal, as the ACP error a client shows. Muse's error kind is kept
/// in the message, since it is the part a person can act on.
fn refused(error: RpcError) -> Error {
    Error::new(-32603, error.to_string())
}

fn unknown_session(session_id: &str) -> Error {
    Error::invalid_params().data(json!(format!("unknown session {session_id}")))
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
