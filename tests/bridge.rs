//! The bridge, driven end to end: a real ACP client on one side, a scripted
//! Muse host on the other, both on in-memory pipes.
//!
//! The fake host speaks MSP as `muse serve` 1.4.7 does on the wire (shapes
//! taken from a live session), and decides what a turn does from its
//! prompt: `hello` replies, `tool` asks for an approval, `wait` runs until it
//! is interrupted or steered, `login` fails for want of a login.

use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::schema::v1::{
    AuthMethod, CancelNotification, ContentBlock, ErrorCode, InitializeRequest, InitializeResponse,
    LoadSessionRequest, McpServer, McpServerStdio, NewSessionRequest, PermissionOptionKind,
    PromptRequest, RequestPermissionOutcome, RequestPermissionRequest, RequestPermissionResponse,
    SelectedPermissionOutcome, SessionNotification, SessionUpdate, SetSessionConfigOptionRequest,
    SetSessionModeRequest, StopReason, ToolCallStatus,
};
use agent_client_protocol::{ByteStreams, Client, ConnectionTo, Responder, UntypedMessage};
use muse_acp::{bridge, msp::Msp};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader, DuplexStream};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

/// What the client saw, and what the host was asked.
#[derive(Clone, Default)]
struct Seen {
    updates: Arc<Mutex<Vec<SessionUpdate>>>,
    permissions: Arc<Mutex<Vec<RequestPermissionRequest>>>,
    host: Arc<Mutex<Vec<(String, Value)>>>,
}

impl Seen {
    fn texts(&self) -> Vec<(String, String)> {
        self.updates
            .lock()
            .unwrap()
            .iter()
            .filter_map(|update| {
                let (kind, chunk) = match update {
                    SessionUpdate::AgentMessageChunk(chunk) => ("agent", chunk),
                    SessionUpdate::AgentThoughtChunk(chunk) => ("thought", chunk),
                    SessionUpdate::UserMessageChunk(chunk) => ("user", chunk),
                    _ => return None,
                };
                let ContentBlock::Text(text) = &chunk.content else {
                    return None;
                };
                Some((kind.to_string(), text.text.clone()))
            })
            .collect()
    }

    fn asked(&self, method: &str) -> Vec<Value> {
        let host = self.host.lock().unwrap();
        host.iter()
            .filter(|(asked, _)| asked == method)
            .map(|(_, params)| params.clone())
            .collect()
    }

    /// Waits until the client has read `text` from the agent.
    async fn until_said(&self, text: &str) {
        for _ in 0..200 {
            if self.texts().iter().any(|(_, said)| said == text) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the agent never said {text:?}; saw {:?}", self.texts());
    }
}

/// Runs `body` as an ACP client against the bridge and a fake host.
async fn with_agent<F>(body: F) -> Seen
where
    F: AsyncFnOnce(
        ConnectionTo<agent_client_protocol::Agent>,
        Seen,
    ) -> Result<(), agent_client_protocol::Error>,
{
    let seen = Seen::default();
    let (client_io, agent_io) = tokio::io::duplex(1 << 16);
    let (bridge_io, host_io) = tokio::io::duplex(1 << 16);

    tokio::spawn(fake_muse(host_io, seen.host.clone()));
    let (bridge_read, bridge_write) = tokio::io::split(bridge_io);
    let (msp, events) = Msp::connect(bridge_read, bridge_write);
    let (agent_read, agent_write) = tokio::io::split(agent_io);
    tokio::spawn(bridge::serve(
        ByteStreams::new(agent_write.compat_write(), agent_read.compat()),
        msp,
        events,
    ));

    let (client_read, client_write) = tokio::io::split(client_io);
    let updates = seen.updates.clone();
    let permissions = seen.permissions.clone();
    let body_seen = seen.clone();
    let run = Client
        .builder()
        .on_receive_notification(
            move |notification: SessionNotification, _cx| {
                let updates = updates.clone();
                async move {
                    updates.lock().unwrap().push(notification.update);
                    Ok(())
                }
            },
            agent_client_protocol::on_receive_notification!(),
        )
        .on_receive_request(
            move |request: RequestPermissionRequest,
                  responder: Responder<RequestPermissionResponse>,
                  _cx| {
                let permissions = permissions.clone();
                async move {
                    let allow = request
                        .options
                        .iter()
                        .find(|option| option.kind == PermissionOptionKind::AllowOnce)
                        .map(|option| option.option_id.clone());
                    permissions.lock().unwrap().push(request);
                    let outcome = match allow {
                        Some(option) => RequestPermissionOutcome::Selected(
                            SelectedPermissionOutcome::new(option),
                        ),
                        None => RequestPermissionOutcome::Cancelled,
                    };
                    responder.respond(RequestPermissionResponse::new(outcome))
                }
            },
            agent_client_protocol::on_receive_request!(),
        )
        .connect_with(
            ByteStreams::new(client_write.compat_write(), client_read.compat()),
            async move |cx| body(cx, body_seen).await,
        );
    tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .expect("the conversation finished in time")
        .expect("the conversation succeeded");
    seen
}

async fn initialize(cx: &ConnectionTo<agent_client_protocol::Agent>) -> InitializeResponse {
    cx.send_request(InitializeRequest::new(ProtocolVersion::V1))
        .block_task()
        .await
        .expect("initialize")
}

async fn open(cx: &ConnectionTo<agent_client_protocol::Agent>) -> String {
    initialize(cx).await;
    let session = cx
        .send_request(NewSessionRequest::new(std::env::temp_dir()))
        .block_task()
        .await
        .expect("session/new");
    session.session_id.0.to_string()
}

fn prompt(session: &str, text: &str) -> PromptRequest {
    PromptRequest::new(session.to_string(), vec![ContentBlock::from(text)])
}

#[tokio::test]
async fn initialize_offers_terminal_login_steering_and_resume() {
    with_agent(async |cx, _| {
        let init = initialize(&cx).await;
        let AuthMethod::Terminal(login) = &init.auth_methods[0] else {
            panic!("expected a terminal login, got {:?}", init.auth_methods);
        };
        assert_eq!(login.args, vec!["login"]);
        assert_eq!(
            init.meta.as_ref().and_then(|meta| meta.get("steering")),
            Some(&json!({"supported": true}))
        );
        assert!(init.agent_capabilities.load_session);
        assert!(
            init.agent_capabilities
                .session_capabilities
                .resume
                .is_some()
        );
        assert!(
            init.agent_capabilities
                .session_capabilities
                .additional_directories
                .is_some()
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn a_prompt_streams_the_reply_and_ends_with_its_usage() {
    let seen = with_agent(async |cx, _| {
        let session = open(&cx).await;
        let answer = cx
            .send_request(prompt(&session, "hello"))
            .block_task()
            .await?;
        assert_eq!(answer.stop_reason, StopReason::EndTurn);
        let usage = answer.usage.expect("usage");
        assert_eq!((usage.input_tokens, usage.output_tokens), (12, 3));
        Ok(())
    })
    .await;
    assert_eq!(
        seen.texts(),
        vec![
            ("thought".to_string(), "Greeting.".to_string()),
            ("agent".to_string(), "echo: ".to_string()),
            ("agent".to_string(), "hello".to_string()),
        ]
    );
    let start = &seen.asked("turn/start")[0];
    assert_eq!(start["input"], json!([{"type": "text", "text": "hello"}]));
    assert_eq!(start["ifBusy"], "queue");
}

#[tokio::test]
async fn a_session_hands_muse_its_folders_and_mcp_servers() {
    let seen = with_agent(async |cx, _| {
        initialize(&cx).await;
        let server = McpServer::Stdio(
            McpServerStdio::new("hotline", "/usr/local/bin/hotline-mcp")
                .args(vec!["--stdio".into()]),
        );
        cx.send_request(
            NewSessionRequest::new(std::env::temp_dir())
                .additional_directories(vec![std::env::temp_dir()])
                .mcp_servers(vec![server]),
        )
        .block_task()
        .await?;
        Ok(())
    })
    .await;
    let start = &seen.asked("session/start")[0];
    assert_eq!(start["workspaceRoots"].as_array().map(Vec::len), Some(2));
    assert_eq!(
        start["config"]["mcpServers"]["hotline"],
        json!({"transport": "stdio", "command": "/usr/local/bin/hotline-mcp", "args": ["--stdio"], "env": {}, "mode": "optional"})
    );
}

#[tokio::test]
async fn an_approval_is_asked_of_the_client_and_decided_with_its_answer() {
    let seen = with_agent(async |cx, _| {
        let session = open(&cx).await;
        let answer = cx
            .send_request(prompt(&session, "tool"))
            .block_task()
            .await?;
        assert_eq!(answer.stop_reason, StopReason::EndTurn);
        Ok(())
    })
    .await;
    let permissions = seen.permissions.lock().unwrap().clone();
    assert_eq!(permissions.len(), 1);
    assert_eq!(
        permissions[0].tool_call.fields.title.as_deref(),
        Some("Run `ls -1`")
    );
    let decided = &seen.asked("approval/decide")[0];
    assert_eq!(decided["choiceId"], "allow-once");
    assert_eq!(
        decided["requirementId"],
        json!({"approvalId": "ap-1", "sourceIndex": 0})
    );

    let updates = seen.updates.lock().unwrap();
    let statuses: Vec<_> = updates
        .iter()
        .filter_map(|update| match update {
            SessionUpdate::ToolCall(call) => Some(call.status),
            SessionUpdate::ToolCallUpdate(update) => update.fields.status,
            _ => None,
        })
        .collect();
    assert_eq!(
        statuses,
        vec![ToolCallStatus::InProgress, ToolCallStatus::Completed]
    );
}

#[tokio::test]
async fn cancel_interrupts_the_running_turn() {
    let seen = with_agent(async |cx, seen| {
        let session = open(&cx).await;
        let running = cx.send_request(prompt(&session, "wait")).block_task();
        let watch = async {
            seen.until_said("working").await;
            cx.send_notification(CancelNotification::new(session.clone()))
                .unwrap();
        };
        let (answer, ()) = tokio::join!(running, watch);
        assert_eq!(answer?.stop_reason, StopReason::Cancelled);
        Ok(())
    })
    .await;
    let interrupt = &seen.asked("turn/interrupt")[0];
    assert_eq!(
        interrupt["turnId"],
        seen.asked("turn/start")[0]["commandId"]
    );
}

#[tokio::test]
async fn steering_reaches_the_running_turn_and_an_idle_one_asks_for_a_prompt() {
    let seen = with_agent(async |cx, seen| {
        let session = open(&cx).await;
        let steer = |text: &str| {
            UntypedMessage::new(
                "_session/steering",
                json!({
                    "sessionId": session,
                    "prompt": [{"type": "text", "text": text}],
                    "_meta": {"steering": {"idleBehavior": "promptRequired"}},
                }),
            )
            .unwrap()
        };
        let idle = cx.send_request(steer("too early")).block_task().await?;
        assert_eq!(idle["outcome"], "promptRequired");

        let running = cx.send_request(prompt(&session, "wait")).block_task();
        let steering = async {
            seen.until_said("working").await;
            cx.send_request(steer("also this")).block_task().await
        };
        let (answer, steered) = tokio::join!(running, steering);
        assert_eq!(steered?["outcome"], "injected");
        assert_eq!(answer?.stop_reason, StopReason::EndTurn);
        Ok(())
    })
    .await;
    let steer = &seen.asked("turn/steer")[0];
    assert_eq!(
        steer["expectedTurnId"],
        seen.asked("turn/start")[0]["commandId"]
    );
    assert!(
        seen.texts()
            .contains(&("agent".to_string(), "steered: also this".to_string()))
    );
}

#[tokio::test]
async fn an_unloaded_session_is_reopened_with_its_tools_and_folders() {
    let seen = with_agent(async |cx, _| {
        initialize(&cx).await;
        let server = McpServer::Stdio(McpServerStdio::new("hotline", "/usr/local/bin/hotline-mcp"));
        let session = cx
            .send_request(NewSessionRequest::new(std::env::temp_dir()).mcp_servers(vec![server]))
            .block_task()
            .await?;
        let id = session.session_id.0.to_string();
        let answer = cx
            .send_request(prompt(&id, "after a nap"))
            .block_task()
            .await?;
        assert_eq!(answer.stop_reason, StopReason::EndTurn);
        Ok(())
    })
    .await;
    let reopened = &seen.asked("session/resume")[0];
    assert_eq!(reopened["sessionId"], "s-1");
    assert!(
        reopened["config"]["mcpServers"]["hotline"].is_object(),
        "{reopened}"
    );
    let starts = seen.asked("turn/start");
    assert_eq!(starts.len(), 2);
    assert_ne!(
        starts[1]["commandId"], starts[0]["commandId"],
        "asked again as a new command"
    );
    assert!(starts[1]["workspaceRoots"].is_array());
    assert!(
        seen.texts()
            .contains(&("agent".to_string(), "awake".to_string()))
    );
}

#[tokio::test]
async fn a_turn_that_needs_a_login_fails_as_auth_required() {
    with_agent(async |cx, _| {
        let session = open(&cx).await;
        let error = cx
            .send_request(prompt(&session, "login"))
            .block_task()
            .await
            .expect_err("the turn needs a login");
        assert_eq!(error.code, ErrorCode::AuthRequired);
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn a_failed_turn_carries_muses_reason() {
    with_agent(async |cx, _| {
        let session = open(&cx).await;
        let error = cx
            .send_request(prompt(&session, "quota"))
            .block_task()
            .await
            .expect_err("the turn is over quota");
        assert!(
            error.message.contains("Subscription quota exhausted"),
            "{}",
            error.message
        );
        Ok(())
    })
    .await;
}

#[tokio::test]
async fn load_replays_the_stored_conversation_before_answering() {
    let seen = with_agent(async |cx, _| {
        initialize(&cx).await;
        cx.send_request(LoadSessionRequest::new("stored", std::env::temp_dir()))
            .block_task()
            .await?;
        Ok(())
    })
    .await;
    assert_eq!(
        seen.texts(),
        vec![
            ("user".to_string(), "earlier question".to_string()),
            ("agent".to_string(), "earlier answer".to_string()),
        ]
    );
    assert_eq!(seen.asked("session/resume")[0]["history"], "inline");
}

#[tokio::test]
async fn the_pickers_set_muses_mode_model_and_effort() {
    let seen = with_agent(async |cx, _| {
        initialize(&cx).await;
        let session = cx
            .send_request(NewSessionRequest::new(std::env::temp_dir()))
            .block_task()
            .await?;
        let id = session.session_id.0.to_string();
        assert_eq!(
            session
                .modes
                .as_ref()
                .map(|modes| modes.current_mode_id.0.to_string()),
            Some("onRequest".into())
        );
        assert_eq!(session.config_options.as_ref().map(Vec::len), Some(2));

        cx.send_request(SetSessionModeRequest::new(id.clone(), "allowAll"))
            .block_task()
            .await?;
        let picked = cx
            .send_request(SetSessionConfigOptionRequest::new(
                id.clone(),
                "model",
                "spark",
            ))
            .block_task()
            .await?;
        assert_eq!(picked.config_options.len(), 2);
        cx.send_request(SetSessionConfigOptionRequest::new(id, "effort", "max"))
            .block_task()
            .await?;
        Ok(())
    })
    .await;
    assert_eq!(seen.asked("session/setApprovalMode")[0]["mode"], "allowAll");
    assert_eq!(
        seen.asked("session/setModel")[0]["model"]["modelId"],
        "spark"
    );
    assert_eq!(
        seen.asked("session/setReasoningEffort")[0]["reasoningEffort"],
        "max"
    );
}

/// A Muse host with just enough behaviour for the tests above.
async fn fake_muse(io: DuplexStream, asked: Arc<Mutex<Vec<(String, Value)>>>) {
    let (read, mut write) = tokio::io::split(io);
    let mut lines = BufReader::new(read).lines();
    let mut running: Option<String> = None;
    let mut unloaded = false;
    while let Ok(Some(line)) = lines.next_line().await {
        let message: Value = serde_json::from_str(&line).unwrap();
        let Some(method) = message["method"].as_str() else {
            continue; // a receipt for one of our requests
        };
        let params = message["params"].clone();
        asked
            .lock()
            .unwrap()
            .push((method.to_string(), params.clone()));
        let id = message["id"].clone();
        let session = params["sessionId"].as_str().unwrap_or("s-1").to_string();
        match method {
            "initialize" => {
                let name = params["clientInfo"]["name"].as_str().unwrap();
                assert!(
                    name.chars()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
                    "muse serve refuses a clientInfo.name like {name:?}"
                );
                answer(
                    &mut write,
                    &id,
                    json!({"grantedCapabilities": ["sessionMcp"], "sessionDurability": "durable"}),
                )
                .await
            }
            "session/start" => {
                answer(
                    &mut write,
                    &id,
                    json!({"session": session_object("s-1"), "viewCursor": ""}),
                )
                .await
            }
            "session/resume" => {
                let history = json!({"mode": "inline", "snapshot": null, "items": [
                    {"itemId": "u-1", "kind": "userMessage", "status": "completed", "text": "earlier question"},
                    {"itemId": "m-1", "kind": "agentMessage", "status": "completed", "text": "earlier answer"},
                    {"itemId": "r-1", "kind": "reminderChild", "status": "completed"}
                ]});
                let result = json!({"session": session_object(&session), "history": history, "pendingRequests": [], "viewCursor": ""});
                answer(&mut write, &id, result).await
            }
            "model/list" => {
                let models = json!({"providerId": "meta", "profileId": null, "source": "providerCatalog", "models": [
                    {"modelId": "wonder", "displayLabel": "Wonder", "providerId": "meta", "isActive": true, "isDefault": true,
                     "variants": ["low", "high", "max"], "defaultReasoningEffort": "high"},
                    {"modelId": "spark", "displayLabel": "Spark", "providerId": "meta", "isActive": false, "isDefault": false,
                     "variants": ["low", "max"], "defaultReasoningEffort": "max"}
                ]});
                answer(&mut write, &id, models).await
            }
            "turn/start" if params["input"][0]["text"] == "after a nap" && !unloaded => {
                // Muse unloaded the idle session; the conversation is on disk.
                unloaded = true;
                let error = json!({"code": -32010, "message": "session is not loaded", "data": {"kind": "sessionNotLoaded"}});
                line_out(
                    &mut write,
                    json!({"jsonrpc": "2.0", "id": id, "error": error}),
                )
                .await;
            }
            "turn/start" => {
                let turn = params["commandId"].as_str().unwrap().to_string();
                let accepted = json!({"commandId": turn, "status": "accepted", "turnId": turn, "startedNewTurn": true, "disposition": "started"});
                answer(&mut write, &id, accepted).await;
                notify(
                    &mut write,
                    "turn/started",
                    json!({"sessionId": session, "turnId": turn}),
                )
                .await;
                match params["input"][0]["text"].as_str().unwrap_or_default() {
                    "hello" => {
                        item(&mut write, &session, "item/started", json!({"itemId": "r-1", "kind": "reasoning", "status": "inProgress", "summary": []})).await;
                        delta(&mut write, &session, "r-1", "summary.0", "Greeting.").await;
                        item(&mut write, &session, "item/completed", json!({"itemId": "r-1", "kind": "reasoning", "status": "completed", "summary": ["Greeting."]})).await;
                        item(&mut write, &session, "item/started", json!({"itemId": "m-1", "kind": "agentMessage", "status": "inProgress", "text": ""})).await;
                        delta(&mut write, &session, "m-1", "text", "echo: ").await;
                        let usage = json!({"sessionId": session, "turnId": turn, "usage": {"inputTokens": 12, "outputTokens": 3, "cachedTokens": 0, "reasoningTokens": 0}});
                        notify(&mut write, "session/tokenUsage", usage).await;
                        item(&mut write, &session, "item/completed", json!({"itemId": "m-1", "kind": "agentMessage", "status": "completed", "text": "echo: hello"})).await;
                        complete(
                            &mut write,
                            &session,
                            &turn,
                            json!({"terminal": "completed"}),
                        )
                        .await;
                    }
                    "tool" => {
                        let call = json!({"itemId": "t-1", "kind": "toolCall", "status": "inProgress", "tool": "shell", "args": "{\"command\":\"ls -1\"}"});
                        item(&mut write, &session, "item/started", call).await;
                        let approval = json!({"jsonrpc": "2.0", "id": 900, "method": "approval/request", "params": {
                            "sessionId": session, "approvalId": "ap-1", "itemId": "t-1", "toolName": "shell", "turnId": turn,
                            "subject": {"kind": "shell", "command": "ls -1"},
                            "currentRequirementId": {"approvalId": "ap-1", "sourceIndex": 0},
                            "availableChoices": [
                                {"choiceId": "allow-once", "label": "Allow once", "decision": "approved", "scope": "once"},
                                {"choiceId": "deny", "label": "Deny", "decision": "denied", "scope": "once"}
                            ]
                        }});
                        line_out(&mut write, approval).await;
                        running = Some(turn);
                    }
                    "wait" => {
                        item(&mut write, &session, "item/completed", json!({"itemId": "m-w", "kind": "agentMessage", "status": "completed", "text": "working"})).await;
                        running = Some(turn);
                    }
                    "after a nap" => {
                        item(&mut write, &session, "item/completed", json!({"itemId": "m-n", "kind": "agentMessage", "status": "completed", "text": "awake"})).await;
                        complete(
                            &mut write,
                            &session,
                            &turn,
                            json!({"terminal": "completed"}),
                        )
                        .await;
                    }
                    "login" => {
                        let error = json!({"kind": "authRequired", "message": "Run `muse login`.", "retryable": false});
                        complete(
                            &mut write,
                            &session,
                            &turn,
                            json!({"terminal": "failed", "error": error}),
                        )
                        .await;
                    }
                    "quota" => {
                        let message = "API error 429: Subscription quota exhausted. Your usage window resets at 2026-10-13T19:32:31Z. (rate_limit_error)";
                        let error =
                            json!({"kind": "modelError", "message": message, "retryable": true});
                        complete(
                            &mut write,
                            &session,
                            &turn,
                            json!({"terminal": "failed", "error": error}),
                        )
                        .await;
                    }
                    other => panic!("no scenario for {other:?}"),
                }
            }
            "approval/decide" => {
                answer(
                    &mut write,
                    &id,
                    json!({"commandId": params["commandId"], "status": "accepted"}),
                )
                .await;
                let turn = running.take().unwrap();
                let status = if params["choiceId"] == "allow-once" {
                    "completed"
                } else {
                    "rejected"
                };
                let call = json!({"itemId": "t-1", "kind": "toolCall", "status": status, "tool": "shell", "visibleOutput": "a.txt\nb.txt"});
                item(&mut write, &session, "item/completed", call).await;
                complete(
                    &mut write,
                    &session,
                    &turn,
                    json!({"terminal": "completed"}),
                )
                .await;
            }
            "turn/interrupt" => {
                answer(
                    &mut write,
                    &id,
                    json!({"commandId": params["commandId"], "status": "accepted"}),
                )
                .await;
                if let Some(turn) = running.take() {
                    complete(
                        &mut write,
                        &session,
                        &turn,
                        json!({"terminal": "cancelled"}),
                    )
                    .await;
                }
            }
            "turn/steer" => {
                let turn = running.take().unwrap();
                answer(
                    &mut write,
                    &id,
                    json!({"commandId": params["commandId"], "status": "accepted", "turnId": turn}),
                )
                .await;
                let said = format!("steered: {}", params["input"][0]["text"].as_str().unwrap());
                item(&mut write, &session, "item/completed", json!({"itemId": "m-s", "kind": "agentMessage", "status": "completed", "text": said})).await;
                complete(
                    &mut write,
                    &session,
                    &turn,
                    json!({"terminal": "completed"}),
                )
                .await;
            }
            "session/setApprovalMode" | "session/setModel" | "session/setReasoningEffort" => {
                answer(
                    &mut write,
                    &id,
                    json!({"commandId": params["commandId"], "status": "accepted"}),
                )
                .await
            }
            "initialized" => {}
            other => panic!("the fake host has no answer for {other}"),
        }
    }
}

fn session_object(id: &str) -> Value {
    json!({
        "sessionId": id, "path": "", "status": "idle", "activeTurnId": null,
        "createdAt": "2026-10-13T00:00:00Z", "updatedAt": "2026-10-13T00:00:00Z",
        "workspaceRoot": "/tmp", "providerId": "meta", "modelId": "wonder", "turnCount": 0,
        "forkedFrom": null, "sideFrom": null,
        "approvalMode": {"mode": "onRequest", "source": "startup", "lastCommandId": null}
    })
}

async fn line_out(write: &mut (impl AsyncWrite + Unpin), message: Value) {
    write
        .write_all(format!("{message}\n").as_bytes())
        .await
        .unwrap();
}

async fn answer(write: &mut (impl AsyncWrite + Unpin), id: &Value, result: Value) {
    line_out(write, json!({"jsonrpc": "2.0", "id": id, "result": result})).await;
}

async fn notify(write: &mut (impl AsyncWrite + Unpin), method: &str, params: Value) {
    line_out(
        write,
        json!({"jsonrpc": "2.0", "method": method, "params": params, "emittedAtMs": 0}),
    )
    .await;
}

async fn item(write: &mut (impl AsyncWrite + Unpin), session: &str, method: &str, item: Value) {
    notify(write, method, json!({"sessionId": session, "item": item})).await;
}

async fn delta(
    write: &mut (impl AsyncWrite + Unpin),
    session: &str,
    item: &str,
    field: &str,
    text: &str,
) {
    notify(
        write,
        "item/delta",
        json!({"sessionId": session, "itemId": item, "field": field, "delta": text}),
    )
    .await;
}

async fn complete(
    write: &mut (impl AsyncWrite + Unpin),
    session: &str,
    turn: &str,
    mut how: Value,
) {
    how["sessionId"] = json!(session);
    how["turnId"] = json!(turn);
    notify(write, "turn/completed", how).await;
}
