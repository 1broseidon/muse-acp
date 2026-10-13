//! MSP's vocabulary read as ACP's.
//!
//! Everything here is a pure function of what Muse said, so the mapping can be
//! tested without a host or a client. The bridge owns the I/O and calls in.
//!
//! The one stateful piece is [`Transcript`]: MSP streams an item as deltas and
//! then re-sends the whole item at each revision, while ACP only appends. The
//! transcript remembers what it has already sent per item, so each byte the
//! person reads goes out once whether it arrived as a delta or a revision.

use agent_client_protocol::schema::v1::{
    ContentBlock, ContentChunk, MessageId, PermissionOption, PermissionOptionKind, Plan, PlanEntry,
    PlanEntryPriority, PlanEntryStatus, SessionConfigOption, SessionConfigOptionCategory,
    SessionConfigSelectOption, SessionMode, SessionModeState, SessionUpdate, ToolCall,
    ToolCallContent, ToolCallStatus, ToolCallUpdate, ToolCallUpdateFields, ToolKind, Usage,
};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::Path;

/// The config option id of the model picker.
pub const MODEL_CONFIG: &str = "model";
/// The config option id of the reasoning effort picker.
pub const EFFORT_CONFIG: &str = "effort";

/// Item kinds that are Muse's own machinery rather than conversation: the
/// reminder children that check a turn's work, hooks, compaction, and side
/// chats. A user message is the client's own words, so it is only shown when
/// a stored session is replayed.
const UNSHOWN: &[&str] = &[
    "reminderChild",
    "hookRun",
    "compaction",
    "sideChat",
    "userMessage",
    "userShell",
];

/// What has already been sent to the client, per item.
#[derive(Default)]
pub struct Transcript {
    items: HashMap<String, Sent>,
}

#[derive(Default)]
struct Sent {
    kind: String,
    /// Text already sent, per streamed field (`text`, `summary.0`, ...).
    fields: HashMap<String, String>,
    tool_announced: bool,
    tool_settled: bool,
}

impl Transcript {
    /// An item at some revision (`item/started`, `item/updated` or
    /// `item/completed`, which all carry the full object).
    pub fn item(&mut self, item: &Value) -> Vec<SessionUpdate> {
        let Some(id) = str_field(item, "itemId") else {
            return Vec::new();
        };
        let kind = str_field(item, "kind").unwrap_or_default();
        let sent = self.items.entry(id.to_string()).or_default();
        sent.kind = kind.to_string();
        match kind {
            "agentMessage" | "reasoning" => streamed_fields(item)
                .into_iter()
                .filter_map(|(field, full)| catch_up(sent, id, &field, &full))
                .collect(),
            kind if UNSHOWN.contains(&kind) => Vec::new(),
            _ => tool_updates(sent, id, item),
        }
    }

    /// An `item/delta`: text appended to one field of an open item.
    pub fn delta(&mut self, item_id: &str, field: &str, delta: &str) -> Vec<SessionUpdate> {
        let Some(sent) = self.items.get_mut(item_id) else {
            return Vec::new();
        };
        if !matches!(sent.kind.as_str(), "agentMessage" | "reasoning") || delta.is_empty() {
            return Vec::new();
        }
        let first = !sent.fields.contains_key(field);
        sent.fields
            .entry(field.to_string())
            .or_default()
            .push_str(delta);
        let text = separated(&sent.kind, field, first, delta);
        vec![chunk(&sent.kind, item_id, text)]
    }
}

/// A stored item, for `session/load`'s replay of a conversation. Unlike a
/// live turn, the person's own messages are part of what is replayed.
pub fn replay(item: &Value) -> Vec<SessionUpdate> {
    if str_field(item, "kind") == Some("userMessage") {
        let Some(text) = str_field(item, "text").filter(|text| !text.is_empty()) else {
            return Vec::new();
        };
        let id = str_field(item, "itemId").unwrap_or_default();
        return vec![SessionUpdate::UserMessageChunk(
            ContentChunk::new(ContentBlock::from(text)).message_id(MessageId::new(id)),
        )];
    }
    Transcript::default().item(item)
}

/// The text fields of a message or reasoning item, as `(field, full text)`
/// in the names `item/delta` uses for them.
fn streamed_fields(item: &Value) -> Vec<(String, String)> {
    let mut fields = Vec::new();
    if let Some(text) = str_field(item, "text") {
        fields.push(("text".to_string(), text.to_string()));
    }
    if let Some(summary) = item.get("summary").and_then(Value::as_array) {
        for (index, part) in summary.iter().enumerate() {
            if let Some(part) = part.as_str() {
                fields.push((format!("summary.{index}"), part.to_string()));
            }
        }
    }
    fields
}

/// The part of `full` the client has not been sent yet. A revision that is not
/// an extension of what was streamed (Muse rewrote it) cannot be taken back
/// over ACP, so it is left as streamed.
fn catch_up(sent: &mut Sent, id: &str, field: &str, full: &str) -> Option<SessionUpdate> {
    let first = !sent.fields.contains_key(field);
    let already = sent.fields.entry(field.to_string()).or_default();
    let rest = full.strip_prefix(already.as_str())?;
    if rest.is_empty() {
        return None;
    }
    let text = separated(&sent.kind, field, first, rest);
    already.push_str(rest);
    Some(chunk(&sent.kind, id, text))
}

/// Reasoning summary parts arrive as separate fields; the client reads one
/// stream of thought, so a new part after the first starts a new paragraph.
fn separated(kind: &str, field: &str, first: bool, text: &str) -> String {
    if kind == "reasoning" && first && field.starts_with("summary.") && field != "summary.0" {
        format!("\n\n{text}")
    } else {
        text.to_string()
    }
}

fn chunk(kind: &str, id: &str, text: String) -> SessionUpdate {
    let chunk = ContentChunk::new(ContentBlock::from(text)).message_id(MessageId::new(id));
    if kind == "reasoning" {
        SessionUpdate::AgentThoughtChunk(chunk)
    } else {
        SessionUpdate::AgentMessageChunk(chunk)
    }
}

/// A tool call, subagent, web search or workflow, shown as an ACP tool call:
/// announced once, then settled once with whatever it produced.
fn tool_updates(sent: &mut Sent, id: &str, item: &Value) -> Vec<SessionUpdate> {
    if sent.tool_settled {
        return Vec::new();
    }
    let status = tool_status(item);
    let settled = status != ToolCallStatus::InProgress;
    let content = if settled {
        tool_content(item)
    } else {
        Vec::new()
    };
    let mut updates = Vec::new();
    if !sent.tool_announced {
        sent.tool_announced = true;
        let call = ToolCall::new(id.to_string(), tool_title(item))
            .kind(tool_kind(item))
            .status(status)
            .content(content.clone())
            .raw_input(str_field(item, "args").and_then(|args| serde_json::from_str(args).ok()));
        updates.push(SessionUpdate::ToolCall(call));
        sent.tool_settled = settled;
        return updates;
    }
    if settled {
        sent.tool_settled = true;
        updates.push(SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(
            id.to_string(),
            ToolCallUpdateFields::new().status(status).content(content),
        )));
    }
    updates
}

fn tool_status(item: &Value) -> ToolCallStatus {
    match str_field(item, "status") {
        Some("inProgress") | None => ToolCallStatus::InProgress,
        Some("completed") => ToolCallStatus::Completed,
        // failed, rejected, cancelled, and any terminal state MSP adds later.
        Some(_) => ToolCallStatus::Failed,
    }
}

fn tool_content(item: &Value) -> Vec<ToolCallContent> {
    let text = str_field(item, "visibleOutput")
        .or_else(|| item.pointer("/result/summary").and_then(Value::as_str))
        .or_else(|| str_field(item, "finalSummary"))
        .or_else(|| str_field(item, "failureReason"))
        .filter(|text| !text.is_empty());
    text.map(|text| vec![ToolCallContent::from(text)])
        .unwrap_or_default()
}

/// The one line a client shows for a tool call: the tool and what it touched.
fn tool_title(item: &Value) -> String {
    let kind = str_field(item, "kind").unwrap_or_default();
    match kind {
        "toolCall" => {
            let tool = str_field(item, "tool").unwrap_or("tool");
            let args: Value = str_field(item, "args")
                .and_then(|args| serde_json::from_str(args).ok())
                .unwrap_or(Value::Null);
            let touched = [
                "command",
                "cmd",
                "path",
                "file_path",
                "filePath",
                "pattern",
                "query",
                "url",
            ]
            .iter()
            .find_map(|key| args.get(key).and_then(argument_text));
            match touched {
                Some(touched) => format!("{tool}: {touched}"),
                None => tool.to_string(),
            }
        }
        "subagent" => {
            let objective = str_field(item, "objective").or_else(|| str_field(item, "role"));
            match objective.and_then(|text| text.lines().next()) {
                Some(objective) => format!("Subagent: {objective}"),
                None => "Subagent".to_string(),
            }
        }
        _ => str_field(item, "fallbackText").unwrap_or(kind).to_string(),
    }
}

/// A shell command may be a string or an argv array; either reads as a line.
fn argument_text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => text.lines().next().map(str::to_string),
        Value::Array(parts) => Some(
            parts
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(" "),
        ),
        _ => None,
    }
}

fn tool_kind(item: &Value) -> ToolKind {
    match str_field(item, "kind") {
        Some("webSearch") => return ToolKind::Fetch,
        Some("subagent") => return ToolKind::Think,
        Some("toolCall") => {}
        _ => return ToolKind::Other,
    }
    let tool = str_field(item, "tool")
        .unwrap_or_default()
        .to_ascii_lowercase();
    let named = |words: &[&str]| words.iter().any(|word| tool.contains(word));
    if named(&["shell", "bash", "exec", "command", "terminal"]) {
        ToolKind::Execute
    } else if named(&["edit", "write", "patch", "replace", "create"]) {
        ToolKind::Edit
    } else if named(&["delete", "remove"]) {
        ToolKind::Delete
    } else if named(&["move", "rename"]) {
        ToolKind::Move
    } else if named(&["fetch", "web", "http", "browse"]) {
        ToolKind::Fetch
    } else if named(&["read", "view", "cat", "open"]) {
        ToolKind::Read
    } else if named(&["grep", "glob", "search", "find", "list"]) {
        ToolKind::Search
    } else if named(&["think", "plan", "todo"]) {
        ToolKind::Think
    } else {
        ToolKind::Other
    }
}

/// A pending approval as an ACP permission request: the tool call it gates,
/// in a sentence, and Muse's own choices as the options.
pub fn permission(params: &Value) -> (ToolCallUpdate, Vec<PermissionOption>) {
    let item_id = str_field(params, "itemId").unwrap_or_default();
    let title = approval_title(params);
    let call = ToolCallUpdate::new(
        item_id.to_string(),
        ToolCallUpdateFields::new()
            .title(title)
            .status(ToolCallStatus::Pending),
    );
    let options = choices(params)
        .iter()
        .filter_map(|choice| {
            let kind = option_kind(choice)?;
            Some(PermissionOption::new(
                str_field(choice, "choiceId")?.to_string(),
                str_field(choice, "label").unwrap_or("Choose").to_string(),
                kind,
            ))
        })
        .collect();
    (call, options)
}

/// The choice to decide with when the client could not answer (it cancelled
/// the request, or gave up waiting): Muse's own one-time denial, or its
/// abort.
pub fn refusal(params: &Value) -> Option<String> {
    let choices = choices(params);
    let by = |decision: &str| {
        choices
            .iter()
            .find(|choice| str_field(choice, "decision") == Some(decision))
            .and_then(|choice| str_field(choice, "choiceId"))
    };
    by("denied").or_else(|| by("abort")).map(str::to_string)
}

fn choices(params: &Value) -> &[Value] {
    params
        .get("availableChoices")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
}

fn option_kind(choice: &Value) -> Option<PermissionOptionKind> {
    let once = str_field(choice, "scope") == Some("once");
    match str_field(choice, "decision")? {
        "approved" if once => Some(PermissionOptionKind::AllowOnce),
        "approved" | "approvedForSession" | "approvedPolicyAmendment" => {
            Some(PermissionOptionKind::AllowAlways)
        }
        "denied" | "abort" if once => Some(PermissionOptionKind::RejectOnce),
        "denied" | "deniedPolicyAmendment" => Some(PermissionOptionKind::RejectAlways),
        "abort" => Some(PermissionOptionKind::RejectOnce),
        // `timedOut` is the host's own outcome, never a person's choice.
        _ => None,
    }
}

/// What an approval is asking, as a sentence somebody can answer.
fn approval_title(params: &Value) -> String {
    let subject = params.get("subject").unwrap_or(&Value::Null);
    let field = |name: &str| str_field(subject, name);
    let tool = str_field(params, "toolName").unwrap_or("a tool");
    match field("kind").unwrap_or_default() {
        "shell" | "process" => match field("command") {
            Some(command) => format!("Run `{command}`"),
            None => format!("Run {tool}"),
        },
        "fileAccess" => {
            let access = field("access").unwrap_or("access");
            let path = field("path")
                .or_else(|| field("target"))
                .unwrap_or("a file");
            format!("{} {path}", capitalized(access))
        }
        "network" => {
            let host = field("host")
                .or_else(|| field("target"))
                .unwrap_or("the network");
            match subject.get("port").and_then(Value::as_u64) {
                Some(port) => format!("Connect to {host}:{port}"),
                None => format!("Connect to {host}"),
            }
        }
        "unixSocket" => format!("Connect to socket {}", field("path").unwrap_or("")),
        _ => match field("target") {
            Some(target) => format!("Use {} on {target}", field("toolName").unwrap_or(tool)),
            None => format!("Use {}", field("toolName").unwrap_or(tool)),
        },
    }
}

fn capitalized(word: &str) -> String {
    let mut chars = word.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().chain(chars).collect(),
        None => String::new(),
    }
}

/// Muse's approval modes as ACP session modes. They are the harness's own
/// runtime policy, which is what an ACP client shows a mode picker for.
pub fn modes(current: &str) -> SessionModeState {
    let available = [
        (
            "onRequest",
            "On request",
            "Ask when a tool call needs more than the sandbox allows.",
        ),
        (
            "promptUnmatched",
            "Ask first",
            "Ask before any tool call your rules do not already allow.",
        ),
        (
            "denyUnmatched",
            "Deny unlisted",
            "Refuse any tool call your rules do not allow, without asking.",
        ),
        (
            "allowAll",
            "Allow all",
            "Run every tool call without asking.",
        ),
    ]
    .into_iter()
    .map(|(id, name, description)| SessionMode::new(id, name).description(description.to_string()))
    .collect();
    SessionModeState::new(current.to_string(), available)
}

/// One model the host will accept, from `model/list`.
#[derive(Clone, Debug, PartialEq)]
pub struct Model {
    pub id: String,
    pub label: String,
    pub provider: Option<String>,
    pub profile: Option<String>,
    pub efforts: Vec<String>,
    pub default_effort: Option<String>,
    pub active: bool,
    pub default: bool,
}

pub fn models(list: &Value) -> Vec<Model> {
    let strings = |entry: &Value, name: &str| -> Vec<String> {
        entry
            .get(name)
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default()
    };
    list.get("models")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .filter_map(|entry| {
            let id = str_field(entry, "modelId")?.to_string();
            let mut efforts = strings(entry, "variants");
            if efforts.is_empty() {
                efforts = strings(entry, "reasoningEffortVariants");
            }
            Some(Model {
                label: str_field(entry, "displayLabel").unwrap_or(&id).to_string(),
                provider: str_field(entry, "providerId").map(str::to_string),
                profile: str_field(entry, "profileId").map(str::to_string),
                efforts,
                default_effort: str_field(entry, "defaultReasoningEffort").map(str::to_string),
                active: entry
                    .get("isActive")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                default: entry
                    .get("isDefault")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
                id,
            })
        })
        .collect()
}

/// The model and reasoning effort pickers, from the catalog and the session's
/// current choices. A model with no effort tiers has no effort picker.
pub fn config_options(
    catalog: &[Model],
    model: Option<&str>,
    effort: Option<&str>,
) -> Vec<SessionConfigOption> {
    let Some(current) = model
        .and_then(|id| catalog.iter().find(|entry| entry.id == id))
        .or_else(|| catalog.iter().find(|entry| entry.active))
        .or_else(|| catalog.iter().find(|entry| entry.default))
        .or_else(|| catalog.first())
    else {
        return Vec::new();
    };
    let choices: Vec<SessionConfigSelectOption> = catalog
        .iter()
        .map(|entry| SessionConfigSelectOption::new(entry.id.clone(), entry.label.clone()))
        .collect();
    let mut options = vec![
        SessionConfigOption::select(MODEL_CONFIG, "Model", current.id.clone(), choices)
            .category(SessionConfigOptionCategory::Model),
    ];
    let effort = effort
        .filter(|effort| current.efforts.iter().any(|tier| tier == effort))
        .or(current.default_effort.as_deref())
        .or(current.efforts.first().map(String::as_str));
    if let Some(effort) = effort.filter(|_| !current.efforts.is_empty()) {
        let tiers: Vec<SessionConfigSelectOption> = current
            .efforts
            .iter()
            .map(|tier| SessionConfigSelectOption::new(tier.clone(), capitalized(tier)))
            .collect();
        options.push(
            SessionConfigOption::select(EFFORT_CONFIG, "Effort", effort.to_string(), tiers)
                .category(SessionConfigOptionCategory::ThoughtLevel),
        );
    }
    options
}

/// The `session/setModel` selection for a catalog entry.
pub fn model_selection(model: &Model) -> Value {
    let mut selection = json!({"modelId": model.id});
    if let Some(provider) = &model.provider {
        selection["providerId"] = json!(provider);
    }
    if let Some(profile) = &model.profile {
        selection["profileId"] = json!(profile);
    }
    selection
}

/// A todo list as an ACP plan. ACP has no cancelled entry, so a cancelled
/// todo leaves the plan rather than reading as done.
pub fn plan(params: &Value) -> Plan {
    let entries = params
        .get("items")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .filter_map(|todo| {
            let status = match str_field(todo, "status")? {
                "pending" => PlanEntryStatus::Pending,
                "inProgress" => PlanEntryStatus::InProgress,
                "completed" => PlanEntryStatus::Completed,
                _ => return None,
            };
            Some(PlanEntry::new(
                str_field(todo, "text")?,
                PlanEntryPriority::Medium,
                status,
            ))
        })
        .collect();
    Plan::new(entries)
}

/// One model call's usage, added to the turn's running total.
pub fn add_usage(total: Option<Usage>, params: &Value) -> Option<Usage> {
    let usage = params.get("usage")?;
    let count = |name: &str| usage.get(name).and_then(Value::as_u64).unwrap_or(0);
    let prior = total.unwrap_or_else(|| Usage::new(0, 0, 0));
    let input = prior.input_tokens + count("inputTokens");
    let output = prior.output_tokens + count("outputTokens");
    let cached = count("cacheReadTokens").max(count("cachedTokens"));
    Some(
        Usage::new(input + output, input, output)
            .cached_read_tokens(prior.cached_read_tokens.unwrap_or(0) + cached)
            .cached_write_tokens(prior.cached_write_tokens.unwrap_or(0) + count("cacheWriteTokens"))
            .thought_tokens(prior.thought_tokens.unwrap_or(0) + count("reasoningTokens")),
    )
}

/// An ACP MCP server as one entry of MSP's `config.mcpServers`, keyed by
/// name. MSP carries stdio and streamable HTTP; an SSE server has no MSP
/// transport and is dropped with a note on stderr.
pub fn mcp_server(
    server: &agent_client_protocol::schema::v1::McpServer,
) -> Option<(String, Value)> {
    use agent_client_protocol::schema::v1::McpServer;
    // A granted server that fails to start should cost the session that one
    // server, not the whole conversation; Muse calls that posture optional.
    match server {
        McpServer::Stdio(stdio) => {
            let env: serde_json::Map<String, Value> = stdio
                .env
                .iter()
                .map(|variable| (variable.name.clone(), json!(variable.value)))
                .collect();
            Some((
                stdio.name.clone(),
                json!({
                    "transport": "stdio",
                    "command": stdio.command,
                    "args": stdio.args,
                    "env": env,
                    "mode": "optional",
                }),
            ))
        }
        McpServer::Http(http) => {
            let headers: serde_json::Map<String, Value> = http
                .headers
                .iter()
                .map(|header| (header.name.clone(), json!(header.value)))
                .collect();
            Some((
                http.name.clone(),
                json!({
                    "transport": "streamableHttp",
                    "url": http.url,
                    "headers": headers,
                    "mode": "optional",
                }),
            ))
        }
        McpServer::Sse(sse) => {
            eprintln!(
                "muse-acp: Muse has no SSE transport for MCP; skipping {}",
                sse.name
            );
            None
        }
        _ => None,
    }
}

/// An ACP prompt as MSP turn input. Text and images carry over; a file link
/// becomes Muse's `@path` mention, relative to the workspace when it is
/// inside it; embedded text rides as text. Audio has no MSP part.
///
/// MSP joins a turn's text parts with nothing between them, so a client that
/// sends its context and the person's words as separate blocks would have
/// them run together. Neighbouring text is joined here, a paragraph apart.
pub fn turn_input(prompt: &[ContentBlock], cwd: &Path) -> Vec<Value> {
    let mut parts: Vec<Value> = Vec::new();
    for block in prompt {
        let text = match block {
            ContentBlock::Text(text) => text.text.clone(),
            ContentBlock::ResourceLink(link) => mention(&link.uri, cwd),
            ContentBlock::Resource(resource) => {
                use agent_client_protocol::schema::v1::EmbeddedResourceResource;
                let EmbeddedResourceResource::TextResourceContents(contents) = &resource.resource
                else {
                    continue;
                };
                format!(
                    "{}\n```\n{}\n```",
                    mention(&contents.uri, cwd),
                    contents.text
                )
            }
            ContentBlock::Image(image) => {
                parts.push(json!({
                    "type": "image",
                    "base64Data": image.data,
                    "mediaType": image.mime_type,
                }));
                continue;
            }
            _ => continue,
        };
        match parts.last_mut().filter(|part| part["type"] == "text") {
            Some(previous) => {
                let joined = format!(
                    "{}\n\n{text}",
                    previous["text"].as_str().unwrap_or_default()
                );
                previous["text"] = json!(joined);
            }
            None => parts.push(json!({"type": "text", "text": text})),
        }
    }
    parts
}

fn mention(uri: &str, cwd: &Path) -> String {
    let Some(path) = uri.strip_prefix("file://") else {
        return uri.to_string();
    };
    match Path::new(path).strip_prefix(cwd) {
        Ok(relative) => format!("@{}", relative.display()),
        Err(_) => format!("@{path}"),
    }
}

fn str_field<'a>(value: &'a Value, name: &str) -> Option<&'a str> {
    value.get(name)?.as_str()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text_of(update: &SessionUpdate) -> (&'static str, String) {
        let (kind, chunk) = match update {
            SessionUpdate::AgentMessageChunk(chunk) => ("message", chunk),
            SessionUpdate::AgentThoughtChunk(chunk) => ("thought", chunk),
            SessionUpdate::UserMessageChunk(chunk) => ("user", chunk),
            other => panic!("not a chunk: {other:?}"),
        };
        let ContentBlock::Text(text) = &chunk.content else {
            panic!("not text")
        };
        (kind, text.text.clone())
    }

    #[test]
    fn a_message_streamed_then_completed_is_sent_once() {
        let mut transcript = Transcript::default();
        let started =
            json!({"itemId": "m1", "kind": "agentMessage", "status": "inProgress", "text": ""});
        assert!(transcript.item(&started).is_empty());
        let delta = transcript.delta("m1", "text", "echo: ");
        assert_eq!(text_of(&delta[0]), ("message", "echo: ".to_string()));
        let completed = json!({"itemId": "m1", "kind": "agentMessage", "status": "completed", "text": "echo: hello"});
        let rest = transcript.item(&completed);
        assert_eq!(rest.len(), 1);
        assert_eq!(text_of(&rest[0]), ("message", "hello".to_string()));
        assert!(transcript.item(&completed).is_empty());
    }

    #[test]
    fn reasoning_summary_parts_read_as_paragraphs_of_one_thought() {
        let mut transcript = Transcript::default();
        transcript.item(&json!({"itemId": "r1", "kind": "reasoning", "status": "inProgress"}));
        let first = transcript.delta("r1", "summary.0", "Looking.");
        let second = transcript.delta("r1", "summary.1", "Found it.");
        assert_eq!(text_of(&first[0]), ("thought", "Looking.".to_string()));
        assert_eq!(
            text_of(&second[0]),
            ("thought", "\n\nFound it.".to_string())
        );
    }

    #[test]
    fn machinery_items_are_not_shown() {
        let mut transcript = Transcript::default();
        let reminder = json!({"itemId": "x", "kind": "reminderChild", "status": "inProgress"});
        assert!(transcript.item(&reminder).is_empty());
        assert!(transcript.delta("x", "text", "anything").is_empty());
    }

    #[test]
    fn a_tool_call_is_announced_once_and_settled_once() {
        let mut transcript = Transcript::default();
        let started = json!({
            "itemId": "t1", "kind": "toolCall", "status": "inProgress",
            "tool": "shell", "args": "{\"command\":\"ls -1\"}"
        });
        let announced = transcript.item(&started);
        let SessionUpdate::ToolCall(call) = &announced[0] else {
            panic!("expected a tool call")
        };
        assert_eq!(call.title, "shell: ls -1");
        assert_eq!(call.kind, ToolKind::Execute);
        assert_eq!(call.status, ToolCallStatus::InProgress);
        assert_eq!(call.raw_input, Some(json!({"command": "ls -1"})));
        assert!(transcript.item(&started).is_empty());

        let completed = json!({
            "itemId": "t1", "kind": "toolCall", "status": "completed",
            "tool": "shell", "visibleOutput": "a.txt\nb.txt"
        });
        let settled = transcript.item(&completed);
        let SessionUpdate::ToolCallUpdate(update) = &settled[0] else {
            panic!("expected a tool call update")
        };
        assert_eq!(update.fields.status, Some(ToolCallStatus::Completed));
        assert_eq!(update.fields.content.as_ref().map(Vec::len), Some(1));
        assert!(transcript.item(&completed).is_empty());
    }

    #[test]
    fn a_rejected_tool_call_fails_with_its_reason() {
        let mut transcript = Transcript::default();
        let rejected = json!({
            "itemId": "t2", "kind": "toolCall", "status": "rejected",
            "tool": "write_file", "args": "{\"path\":\"/etc/hosts\"}", "failureReason": "denied by user"
        });
        let SessionUpdate::ToolCall(call) = &transcript.item(&rejected)[0] else {
            panic!("expected a tool call")
        };
        assert_eq!(call.status, ToolCallStatus::Failed);
        assert_eq!(call.kind, ToolKind::Edit);
        assert_eq!(call.content.len(), 1);
    }

    #[test]
    fn replay_includes_the_persons_own_words() {
        let user =
            json!({"itemId": "u1", "kind": "userMessage", "status": "completed", "text": "hi"});
        assert_eq!(text_of(&replay(&user)[0]), ("user", "hi".to_string()));
    }

    fn approval() -> Value {
        json!({
            "approvalId": "a1", "itemId": "t1", "toolName": "shell",
            "subject": {"kind": "shell", "command": "rm -rf build"},
            "currentRequirementId": {"approvalId": "a1", "sourceIndex": 0},
            "availableChoices": [
                {"choiceId": "c-once", "label": "Allow once", "decision": "approved", "scope": "once"},
                {"choiceId": "c-session", "label": "Allow for session", "decision": "approvedForSession", "scope": "session"},
                {"choiceId": "c-deny", "label": "Deny", "decision": "denied", "scope": "once"},
                {"choiceId": "c-timeout", "label": "Timed out", "decision": "timedOut", "scope": "once"}
            ]
        })
    }

    #[test]
    fn an_approval_becomes_a_permission_with_muses_choices() {
        let (call, options) = permission(&approval());
        assert_eq!(call.tool_call_id.0.as_ref(), "t1");
        assert_eq!(call.fields.title.as_deref(), Some("Run `rm -rf build`"));
        let kinds: Vec<_> = options
            .iter()
            .map(|option| (option.option_id.0.to_string(), option.kind))
            .collect();
        assert_eq!(
            kinds,
            vec![
                ("c-once".to_string(), PermissionOptionKind::AllowOnce),
                ("c-session".to_string(), PermissionOptionKind::AllowAlways),
                ("c-deny".to_string(), PermissionOptionKind::RejectOnce),
            ]
        );
    }

    #[test]
    fn an_unanswered_approval_is_denied_once() {
        assert_eq!(refusal(&approval()).as_deref(), Some("c-deny"));
    }

    #[test]
    fn the_effort_picker_follows_the_current_model() {
        let catalog = models(&json!({"models": [
            {"modelId": "wonder", "displayLabel": "Wonder", "isActive": true, "isDefault": true,
             "variants": ["low", "high"], "defaultReasoningEffort": "high"},
            {"modelId": "glimmer", "displayLabel": "Glimmer", "isActive": false, "isDefault": false}
        ]}));
        let options = config_options(&catalog, None, None);
        assert_eq!(options.len(), 2);
        assert_eq!(options[0].id.0.as_ref(), MODEL_CONFIG);
        assert_eq!(options[1].id.0.as_ref(), EFFORT_CONFIG);
        assert_eq!(
            config_options(&catalog, Some("glimmer"), Some("high")).len(),
            1
        );
    }

    #[test]
    fn a_workspace_file_is_mentioned_relative_to_it() {
        let cwd = Path::new("/work/repo");
        assert_eq!(
            mention("file:///work/repo/src/main.rs", cwd),
            "@src/main.rs"
        );
        assert_eq!(mention("file:///tmp/notes.md", cwd), "@/tmp/notes.md");
    }

    #[test]
    fn neighbouring_text_blocks_are_one_part_a_paragraph_apart() {
        let prompt = [
            ContentBlock::from("You are in a room."),
            ContentBlock::from("hello there"),
        ];
        assert_eq!(
            turn_input(&prompt, Path::new("/work")),
            vec![json!({"type": "text", "text": "You are in a room.\n\nhello there"})]
        );
    }

    #[test]
    fn usage_accumulates_across_model_calls() {
        let call = json!({"usage": {"inputTokens": 10, "outputTokens": 5, "cachedTokens": 2, "reasoningTokens": 1}});
        let total = add_usage(add_usage(None, &call), &call).expect("usage");
        assert_eq!(
            (total.input_tokens, total.output_tokens, total.total_tokens),
            (20, 10, 30)
        );
        assert_eq!(total.cached_read_tokens, Some(4));
    }
}
