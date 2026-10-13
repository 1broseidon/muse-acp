# muse-acp

Muse Code as an [Agent Client Protocol](https://agentclientprotocol.com) agent.

Muse Code embeds through its own protocol, MSP (`muse serve`), not ACP. This
adapter is an ACP agent that starts `muse serve` behind itself and translates,
so any ACP client (Zed, [Hotline](https://github.com/1broseidon/hotline),
anything else that speaks ACP) can run Muse Code as it runs Claude Code or
Codex. It runs on your own Muse login, so a Muse subscription works. The
model-only Responses API takes an API key and cannot use the subscription.

Nothing about Muse is reimplemented. Your settings, sandbox, approval rules,
skills and session history are Muse's own, and Muse runs every tool itself.

## Install

```bash
cargo install --git https://github.com/1broseidon/muse-acp
```

You also need Muse Code itself (`muse` on your `PATH`), signed in once:

```bash
muse login
```

## Use

### Zed

In `settings.json`:

```json
{
  "agent_servers": {
    "Muse Code": {
      "command": "muse-acp",
      "args": [],
      "env": {}
    }
  }
}
```

### Any ACP client

Start `muse-acp` and speak ACP on its stdin and stdout. Arguments after `--`
go to `muse serve` unchanged:

```bash
muse-acp                                          # your Muse settings
muse-acp -- --provider meta --model muse-spark-1.3
muse-acp -- --disable-sandbox                     # sandbox posture is fixed per host
muse-acp --muse /opt/muse/bin/muse                # a muse that is not on PATH
```

### Signing in

The adapter offers one ACP sign-in method, a terminal method that runs
`muse-acp login`, which runs `muse login`. A client that supports terminal
auth shows it as **Log in with Meta** when a turn fails for want of a login.

`META_API_KEY` is removed from `muse serve`'s environment, because Muse
prefers it over the account login and this adapter exists to use the
subscription. A key stored with `muse auth set` still works.

## What maps to what

| ACP | MSP |
|---|---|
| `initialize` | `initialize`, requesting per-session MCP and declining Muse's own question dialogs |
| `session/new` (cwd, extra folders, MCP servers) | `session/start` (`workspaceRoots`, `config.mcpServers`) |
| `session/load` | `session/resume` with inline history, replayed as updates |
| `session/resume` | `session/resume` without history |
| `session/prompt` | `turn/start`, answered when `turn/completed` arrives |
| `session/cancel` | `turn/interrupt` |
| `_session/steering` (extension) | `turn/steer`, aimed at the running turn by id |
| `session/update` | `item/*` (messages, reasoning, tool calls, subagents), `session/todoListChanged` as a plan |
| `session/request_permission` | `approval/request` → `approval/decide`, one stage at a time |
| session modes | Muse's approval modes: on request, ask first, deny unlisted, allow all |
| config options: model, effort | `model/list`, `session/setModel`, `session/setReasoningEffort` |
| `auth_required` error | a turn that failed with `authRequired` |

Muse's internal reminder checks, hooks, compaction and side chats are left
out of the transcript. A turn that fails, such as a 429 when the subscription
window is used up, fails the prompt with Muse's own message.

## Limits

- MCP over SSE has no MSP transport, so SSE servers are skipped. stdio and
  streamable HTTP pass through.
- Audio prompts have no MSP part and are dropped.
- Muse's own question dialogs (`userInput/request`) are declined at
  initialize, because ACP has nowhere to show them.
- Subscription usage windows (`usage/read`) have no ACP field and are not
  shown.
- Muse features with no ACP equivalent (goals, workflows, plugins, recaps,
  forks) are not exposed. Use them from `muse` directly.

## Develop

```bash
cargo test
```

`tests/bridge.rs` drives the adapter end to end against a scripted Muse host
whose wire shapes come from a live `muse serve`. `tests/live.rs` runs the
built binary in front of a real `muse serve --provider echo`, which needs no
account or network, and skips itself when `muse` is not installed.

## License

MIT or Apache-2.0, at your option.
