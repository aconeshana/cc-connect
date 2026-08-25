# Session Host agent

The `sessionhost` agent connects cc-connect to an application that already
owns an interactive coding session. It is intended for multi-end clients where
the same session is rendered locally in a terminal UI and remotely in an IM
thread.

Unlike the `claudecode` adapter, `sessionhost` does not launch a headless CLI,
does not use Claude Code's `stream-json` protocol, and does not parse PTY
output. The host emits semantic events before terminal rendering, so cc-connect
receives structured text, tools, permissions, and questions without depending
on ANSI text or a particular TUI.

## Configuration

```toml
env_file = "~/.cc-connect/credentials.env"

[projects.agent]
type = "sessionhost"

[projects.agent.options]
auth_token_env = "CC_SESSION_LINK_TOKEN"
# Backward-compatible single-channel form. It is available in the picker but
# no longer auto-binds local sessions.
bind_session_key = "feishu:oc_chat_id:ou_owner"
request_timeout_seconds = 30
max_frame_bytes = 16777216
# Multi-IM form. The host footer shows one option per configured target and a
# session can activate at most one target at a time.
[projects.agent.options.collaboration_targets]
feishu = "feishu:oc_chat_id:ou_owner"
slack = "slack:C0123456789:U0123456789"
```

When cc-connect is launched by the Java JAR, `endpoint` and `work_dir` are
injected through `CC_SESSION_LINK_ENDPOINT` and `CC_SESSION_WORK_DIR`. A
standalone launch may still set both values in the agent options.

For Feishu, enable `thread_isolation = true`. The Java launcher also injects
the cc-connect data directory and project name through the normal composition
path, so host-thread roots are persisted under
`<data_dir>/session-host/threads/` with mode `0600`. After a sidecar restart,
replies in an existing bound thread still route to the same host session and
remain exempt from repeated `@bot` mentions.

The authentication token is accepted only through an environment variable.
For an embedded deployment, the Java application should generate a random
token for each sidecar launch and pass it to cc-connect through the environment.
The Unix socket and its parent directory should be accessible only by the
current OS user.

Platform credentials may use `${VAR}` placeholders. When the Java application
is not launched from a shell that exports those variables, set top-level
`env_file` to a mode-`0600` dotenv file. Relative paths are resolved beside the
TOML file, and an existing process environment value takes precedence. Secrets
remain outside the TOML, repository, and bundled JAR.

## Session Link Protocol v1

Transport is newline-delimited JSON over a local Unix domain socket. Each
frame contains a strict, versioned envelope:

```json
{
  "protocol": "session-link",
  "version": 1,
  "kind": "request",
  "name": "turn.submit",
  "id": "cc-3",
  "session_id": "019...",
  "payload": {}
}
```

Responses set `kind` to `response` and use `reply_to` to reference the request
ID. Rejected requests set `kind` to `error` and include an error object.
Unsolicited semantic output uses `kind: "event"` and a `session_id`.

Current requests:

- `link.hello`: authenticates cc-connect and negotiates protocol v1.
- `session.open`: creates a session or attaches to `requested_session_id`. A
  remote resume may include `activation_id`; the response echoes it together
  with the authoritative `session_id` and monotonic `activation_generation`.
- `session.list`: returns host-owned resumable sessions.
- `session.detach`: releases this cc-connect attachment without terminating the
  host-owned model session.
- `turn.submit`: submits user text and binary attachments to the session.
- `interaction.respond`: answers a native permission or AskUserQuestion request.
- `model.get`: returns the current live model and the choices accepted by the
  attached host session.
- `model.set`: applies a validated model choice to that session without
  detaching or restarting its PTY.
- `effort.get`: returns the session's configured and effective reasoning effort
  together with the choices accepted by the host.
- `effort.set`: applies a validated reasoning effort to that session without
  detaching, clearing history, or restarting its PTY.
- `compact.run`: executes native manual compaction for the attached active
  session, with optional summarization instructions, and returns the host's
  display result after completion.

Current events:

- `turn.started`
- `turn.model`
- `output.text`
- `output.thinking`
- `tool.started`
- `tool.completed`
- `interaction.requested`
- `interaction.resolved`
- `interaction.unsupported`
- `turn.completed`
- `session.error`
- `session.activated`
- `collaboration.changed`

`turn.started` carries the local user's display text, permission mode, and any
per-turn model/effort override. `turn.model` follows with the effective model
reported by the host. cc-connect mirrors these events, then thinking, text,
tool starts, and tool results in order, so an IM-bound thread is a useful live
transcript even when the turn was initiated in the local PTY.

`interaction.requested` carries the native tool name, raw input, request ID,
and optional structured questions. It may also carry the host's decision
reason, suggested rule, worker identity, destructive warning, blocked path,
custom request message, and tool description. cc-connect maps it directly to
its existing permission/card flow and includes the current turn's user input,
model, workspace, model output, and recent tools in the approval presentation.
All display fields and callback values are bounded before they are sent to an
IM platform. This avoids injecting a separate AskHuman prompt into the model
context while still giving the approver enough context to decide.

`interaction.resolved` identifies the winning endpoint and carries
`updated_input` when an interactive tool such as `AskUserQuestion` rewrites its
input. cc-connect uses that semantic answer to replace the exact pending IM card
with a button-free resolved view; the notification never enters model history.
Permission and single-choice question callback payloads include the native
interaction request ID. The engine accepts them only when that ID matches the
currently pending interaction. Legacy, missing, already-resolved, and otherwise
stale callbacks are consumed without reaching `turn.submit`, so an old card can
neither answer a newer interaction nor become an accidental user prompt.

`interaction.unsupported` is a secret-free endpoint capability notice. It
carries only the native request ID, interaction kind, and required client
action. cc-connect renders it as a standalone localized message and never
adds it to assistant text, session history, permission state, or model input.
The initial `sudo_password` use reports `complete_in_tui`; the command and
password are deliberately absent from the wire payload.

Platforms use native buttons where their interaction model fits. Feishu uses
buttons for permissions and single-choice questions; multi-select with a
free-text `Other` choice is rendered as numbered options plus an explicit
natural-language reply path so the full answer remains representable without
inventing a model-visible AskHuman prompt.

`session.activated` includes the authoritative `origin` (`local` or `remote`),
the optional causal `activation_id`, and the same monotonic
`activation_generation` returned by `session.open`. These fields fence delayed
resume completions and ensure a remote resume that passes through the native
TUI is published exactly once as `remote`. The Go Session Link client also
captures the previously authoritative session ID under the same activation
fence. A terminal-origin resume uses that causal predecessor to carry the exact
current collaboration thread to the newly active transcript, even when the
target transcript has never been bound to an IM or has an older historical
thread. This value is client-local lifecycle metadata rather than a new wire
field. Shared-app routing still requires the current sidecar to own the
thread's captured route generation, so a sibling process cannot inherit it.
The terminal footer defaults each host session to
`Collaboration: Off`. Selecting one channel emits `collaboration.changed`
immediately, without entering the model input queue. Feishu then creates a root message and stores a
`feishu:<chat>:root:<message>` session key, so local PTY output streams into the
thread before any IM prompt is sent. Selecting `Off` detaches the cc-connect
subscription without interrupting the host-owned model turn. Selecting another
channel atomically replaces the active surface; persisted bindings remain
available for later reuse. For an IM-initiated conversation, the
inbound path prepares or restores the thread before `session.open`; this makes
the first model reply land in the thread and prevents a second attachment race.
Replying in an old thread calls `session.open(requested_session_id)` before
`turn.submit`, which resumes that transcript in the Java terminal. Platforms
can add equivalent behavior by implementing the idempotent
`core.SessionThreadBinder`; platforms without that capability fall back to
their reconstructed proactive reply context.

`/model` uses the same session identity. Listing models, selecting a card row,
or switching by alias targets the Java session bound to that IM thread. When
the thread is not the currently active local transcript, cc-connect first
reactivates it with `session.open(requested_session_id)`, then performs
`model.get` / `model.set`. The switch keeps the existing Session Link
attachment and semantic event stream; it does not clear history or create a
new PTY process.

`/effort` (and its `/reasoning` alias) follows the same session-scoped path.
With no argument it renders the current configured and effective values; a
card row, number, or named choice calls `effort.set` for the Java session bound
to that thread. Session Host accepts `auto`, `low`, `medium`, `high`, `xhigh`,
and `max`. The host remains authoritative for model support and for the
effective value when `auto` or model-specific clamping applies. Unlike the
generic cc-connect reasoning switch, this live host operation does not reset
the agent session or discard transcript history.

`/compact [instructions]` uses `compact.run` instead of forwarding a slash
string through `turn.submit`. The Java host executes its existing native
`CompactCommand`, including microcompact, hooks, summary generation, transcript
replacement, persistence, progress UI, and optional custom instructions. The
operation is bound to the thread's session ID, and both terminal and IM input
are serialized behind it until completion. Session Link limits instruction
size and rejects unsafe control characters; it does not expose a generic remote
slash-command execution primitive.

In Session Host mode, `/new [name]` is also application-owned. cc-connect calls
`session.open` with an empty requested ID and atomically switches the current
IM thread to the returned Java session before acknowledging the command. It
does not create a sibling thread, does not submit `/new` to the model, and keeps
the previous session intact so `/resume` can restore it later.

`/resume` is a first-class Session Host command. With no argument it renders a
paged card containing the current TUI project's resumable sessions and marks
the session currently bound to the thread. `/list` reuses the same card.
`/resume <number|name|ID-prefix>` selects directly, while every card callback
contains the full session ID (`act:/resume <session-id>`) so list reordering
cannot select a different transcript. Session Host disables `/switch` and
guides users to `/resume`; generic CLI agents keep their existing `/switch`
and native `/resume` behavior.

A resume never creates a thread or launches a process. It runs inside the TUI
process that owns the route, fails without changing state while that thread has
an active turn, and serializes concurrent choices per thread. cc-connect first
prepares the target Session Link attachment, then generation-fences and
atomically switches the durable thread mapping and live reader, and only then
detaches the old attachment. Selecting the current session is idempotent. A
remote card shows a loading state and updates in place to success or failure;
a local TUI resume sends `↩ Resumed in TUI · <session title> · N messages loaded` and
refreshes the most recently tracked resume card. A remote resume does not send
that extra terminal notice.

Only messages inside a bound thread are eligible for Session Host routing.
Ordinary text, slash commands, and attachments sent in the bot's main chat are
deduplicated across sidecars and silently consumed, regardless of whether zero,
one, or several TUI processes are online. Card actions continue through the
durable route owner/generation fence, so only the TUI process that owns the
thread can perform the resume.

`turn.submit.message_id` is an idempotency key scoped to the host session.
Retries after a response/socket race share one in-flight result and do not run
the same IM message twice. On the outbound side, Java uses one bounded writer
lane per Session Link connection: semantic publication never waits on a slow
socket, frame order is preserved, and an unresponsive remote is disconnected
without blocking the local PTY/model turn.

The host retains a bounded replay of the latest semantic turn until the next
turn starts. When a user enables collaboration after a fast local turn has
already begun, the new subscription first receives that replay prefix
and then live events, preserving order and preventing the thread from beginning
at the first permission card.

## Embedded Java sidecar scope

The Java application bundles a selectively compiled cc-connect containing only
the `sessionhost` agent while retaining the upstream IM platform adapters for
macOS and Linux on amd64/arm64. Feishu currently has the most complete
persistent thread/card lifecycle; Slack and Discord also provide native
host-thread binding, and other adapters fall back to their existing
conversation reply-context reconstruction.
Set `CLAUDE_CODE_IM_CONFIG` to an explicit TOML file, or place it at
`~/.claude/cc-connect.toml`. `CLAUDE_CODE_CC_CONNECT_BINARY` remains available
for development overrides. Windows is not advertised until the transport has a
named-pipe implementation; Session Link v1 currently requires a Unix socket.
