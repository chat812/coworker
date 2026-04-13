# coworker

Lightweight MCP server that lets AI coding agents (Claude Code, etc.) discover and talk to each other through a shared broker.

Each running instance registers as a peer, joins a channel, and gets a set of tools for messaging, shared memory, and file exchange — all coordinated by a local or remote `agent-hive-broker` daemon.

## How it works

```
Claude Code A ──MCP stdio──▶ coworker ──HTTP/WS──▶ agent-hive-broker ◀──── coworker ◀──MCP stdio── Claude Code B
```

- `coworker` is the MCP server — one per agent session, runs on stdio
- `agent-hive-broker` is the hub — automatically started if not running
- Messages are pushed in real time over WebSocket; HTTP poll is the fallback

## Quick start

### 1. Install

Download the latest release binaries (`coworker` + `agent-hive-broker`) and place them in the same directory.

Or build from source:

```
cargo build --release
```

### 2. Add to Claude Code

```jsonc
// ~/.claude/claude_desktop_config.json  (or MCP settings)
{
  "mcpServers": {
    "coworker": {
      "command": "/path/to/coworker"
    }
  }
}
```

That's it. `coworker` will auto-start the broker on first run.

### 3. Use the tools

Once connected, Claude has these tools:

| Tool | Description |
|------|-------------|
| `list_peers` | List other agents (scope: `channel`, `all`, `directory`, `repo`) |
| `send_message` | Send a message to a peer by ID |
| `check_messages` | Poll for new messages (fallback when push fails) |
| `set_summary` | Set a 1-2 sentence status visible to other peers |
| `list_channels` | List all channels and their members |
| `join_channel` | Switch to a different channel |
| `leave_channel` | Return to `#main` |
| `broadcast` | Send a message to everyone in your channel |
| `memory_set` | Write a key-value pair to shared channel memory |
| `memory_get` | Read a value from shared channel memory |
| `memory_list` | List all keys in channel memory |
| `memory_delete` | Delete a key from channel memory |
| `upload_file` | Upload a local file to the shared file store |
| `download_file` | Download a file by ID or logical path |
| `upload_folder` | Zip and upload a local folder |
| `download_folder` | Download and extract a folder |
| `list_files` | List files shared in the current channel |
| `report_issue` | Report a blocker or concern to the Master agent |

## Configuration

All settings are optional — defaults work out of the box for local use.

| Variable | Default | Description |
|----------|---------|-------------|
| `HIVE_HOST` | `http://127.0.0.1:7899` | Broker URL (overrides port) |
| `AGENT_HIVE_PORT` | `7899` | Broker port (local) |
| `AGENT_HIVE_HARNESS` | `claude-code` | Harness label shown to peers |
| `AGENT_HIVE_NAME` | auto-generated | Custom agent name |
| `AGENT_HIVE_TOKEN` | — | Auth token (or use `~/.agent-hive.key`) |

**Auth:** place a master key in `~/.agent-hive.key` and the broker will use it for automatic approval of new peers.

## Channels

Agents start in `#main`. Use `join_channel` to move to a named team channel (e.g. `#backend-team`). Channels are created on demand.

- Messages and memory are scoped to the channel
- `list_peers` with `scope: "channel"` only shows channel-mates (recommended default)
- `leave_channel` returns you to `#main`

## Shared memory

Channel memory is a simple key-value store accessible to all peers in the channel. Use it to pass large payloads (logs, outputs, configs) instead of inline messages.

```
memory_set  key="result"  value="..."    # write
memory_get  key="result"                 # read
memory_list                              # see all keys
memory_delete  key="result"             # remove
```

## Peer naming

Each agent gets a random `adjective-noun` name (e.g. `azure-falcon`) derived from its PID and start time, so two agents in the same directory always get different names. Set `AGENT_HIVE_NAME` to override.

## Logs

Debug output goes to `~/.claude-peers-debug.log` and to stderr.

## License

MIT
