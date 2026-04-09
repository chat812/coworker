use std::env;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use reqwest::Client;
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::model::{
    CustomNotification, ExperimentalCapabilities, Implementation, ServerCapabilities, ServerInfo,
    ServerNotification,
};
use rmcp::service::NotificationContext;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::{tool, tool_handler, tool_router, RoleServer, ServerHandler, ServiceExt};
use rmcp::schemars::{self, JsonSchema};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

// --- Logging ---

fn log_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".claude-peers-debug.log")
}

fn log(msg: &str) {
    let line = format!("[coworker] {}\n", msg);
    eprint!("{}", line);
    if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(log_path()) {
        let _ = f.write_all(line.as_bytes());
    }
}

macro_rules! flog {
    ($($arg:tt)*) => { log(&format!($($arg)*)) };
}

// --- Broker API types ---

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Peer {
    id: String,
    name: String,
    pid: u32,
    cwd: String,
    git_root: Option<String>,
    tty: Option<String>,
    harness: String,
    hostname: String,
    summary: String,
    status: String,
    registered_at: String,
    last_seen: String,
}

#[derive(Debug, Deserialize)]
struct RegisterResponse {
    id: String,
    token: String,
}

#[derive(Debug, Deserialize)]
struct AuthStatusResponse {
    status: String,
    #[allow(dead_code)]
    peer_id: String,
}

#[derive(Debug, Clone, Deserialize)]
struct Message {
    from_id: String,
    text: String,
    sent_at: String,
}

#[derive(Debug, Deserialize)]
struct PollMessagesResponse {
    messages: Vec<Message>,
}

#[derive(Debug, Deserialize)]
struct SendResult {
    ok: bool,
    error: Option<String>,
}

// --- Broker client ---

struct BrokerClient {
    http: Client,
    broker_url: String,
    token: Mutex<Option<String>>,
}

impl BrokerClient {
    fn new(broker_url: String) -> Self {
        Self {
            http: Client::new(),
            broker_url,
            token: Mutex::new(None),
        }
    }

    async fn set_token(&self, token: String) {
        *self.token.lock().await = Some(token);
    }

    async fn post<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        body: &impl Serialize,
    ) -> Result<T, String> {
        let token = self.token.lock().await.clone();
        let mut req = self
            .http
            .post(format!("{}{}", self.broker_url, path))
            .json(body);
        if let Some(ref t) = token {
            req = req.bearer_auth(t);
        }
        let res = req
            .send()
            .await
            .map_err(|e| format!("Broker request failed ({}): {}", path, e))?;
        if !res.status().is_success() {
            let status = res.status();
            let text = res.text().await.unwrap_or_default();
            return Err(format!("Broker error ({}): {} {}", path, status, text));
        }
        res.json()
            .await
            .map_err(|e| format!("Broker response parse error ({}): {}", path, e))
    }

    async fn health_check(&self) -> bool {
        let url = format!("{}/health", self.broker_url);
        match self
            .http
            .get(&url)
            .timeout(Duration::from_secs(2))
            .send()
            .await
        {
            Ok(res) => res.status().is_success(),
            Err(_) => false,
        }
    }

    fn is_local(&self) -> bool {
        let url = self.broker_url.to_lowercase();
        url.contains("127.0.0.1") || url.contains("localhost")
    }
}

// --- State ---

struct PeerState {
    id: Option<String>,
    cwd: String,
    git_root: Option<String>,
}

// --- MCP Server ---

#[derive(Clone)]
struct CoworkerServer {
    broker: Arc<BrokerClient>,
    state: Arc<Mutex<PeerState>>,
    tool_router: ToolRouter<Self>,
}

impl CoworkerServer {
    fn new(broker: Arc<BrokerClient>, state: Arc<Mutex<PeerState>>) -> Self {
        Self {
            broker,
            state,
            tool_router: Self::tool_router(),
        }
    }
}

// --- Tool parameter types ---

#[derive(Debug, Deserialize, JsonSchema)]
struct ListPeersParams {
    #[schemars(description = "Required. One of: \"all\" (everyone), \"network\" (same as all), \"directory\" (same cwd), \"repo\" (same git repo).")]
    scope: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct SendMessageParams {
    #[schemars(description = "The peer ID to send to — copy it exactly from list_peers (e.g. 'abc12345'). This field is named to_id.")]
    #[serde(alias = "to")]
    to_id: String,
    #[schemars(description = "The message text to send")]
    message: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct SetSummaryParams {
    #[schemars(description = "A 1-2 sentence summary of your current work")]
    summary: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct CheckMessagesParams {}

#[derive(Debug, Deserialize, JsonSchema)]
struct ListChannelsParams {}

#[derive(Debug, Deserialize, JsonSchema)]
struct JoinChannelParams {
    #[schemars(description = "The channel name to join (e.g. 'backend-team')")]
    channel: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct LeaveChannelParams {}

#[derive(Debug, Deserialize)]
struct ChannelPeerSummary {
    id: String,
    name: String,
}

#[derive(Debug, Deserialize)]
struct ChannelInfo {
    name: String,
    peers: Vec<ChannelPeerSummary>,
}

#[derive(Debug, Deserialize)]
struct JoinChannelResult {
    ok: bool,
    channel: String,
    error: Option<String>,
}

#[tool_router]
impl CoworkerServer {
    #[tool(
        name = "list_peers",
        description = "List other AI coding instances on the network. Returns their ID, harness type, hostname, working directory, git repo, and summary."
    )]
    async fn list_peers(
        &self,
        Parameters(ListPeersParams { scope }): Parameters<ListPeersParams>,
    ) -> String {
        let state = self.state.lock().await;
        let body = serde_json::json!({
            "scope": scope,
            "cwd": state.cwd,
            "git_root": state.git_root,
            "exclude_id": state.id,
        });
        drop(state);

        match self.broker.post::<Vec<Peer>>("/list-peers", &body).await {
            Ok(peers) if peers.is_empty() => {
                format!("No other instances found (scope: {}).", scope)
            }
            Ok(peers) => {
                let lines: Vec<String> = peers
                    .iter()
                    .map(|p| {
                        let peer_name = if p.name.is_empty() { p.id.clone() } else { p.name.clone() };
                        let mut parts = vec![
                            format!("Name: {}", peer_name),
                            format!("ID: {}", p.id),
                            format!("Harness: {}", p.harness),
                            format!("Host: {}", p.hostname),
                            format!("CWD: {}", p.cwd),
                        ];
                        if let Some(ref gr) = p.git_root {
                            parts.push(format!("Repo: {}", gr));
                        }
                        if let Some(ref tty) = p.tty {
                            parts.push(format!("TTY: {}", tty));
                        }
                        if !p.summary.is_empty() {
                            parts.push(format!("Summary: {}", p.summary));
                        }
                        parts.push(format!("Last seen: {}", p.last_seen));
                        parts.join("\n  ")
                    })
                    .collect();
                format!(
                    "Found {} peer(s) (scope: {}):\n\n{}",
                    peers.len(),
                    scope,
                    lines.join("\n\n")
                )
            }
            Err(e) => format!("Error listing peers: {}", e),
        }
    }

    #[tool(
        name = "send_message",
        description = "Send a message to another AI coding instance by peer ID. The message will be pushed into their session immediately via channel notification."
    )]
    async fn send_message(
        &self,
        Parameters(SendMessageParams { to_id, message }): Parameters<SendMessageParams>,
    ) -> String {
        let state = self.state.lock().await;
        let my_id = match &state.id {
            Some(id) => id.clone(),
            None => return "Not registered with broker yet".to_string(),
        };
        drop(state);

        let body = serde_json::json!({
            "from_id": my_id,
            "to_id": to_id,
            "text": message,
        });

        match self.broker.post::<SendResult>("/send-message", &body).await {
            Ok(r) if r.ok => format!("Message sent to peer {}", to_id),
            Ok(r) => format!("Failed to send: {}", r.error.unwrap_or_default()),
            Err(e) => format!("Error sending message: {}", e),
        }
    }

    #[tool(
        name = "set_summary",
        description = "Set a brief summary (1-2 sentences) of what you are currently working on. This is visible to other instances when they list peers."
    )]
    async fn set_summary(
        &self,
        Parameters(SetSummaryParams { summary }): Parameters<SetSummaryParams>,
    ) -> String {
        let state = self.state.lock().await;
        let my_id = match &state.id {
            Some(id) => id.clone(),
            None => return "Not registered with broker yet".to_string(),
        };
        drop(state);

        let body = serde_json::json!({ "id": my_id, "summary": summary });
        match self.broker.post::<serde_json::Value>("/set-summary", &body).await {
            Ok(_) => format!("Summary updated: \"{}\"", summary),
            Err(e) => format!("Error setting summary: {}", e),
        }
    }

    #[tool(
        name = "check_messages",
        description = "Manually check for new messages from other instances. Messages are normally pushed automatically via channel notifications, but you can use this as a fallback."
    )]
    async fn check_messages(
        &self,
        Parameters(CheckMessagesParams {}): Parameters<CheckMessagesParams>,
    ) -> String {
        let state = self.state.lock().await;
        let my_id = match &state.id {
            Some(id) => id.clone(),
            None => return "Not registered with broker yet".to_string(),
        };
        drop(state);

        let body = serde_json::json!({ "id": my_id });
        match self
            .broker
            .post::<PollMessagesResponse>("/poll-messages", &body)
            .await
        {
            Ok(r) if r.messages.is_empty() => "No new messages.".to_string(),
            Ok(r) => {
                let lines: Vec<String> = r
                    .messages
                    .iter()
                    .map(|m| format!("From {} ({}):\n{}", m.from_id, m.sent_at, m.text))
                    .collect();
                format!(
                    "{} new message(s):\n\n{}",
                    r.messages.len(),
                    lines.join("\n\n---\n\n")
                )
            }
            Err(e) => format!("Error checking messages: {}", e),
        }
    }

    #[tool(
        name = "list_channels",
        description = "List all available channels on the network, along with which peers are in each channel."
    )]
    async fn list_channels(
        &self,
        Parameters(ListChannelsParams {}): Parameters<ListChannelsParams>,
    ) -> String {
        match self
            .broker
            .post::<Vec<ChannelInfo>>("/list-channels", &serde_json::json!({}))
            .await
        {
            Ok(channels) if channels.is_empty() => "No channels found.".to_string(),
            Ok(channels) => {
                let lines: Vec<String> = channels
                    .iter()
                    .map(|ch| {
                        let peer_list = if ch.peers.is_empty() {
                            "(empty)".to_string()
                        } else {
                            ch.peers.iter().map(|p| {
                                if p.name.is_empty() { p.id.clone() } else { p.name.clone() }
                            }).collect::<Vec<_>>().join(", ")
                        };
                        format!("#{} — {} peer(s): {}", ch.name, ch.peers.len(), peer_list)
                    })
                    .collect();
                lines.join("\n")
            }
            Err(e) => format!("Error listing channels: {}", e),
        }
    }

    #[tool(
        name = "join_channel",
        description = "Switch to a different channel. Automatically leaves your current channel first. You will only receive messages from peers in the same channel."
    )]
    async fn join_channel(
        &self,
        Parameters(JoinChannelParams { channel }): Parameters<JoinChannelParams>,
    ) -> String {
        let state = self.state.lock().await;
        let my_id = match &state.id {
            Some(id) => id.clone(),
            None => return "Not registered with broker yet".to_string(),
        };
        drop(state);

        let body = serde_json::json!({ "id": my_id, "channel": channel });
        match self.broker.post::<JoinChannelResult>("/join-channel", &body).await {
            Ok(r) if r.ok => {
                let s = self.state.lock().await;
                save_channel(s.git_root.as_deref(), &s.cwd, &r.channel);
                format!("Joined channel #{}", r.channel)
            }
            Ok(r) => format!("Failed to join channel: {}", r.error.unwrap_or_default()),
            Err(e) => format!("Error joining channel: {}", e),
        }
    }

    #[tool(
        name = "leave_channel",
        description = "Leave your current channel and return to the main channel."
    )]
    async fn leave_channel(
        &self,
        Parameters(LeaveChannelParams {}): Parameters<LeaveChannelParams>,
    ) -> String {
        let state = self.state.lock().await;
        let my_id = match &state.id {
            Some(id) => id.clone(),
            None => return "Not registered with broker yet".to_string(),
        };
        drop(state);

        let body = serde_json::json!({ "id": my_id });
        match self.broker.post::<serde_json::Value>("/leave-channel", &body).await {
            Ok(_) => {
                let s = self.state.lock().await;
                save_channel(s.git_root.as_deref(), &s.cwd, "main");
                "Left channel — back in #main".to_string()
            }
            Err(e) => format!("Error leaving channel: {}", e),
        }
    }
}

#[tool_handler]
impl ServerHandler for CoworkerServer {
    fn get_info(&self) -> ServerInfo {
        let mut experimental = ExperimentalCapabilities::new();
        experimental.insert(
            "claude/channel".to_string(),
            serde_json::from_value(serde_json::json!({})).unwrap(),
        );

        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_experimental_with(experimental)
                .build(),
        )
        .with_server_info(Implementation::new("claude-peers", "0.2.0"))
        .with_instructions(
            "You are connected to the claude-peers network. Other AI coding instances on this network can see you and send you messages.\n\n\
             When you receive a <channel source=\"claude-peers\" ...> message: read it, but only reply if the peer is asking you a direct question or requesting something specific. Do NOT send greetings back, do NOT continue small talk, and do NOT ask follow-up questions — this causes expensive token chains between agents. A simple acknowledgement is fine only when a peer asks for one.\n\n\
             Read the from_id, from_summary, from_cwd, and from_harness attributes to understand who sent the message. Reply by calling send_message with their from_id.\n\n\
             Available tools:\n\
             - list_peers: Discover other AI coding instances (scope: all/network/directory/repo)\n\
             - send_message: Send a message to another instance by ID\n\
             - set_summary: Set a 1-2 sentence summary of what you're working on (visible to other peers)\n\
             - check_messages: Manually check for new messages\n\
             - list_channels: See all available channels and who is in them\n\
             - join_channel: Switch to a different channel (leaves current first; only peers in the same channel can message each other)\n\
             - leave_channel: Leave your current channel and return to #main\n\n\
             When you start, proactively call set_summary to describe what you're working on. This helps other instances understand your context."
        )
    }

    async fn on_initialized(
        &self,
        context: NotificationContext<RoleServer>,
    ) {
        flog!("on_initialized called — starting polling and heartbeat loops");
        let broker = self.broker.clone();
        let state = self.state.clone();
        let peer = context.peer.clone();

        // Message polling loop
        tokio::spawn(async move {
            flog!("Polling loop task started");
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;

                let my_id = {
                    let s = state.lock().await;
                    match &s.id {
                        Some(id) => id.clone(),
                        None => {
                            flog!("Poll: no peer ID yet, skipping");
                            continue;
                        }
                    }
                };

                let body = serde_json::json!({ "id": my_id });
                let result = match broker
                    .post::<PollMessagesResponse>("/poll-messages", &body)
                    .await
                {
                    Ok(r) => {
                        if !r.messages.is_empty() {
                            flog!("Poll: {} new message(s)", r.messages.len());
                        }
                        r
                    }
                    Err(e) => {
                        flog!("Poll error: {}", e);
                        continue;
                    }
                };

                for msg in result.messages {
                    // Look up sender info
                    let (from_summary, from_cwd, from_harness) = {
                        let s = state.lock().await;
                        let list_body = serde_json::json!({
                            "scope": "all",
                            "cwd": s.cwd,
                            "git_root": s.git_root,
                        });
                        drop(s);
                        match broker.post::<Vec<Peer>>("/list-peers", &list_body).await {
                            Ok(peers) => {
                                if let Some(sender) = peers.iter().find(|p| p.id == msg.from_id) {
                                    (
                                        sender.summary.clone(),
                                        sender.cwd.clone(),
                                        sender.harness.clone(),
                                    )
                                } else {
                                    (String::new(), String::new(), String::new())
                                }
                            }
                            Err(_) => (String::new(), String::new(), String::new()),
                        }
                    };

                    let notification = CustomNotification::new(
                        "notifications/claude/channel",
                        Some(serde_json::json!({
                            "content": msg.text,
                            "meta": {
                                "from_id": msg.from_id,
                                "from_summary": from_summary,
                                "from_cwd": from_cwd,
                                "from_harness": from_harness,
                                "sent_at": msg.sent_at,
                            }
                        })),
                    );

                    flog!("Sending notification for message from {}", msg.from_id);
                    match peer
                        .send_notification(ServerNotification::CustomNotification(notification))
                        .await
                    {
                        Err(e) => flog!("Notification send error: {}", e),
                        Ok(_) => {
                            let preview: String = msg.text.chars().take(80).collect();
                            flog!("Notification sent OK. Message: {}", preview);
                        }
                    }
                }
            }
        });

        // Heartbeat loop
        let broker2 = self.broker.clone();
        let state2 = self.state.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(15)).await;
                let my_id = {
                    let s = state2.lock().await;
                    match &s.id {
                        Some(id) => id.clone(),
                        None => continue,
                    }
                };
                let body = serde_json::json!({ "id": my_id });
                let _ = broker2
                    .post::<serde_json::Value>("/heartbeat", &body)
                    .await;
            }
        });
    }
}

// --- Utility functions ---

const NAME_ADJECTIVES: &[&str] = &[
    "amber", "arcane", "arctic", "azure", "bright", "cobalt", "crimson", "crystal",
    "calm", "dawn", "dusk", "ember", "emerald", "gilded", "golden", "gentle",
    "iron", "ivory", "jade", "keen", "lunar", "mystic", "neon", "obsidian",
    "pearl", "radiant", "ruby", "sapphire", "serene", "silent", "silver", "solar",
    "stellar", "stone", "swift", "twilight", "velvet", "verdant", "violet", "warm",
];

const NAME_NOUNS: &[&str] = &[
    "anvil", "aurora", "beacon", "brook", "catalyst", "cipher", "comet", "crane",
    "delta", "drift", "falcon", "flame", "forge", "frost", "gale", "garden",
    "harbor", "hawk", "horizon", "kite", "lynx", "meadow", "nebula", "nexus",
    "oracle", "peak", "phoenix", "prism", "raven", "reef", "ridge", "river",
    "sage", "stone", "summit", "tide", "valley", "vector", "wave", "wolf",
];

fn generate_fancy_name() -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::time::{SystemTime, UNIX_EPOCH};
    // Use time + pid as seed for randomness without a rand crate
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
        ^ (std::process::id() as u128);
    let mut h = DefaultHasher::new();
    seed.hash(&mut h);
    let v1 = h.finish() as usize;
    seed.wrapping_add(1).hash(&mut h);
    let v2 = h.finish() as usize;
    let adj = NAME_ADJECTIVES[v1 % NAME_ADJECTIVES.len()];
    let noun = NAME_NOUNS[v2 % NAME_NOUNS.len()];
    format!("{}-{}", adj, noun)
}

fn load_saved_channel(git_root: Option<&str>, cwd: &str) -> Option<String> {
    let dir = git_root.unwrap_or(cwd);
    let path = std::path::Path::new(dir).join(".claude-peers").join("channel");
    std::fs::read_to_string(&path)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn save_channel(git_root: Option<&str>, cwd: &str, channel: &str) {
    let dir = git_root.unwrap_or(cwd);
    let config_dir = std::path::Path::new(dir).join(".claude-peers");
    let _ = std::fs::create_dir_all(&config_dir);
    let _ = std::fs::write(config_dir.join("channel"), channel);
}

fn load_or_generate_name(git_root: Option<&str>, cwd: &str) -> String {
    let dir = git_root.unwrap_or(cwd);
    let config_dir = std::path::Path::new(dir).join(".claude-peers");
    let name_path = config_dir.join("name");
    // Try to read existing name
    if let Ok(existing) = std::fs::read_to_string(&name_path) {
        let trimmed = existing.trim().to_string();
        if !trimmed.is_empty() {
            return trimmed;
        }
    }
    // Generate and persist
    let name = generate_fancy_name();
    let _ = std::fs::create_dir_all(&config_dir);
    let _ = std::fs::write(&name_path, &name);
    name
}

fn get_git_root(cwd: &str) -> Option<String> {
    Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .current_dir(cwd)
        .output()
        .ok()
        .and_then(|o| {
            if o.status.success() {
                Some(String::from_utf8_lossy(&o.stdout).trim().to_string())
            } else {
                None
            }
        })
}

fn read_master_key() -> Option<String> {
    let home = dirs::home_dir()?;
    let key_path = home.join(".claude-peers.key");
    std::fs::read_to_string(key_path)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

// --- Main ---

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let broker_url = env::var("CLAUDE_PEERS_BROKER_URL").unwrap_or_else(|_| {
        let port = env::var("CLAUDE_PEERS_PORT").unwrap_or_else(|_| "7899".to_string());
        format!("http://127.0.0.1:{}", port)
    });
    let harness = env::var("CLAUDE_PEERS_HARNESS").unwrap_or_else(|_| "claude-code".to_string());

    let broker = Arc::new(BrokerClient::new(broker_url.clone()));

    // Set initial token from env or master key file
    if let Ok(token) = env::var("CLAUDE_PEERS_TOKEN") {
        broker.set_token(token).await;
    } else if let Some(key) = read_master_key() {
        broker.set_token(key).await;
        log("Using master key from ~/.claude-peers.key");
    }

    // Ensure broker is running
    if !broker.health_check().await {
        if broker.is_local() {
            let exe = env::current_exe().unwrap_or_else(|_| PathBuf::from("coworker"));
            let broker_bin = exe.parent().unwrap().join(if cfg!(windows) {
                "claude-peers-broker.exe"
            } else {
                "claude-peers-broker"
            });

            if broker_bin.exists() {
                log(&format!(
                    "Starting broker daemon ({})...",
                    broker_bin.display()
                ));
                let _child = Command::new(&broker_bin)
                    .stdin(std::process::Stdio::null())
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::inherit())
                    .spawn()
                    .map_err(|e| format!("Failed to spawn broker: {}", e))?;

                for _ in 0..30 {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    if broker.health_check().await {
                        log("Broker started");
                        break;
                    }
                }
            }

            if !broker.health_check().await {
                return Err("Broker not reachable and could not be started".into());
            }
        } else {
            return Err(format!("Remote broker at {} is not reachable", broker_url).into());
        }
    } else {
        log("Broker already running");
    }

    // Gather context
    let cwd = env::current_dir()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|_| ".".to_string());
    let git_root = get_git_root(&cwd);
    let my_hostname = hostname::get()
        .map(|h| h.to_string_lossy().to_string())
        .unwrap_or_else(|_| "unknown".to_string());

    let my_name = load_or_generate_name(git_root.as_deref(), &cwd);

    log(&format!("CWD: {}", cwd));
    log(&format!(
        "Git root: {}",
        git_root.as_deref().unwrap_or("(none)")
    ));
    log(&format!("Harness: {}", harness));
    log(&format!("Hostname: {}", my_hostname));
    log(&format!("Name: {}", my_name));

    // Register with broker
    let reg_body = serde_json::json!({
        "name": my_name,
        "pid": std::process::id(),
        "cwd": cwd,
        "git_root": git_root,
        "tty": serde_json::Value::Null,
        "harness": harness,
        "hostname": my_hostname,
        "summary": "",
    });

    let reg: RegisterResponse = broker
        .post("/register", &reg_body)
        .await
        .map_err(|e| format!("Failed to register with broker: {}", e))?;

    log(&format!(
        "Registered as peer {} (pending approval)",
        reg.id
    ));

    // Switch to session token
    broker.set_token(reg.token.clone()).await;

    // Auto-approve if local + have master key
    if broker.is_local() {
        if let Some(master_key) = read_master_key() {
            let approve_res = reqwest::Client::new()
                .post(format!("{}/auth/approve", broker_url))
                .bearer_auth(&master_key)
                .json(&serde_json::json!({ "peer_id": reg.id }))
                .send()
                .await;
            match approve_res {
                Ok(r) if r.status().is_success() => log("Auto-approved (local + master key)"),
                _ => {
                    wait_for_approval(&broker, &reg.token).await?;
                }
            }
        } else {
            wait_for_approval(&broker, &reg.token).await?;
        }
    } else {
        wait_for_approval(&broker, &reg.token).await?;
    }

    // Rejoin last channel
    if let Some(saved_ch) = load_saved_channel(git_root.as_deref(), &cwd) {
        if saved_ch != "main" {
            let body = serde_json::json!({ "id": reg.id, "channel": saved_ch });
            match broker.post::<JoinChannelResult>("/join-channel", &body).await {
                Ok(r) if r.ok => flog!("Rejoined saved channel #{}", r.channel),
                Ok(_r) => {
                    flog!("Saved channel #{} no longer exists, falling back to #main", saved_ch);
                    save_channel(git_root.as_deref(), &cwd, "main");
                }
                Err(e) => flog!("Failed to rejoin channel: {}", e),
            }
        }
    }

    // Set up state
    let state = Arc::new(Mutex::new(PeerState {
        id: Some(reg.id.clone()),
        cwd,
        git_root,
    }));

    // Create and run MCP server
    let server = CoworkerServer::new(broker.clone(), state);
    let (stdin, stdout) = rmcp::transport::stdio();

    log("Starting MCP server on stdio...");

    let running = server.serve((stdin, stdout)).await?;

    log("MCP server running (serve() returned)");

    // Unregister on shutdown
    let broker_cleanup = broker.clone();
    let id_cleanup = reg.id.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        let body = serde_json::json!({ "id": id_cleanup });
        let _ = broker_cleanup
            .post::<serde_json::Value>("/unregister", &body)
            .await;
        log("Unregistered from broker");
        std::process::exit(0);
    });

    running.waiting().await?;

    // Cleanup on normal exit
    let body = serde_json::json!({ "id": reg.id });
    let _ = broker
        .post::<serde_json::Value>("/unregister", &body)
        .await;
    log("Unregistered from broker");

    Ok(())
}

async fn wait_for_approval(
    broker: &BrokerClient,
    token: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    log("Waiting for admin approval...");
    loop {
        let body = serde_json::json!({ "token": token });
        match broker
            .post::<AuthStatusResponse>("/auth/status", &body)
            .await
        {
            Ok(r) if r.status == "approved" => {
                log("Approved by admin!");
                return Ok(());
            }
            Ok(r) if r.status == "rejected" => {
                return Err("Connection rejected by admin".into());
            }
            Ok(_) => {} // still pending
            Err(e) => {
                if e.contains("rejected") {
                    return Err(e.into());
                }
                log(&format!("Approval poll error: {}", e));
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}
