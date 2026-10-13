//! The binary in front of a real `muse serve`, on Muse's echo provider: no
//! account, no network, no quota. Skipped when `muse` is not installed.

use agent_client_protocol::schema::ProtocolVersion;
use agent_client_protocol::schema::v1::{
    ContentBlock, InitializeRequest, NewSessionRequest, PromptRequest, SessionNotification,
    SessionUpdate, StopReason,
};
use agent_client_protocol::{ByteStreams, Client};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::process::Command;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

#[tokio::test]
async fn a_real_muse_host_answers_through_the_adapter() {
    if std::process::Command::new("muse")
        .arg("--version")
        .output()
        .is_err()
    {
        eprintln!("skipped: `muse` is not on PATH");
        return;
    }
    let workspace = std::env::temp_dir().join(format!("muse-acp-live-{}", std::process::id()));
    std::fs::create_dir_all(&workspace).unwrap();
    let mut adapter = Command::new(env!("CARGO_BIN_EXE_muse-acp"))
        .args(["--", "--provider", "echo", "--no-session-log"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("muse-acp starts");
    let stdin = adapter.stdin.take().unwrap();
    let stdout = adapter.stdout.take().unwrap();

    let said = Arc::new(Mutex::new(String::new()));
    let heard = said.clone();
    let conversation = Client
        .builder()
        .on_receive_notification(
            move |notification: SessionNotification, _cx| {
                let heard = heard.clone();
                async move {
                    if let SessionUpdate::AgentMessageChunk(chunk) = notification.update
                        && let ContentBlock::Text(text) = chunk.content
                    {
                        heard.lock().unwrap().push_str(&text.text);
                    }
                    Ok(())
                }
            },
            agent_client_protocol::on_receive_notification!(),
        )
        .connect_with(
            ByteStreams::new(stdin.compat_write(), stdout.compat()),
            async |cx| {
                let init = cx
                    .send_request(InitializeRequest::new(ProtocolVersion::V1))
                    .block_task()
                    .await?;
                assert_eq!(
                    init.agent_info.map(|info| info.name),
                    Some("muse-acp".to_string())
                );
                let session = cx
                    .send_request(NewSessionRequest::new(&workspace))
                    .block_task()
                    .await?;
                let prompt =
                    PromptRequest::new(session.session_id, vec![ContentBlock::from("hello there")]);
                let answer = cx.send_request(prompt).block_task().await?;
                assert_eq!(answer.stop_reason, StopReason::EndTurn);
                Ok(())
            },
        );
    tokio::time::timeout(Duration::from_secs(30), conversation)
        .await
        .expect("the conversation finished in time")
        .expect("the conversation succeeded");
    assert_eq!(said.lock().unwrap().as_str(), "echo: hello there");
    let _ = std::fs::remove_dir_all(&workspace);
}
