//! Process supervision for `main`: run the HTTP server and the scheduler
//! task side by side, and notice if the scheduler dies on its own.
//!
//! Why: if the scheduler task panicked or returned while the HTTP server kept
//! running, the process would look healthy (`/healthz` says ok) but the guide
//! would silently stop being refreshed. Exiting non-zero instead lets the
//! supervisor (Kubernetes, systemd, Docker) restart the process. All the
//! request limits are persisted, so a restart costs no extra requests.

use std::future::Future;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// How the process should end.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Exit {
    /// Normal shutdown: exit 0.
    Clean,
    /// Something died or failed: log this and exit non-zero.
    Failed(String),
}

/// Wait for `server` and the `scheduler` task. `shutdown` is the token both
/// obey (the server stops serving when it is cancelled, the scheduler stops
/// after its in-flight window).
///
/// * Normal shutdown (token cancelled, e.g. by SIGTERM): both finish, and the
///   scheduler is awaited, so its in-flight merge completes. `Exit::Clean`.
/// * The scheduler ends before shutdown (panic or return): the token is
///   cancelled so the server stops too, and `Exit::Failed` says why.
/// * The server ends or fails first: the scheduler is stopped and awaited.
pub async fn supervise<S, E>(
    scheduler: JoinHandle<()>,
    server: S,
    shutdown: CancellationToken,
) -> Exit
where
    S: Future<Output = Result<(), E>>,
    E: std::fmt::Display,
{
    tokio::pin!(server);
    let mut scheduler = scheduler;
    tokio::select! {
        joined = &mut scheduler => {
            // Read before cancelling: a scheduler that stopped because shutdown
            // was requested is fine; one that stopped by itself is not.
            let expected = shutdown.is_cancelled();
            shutdown.cancel();
            let served = server.await;
            match (expected, joined) {
                (_, Err(e)) => Exit::Failed(format!("scheduler task failed: {}", join_error_text(e))),
                (false, Ok(())) => Exit::Failed("scheduler task stopped unexpectedly".into()),
                (true, Ok(())) => match served {
                    Ok(()) => Exit::Clean,
                    Err(e) => Exit::Failed(format!("server error: {e}")),
                },
            }
        }
        served = &mut server => {
            shutdown.cancel();
            // Let the scheduler finish its in-flight window.
            let joined = scheduler.await;
            match (served, joined) {
                (_, Err(e)) => Exit::Failed(format!("scheduler task failed: {}", join_error_text(e))),
                (Err(e), Ok(())) => Exit::Failed(format!("server error: {e}")),
                (Ok(()), Ok(())) => Exit::Clean,
            }
        }
    }
}

/// A join error as text, with the panic message when there is one.
fn join_error_text(e: tokio::task::JoinError) -> String {
    if e.is_panic() {
        let payload = e.into_panic();
        let msg = payload
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| payload.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "unknown panic".into());
        format!("panicked: {msg}")
    } else {
        e.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stand-in for the HTTP server: serves until the token is cancelled.
    async fn server(token: CancellationToken) -> Result<(), String> {
        token.cancelled().await;
        Ok(())
    }

    #[tokio::test]
    async fn panicking_scheduler_stops_the_server_and_fails() {
        let token = CancellationToken::new();
        let sched = tokio::spawn(async { panic!("boom in tick") });
        let exit = supervise(sched, server(token.clone()), token.clone()).await;
        assert_eq!(
            exit,
            Exit::Failed("scheduler task failed: panicked: boom in tick".into())
        );
        assert!(token.is_cancelled(), "the server was told to stop");
    }

    #[tokio::test]
    async fn scheduler_returning_before_shutdown_fails() {
        let token = CancellationToken::new();
        let sched = tokio::spawn(async {});
        let exit = supervise(sched, server(token.clone()), token.clone()).await;
        assert_eq!(
            exit,
            Exit::Failed("scheduler task stopped unexpectedly".into())
        );
        assert!(token.is_cancelled());
    }

    #[tokio::test]
    async fn normal_shutdown_waits_for_the_scheduler_and_is_clean() {
        let token = CancellationToken::new();
        let finished = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (t, f) = (token.clone(), finished.clone());
        // Like the real scheduler: stops on cancel, after finishing its work.
        let sched = tokio::spawn(async move {
            t.cancelled().await;
            tokio::task::yield_now().await;
            f.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        let canceller = token.clone();
        tokio::spawn(async move { canceller.cancel() });
        let exit = supervise(sched, server(token.clone()), token).await;
        assert_eq!(exit, Exit::Clean);
        assert!(finished.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn server_error_stops_the_scheduler_and_fails() {
        let token = CancellationToken::new();
        let t = token.clone();
        let sched = tokio::spawn(async move { t.cancelled().await });
        let exit = supervise(sched, async { Err::<(), _>("bind lost") }, token).await;
        assert_eq!(exit, Exit::Failed("server error: bind lost".into()));
    }
}
