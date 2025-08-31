use std::sync::{Arc, Mutex};

use e4pty::prelude::PtyCtl;

pub struct PtyCtlImpl {
    pub exit_status: Arc<Mutex<Option<i32>>>,
}

#[cfg_attr(feature = "async-trait", async_trait::async_trait)]
impl PtyCtl for PtyCtlImpl {
    async fn wait(&mut self) -> e4pty::Result<i32> {
        loop {
            if let Some(exit_status) = self.exit_status.lock().unwrap().take() {
                return Ok(exit_status);
            }
            tokio::task::yield_now().await;
        }
    }
}
