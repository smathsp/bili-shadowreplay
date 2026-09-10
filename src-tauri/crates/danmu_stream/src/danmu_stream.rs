use std::sync::Arc;

use tokio::sync::{mpsc, RwLock};

use crate::{
    provider::{new, DanmuProvider, ProviderType},
    DanmuMessageType, DanmuStreamError,
};

#[derive(Clone)]
pub struct DanmuStream {
    pub provider_type: ProviderType,
    pub identifier: String,
    pub room_id: String,
    pub provider: Arc<RwLock<Box<dyn DanmuProvider>>>,
    tx: mpsc::Sender<DanmuMessageType>,
    rx: Arc<RwLock<mpsc::Receiver<DanmuMessageType>>>,
}

impl DanmuStream {
    pub async fn new(
        provider_type: ProviderType,
        identifier: &str,
        room_id: &str,
    ) -> Result<Self, DanmuStreamError> {
        // Bound provider-to-recorder buffering so a slow Docker bind mount or
        // temporary disk stall cannot grow memory without limit. Providers
        // await capacity, preserving complete events instead of dropping them.
        let (tx, rx) = mpsc::channel(4096);
        let provider = new(provider_type, identifier, room_id).await?;
        Ok(Self {
            provider_type,
            identifier: identifier.to_string(),
            room_id: room_id.to_string(),
            provider: Arc::new(RwLock::new(provider)),
            tx,
            rx: Arc::new(RwLock::new(rx)),
        })
    }

    pub async fn start(&self) -> Result<(), DanmuStreamError> {
        // Provider methods take &self, so a shared guard lets `stop` signal a
        // long-running connection instead of waiting forever on this lock.
        self.provider.read().await.start(self.tx.clone()).await
    }

    pub async fn stop(&self) -> Result<(), DanmuStreamError> {
        self.provider.read().await.stop().await
    }

    /// Stop accepting provider messages while keeping already queued events
    /// available to `recv`, so recorders can drain them before closing storage.
    pub async fn close_receiver(&self) {
        self.rx.write().await.close();
    }

    pub async fn recv(&self) -> Result<Option<DanmuMessageType>, DanmuStreamError> {
        Ok(self.rx.write().await.recv().await)
    }
}
