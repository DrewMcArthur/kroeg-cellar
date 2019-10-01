use crate::CellarEntityStore;
use kroeg_tap::{QueueItem, QueueStore, StoreError};

#[async_trait::async_trait]
impl<'a> QueueStore for CellarEntityStore<'a> {
    async fn get_item(&mut self) -> Result<Option<QueueItem>, StoreError> {
        let item = self.pop_queue().await?;

        Ok(item.map(|(event, data)| QueueItem { id: 0, event, data }))
    }

    async fn mark_success(&mut self, _: QueueItem) -> Result<(), StoreError> {
        Ok(())
    }

    async fn mark_failure(&mut self, item: QueueItem) -> Result<(), StoreError> {
        let QueueItem { event, data, .. } = item;
        self.push_queue(event, data).await
    }

    async fn add(&mut self, event: String, data: String) -> Result<(), StoreError> {
        self.push_queue(event, data).await
    }
}
