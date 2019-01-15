use crate::CellarEntityStore;
use futures::{future, Future};
use kroeg_tap::{QueueItem, QueueStore};
use tokio_postgres::error::Error;

pub struct CellarQueueItem {
    pub event: String,
    pub data: String,
}

impl QueueItem for CellarQueueItem {
    fn event(&self) -> &str {
        &self.event
    }

    fn data(&self) -> &str {
        &self.data
    }
}

impl QueueStore for CellarEntityStore {
    type Item = CellarQueueItem;
    type Error = Error;

    type GetItemFuture = Box<
        Future<Item = (Option<Self::Item>, Self), Error = (Self::Error, Self)> + Send + 'static,
    >;

    fn get_item(self) -> Self::GetItemFuture {
        Box::new(self.pop_queue().map(|(a, store)| {
            (
                a.map(|(event, data)| CellarQueueItem { event, data }),
                store,
            )
        }))
    }

    type MarkFuture = Box<Future<Item = Self, Error = (Self::Error, Self)> + Send + 'static>;

    fn mark_success(self, item: Self::Item) -> Self::MarkFuture {
        Box::new(future::ok(self))
    }
    fn mark_failure(self, item: Self::Item) -> Self::MarkFuture {
        Box::new(self.push_queue(item.event, item.data))
    }

    fn add(self, event: String, data: String) -> Self::MarkFuture {
        Box::new(self.push_queue(event, data))
    }
}
