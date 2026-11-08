use std::sync::{
    Arc, Mutex,
    mpsc::{Receiver, Sender, channel},
};

use crate::task::Task;

pub struct Executor {
    ready_queue_tx: Sender<Arc<Task>>,
    ready_queue_rx: Receiver<Arc<Task>>,
}

#[derive(Clone)]
pub struct Spawner {
    tx: Sender<Arc<Task>>,
}

impl Executor {
    pub fn new() -> (Self, Spawner) {
        let (tx, rx) = channel();
        (
            Executor {
                ready_queue_rx: rx,
                ready_queue_tx: tx.clone(),
            },
            Spawner { tx },
        )
    }

    pub fn run(&self) {
        while let Ok(task) = self.ready_queue_rx.recv() {
            task.poll();
        }
    }
}

impl Spawner {
    pub fn spawner<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let task = Arc::new(Task {
            future: Mutex::new(Box::pin(future)),
            tx: self.tx.clone(),
        });
        let _ = self.tx.send(task);
    }
}
