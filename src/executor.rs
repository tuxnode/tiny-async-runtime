use std::sync::{
    Arc,
    mpsc::{Receiver, Sender, channel},
};

use crate::task::Task;

pub struct Executor {
    // The executor intentionally does not keep a Sender: once every Spawner
    // (and every live Task) is dropped, recv() returns Err and run() exits.
    ready_queue_rx: Receiver<Arc<Task>>,
}

#[derive(Clone)]
pub struct Spawner {
    tx: Sender<Arc<Task>>,
}

impl Executor {
    pub fn new() -> (Self, Spawner) {
        let (tx, rx) = channel();
        (Executor { ready_queue_rx: rx }, Spawner { tx })
    }

    /// Runs tasks until all `Spawner` handles and tasks have been dropped.
    ///
    /// In particular, keep a `Spawner` alive if you plan to spawn more work
    /// later; drop it when no more tasks can be created and you want `run` to
    /// return once the queue is empty.
    pub fn run(&self) {
        while let Ok(task) = self.ready_queue_rx.recv() {
            task.poll();
        }
    }
}

impl Spawner {
    pub fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let task = Arc::new(Task::new(future, self.tx.clone()));
        let _ = self.tx.send(task);
    }
}
