use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::Sender,
    },
    task::{Context, Wake, Waker},
};

pub struct Task {
    pub(crate) future: Mutex<Option<Pin<Box<dyn Future<Output = ()> + Send>>>>,

    pub(crate) tx: Sender<Arc<Task>>,

    /// Whether the task is already waiting in the executor's ready queue.
    scheduled: AtomicBool,

    /// Whether the future has completed and must never be polled again.
    completed: AtomicBool,
}

impl Task {
    pub(crate) fn new(
        future: impl Future<Output = ()> + Send + 'static,
        tx: Sender<Arc<Task>>,
    ) -> Self {
        Self {
            future: Mutex::new(Some(Box::pin(future))),
            tx,
            scheduled: AtomicBool::new(true),
            completed: AtomicBool::new(false),
        }
    }

    pub fn poll(self: Arc<Self>) {
        // The task is running now, so any wake from this point on must put it
        // back into the ready queue.
        self.scheduled.store(false, Ordering::Release);

        if self.completed.load(Ordering::Acquire) {
            return;
        }

        let waker = Waker::from(self.clone());
        let mut cx = Context::from_waker(&waker);
        let mut slot = self.future.lock().unwrap();

        // The future is taken out once it completes, so duplicate wake-ups can
        // never poll a completed future again.
        let Some(future) = slot.as_mut() else {
            return;
        };

        if future.as_mut().poll(&mut cx).is_ready() {
            self.completed.store(true, Ordering::Release);
            *slot = None;
        }
    }
}

impl Wake for Task {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if self.completed.load(Ordering::Acquire) {
            return;
        }

        // Only enqueue the task if it is not already waiting in the queue.
        if !self.scheduled.swap(true, Ordering::AcqRel) && self.tx.send(self.clone()).is_err() {
            // The executor is gone; allow a future wake attempt to try again.
            self.scheduled.store(false, Ordering::Release);
        }
    }
}
