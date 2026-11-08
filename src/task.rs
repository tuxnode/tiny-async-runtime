use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, mpsc::Sender},
    task::{Context, Wake},
};

pub struct Task {
    pub(crate) future: Mutex<Pin<Box<dyn Future<Output = ()> + Send>>>,

    pub(crate) tx: Sender<Arc<Task>>,
}

impl Task {
    pub fn poll(self: Arc<Self>) {
        let waker = futures_util_waker(self.clone());
        let mut cx = Context::from_waker(&waker);
        let mut future = self.future.lock().unwrap();
        let _ = future.as_mut().poll(&mut cx);
    }
}

impl Wake for Task {
    fn wake(self: Arc<Self>) {}
}

pub fn futures_util_waker(task: Arc<Task>) -> std::task::Waker {
    std::task::Waker::from(task)
}
