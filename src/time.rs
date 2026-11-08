use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    thread,
    time::Duration,
};

pub async fn sleep(duration: Duration) {
    TimeFuture::new(duration).await;
}

struct TimeFuture {
    shared_state: Arc<Mutex<SharedState>>,
}

struct SharedState {
    completed: bool,
    waker: Option<std::task::Waker>,
}

impl TimeFuture {
    fn new(duration: Duration) -> Self {
        let shared_state = Arc::new(Mutex::new(SharedState {
            completed: false,
            waker: None,
        }));
        let thread_shared_state = shared_state.clone();
        thread::spawn(move || {
            thread::sleep(duration);

            // Mark the timer as completed and take the waker under the same lock,
            // so a poll can never observe "not completed" with the waker lost.
            let waker = {
                let mut state = thread_shared_state.lock().unwrap();
                state.completed = true;
                state.waker.take()
            };

            // Wake without holding the lock: wake() may run arbitrary code.
            if let Some(waker) = waker {
                waker.wake();
            }
        });
        TimeFuture { shared_state }
    }
}

impl Future for TimeFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.shared_state.lock().unwrap();
        if state.completed {
            Poll::Ready(())
        } else {
            state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}
