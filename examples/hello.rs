use std::time::{Duration, Instant};

use tiny_async_runtime::{Executor, sleep};

fn main() {
    let (executor, spawner) = Executor::new();
    let start = Instant::now();

    // 任务 1：每秒打印一次，共 3 次。
    // 同一个任务会被反复唤醒、反复 poll，直到循环结束。
    spawner.spawn(async move {
        for i in 1..=3 {
            sleep(Duration::from_secs(1)).await;
            println!("[{:>6.3}s] ticker: tick {i}", elapsed(start));
        }
        println!("[{:>6.3}s] ticker: done", elapsed(start));
    });

    // 任务 2：等待 2 秒。
    // 它和 ticker 是并发等待的，2 秒时就会结束，
    // 而不是等 ticker 的 1 秒结束后再开始等 2 秒。
    spawner.spawn(async move {
        println!("[{:>6.3}s] slow: waiting for 2s", elapsed(start));
        sleep(Duration::from_secs(2)).await;
        println!("[{:>6.3}s] slow: done", elapsed(start));
    });

    // 任务 3：运行过程中再创建一个子任务。
    // 这要求 Spawner 实现 Clone，并且 executor 会一直运行到所有任务结束。
    let child_spawner = spawner.clone();
    spawner.spawn(async move {
        sleep(Duration::from_millis(500)).await;
        println!("[{:>6.3}s] parent: spawning child", elapsed(start));

        child_spawner.spawn(async move {
            sleep(Duration::from_millis(700)).await;
            println!("[{:>6.3}s] child: done", elapsed(start));
        });
    });

    // 主线程不再创建任务。所有 Sender 都释放后，run() 会在任务队列清空时返回。
    drop(spawner);

    println!("[{:>6.3}s] runtime: start", elapsed(start));
    executor.run();
    println!("[{:>6.3}s] runtime: all tasks finished", elapsed(start));
}

fn elapsed(start: Instant) -> f64 {
    start.elapsed().as_secs_f64()
}
