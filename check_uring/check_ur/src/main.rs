use std::time::Duration;

fn main() {
    println!("PID = {}", std::process::id());

    tokio_uring::start(async {
        println!("inside tokio-uring");

        // Keep the runtime alive without Tokio timers
        loop {
            std::thread::sleep(Duration::from_secs(1));
        }
    });
}
