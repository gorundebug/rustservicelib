use super::RuntimeEnvironment;
use crate::runtime::config::CallSemantics;
use std::{process::Command, time::Duration};

const CHILD_ENV: &str = "SERVICELIB_PARALLEL_PANIC_CHILD";
const ENTERED: &str = "parallel-panic-callback-entered";
const PANIC: &str = "parallel-panic-original-payload";
const SURVIVED: &str = "parallel-panic-process-survived";

#[tokio::test]
async fn parallel_callback_panic_terminates_process() {
    if std::env::var_os(CHILD_ENV).is_some() {
        std::thread::spawn(|| {
            std::thread::sleep(Duration::from_secs(5));
            std::process::exit(99);
        });
        let environment = RuntimeEnvironment::new(CallSemantics::ParallelCall);
        environment.spawn_parallel(async {
            eprintln!("{ENTERED}");
            panic!("{PANIC}");
        });
        environment.drain_parallel().await;
        eprintln!("{SURVIVED}");
        return;
    }

    let output = Command::new(std::env::current_exe().expect("test executable"))
        .args([
            "--exact",
            "runtime::environment::parallel_panic_contract_tests::parallel_callback_panic_terminates_process",
            "--nocapture",
        ])
        .env(CHILD_ENV, "1")
        .output()
        .expect("run panic probe in a separate process");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains(ENTERED), "callback not entered: {stderr}");
    assert!(stderr.contains(PANIC), "original panic missing: {stderr}");
    assert_eq!(
        output.status.code(),
        Some(2),
        "unhandled ParallelCall panic must terminate the process as in Go; stderr: {stderr}"
    );
    assert!(!stderr.contains(SURVIVED), "process continued after panic");
}
