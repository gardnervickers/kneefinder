use std::process::Command;

#[test]
fn coordinator_drives_two_independent_tcp_clients_end_to_end() {
    let output = Command::new(env!("CARGO_BIN_EXE_kneefinder-queue-demo"))
        .arg("e2e-tcp")
        .output()
        .expect("multi-client TCP E2E process should start");

    assert!(
        output.status.success(),
        "multi-client TCP E2E failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains(
            "multi-client TCP E2E passed: 2 clients, 40 operations each, 80 successful total"
        ),
        "multi-client success marker missing from stdout: {}",
        String::from_utf8_lossy(&output.stdout)
    );
}
