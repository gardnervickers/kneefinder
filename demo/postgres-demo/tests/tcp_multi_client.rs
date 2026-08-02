use std::process::Command;

#[test]
#[ignore = "requires PostgreSQL; run with KNEEFINDER_POSTGRES_URL set"]
fn coordinator_drives_two_independent_tcp_clients_end_to_end() {
    let output = Command::new(env!("CARGO_BIN_EXE_kneefinder-postgres-demo"))
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
            "multi-client TCP E2E passed: 2 clients, 50 operations each, 100 successful total"
        ),
        "multi-client success marker missing from stdout: {}",
        String::from_utf8_lossy(&output.stdout)
    );
}
