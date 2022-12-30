fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(false)
        .compile(
            &["proto/remote_execution.proto"],
            &["proto"],
        )?;
    Ok(())
}
