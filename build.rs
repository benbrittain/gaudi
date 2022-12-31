fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure().build_server(true).compile(
        &[
            "proto/build/bazel/remote/execution/v2/remote_execution.proto",
            "proto/google/bytestream/bytestream.proto",
        ],
        &["proto"],
    )?;
    Ok(())
}
