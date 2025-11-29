fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var_os("GENERATE_GRPC").is_some() {
        tonic_build::configure()
            .build_client(true)
            .build_server(true)
            .compile_well_known_types(true)
            .extern_path(".google.protobuf.Timestamp", "::prost_types::Timestamp")
            .out_dir("src/grpc/generated")
            .compile_protos(&["proto/grpc.proto"], &["proto"])?;
    }

    Ok(())
}
