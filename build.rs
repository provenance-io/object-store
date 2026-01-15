fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(
            &[
                "proto/admin.proto",
                "proto/dime.proto",
                "proto/mailbox.proto",
                "proto/object.proto",
                "proto/public_key.proto",
            ],
            &["proto"],
        )?;

    Ok(())
}
