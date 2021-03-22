fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_client(false)
        .build_server(true)
        .compile(&["proto/public_key.proto", "proto/object.proto", "proto/dime.proto"], &["proto"])?;

    // tonic_build::configure()
    //     .build_client(false)
    //     .build_server(true)
    //     .compile(&["proto/object.proto"], &["proto"])?;

    // tonic_build::compile_protos("proto/util.proto")?;
    //tonic_build::compile_protos("proto/public_key.proto")?;
    //tonic_build::compile_protos("proto/object.proto")?;

    Ok(())
}
