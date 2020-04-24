extern crate protobuf_codegen_pure;

fn main() {
    protobuf_codegen_pure::Codegen::new()
        .out_dir("src/protocol")
        .inputs(&[
            "protocol/resolver.proto",
            "protocol/publisher.proto",
            "protocol/shared.proto",
        ])
        .customize(protobuf_codegen_pure::Customize {
            carllerche_bytes_for_bytes: Some(true),
            carllerche_bytes_for_string: Some(true),
            expose_oneof: Some(true),
            expose_fields: Some(true),
            ..Default::default()
        })
        .include("protocol")
        .run()
        .expect("Codegen failed.");
}
