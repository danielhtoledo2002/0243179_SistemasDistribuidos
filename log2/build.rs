// fn main() {
//     prost_build::compile_protos(&["src/log.proto"], &["src/"]).unwrap();
// }

fn main() {
    let proto_file = "src/log.proto";

    prost_build::Config::new()
        .out_dir("src/comp")
        .compile_protos(&[proto_file], &["proto/"])
        .expect("Failed to compile Protobuf files");
}
