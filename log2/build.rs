fn main() {
    prost_build::compile_protos(&["src/log.proto"], &["src/"]).unwrap();
}
