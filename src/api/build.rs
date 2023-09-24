fn main() {
    tonic_build::configure()
        .compile(&["proto/shm.proto"], &["."])
        .expect("compile proto");
}
