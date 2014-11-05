# Feed splitter in rust

1. Install rust:
	```
	curl -s https://static.rust-lang.org/rustup.sh | sudo sh
	```
2. Build project:
	```
	cargo build --release
	```
3. Run:
	```
	./target/release/feed_splitter-rust  path_to_csv_file
	```

To see the log output run with:
	```
	RUST_LOG=info feed_splitter-rust
	```
