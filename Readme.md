# Feed splitter in rust

1. Build project:
	```
	docker build .
	docker run -v /Users/robinbortlik/reevoo/feed_splitter-rust:/app -it 16c7f43bccab sh
	source $HOME/.cargo/env
	cargo +nightly build
	```
2. Run:
	```
	./target/release/feed_splitter-rust  path_to_csv_file
	```

To see the log output run with:
	```
	RUST_LOG=info feed_splitter-rust
	```
