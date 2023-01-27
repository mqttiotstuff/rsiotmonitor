

## cross compilation

	 docker run -ti -v `pwd`:/code rscross cargo build


running simple publish 

	RUST_LOG=debug ./rsiotmonitor --client-id myclient --url mqtt://mqtt.frett27.net:1883 --username sys --password ufdx80wu  publish --qos 1 hello world



target for old glibc

info: downloading component 'rust-std' for 'armv7-unknown-linux-musleabihf'


X64 with static glibc

	RUSTFLAGS="-C target-feature=+crt-static" cargo build --target x86_64-unknown-linux-gnu


Profiling

	cargo run --features="profile-with-tracy" 


GNU EABI COMPILE

	docker run --rm -ti -v `pwd`:/code rscross bash -c "cargo build --target=armv7-unknown-linux-gnueabihf"

MUSL COMPILE

	install musl-dev musl-tools packages

	docker run --rm -ti -v `pwd`:/code rscross bash -c "CC=/usr/bin/musl-gcc cargo build --target=armv7-unknown-linux-musleabihf"


	CC=/usr/bin/musl-gcc cargo build --target=armv7-unknown-linux-musleabihf


