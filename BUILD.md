

## GNU eabihf cross compilation (on raspian manner)

This build needs a root or the target image, inside the rscross docker

	# build the image
	cd docker
	docker build -t rscross .
	cd ..

	# cross compile
	docker run -ti -v `pwd`:/code rscross cargo build


	docker run --rm -ti -v `pwd`:/code rscross bash -c "cargo build -target=armv7-unknown-linux-gnueabihf"

	docker run --rm -ti -v `pwd`:/code rscross bash -c "cargo build -target=x86_64-unknown-linux-gnu"



## x64 build with older glibc release

using a docker with old rust compiled 

	construct image from docker_old_glibc, with name buster-with-build:latest 

	docker run --rm --user "$(id -u)":"$(id -g)" -v "$PWD":/usr/src/myapp -w /usr/src/myapp buster-with-build:latest cargo build --release


### MUSL COMPILE EVALUATION 


	target for old glibc

	info: downloading component 'rust-std' for 'armv7-unknown-linux-musleabihf'

	install musl-dev musl-tools packages

	docker run --rm -ti -v `pwd`:/code rscross bash -c "CC=/usr/bin/musl-gcc cargo build --target=armv7-unknown-linux-musleabihf"


	CC=/usr/bin/musl-gcc cargo build --target=armv7-unknown-linux-musleabihf


## profiling

record the function timing , using perf

	> perf record -F 99 -g <some-command-here>

it create the perf.data file, with all symbols
analyse it with the hotspot GUI (available in the distribution)
