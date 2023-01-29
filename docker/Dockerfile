FROM rustembedded/cross:armv7-unknown-linux-gnueabihf

COPY rustup.sh /usr/local/bin/rustup.sh
COPY cargo-config /cargo/config

ENV CARGO_HOME=/cargo \
    PATH=/cargo/bin:$PATH \
    USER=root

RUN bash /usr/local/bin/rustup.sh -y && \
    rustup default stable && \
    rustup target add armv7-unknown-linux-gnueabihf && \
    rustup target add armv7-unknown-linux-musleabihf && \
    rustup target add x86_64-unknown-linux-gnu

RUN cargo install cross

# Install multistrap
RUN apt-get update && apt-get install multistrap --assume-yes
# Put the target system here in /sysroot-armhf
RUN mkdir /sysroot-armhf
# We'll explain this in just a second
COPY multistrap-config /
WORKDIR /
RUN multistrap -a armhf -f multistrap-config 

ENV PKG_CONFIG_LIBDIR_armv7_unknown_linux_gnueabihf=/sysroot-armhf/usr/lib/arm-linux-gnueabihf/pkgconfig
ENV PKG_CONFIG_SYSROOT_DIR_armv7_unknown_linux_gnueabihf=/sysroot-armhf

RUN mkdir -p /code
WORKDIR /code


