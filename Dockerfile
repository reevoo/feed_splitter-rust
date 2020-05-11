FROM ruby:2.3.7-alpine3.7

RUN apk add --update \
    curl \
    libc6-compat \
    libgcc \
    gcc \
    cmake

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
RUN source $HOME/.cargo/env && rustup toolchain install nightly



WORKDIR /app

COPY . .
