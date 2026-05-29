#!/usr/bin/env bash
set -euo pipefail

case "$(uname -s)" in
  Linux)
    sudo apt-get update
    sudo apt-get install -y \
      autoconf \
      bison \
      build-essential \
      ca-certificates \
      ccache \
      cmake \
      flex \
      g++-14 \
      gcc-14 \
      git \
      libicu-dev \
      liblz4-dev \
      libpam0g-dev \
      libperl-dev \
      libpq-dev \
      libreadline-dev \
      libssl-dev \
      libxml2-dev \
      libxslt1-dev \
      libzstd-dev \
      ninja-build \
      pkg-config \
      python3 \
      python3-pip \
      python3-venv \
      uuid-dev \
      zlib1g-dev

    sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-14 100
    sudo update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-14 100
    sudo update-alternatives --install /usr/bin/cc cc /usr/bin/gcc-14 100
    sudo update-alternatives --install /usr/bin/c++ c++ /usr/bin/g++-14 100
    ;;
  Darwin)
    brew update
    brew install \
      autoconf \
      bison \
      ccache \
      cmake \
      flex \
      icu4c \
      ninja \
      openssl@3 \
      pkg-config \
      readline \
      zlib
    ;;
  *)
    echo "unsupported CI OS: $(uname -s)" >&2
    exit 1
    ;;
esac
