#!/bin/bash

die() {
  echo "$@" 1>&2 ; popd 2>/dev/null; exit 1
}

build_install_zstd() {
  TEMP=`mktemp -d` || die "failed to create zstd tmp build dir"
  pushd $TEMP
  wget https://github.com/facebook/zstd/archive/v1.3.2.tar.gz
  tar xzvf v1.3.2.tar.gz || die "failed to uncompress zstd repo"
  cd zstd-1.3.2 && mkdir build && cd build
  sudo make install PREFIX='/usr' || die "zstd build install failed"
  popd && rm -rf $TEMP
}

build_install_blosc() {
  TEMP=`mktemp -d` || die "failed to create blosc tmp build dir"
  pushd $TEMP
  wget https://github.com/Blosc/c-blosc/archive/v1.12.1.tar.gz
  tar xvzf v1.12.1.tar.gz || die "failed to uncompress blosc repo"
  cd c-blosc-1.12.1 && mkdir build && cd build
  cmake -DCMAKE_INSTALL_PREFIX='/usr' .. || die "blosc cmake failed"
  sudo cmake --build . --target install || die "bloc build install failed"
  popd && rm -rf $TEMP
}

install_apt_pkgs() {
  sudo apt-get -y install g++ wget cmake \
    zlib1g-dev libbz2-dev liblz4-dev || die "could not install apt pkg dependencies"
}

install_yum_pkgs() {
  sudo yum -y install g++ wget cmake \
    zlib-devel bzip2-devel lz4-devel || die "could not install yum pkg dependencies"
}

install_brew_pkgs() {
  brew install wget cmake lzlib lz4 bzip2 zstd || die "could not install brew pkg dependencies"
}

install_deps() {
  if [ -n "$(command -v apt-get)" ]; then
    install_apt_pkgs 
    # zstd is in later versions of Ubuntu
    if [[ $(apt-cache search libzstd-dev) ]]; then
      sudo apt-get -y install libzstd-dev
    else
      build_install_zstd
    fi
    build_install_blosc
  elif [ -n "$(command -v yum)" ]; then
    install_yum_pkgs
    build_install_zstd
    build_install_blosc
  elif [ -n "$(command -v brew)" ]; then
    install_brew_pkgs
    build_install_blosc
  else
    die "unsupported package management system"
  fi 
}

run() {
  install_deps
}

run 
