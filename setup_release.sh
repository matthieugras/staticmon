#!/bin/bash
conan install . -if builddir --build -pr=profile_release
CC=clang CXX=clang++ cmake -G Ninja -DCMAKE_INSTALL_PREFIX="$PWD/install" -DUSE_JEMALLOC=ON -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_CXX_FLAGS="-flto=full -march=native -fuse-ld=lld" -DCMAKE_C_FLAGS="-flto=full -march=native -fuse-ld=lld" -S . -B builddir
