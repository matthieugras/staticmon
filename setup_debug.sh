#!/bin/bash
conan install . -if builddir_debug --build -pr profile_debug
CXX=clang++ CC=clang cmake -G Ninja -DCMAKE_INSTALL_PREFIX="$PWD/install" -DCMAKE_BUILD_TYPE=Debug -DENABLE_SANITIZERS=ON -DCMAKE_CXX_FLAGS="-fuse-ld=lld -D_GLIBCXX_DEBUG" -DCMAKE_C_FLAGS="-fuse-ld=lld" -S . -B builddir_debug
