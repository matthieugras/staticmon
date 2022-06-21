#!/bin/bash
PROJECT_MODE=Debug
PROFILE_MODE=Debug
read -p "Compiler (G)CC or (C)lang? " choice
case "$choice" in 
  g|G ) 
    CC=gcc
    CXX=g++ ;;
  c|C ) 
    CC=clang
    CXX=clang++ ;;
  * ) 
    echo "invalid input"
    exit 1 ;;
esac

read -p "Compiler version (Major)? " COMPILER_VERSION

read -p "Build in release mode (y/n)? " choice
case "$choice" in 
  y|Y ) 
    PROFILE_MODE=Release
    PROJECT_MODE=RelWithDebInfo
    if [ "$CC" == "clang" ]; then
      CFLAGS="-flto=full -march=native -g"
      CXXFLAGS="-flto=full -march=native -g"
    else
      CFLAGS="-flto -march=native -g"
      CXXFLAGS="-flto -march=native -g"
    fi ;;
  n|N ) 
    CFLAGS=""
    CXXFLAGS="-D_GLIBCXX_DEBUG" ;;
  * ) 
    echo "invalid input"
    exit 1 ;;
esac


if [ "$PROFILE_MODE" == "Release" ]; then
cat > configure.sh <<- EOM
#!/bin/bash
rm -r builddir
conan install . -if builddir --build=missing -pr=profile
CXX=${CXX} CC=${CC} cmake -G Ninja -DCMAKE_INSTALL_PREFIX="${PWD}/install" -DUSE_JEMALLOC=ON -DCMAKE_BUILD_TYPE=${PROJECT_MODE} -S . -B builddir
EOM
else
cat > configure.sh <<- EOM
#!/bin/bash
rm -r builddir
conan install . -if builddir --build=missing -pr profile
CXX=${CXX} CC=${CC} cmake -G Ninja -DCMAKE_INSTALL_PREFIX="$PWD/install" -DCMAKE_BUILD_TYPE=${PROJECT_MODE} -DENABLE_SANITIZERS=ON -S . -B builddir
EOM
fi

cat > profile <<- EOM
[settings]
os=Linux
os_build=Linux
arch=x86_64
arch_build=x86_64
compiler=${CC}
compiler.libcxx=libstdc++11
compiler.cppstd=20
compiler.version=${COMPILER_VERSION}
build_type=${PROFILE_MODE}
[options]
[build_requires]
[env]
CC=${CC}
CXX=${CXX}
CFLAGS="${CFLAGS}"
CXXFLAGS="${CXXFLAGS}"
EOM


chmod +x configure.sh
echo "Run ./configure to setup the project, ninja -C builddir to build it"
