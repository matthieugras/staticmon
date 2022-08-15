# staticmon

Similar to [Cppmon](https://github.com/matthieugras/cppmon), but compiles an optimized monitor for each formula.
Formula files have to be preprocessed with a [modified Monpoly version](https://github.com/matthieugras/monpoly), producing two header files. The generated header files contain information about the formula and are used to produce an optimized monitor using C++ template metaprogramming.

## Requirements
  - Static version of libstdc++
  - GCC >= 11 or Clang >= 13
  - Conan package manager
  - CMake >= 3.22
  - Ninja build tool

## Compilation steps
1. Move to root of repository
2. `./setup.sh` and select compiler and build mode
3. `./configure.sh`
5. `monpoly -sig bla.sig -formula bla.mfotl -explicitmon -explicitmon_prefix=./src/staticmon/input_formula`
6. `ninja -C builddir`
7. Resulting in binary `./builddir/bin/staticmon`
8. To run the monitor: `./builddir/bin/staticmon --log bla.log`

## Notes
- Has some easy to fix performance regressions
- Debug builds can be several orders of magnitude slower than release builds
- GCC seems to generate better code
- The compilation step with Ninja can produce very long compiler warnings
- Did not test the monitor with string event values; may be super slow if forgot to move strings in some places
- `LetPast` not supported in the master branch of the modified monpoly code (still need to handle cases like `LETPAST P(x) = PREV P(x) IN P(x)`; where `x` is polymorphic)
