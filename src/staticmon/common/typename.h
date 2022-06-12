#pragma once
#include <boost/core/demangle.hpp>
#include <boost/mp11.hpp>
#include <cstdlib>
#include <cxxabi.h>
#include <fmt/format.h>
#include <memory>
#include <string>
#include <tuple>
#include <type_traits>
#include <typeinfo>

using namespace boost::mp11;

inline std::string demangle(const char *name) {
  int status = -4;
  std::unique_ptr<char, void (*)(void *)> res{
    abi::__cxa_demangle(name, NULL, NULL, &status), std::free};

  return (status == 0) ? res.get() : name;
}

template<typename T>
struct non_default_print : std::false_type {};

template<typename T, typename = void>
struct TypePrinter;

template<typename T>
struct TypePrinter<T, std::enable_if_t<!non_default_print<T>::value>> {
  static std::string print_type() {
    std::string demangled(demangle(typeid(T).name()));
    return fmt::format("{}", demangled);
  }
};

template<template<typename...> typename L, typename... T>
struct TypePrinter<L<T...>> {
  static std::string print_type() {
    auto formatted_elems = std::make_tuple(TypePrinter<T>::print_type()...);
    return fmt::format("{{unknown type: [{}]}}",
                       fmt::join(formatted_elems, ", "));
  }
};
template<template<typename...> typename L, typename... T>
struct non_default_print<L<T...>> : std::true_type {};

template<typename... T>
struct TypePrinter<mp_list<T...>> {
  static std::string print_type() {
    auto formatted_elems = std::make_tuple(TypePrinter<T>::print_type()...);
    return fmt::format("[{}]", fmt::join(formatted_elems, ", "));
  }
};
template<typename... T>
struct non_default_print<mp_list<T...>> : std::true_type {};

template<typename... T>
struct TypePrinter<std::tuple<T...>> {
  static std::string print_type() {
    auto formatted_elems = std::make_tuple(TypePrinter<T>::print_type()...);
    return fmt::format("({})", fmt::join(formatted_elems, ", "));
  }
};
template<typename... T>
struct non_default_print<std::tuple<T...>> : std::true_type {};

template<typename T, T v>
struct TypePrinter<std::integral_constant<T, v>> {
  static std::string print_type() { return fmt::format("{}", v); }
};
template<typename T, T v>
struct non_default_print<std::integral_constant<T, v>> : std::true_type {};

template<>
struct TypePrinter<void> {
  static std::string print_type() { return "∅"; }
};
