#pragma once
#include <fmt/format.h>
#include <optional>

struct trivial_parser {
  constexpr auto parse(fmt::format_parse_context &ctx) const -> decltype(auto) {
    auto it = ctx.begin();
    if (it != ctx.end() && *it != '}')
      throw fmt::format_error("invalid format - only empty format strings are "
                              "accepted for database related types");
    return it;
  }
};

namespace fmt {
template<typename T>
struct formatter<std::optional<T>> : trivial_parser {

  template<typename FormatContext>
  auto format
    [[maybe_unused]] (const std::optional<T> &arg, FormatContext &ctx) const
    -> decltype(auto) {
    if (arg) {
      return fmt::format_to(ctx.out(), "{}", *arg);
    } else {
      return fmt::format_to(ctx.out(), "((empty))");
    }
  }
};
}// namespace fmt
