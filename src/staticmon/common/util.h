#pragma once
#include <fmt/format.h>

struct trivial_parser {
  constexpr auto parse(fmt::format_parse_context &ctx) const -> decltype(auto) {
    auto it = ctx.begin();
    if (it != ctx.end() && *it != '}')
      throw fmt::format_error("invalid format - only empty format strings are "
                              "accepted for database related types");
    return it;
  }
};
