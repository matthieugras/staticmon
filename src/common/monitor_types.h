#pragma once
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <cstdint>
#include <fmt/format.h>
#include <mp_helpers.h>
#include <string>
#include <tuple>
#include <utility>
#include <variant>
#include <vector>

using pred_id_t = std::size_t;
using ts_t = std::size_t;
using event_data = std::variant<std::int64_t, double, std::string>;
using event = std::vector<event_data>;
using database_elem = std::vector<event>;
using database = absl::flat_hash_map<pred_id_t, database_elem>;
using timestamped_database = std::pair<std::size_t, database>;
using ts_list = std::vector<ts_t>;
enum arg_types {
  INT_TYPE,
  FLOAT_TYPE,
  STRING_TYPE
};

// (predicate_name, arity) -> (predicate_id, predicate types)
using pred_map_t =
  absl::flat_hash_map<std::pair<std::string, size_t>,
                      std::pair<pred_id_t, std::vector<arg_types>>>;

template<typename T>
class monpoly_fmt {
  friend struct fmt::formatter<monpoly_fmt<T>>;

public:
  explicit monpoly_fmt(const T &t) : t(t){};

private:
  const T &t;
};

template<typename T>
struct fmt::formatter<monpoly_fmt<T>, char> {

  constexpr auto parse [[maybe_unused]] (format_parse_context &ctx)
  -> decltype(auto) {
    auto it = ctx.begin();
    if (it != ctx.end() && *it != '}')
      throw format_error("invalid format - only empty format strings are "
                         "accepted for database related types");
    return it;
  }

  template<typename FormatContext>
  auto format
    [[maybe_unused]] (const monpoly_fmt<T> &arg_wrapper, FormatContext &ctx)
    -> decltype(auto) {
    const auto &arg = arg_wrapper.t;
    if constexpr (std::is_same_v<T, event>) {
      return fmt::format_to(ctx.out(), "{}", fmt::join(arg, ","));
    } else if constexpr (std::is_same_v<T, database_elem>) {
      auto curr_end = ctx.out();
      for (auto it = arg.cbegin(); it != arg.cend();) {
        curr_end = fmt::format_to(curr_end, "{}", monpoly_fmt(*it));
        if ((++it) != arg.cend())
          curr_end = fmt::format_to(curr_end, ")(");
      }
      return curr_end;
    } else if constexpr (std::is_same_v<T, database>) {
      auto curr_end = ctx.out();
      for (auto it = arg.cbegin(); it != arg.cend();) {
        curr_end = fmt::format_to(curr_end, "{}({})", it->first,
                                  monpoly_fmt(it->second));
        if ((++it) != arg.cend())
          curr_end = fmt::format_to(curr_end, " ");
      }
      return curr_end;
    } else if constexpr (std::is_same_v<T, timestamped_database>) {
      return fmt::format_to(ctx.out(), "@{} {};", arg.first,
                            monpoly_fmt(arg.second));
    } else {
      static_assert(always_false_v<T>, "not exhaustive");
    }
  }
};
