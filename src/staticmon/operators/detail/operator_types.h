#pragma once
#include <absl/container/flat_hash_map.h>
#include <cstdint>
#include <fmt/format.h>
#include <staticmon/common/mp_helpers.h>
#include <staticmon/common/util.h>
#include <string>
#include <type_traits>
#include <variant>

using pred_id_t = std::size_t;
using ts_t = std::size_t;
using event_data = std::variant<std::int64_t, double, std::string>;
using event = std::vector<event_data>;
using database_table = std::vector<event>;
using database = absl::flat_hash_map<pred_id_t, std::vector<database_table>>;
using timestamped_database = std::pair<std::size_t, database>;
using ts_list = std::vector<ts_t>;
enum arg_types {
  INT_TYPE,
  FLOAT_TYPE,
  STRING_TYPE
};

// predicate_name -> (predicate_id, predicate types)
using pred_map_t =
  absl::flat_hash_map<std::string,
                      std::pair<pred_id_t, std::vector<arg_types>>>;

template<typename... Args>
struct fmt::formatter<std::variant<Args...>, char> : trivial_parser {
  template<typename FormatContext>
  auto format [[maybe_unused]] (const std::variant<Args...> &arg,
                                FormatContext &ctx) const -> decltype(auto) {
    auto visit_fn = [&ctx](auto &&arg) {
      return fmt::format_to(ctx.out(), "{}", std::forward<decltype(arg)>(arg));
    };
    return std::visit(visit_fn, arg);
  }
};
