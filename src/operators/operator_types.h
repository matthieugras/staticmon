#pragma once
#include <cstdint>
#include <string>
#include <absl/container/flat_hash_map.h>
#include <fmt/format.h>
#include <mp_helpers.h>

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
