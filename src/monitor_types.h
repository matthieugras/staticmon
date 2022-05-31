#pragma once
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <cstdint>
#include <string>
#include <tuple>
#include <variant>
#include <vector>


using pred_id_t = std::size_t;
using ts_t = std::size_t;
using event_data = std::variant<std::int64_t, double, std::string>;
using event = std::vector<event_data>;
using database = absl::flat_hash_map<pred_id_t, std::vector<std::vector<event>>>;
using ts_list = std::vector<ts_t>;
