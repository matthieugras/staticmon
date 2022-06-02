#pragma once
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <boost/mp11.hpp>
#include <cstdint>
#include <string>
#include <tuple>
#include <variant>
#include <vector>

using namespace boost::mp11;


using pred_id_t = std::size_t;
using ts_t = std::size_t;
using event_data = std::variant<std::int64_t, double, std::string>;
using event = std::vector<event_data>;
using database =
  absl::flat_hash_map<pred_id_t, std::vector<std::vector<event>>>;
using ts_list = std::vector<ts_t>;

template<typename T>
struct clean_monitor_cst_ty_impl {
  using type = mp_if<std::is_same<T, std::string_view>, std::string,
        mp_if<std::is_same<T, double>, double,
              mp_if<std::is_same<T, std::int64_t>, std::int64_t,
                    mp_if<std::is_same<T, std::string>, std::string, void>>>>;
  static_assert(!std::is_same_v<type, void>, "unknown cst type");
};

template<typename T>
using clean_monitor_cst_ty = typename clean_monitor_cst_ty_impl<T>::type;
