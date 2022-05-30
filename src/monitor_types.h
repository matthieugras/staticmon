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

template<typename L, typename T, typename F>
using f_res_t = typename F::template result_info<L, T>::ResT;

template<typename L, typename T, typename F>
using f_res_l = typename F::template result_info<L, T>::ResL;

// template<typename L, typename T, typename ...F>
// struct f_res_ls_impl;
//
// template<typename L, typename T, typename ...F>
// struct f_res_ts_impl;
//
// template<typename L, typename T, typename ...F>
// using f_res_ls = typename f_res_ls_impl<L, T, F...>::type;
//
// template<typename L, typename T, typename ...F>
// using f_res_ts = typename f_res_ts_impl<L, T, F...>::type;
//
// template<typename L, typename T, typename F>
// struct f_res_ls_impl<L, T, F> {
//   using type = f_res_l<L, T, F>;
// };
//
// template<typename L, typename T, typename F1, typename F2, typename... F>
// struct f_res_ls_impl<L, T, F1, F2, F...> {
//   using type = f_res_l
// };
