#pragma once
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <algorithm>
#include <boost/mp11.hpp>
#include <fmt/format.h>
#include <iostream>
#include <iterator>
#include <staticmon/common/mp_helpers.h>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

template<typename S>
void check_no_some_empty_tab(const S &s) {
  assert(std::all_of(s.begin(), s.end(),
                     [](const auto &t) { return !t || !t->empty(); }));
}

namespace table_util {
using namespace boost::mp11;

template<typename... Args>
using table = absl::flat_hash_set<std::tuple<Args...>>;

template<typename T>
using tab_t_of_row_t = mp_rename<T, table>;

inline table<> unit_table() {
  table_util::table<> tab;
  tab.emplace();
  return tab;
}

template<typename T>
table<T> singleton_table(T &&val) {
  table<T> tab;
  tab.emplace(std::forward<T>(val));
  return tab;
}

template<typename Row>
tab_t_of_row_t<Row> single_row_table(Row &&r) {
  tab_t_of_row_t<Row> tab;
  tab.emplace(std::forward<Row>(r));
  return tab;
}

template<typename L1, typename L2, typename T1, typename T2>
struct join_info;

template<typename L1, typename L2>
using comp_unique_idx = mp_transform<
  mp_second,
  mp_filter_q<mp_bind<mp_not, mp_bind<mp_contains, L2, mp_bind_back<mp_first>>>,
              mp_zip<L1, mp_iota<mp_size<L1>>>>>;

template<typename L1, typename L2>
using comp_common_idx =
  mp_filter_q<mp_bind_back<mp_not_same, mp_size<L1>>,
              mp_transform_q<mp_bind_front<mp_find, L1>, L2>>;

template<typename Idxs, typename L1, typename L2>
using comp_matching_cols =
  mp_transform_q<mp_bind<mp_find, L2, mp_bind_front<mp_at, L1>>, Idxs>;
namespace detail {

  template<typename Idxs, typename... Args>
  auto make_join_map(const table<Args...> &t) {
    using T = std::tuple<Args...>;
    using Tab = table<Args...>;
    using ConstPtr = typename Tab::const_pointer;
    using ResTuple = mp_apply_idxs<T, Idxs>;
    using JoinMap = absl::flat_hash_map<ResTuple, std::vector<ConstPtr>>;
    JoinMap jmap;
    jmap.reserve(t.size());
    for (const auto &row : t)
      jmap[project_row<Idxs>(row)].emplace_back(&row);
    return jmap;
  }

  template<typename Idxs, typename... Args>
  auto make_anti_join_set(const table<Args...> &t) {
    using T = std::tuple<Args...>;
    using ResTuple = mp_apply_idxs<T, Idxs>;
    using AJoinSet = absl::flat_hash_set<ResTuple>;
    AJoinSet ajset;
    ajset.reserve(t.size());
    for (const auto &row : t)
      ajset.emplace(project_row<Idxs>(row));
    return ajset;
  }

  template<bool hash_left, typename L1, typename L2, typename... Args1,
           typename... Args2>
  auto table_join_dispatch(const table<Args1...> &tab1,
                           const table<Args2...> &tab2) {
    using T1 = std::tuple<Args1...>;
    using T2 = std::tuple<Args2...>;
    using jinfo = join_info<L1, L2, T1, T2>;
    using l2_unique = typename jinfo::l2_unique;
    using l1_common = typename jinfo::l1_common;
    using l2_common = typename jinfo::l2_common;
    using join_idxs = mp_list<mp_iota<mp_size<L1>>, l2_unique>;
    using join_map_idxs = std::conditional_t<hash_left, l1_common, l2_common>;
    using project_idxs = std::conditional_t<hash_left, l2_common, l1_common>;
    using res_tab_t = tab_t_of_row_t<typename jinfo::ResT>;

    auto jmap =
      make_join_map<join_map_idxs>(constexpr_if<hash_left>(tab1, tab2));
    res_tab_t res;
    for (const auto &tup : constexpr_if<hash_left>(tab2, tab1)) {
      auto proj = project_row<project_idxs>(tup);
      auto it = jmap.find(proj);
      if (it != jmap.cend()) {
        for (auto ptr : it->second) {
          const auto &t1 = constexpr_if<hash_left>(*ptr, tup);
          const auto &t2 = constexpr_if<hash_left>(tup, *ptr);
          res.emplace(project_row_mult<join_idxs>(t1, t2));
        }
      }
    }
    return res;
  }

  template<typename L1, typename L2, typename... Args1, typename... Args2>
  auto cartesian_product(const table<Args1...> &tab1,
                         const table<Args2...> &tab2) {
    using res_tab_t = table<Args1..., Args2...>;
    res_tab_t res;
    using projection_idxs = mp_list<mp_iota<mp_size<L1>>, mp_iota<mp_size<L2>>>;

    for (const auto &t1 : tab1) {
      for (const auto &t2 : tab2)
        res.emplace(project_row_mult<projection_idxs>(t1, t2));
    }
    return res;
  }
}// namespace detail

template<typename LIn, typename LOut>
using get_reorder_mask = mp_transform_q<mp_bind<mp_find, LIn, _1>, LOut>;

template<typename LIn, typename LOut, typename T>
struct reorder_info {
  static_assert(mp_size<LIn>::value == mp_size<LOut>::value,
                "layouts must have same size");
  static_assert(mp_all_of_q<get_reorder_mask<LIn, LOut>,
                            mp_bind<mp_less, _1, mp_size<LIn>>>::value,
                "layouts are not compatible");

  using ResL = LOut;
  using ResT = mp_apply_idxs<T, get_reorder_mask<LIn, LOut>>;
};

template<typename L1, typename L2, typename T1, typename T2>
struct join_info {
  using l1_common = comp_common_idx<L1, L2>;
  using l2_common = comp_matching_cols<l1_common, L1, L2>;
  using l2_unique = comp_unique_idx<L2, L1>;
  using ResT = mp_append<T1, mp_apply_idxs<T2, l2_unique>>;
  using ResL = mp_append<L1, mp_apply_idxs<L2, l2_unique>>;
};


template<typename L2, typename T2>
requires(mp_size<L2>::value >
         0) struct join_info<mp_list<>, L2, std::tuple<>, T2> {
  using l1_common = mp_list<>;
  using l2_common = mp_list<>;
  using l2_unique = mp_iota<mp_size<L2>>;
  using ResT = T2;
  using ResL = L2;
};

template<typename L1, typename T1>
struct join_info<L1, mp_list<>, T1, std::tuple<>> {
  using l1_common = mp_list<>;
  using l2_common = mp_list<>;
  using l2_unique = mp_list<>;
  using ResT = T1;
  using ResL = L1;
};

template<typename L1, typename L2, typename T1, typename T2>
struct join_result_info {
  using ResL = typename join_info<L1, L2, T1, T2>::ResL;
  using ResT = typename join_info<L1, L2, T1, T2>::ResT;
};

template<typename L1, typename L2, typename... Args1, typename... Args2>
auto table_join(table<Args1...> &tab1, table<Args2...> &tab2) {
  using T1 = std::tuple<Args1...>;
  using T2 = std::tuple<Args2...>;
  using jinfo = join_info<L1, L2, T1, T2>;
  static constexpr bool no_common_cols =
    mp_empty<typename jinfo::l1_common>::value;
  static constexpr bool l_no_cols = sizeof...(Args1) == 0;
  static constexpr bool r_no_cols = sizeof...(Args2) == 0;

  if constexpr (l_no_cols) {
    return std::move(tab2);
  } else if constexpr (r_no_cols) {
    return std::move(tab1);
  } else if constexpr (no_common_cols) {
    return detail::cartesian_product<L1, L2>(tab1, tab2);
  } else {
    auto hash_left = tab1.size() < tab2.size();
    if (hash_left)
      return detail::table_join_dispatch<true, L1, L2>(tab1, tab2);
    else
      return detail::table_join_dispatch<false, L1, L2>(tab1, tab2);
  }
}

template<typename L1, typename L2, typename T1, typename T2>
struct union_result_info {
  using ResT = T1;
  using ResL = L1;
};

template<typename L1, typename L2, typename... Args1, typename... Args2>
auto table_union(table<Args1...> &&tab1, table<Args2...> &tab2) {
  using T1 = std::tuple<Args1...>;
  using T2 = std::tuple<Args2...>;

  tab1.reserve(tab1.size() + tab2.size());
  if constexpr (std::is_same_v<L1, L2>) {
    static_assert(std::is_same_v<T1, T2>, "layouts same but not row types");
    tab1.merge(tab2);
  } else {
    using reorder_mask = get_reorder_mask<L2, L1>;
    static_assert(std::is_same_v<mp_apply_idxs<T2, reorder_mask>, T1>,
                  "internal error, reorder mask incorrect");
    for (auto &row : tab2)
      tab1.emplace(project_row<reorder_mask>(std::move(row)));
  }
  return std::move(tab1);
}

template<typename L1, typename L2, typename T1, typename T2>
struct anti_join_info {
  using ResL = L1;
  using ResT = T1;
};

template<typename L1, typename L2, typename... Args1, typename... Args2>
auto table_anti_join(const table<Args1...> &tab1, const table<Args2...> &tab2) {
  using T1 = std::tuple<Args1...>;
  using T2 = std::tuple<Args2...>;
  using res_tab_t = table<Args1...>;
  using jinfo = join_info<L1, L2, T1, T2>;
  static_assert(mp_empty<typename jinfo::l2_unique>::value,
                "not a valid antijoin");
  using proj_mask1 = typename jinfo::l1_common;
  using proj_mask2 = typename jinfo::l2_common;
  auto jset = detail::make_anti_join_set<proj_mask2>(tab2);
  res_tab_t res;
  for (const auto &row : tab1) {
    auto proj_r = project_row<proj_mask1>(row);
    if (!jset.contains(proj_r))
      res.emplace(row);
  }
  return res;
}
}// namespace table_util
