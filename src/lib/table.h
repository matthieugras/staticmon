#pragma once
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <boost/mp11.hpp>
#include <fmt/format.h>
#include <mp_helpers.h>
#include <iostream>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace table_util {
using namespace boost::mp11;

template<typename... Args>
using table = absl::flat_hash_set<std::tuple<Args...>>;

template<typename T>
using tab_t_of_row_t = mp_rename<T, table>;

template<typename L1, typename L2, typename T1, typename T2>
struct join_info;

namespace detail {
  template<typename L1, typename L2, typename T1, typename T2>
  struct join_info_impl {
    using l1_idx_map = mp_transform<mp_list, L1, mp_iota<mp_size<L1>>>;
    using l2_idx = mp_transform<mp_list, mp_iota<mp_size<L2>>, L2>;
    template<typename IdxVar>
    using match_idx_fn =
      mp_list<mp_map_find<l1_idx_map, mp_at_c<IdxVar, 1>>, mp_at_c<IdxVar, 0>>;
    using matching_idx = mp_transform<match_idx_fn, l2_idx>;
    template<typename Idxs>
    using part_cond = mp_not<std::is_void<mp_at_c<Idxs, 0>>>;
    using common_uniq2 = mp_partition<matching_idx, part_cond>;
    using l2_unique = mp_project_c<mp_second<common_uniq2>, 1>;
    using common = mp_first<common_uniq2>;
    using unzipped_common = mp_list<mp_project_c<mp_project_c<common, 0>, 1>,
                                    mp_project_c<common, 1>>;
    // using l1_unique =
    //   mp_set_difference<mp_iota<mp_size<L1>>, mp_at_c<unzipped_common, 0>>;
    using l1_common = mp_at_c<unzipped_common, 0>;
    using l2_common = mp_at_c<unzipped_common, 1>;
    using result_row_type = mp_append<L1, mp_apply_idxs<L2, l2_unique>>;
    using result_row_idxs = mp_append<T1, mp_apply_idxs<T2, l2_unique>>;
  };

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
    using T1 = std::tuple<Args1...>;
    using T2 = std::tuple<Args2...>;
    using jinfo = join_info<L1, L2, T1, T2>;
    typename jinfo::result_tab_type res;
    using projection_idxs =
      mp_append<mp_iota<mp_size<L1>>, typename jinfo::l2_unique>;

    res.reserve(tab1.size() * tab2.size());
    for (const auto &t1 : tab1) {
      for (const auto &t2 : tab2)
        res.emplace(project_row_mult<projection_idxs>(t1, t2));
    }
    return res;
  }

  template<typename LIn, typename LOut>
  using get_reorder_mask = mp_transform_q<mp_bind<mp_find, LIn, _1>, LOut>;

}// namespace detail

template<typename LIn, typename LOut, typename T>
struct reorder_info {
  static_assert(mp_size<LIn>::value == mp_size<LOut>::value,
                "layouts must have same size");
  static_assert(
    mp_all_of_q<detail::get_reorder_mask<LIn, LOut>,
                mp_bind<mp_not, mp_bind<mp_less, _1, mp_size<LIn>>>>::value,
    "layouts are not compatible");

  using ResL = LOut;
  using ResT = mp_apply_idxs<T, detail::get_reorder_mask<LIn, LOut>>;
};

template<typename LIn, typename LOut, typename... ArgsIn>
auto reorder_table(const table<ArgsIn...> &tab) {
  using T = std::tuple<ArgsIn...>;
  using reorder_mask = detail::get_reorder_mask<LIn, LOut>;
  using info = reorder_info<LIn, LOut, T>;
  using res_tab_t = tab_t_of_row_t<typename info::ResT>;

  res_tab_t res;
  res.reserve(tab.size());
  for (const auto &row : tab)
    res.emplace(project_row<reorder_mask>(row));
  return res;
}
template<typename L1, typename L2, typename T1, typename T2>
struct join_info {
  using impl = detail::join_info_impl<L1, L2, T1, T2>;
  using l1_common = typename impl::l1_common;
  using l2_common = typename impl::l2_common;
  using l2_unique = typename impl::l2_unique;
  using ResT = typename impl::result_row_type;
  using ResL = typename impl::result_row_idxs;
};


template<typename L1, typename L2, typename T1, typename T2>
struct join_result_info {
  using ResL = typename join_info<L1, L2, T1, T2>::ResL;
  using ResT = typename join_info<L1, L2, T1, T2>::ResT;
};

template<typename L1, typename L2, typename... Args1, typename... Args2>
auto table_join(const table<Args1...> &tab1, const table<Args2...> &tab2) {
  using T1 = std::tuple<Args1...>;
  using T2 = std::tuple<Args2...>;
  using jinfo = join_info<L1, L2, T1, T2>;
  using res_tab_t =
    tab_t_of_row_t<typename join_result_info<L1, L2, T1, T2>::ResT>;
  static constexpr bool no_common_cols =
    mp_empty<typename jinfo::l1_common>::value;

  if (tab1.empty() || tab2.empty())
    return res_tab_t();
  if constexpr (no_common_cols) {
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
auto table_union(const table<Args1...> &tab1, const table<Args2...> &tab2) {
  using T1 = std::tuple<Args1...>;
  using T2 = std::tuple<Args2...>;
  using res_tab_t =
    tab_t_of_row_t<typename union_result_info<L1, L2, T1, T2>::ResT>;

  res_tab_t res;
  res.reserve(tab1.size() + tab2.size());
  if constexpr (std::is_same_v<L1, L2>) {
    static_assert(std::is_same_v<T1, T2>, "layouts same but not row types");
    res.insert(tab2.cbegin(), tab2.cend());
  } else {
    using reorder_mask = detail::get_reorder_mask<L2, L1>;
    static_assert(std::is_same_v<mp_apply_idxs<T2, reorder_mask>, T1>,
                  "internal error, reorder mask incorrect");
    for (const auto &row : tab2)
      res.emplace(project_row<reorder_mask>(row));
  }
  return res;
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
