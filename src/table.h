#pragma once
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <boost/mp11.hpp>
#include <fmt/format.h>
#include <iostream>
#include <mp_helpers.h>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace table_util {
using namespace boost::mp11;

template<bool cond_v, typename Then, typename OrElse>
decltype(auto) constexpr_if(Then &&then, OrElse &&or_else) {
  if constexpr (cond_v) {
    return std::forward<Then>(then);
  } else {
    return std::forward<OrElse>(or_else);
  }
}

template<typename... Args>
using table = absl::flat_hash_set<std::tuple<Args...>>;

namespace detail {
  template<typename Unique1, typename Common1, typename Unique2, typename T1,
           typename T2>
  using get_join_result_types_idxs =
    mp_append<mp_apply_idxs<T1, Unique1>, mp_apply_idxs<T1, Common1>,
              mp_apply_idxs<T2, Unique2>>;

  template<typename L1, typename L2, typename T1, typename T2>
  struct get_join_layout_impl {
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
    using l1_unique =
      mp_set_difference<mp_iota<mp_size<L1>>, mp_at_c<unzipped_common, 0>>;
    using l1_common = mp_at_c<unzipped_common, 0>;
    using l2_common = mp_at_c<unzipped_common, 1>;
    using result_row_type =
      detail::get_join_result_types_idxs<l1_unique, l1_common, l2_unique, T1,
                                         T2>;
    using result_row_idxs =
      detail::get_join_result_types_idxs<l1_unique, l1_common, l2_unique, L1,
                                         L2>;
  };

  template<typename Idxs, typename Tab>
  auto make_join_map(const Tab &t) {
    using T = typename Tab::key_type;
    using ConstPtr = typename Tab::const_pointer;
    using ResTuple = mp_apply_idxs<T, Idxs>;
    using JoinMap = absl::flat_hash_map<ResTuple, std::vector<ConstPtr>>;
    JoinMap jmap{};
    jmap.reserve(t.size());
    for (const auto &row : t)
      jmap[project_row<Idxs>(row)].emplace_back(&row);
    return jmap;
  }

  template<bool hash_left, typename JoinInfo, typename Tab1, typename Tab2>
  typename JoinInfo::result_tab_type table_join_dispatch(const Tab1 &tab1,
                                                         const Tab2 &tab2) {
    using JoinMapIdxs =
      std::conditional_t<hash_left, typename JoinInfo::l1_common,
                         typename JoinInfo::l2_common>;
    using ProjectIdxs =
      std::conditional_t<hash_left, typename JoinInfo::l2_common,
                         typename JoinInfo::l1_common>;
    auto jmap = make_join_map<JoinMapIdxs>(constexpr_if<hash_left>(tab1, tab2));
    typename JoinInfo::result_tab_type res;
    for (const auto &tup : constexpr_if<hash_left>(tab2, tab1)) {
      auto proj = project_row<ProjectIdxs>(tup);
      auto it = jmap.find(proj);
      if (it != jmap.cend()) {
        for (auto ptr : it->second) {
          const auto &t1 = constexpr_if<hash_left>(*ptr, tup);
          const auto &t2 = constexpr_if<hash_left>(tup, *ptr);
          res.emplace(
            project_row_mult<typename JoinInfo::join_idxs>(t1, t1, t2));
        }
      }
    }
    return res;
  }

  template<typename JoinInfo, typename Tab1, typename Tab2>
  auto cartesian_product(const Tab1 &tab1, const Tab2 &tab2) {
    typename JoinInfo::result_tab_type res;
    using ProjectIdxs =
      mp_list<typename JoinInfo::l1_unique, typename JoinInfo::l2_unique>;
    res.reserve(tab1.size() * tab2.size());
    for (const auto &t1 : tab1) {
      for (const auto &t2 : tab2) {
        res.emplace(project_row_mult<ProjectIdxs>(t1, t2));
      }
    }
    return res;
  }

}// namespace detail

template<typename L1, typename L2, typename T1, typename T2>
struct get_join_layout {
  using impl = typename detail::get_join_layout_impl<L1, L2, T1, T2>;
  using l1_unique = typename impl::l1_unique;
  using l1_common = typename impl::l1_common;
  using l2_common = typename impl::l2_common;
  using l2_unique = typename impl::l2_unique;
  using join_idxs = mp_list<l1_unique, l1_common, l2_unique>;
  using result_tab_type =
    mp_rename<mp_push_front<typename impl::result_row_type,
                            typename impl::result_row_idxs>,
              table>;
};


template<typename L1, typename L2, typename T1, typename T2>
typename get_join_layout<L1, L2, T1, T2>::result_tab_type
table_join(const mp_rename<T1, table> &tab1, const mp_rename<T2, table> &tab2) {
  using JoinInfo = get_join_layout<L1, L2, T1, T2>;
  if (tab1.empty() || tab2.empty())
    return typename JoinInfo::result_tab_type{};
  if constexpr (mp_empty<typename JoinInfo::l1_common>::value) {
    return detail::cartesian_product<JoinInfo>(tab1, tab2);
  } else {
    auto hash_left = tab1.size() < tab2.size();
    if (hash_left)
      return detail::table_join_dispatch<true, JoinInfo>(tab1, tab2);
    else
      return detail::table_join_dispatch<false, JoinInfo>(tab1, tab2);
  }
}
}// namespace table_util
