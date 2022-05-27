#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <boost/mp11.hpp>
#include <fmt/format.h>
#include <iostream>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace table_util {
using namespace boost::mp11;

template<typename L>
struct mp_unzip_impl {
  template<typename Lists>
  struct main_branch_impl {
    using num_ls = mp_size<mp_first<Lists>>;
    using init_val = mp_repeat<mp_list<mp_list<>>, num_ls>;
    using type =
      mp_fold_q<Lists, init_val,
                mp_bind<mp_transform_q, mp_quote<mp_push_back>, _1, _2>>;
  };
  template<typename Lists>
  using main_branch = typename main_branch_impl<Lists>::type;

  using type = mp_eval_if<mp_empty<L>, mp_list<>, main_branch, L>;
};

template<typename L>
using mp_unzip = typename mp_unzip_impl<L>::type;

template<typename L, typename I>
using mp_project = mp_transform_q<mp_bind<mp_at, _1, I>, L>;

template<typename L, std::size_t I>
using mp_project_c = mp_project<L, mp_size_t<I>>;

template<typename... L>
using mp_zip = mp_transform<mp_list, L...>;

template<typename T, typename E>
struct mp_apply_idx_impl;

template<typename Idxs, template<typename...> typename L, typename... E>
struct mp_apply_idx_impl<L<E...>, Idxs> {
  using type =
    mp_rename<mp_transform_q<mp_bind<mp_at, mp_list<E...>, _1>, Idxs>, L>;
};

template<typename Idxs, typename L>
using mp_apply_idxs = typename mp_apply_idx_impl<Idxs, L>::type;

template<bool cond_v, typename Then, typename OrElse>
decltype(auto) constexpr_if(Then &&then, OrElse &&or_else) {
  if constexpr (cond_v) {
    return std::forward<Then>(then);
  } else {
    return std::forward<OrElse>(or_else);
  }
}

template<typename L, typename... Args>
struct table : public absl::flat_hash_set<std::tuple<Args...>> {
  using row_layout = L;
};

namespace detail {
  template<typename Unique1, typename Common1, typename Unique2, typename T1,
           typename T2>
  using get_join_result_types_idxs =
    mp_append<mp_apply_idxs<T1, Unique1>, mp_apply_idxs<T1, Common1>,
              mp_apply_idxs<T2, Unique2>>;

  template<typename Idxs>
  struct compute_projection_idxs_impl {
    using init_val = mp_list<mp_list<>, mp_size_t<0>>;
    template<typename Acc, typename IdxList>
    using fold_fn = mp_list<
      mp_append<
        mp_first<Acc>,
        mp_zip<mp_repeat<mp_list<mp_second<Acc>>, mp_size<IdxList>>, IdxList>>,
      mp_size_t<mp_second<Acc>::value + 1>>;
    using type = mp_unzip<mp_first<mp_fold<Idxs, init_val, fold_fn>>>;
  };

  template<typename Idxs>
  using compute_projection_idxs =
    typename compute_projection_idxs_impl<Idxs>::type;

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

  template<typename... InnerIdx, typename... OuterIdx, typename Tuples>
  auto project_row_mult_helper(mp_list<InnerIdx...>, mp_list<OuterIdx...>,
                               Tuples &&tups) {
    return std::tuple(std::get<OuterIdx::value>(
      std::get<InnerIdx::value>(std::forward<Tuples>(tups)))...);
  }


  template<typename Idxs, typename... Rows>
  auto project_row_mult(Rows &&...row) {
    using idx_pairs = compute_projection_idxs<Idxs>;
    return project_row_mult_helper(mp_first<idx_pairs>{},
                                   mp_second<idx_pairs>{},
                                   std::forward_as_tuple(row...));
  }

  template<typename Idx, typename Row>
  auto project_row(Row &&row) {
    return project_row_mult<mp_list<Idx>>(row);
  }

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

template<typename Tab1, typename Tab2>
struct get_join_layout {
  using L1 = typename Tab1::row_layout;
  using L2 = typename Tab2::row_layout;
  using T1 = typename Tab1::key_type;
  using T2 = typename Tab2::key_type;
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


template<typename Tab1, typename Tab2>
typename get_join_layout<Tab1, Tab2>::result_tab_type
table_join(const Tab1 &tab1, const Tab2 &tab2) {
  using JoinInfo = get_join_layout<Tab1, Tab2>;
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
