#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <boost/mp11.hpp>
#include <fmt/format.h>
#include <iostream>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

using namespace boost::mp11;

template<typename T>
struct TypePrinter;

template<typename... T>
struct TypePrinter<mp_list<T...>> {
  static std::string print_type() {
    auto formatted_elems = std::make_tuple(TypePrinter<T>::print_type()...);
    return fmt::format("[{}]", fmt::join(formatted_elems, ", "));
  }
};

template<typename T, T v>
struct TypePrinter<std::integral_constant<T, v>> {
  static std::string print_type() { return fmt::format("{}", v); }
};

template<>
struct TypePrinter<void> {
  static std::string print_type() { return "∅"; }
};

template<typename... Args>
using table = absl::flat_hash_set<std::tuple<Args...>>;

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

template<typename L1, typename L2>
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
  using unzipped_common =
    mp_list<mp_project_c<mp_project_c<common, 0>, 1>, mp_project_c<common, 1>>;
  using l1_unique =
    mp_set_difference<mp_iota<mp_size<L1>>, mp_at_c<unzipped_common, 0>>;
  using type =
    mp_push_front<mp_push_back<unzipped_common, l2_unique>, l1_unique>;
};

template<typename L1, typename L2>
using get_join_layout = typename get_join_layout_impl<L1, L2>::type;

template<typename... InnerIdx, typename... OuterIdx, typename Tuples>
auto project_row_mult_helper(mp_list<InnerIdx...>, mp_list<OuterIdx...>,
                             Tuples &&tups) {
  return std::tuple(std::get<OuterIdx::value>(
    std::get<InnerIdx::value>(std::forward<Tuples>(tups)))...);
}

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

template<typename Idxs, typename... Rows>
auto project_row_mult(Rows &&...row) {
  using idx_pairs = compute_projection_idxs<Idxs>;
  return project_row_mult_helper(mp_first<idx_pairs>{}, mp_second<idx_pairs>{},
                                 std::forward_as_tuple(row...));
}

template<typename Idx, typename Row>
auto project_row(Row &&row) {
  return project_row_mult<Idx>(row);
}


template<typename T1, typename T2, typename TRes, typename JoinMap,
         typename JoinInfo>
auto join_tuples(const T1 &t1, const T2 &t2, const JoinMap &jmap, TRes &res,
                 bool hashed_left) {
  using IDXS =
    mp_list<mp_at_c<JoinInfo, 0>, mp_at_c<JoinInfo, 1>, mp_at_c<JoinInfo, 3>>;
  if (hashed_left) {
    auto proj2 = project_row<mp_at_c<JoinInfo, 2>>(t2);
    auto it = jmap.find(proj2);
    if (it != jmap.cend()) {
      for (auto ptr : it->second)
        res.emplace(project_row_mult<IDXS>(t1, t1, t2));
    }
  } else {
    auto proj1 = project_row<mp_at_c<JoinInfo, 1>>(t1);
    auto it = jmap.find(proj1);
    if (it != jmap.cend()) {
      for (auto ptr : it->second)
        res.emplace(project_row_mult<IDXS>(t1, t1, t2));
    }
  }
}

template<typename Idxs, typename T>
auto make_join_map(const T &t) {
  using ConstPtr = typename T::const_pointer;
  using ResTuple =
    std::remove_cvref_t<decltype(project_row<Idxs>(*t.cbegin()))>;
  using JoinMap = absl::flat_hash_map<ResTuple, ConstPtr>;
  JoinMap jmap{};
  jmap.reserve(t.size());
  for (const auto &row : t)
    jmap.try_emplace(project_row<Idxs>(row), &row);
  return jmap;
}


template<typename L1, typename L2, typename T1, typename T2>
auto table_join(const T1 &tabl, const T2 &tabr) {
  using JINFO = get_join_layout_impl<L1, L2>;
  auto hash_left = tabl.size() < tabr.size();
  if (hash_left) {
    auto jmap = make_join_map<mp_at_c<JINFO, 1>>(tabl);
  } else {
    auto jmap = make_join_map<mp_at_c<JINFO, 2>>(tabr);
  }
}
