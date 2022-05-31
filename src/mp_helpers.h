#pragma once
#include <boost/mp11.hpp>
#include <cstdint>
#include <tuple>

using namespace boost::mp11;

template<class>
inline constexpr bool always_false_v = false;

template<typename T>
using is_not_void = mp_not<std::is_void<T>>;

template<std::int64_t i>
using mp_int64_t = std::integral_constant<std::int64_t, i>;

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

template<typename LSize, typename Idxs>
using mp_complement_idxs =
  mp_filter_q<mp_bind<mp_not, mp_bind<mp_contains, Idxs, _1>>, mp_iota<LSize>>;

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

template<typename... InnerIdx, typename... OuterIdx, typename Tuples>
auto project_row_mult_helper(mp_list<InnerIdx...>, mp_list<OuterIdx...>,
                             Tuples &&tups) {
  return std::tuple(std::get<OuterIdx::value>(
    std::get<InnerIdx::value>(std::forward<Tuples>(tups)))...);
}


template<typename Idxs, typename... Rows>
auto project_row_mult(Rows &&...row) {
  using idx_pairs = compute_projection_idxs<Idxs>;
  return project_row_mult_helper(mp_first<idx_pairs>{}, mp_second<idx_pairs>{},
                                 std::forward_as_tuple(row...));
}

template<typename Idxs, typename Row>
auto project_row(Row &&row) {
  return project_row_mult<mp_list<Idxs>>(row);
}
