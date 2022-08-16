#pragma once
#include <boost/mp11.hpp>
#include <cstdint>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <vector>

using namespace boost::mp11;

template<typename T>
inline constexpr bool is_number_type =
  std::is_same_v<T, std::int64_t> || std::is_same_v<T, double>;

template<typename... T>
using remove_cv_refs_t = mp_transform<std::remove_cvref_t, mp_list<T...>>;

template<typename T1, typename T2>
using mp_not_same = mp_not<std::is_same<T1, T2>>;

template<typename L>
struct mp_group_by_snd_impl {
  template<typename T, typename U>
  using mp_less_snd = mp_less<mp_second<T>, mp_second<U>>;

  template<typename VO, typename TO>
  struct fold_fn_impl {
    template<typename V, typename T>
    using new_pair_branch =
      mp_push_back<V, mp_list<mp_second<T>, mp_list<mp_first<T>>>>;

    template<typename V, typename T>
    struct old_pair_branch_impl {
      using v_back = mp_back<V>;
      using type =
        mp_push_back<mp_pop_back<V>,
                     mp_list<mp_first<v_back>,
                             mp_push_back<mp_second<v_back>, mp_first<T>>>>;
    };

    template<typename V, typename T>
    using old_pair_branch = typename old_pair_branch_impl<V, T>::type;

    template<typename V, typename T>
    using not_empty_branch =
      mp_if<mp_not<std::is_same<mp_second<T>, mp_first<mp_back<V>>>>,
            new_pair_branch<V, T>, old_pair_branch<V, T>>;

    using type = mp_eval_if<mp_empty<VO>, new_pair_branch<VO, TO>,
                            not_empty_branch, VO, TO>;
  };

  template<typename V, typename T>
  using fold_fn = typename fold_fn_impl<V, T>::type;

  // Assuming that sort is stable
  using type = mp_fold<mp_sort<L, mp_less_snd>, mp_list<>, fold_fn>;
};

template<typename L>
using mp_group_by_snd = typename mp_group_by_snd_impl<L>::type;

template<typename L>
struct mp_group_by_fst_impl {
  template<typename T, typename U>
  using mp_less_fst = mp_less<mp_first<T>, mp_first<U>>;

  template<typename VO, typename TO>
  struct fold_fn_impl {
    template<typename V, typename T>
    using new_pair_branch =
      mp_push_back<V, mp_list<mp_first<T>, mp_list<mp_second<T>>>>;

    template<typename V, typename T>
    struct old_pair_branch_impl {
      using v_back = mp_back<V>;
      using type =
        mp_push_back<mp_pop_back<V>,
                     mp_list<mp_first<v_back>,
                             mp_push_back<mp_second<v_back>, mp_second<T>>>>;
    };

    template<typename V, typename T>
    using old_pair_branch = typename old_pair_branch_impl<V, T>::type;

    template<typename V, typename T>
    using not_empty_branch =
      mp_if<mp_not<std::is_same<mp_first<T>, mp_first<mp_back<V>>>>,
            new_pair_branch<V, T>, old_pair_branch<V, T>>;

    using type = mp_eval_if<mp_empty<VO>, new_pair_branch<VO, TO>,
                            not_empty_branch, VO, TO>;
  };

  template<typename V, typename T>
  using fold_fn = typename fold_fn_impl<V, T>::type;

  // Assuming that sort is stable
  using type = mp_fold<mp_sort<L, mp_less_fst>, mp_list<>, fold_fn>;
};

template<typename L>
using mp_group_by_fst = typename mp_group_by_fst_impl<L>::type;

template<typename L, typename V>
struct mp_find_partial_impl {
  using type = mp_find<L, V>;

  static_assert(type::value < mp_size<L>::value, "index out of bounds");
};

template<typename L, typename V>
using mp_find_partial = typename mp_find_partial_impl<L, V>::type;

template<typename T, typename... Args>
std::vector<std::remove_cvref_t<T>> make_vector(T &&fst_arg, Args &&...args) {
  std::vector<std::remove_cvref_t<T>> res;
  res.emplace_back(std::forward<T>(fst_arg));
  (((void) res.emplace_back(std::forward<Args>(args))), ...);
  return res;
}

template<typename T>
struct clean_monitor_cst_ty_impl {
  using type =
    mp_if<std::is_same<T, std::string_view>, std::string,
          mp_if<std::is_same<T, double>, double,
                mp_if<std::is_same<T, std::int64_t>, std::int64_t,
                      mp_if<std::is_same<T, std::string>, std::string, void>>>>;
  static_assert(!std::is_same_v<type, void>, "unknown cst type");
};

template<typename T>
using clean_monitor_cst_ty = typename clean_monitor_cst_ty_impl<T>::type;

template<bool cond_v, typename Then, typename OrElse>
decltype(auto) constexpr_if(Then &&then, OrElse &&or_else) {
  if constexpr (cond_v) {
    return std::forward<Then>(then);
  } else {
    return std::forward<OrElse>(or_else);
  }
}

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
  static_assert(mp_size<Idxs>::value == sizeof...(Rows),
                "must have as many lists of idxs as rows");
  using idx_pairs = compute_projection_idxs<Idxs>;
  return project_row_mult_helper(mp_first<idx_pairs>{}, mp_second<idx_pairs>{},
                                 std::forward_as_tuple(row...));
}

template<std::size_t... idxs, typename Row>
auto project_row_helper(Row &&row, mp_list<mp_size_t<idxs>...>) {
  return std::tuple(std::get<idxs>(std::forward<Row>(row))...);
}

template<typename Idxs, typename Row>
auto project_row(Row &&row) {
  return project_row_helper(std::forward<Row>(row), Idxs{});
}
