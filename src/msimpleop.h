#pragma once
#include <boost/mp11.hpp>
#include <monitor_types.h>
#include <mp_helpers.h>
#include <mterm.h>
#include <optional>
#include <string>
#include <string_view>
#include <table.h>
#include <tuple>
#include <vector>

using namespace boost::mp11;

template<typename L, typename VarL>
using get_var_idx = mp_transform_q<mp_bind<mp_find, L, _1>, VarL>;

template<typename LSize, typename Idxs>
using complement_idxs =
  mp_transform_q<mp_bind<mp_not, mp_bind<mp_contains, Idxs, _1>>,
                 mp_iota<LSize>>;

template<typename... Vars>
struct mexists {
  template<typename L, typename T>
  struct idx_computation {
    using arg_size = mp_size<L>;
    using drop_idxs = get_var_idx<L, mp_list<Vars...>>;
    using keep_idxs = complement_idxs<arg_size, drop_idxs>;
    using res_layout = mp_apply_idxs<keep_idxs, L>;
    using res_type = mp_apply_idxs<keep_idxs, T>;
  };

  template<typename L, typename T>
  struct result_info {
    using ResL = typename idx_computation<L, T>::res_layout;
    using ResT = typename idx_computation<L, T>::res_type;
  };

  template<typename L, typename T>
  static std::optional<typename result_info<L, T>::ResT> eval(T row) {
    using keep_idxs = typename idx_computation<L, T>::keep_idxs;
    return std::move(project_row<keep_idxs>(std::move(row)));
  }
};

struct cst_eq;
struct cst_less;
struct cst_less_eq;

template<typename IsNeg, typename CstTy, typename Term1, typename Term2>
struct mandrel {
  template<typename L, typename T>
  struct result_info {
    using ResL = L;
    using ResT = T;
  };

  template<typename L, typename T>
  static std::optional<T> eval(T row) {
    using t1_res_t = t_res_t<L, T, Term1>;
    using t2_res_t = t_res_t<L, T, Term2>;

    static_assert(std::is_same_v<t1_res_t, t2_res_t>,
                  "terms in constraints must have same type");
    constexpr bool is_neg = IsNeg::value;
    auto res1 = Term1::template eval<L, T>(row);
    auto res2 = Term2::template eval<L, T>(row);
    static_assert(std::is_same_v<decltype(res1), t1_res_t>,
                  "term does not match computed type");
    static_assert(std::is_same_v<decltype(res1), t2_res_t>,
                  "term does not match computed type");
    bool keep_row;

    if constexpr (is_neg && std::is_same_v<CstTy, cst_eq>)
      keep_row = !(res1 == res2);
    else if constexpr (is_neg && std::is_same_v<CstTy, cst_less>)
      keep_row = !(res1 < res2);
    else if constexpr (is_neg && std::is_same_v<CstTy, cst_less_eq>)
      keep_row = !(res1 <= res2);
    else if constexpr (is_neg && std::is_same_v<CstTy, cst_eq>)
      keep_row = res1 == res2;
    else if constexpr (!is_neg && std::is_same_v<CstTy, cst_eq>)
      keep_row = res1 == res2;
    else if constexpr (!is_neg && std::is_same_v<CstTy, cst_less>)
      keep_row = res1 < res2;
    else if constexpr (!is_neg && std::is_same_v<CstTy, cst_less_eq>)
      keep_row = res1 <= res2;
    else
      static_assert(always_false_v<CstTy>, "unknown constraint type");

    if (keep_row)
      return std::move(row);
    else
      return std::nullopt;
  }
};

template<typename ResVar, typename Term>
struct mandassign {
  template<typename L, typename T>
  struct result_info {
    using ResL = mp_push_back<L, ResVar>;
    using ResT = mp_push_back<T, t_res_t<L, T, Term>>;
  };

  template<typename... Args, typename AppArg>
  static std::tuple<Args..., AppArg> append_to_tuple(std::tuple<Args...> tup,
                                                     AppArg app_arg) {
    return tuple_apply(
      [&app_arg](auto &&...tup_val) {
        return std::tuple(std::forward<decltype(tup_val)>(tup_val)...,
                          std::move(app_arg));
      },
      std::move(tup));
  }

  template<typename L, typename T>
  static std::optional<typename result_info<L, T>::ResT> eval(T row) {
    auto t_res = Term::template eval<L, T>(row);
    static_assert(
      std::is_same_v<decltype(t_res), typename Term::template ResT<L, T>>,
      "term has not expected type");
    return append_to_tuple(std::move(row), std::move(t_res));
  }
};

template<typename... Ops>
struct simpleops;

template<typename Op>
struct simpleops<Op> {
  template<typename L, typename T>
  struct result_info {
    using ResL = f_res_l<L, T, Op>;
    using ResT = f_res_t<L, T, Op>;
  };

  template<typename L, typename T>
  static std::optional<typename result_info<L, T>::ResT> eval(T row) {
    return Op::template eval<L, T>(std::move(row));
  }
};

template<typename Op1, typename Op2, typename... Ops>
struct simpleops<Op1, Op2, Ops...> {
  template<typename L, typename T>
  struct result_info {
    using ResL =
      f_res_l<f_res_l<L, T, Op1>, f_res_t<L, T, Op1>, simpleops<Op2, Ops...>>;
    using ResT =
      f_res_t<f_res_l<L, T, Op1>, f_res_t<L, T, Op1>, simpleops<Op2, Ops...>>;
  };

  template<typename L, typename T>
  static std::optional<typename result_info<L, T>::ResT> eval(T row) {
    f_res_t<L, T, Op1> op1_res = Op1::template eval<L, T>(std::move(row));
    if (op1_res)
      return simpleops<Op2, Ops...>::template eval<f_res_t<L, T, Op1>,
                                                   f_res_l<L, T, Op1>>(
        std::move(op1_res));
    else
      return std::nullopt;
  }
};

template<typename Ops, typename MFormula>
struct mfusedsimpleop {
  template<typename L, typename T>
  struct result_info {
    using ResL = f_res_l<f_res_l<L, T, MFormula>, f_res_t<L, T, MFormula>, Ops>;
    using ResT = f_res_t<f_res_l<L, T, MFormula>, f_res_t<L, T, MFormula>, Ops>;
  };

  template<typename L, typename T>
  std::vector<mp_rename<typename result_info<L, T>::ResT, table_util::table>>
  eval(database &db, const ts_list &ts) {
    using res_row_t = typename result_info<L, T>::ResT;
    using res_tab_t = mp_rename<res_row_t, table_util::table>;
    using rec_tab_t = mp_rename<f_res_t<L, T, MFormula>, table_util::table>;
    auto rec_res = f_.template eval<L>(db, ts);
    static_assert(std::is_same_v<decltype(rec_res), std::vector<rec_tab_t>>,
                  "table type unexpected");
    std::vector<res_tab_t> res;
    res.reserve(rec_res.size());
    for (auto &rec_tab : rec_res) {
      res_tab_t tab;
      tab.reserve(rec_tab.size());
      for (const auto &row : rec_tab) {
        auto row_new = Ops::template eval<f_res_l<L, T, MFormula>>(row);
        static_assert(std::is_same_v<decltype(row_new), res_row_t>,
                      "unexpected row type");
        tab.emplace(std::move(row_new));
      }
      res.emplace_back(std::move(tab));
    }
    return res;
  }

  MFormula f_;
};
