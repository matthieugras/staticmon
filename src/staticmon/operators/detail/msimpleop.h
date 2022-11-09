#pragma once
#include <absl/container/flat_hash_map.h>
#include <boost/mp11.hpp>
#include <boost/variant2.hpp>
#include <optional>
#include <staticmon/common/mp_helpers.h>
#include <staticmon/common/table.h>
#include <staticmon/common/table_diff.h>
#include <staticmon/operators/detail/mterm.h>
#include <staticmon/operators/detail/operator_types.h>
#include <tuple>
#include <vector>

using namespace boost::mp11;

template<typename L, typename T, typename F>
using f_res_t = typename F::template result_info<L, T>::ResT;

template<typename L, typename T, typename F>
using f_res_l = typename F::template result_info<L, T>::ResL;

template<typename L, typename VarL>
using get_var_idx = mp_transform_q<mp_bind<mp_find, L, _1>, VarL>;

template<std::size_t... Vars>
struct mexists {
  template<typename L, typename T>
  struct idx_computation {
    using l_vars = mp_list_c<std::size_t, Vars...>;
    static_assert(!mp_empty<l_vars>::value,
                  "exists must bind at least one variable");
    static_assert(mp_all_of_q<l_vars, mp_bind<mp_contains, L, _1>>::value,
                  "Exists var not found");
    using arg_size = mp_size<L>;
    using drop_idxs = get_var_idx<L, l_vars>;
    using keep_idxs = mp_complement_idxs<arg_size, drop_idxs>;
    using res_layout = mp_apply_idxs<L, keep_idxs>;
    using res_type = mp_apply_idxs<T, keep_idxs>;
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

template<bool is_neg, typename CstTy, typename Term1, typename Term2>
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
    auto res1 = Term1::template eval<L>(row);
    auto res2 = Term2::template eval<L>(row);
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
    else if constexpr (!is_neg && std::is_same_v<CstTy, cst_eq>)
      keep_row = res1 == res2;
    else if constexpr (!is_neg && std::is_same_v<CstTy, cst_less>)
      keep_row = res1 < res2;
    else if constexpr (!is_neg && std::is_same_v<CstTy, cst_less_eq>)
      keep_row = res1 <= res2;
    else
      static_assert(always_false_v<CstTy>, "unknown constraint type");

    if (keep_row)
      return row;
    else
      return std::nullopt;
  }
};

template<std::size_t ResVar, typename Term>
struct mandassign {
  template<typename L, typename T>
  struct result_info {
    using ResL = mp_push_back<L, mp_size_t<ResVar>>;
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
    auto t_res = Term::template eval<L>(row);
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
    return Op::template eval<L>(std::move(row));
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
    using op1_res_l = f_res_l<L, T, Op1>;
    using op1_res_t = f_res_t<L, T, Op1>;
    auto op1_res = Op1::template eval<L>(std::move(row));
    static_assert(std::is_same_v<decltype(op1_res), std::optional<op1_res_t>>,
                  "op1_res has invalid type");
    if (op1_res)
      return simpleops<Op2, Ops...>::template eval<op1_res_l>(
        std::move(*op1_res));
    else
      return std::nullopt;
  }
};

template<typename Ops, typename MFormula>
struct mfusedsimpleop_impl {
  using formula_res_l = typename MFormula::ResL;
  using formula_res_t = typename MFormula::ResT;
  using formula_res_tab_t = table_util::tab_t_of_row_t<formula_res_t>;
  using ResL = f_res_l<formula_res_l, formula_res_t, Ops>;
  using ResT = f_res_t<formula_res_l, formula_res_t, Ops>;
  using res_tab_t = mp_rename<ResT, table_util::table>;

  std::optional<res_tab_t> eval_tab(std::optional<formula_res_tab_t> &t) {
    if (!t)
      return {};
    assert(!t->empty());
    res_tab_t tab;
    tab.reserve(t->size());
    for (const auto &row : *t) {
      auto row_new = Ops::template eval<formula_res_l>(row);
      static_assert(std::is_same_v<decltype(row_new), std::optional<ResT>>,
                    "unexpected row type");
      if (row_new)
        tab.emplace(std::move(*row_new));
    }
    if (tab.empty())
      return {};
    else
      return tab;
  }

  std::vector<std::optional<res_tab_t>> eval(database &db, const ts_list &ts) {
    auto rec_res = f_.eval(db, ts);
    std::vector<std::optional<res_tab_t>> res;
    res.reserve(rec_res.size());
    for (auto &rec_tab : rec_res)
      res.emplace_back(eval_tab(rec_tab));
    return res;
  }

  MFormula f_;
};

template<typename Ops, typename MFormula>
struct mfusedsimpleop : mfusedsimpleop_impl<Ops, MFormula> {
  using Base = mfusedsimpleop_impl<Ops, MFormula>;
  static constexpr bool ResImmutable = false;

  using Base::eval;
};

template<typename Ops, typename MFormula>
requires(MFormula::DiffRes) struct mfusedsimpleop<Ops, MFormula>
    : mfusedsimpleop_impl<Ops, MFormula> {
  using Base = mfusedsimpleop_impl<Ops, MFormula>;
  static constexpr bool DiffRes = true;
  using formula_res_l = typename Base::formula_res_l;
  using formula_res_t = typename Base::formula_res_t;
  using formula_res_tab_t = typename Base::formula_res_tab_t;
  using formula_diff_t = table_diff<formula_res_tab_t>;
  using ResT = typename Base::ResT;
  using res_tab_t = table_util::tab_t_of_row_t<ResT>;
  using diff_t = table_diff<res_tab_t>;
  using res_t = boost::variant2::variant<std::optional<res_tab_t>, diff_t>;

  template<typename F>
  void iterate(const formula_res_tab_t &t, F f) {
    for (const auto &row : t) {
      auto row_new = Ops::template eval<formula_res_l>(row);
      static_assert(std::is_same_v<decltype(row_new), std::optional<ResT>>,
                    "unexpected row type");
      if (row_new)
        f(*row_new);
    }
  }

  std::vector<res_t> eval(database &db, const ts_list &ts) {
    auto rec_res = f_.eval(db, ts);
    std::vector<res_t> res;
    res.reserve(rec_res.size());
    for (auto &rec_tab : rec_res) {
      auto visitor = [this](auto &&arg) -> res_t {
        using T = std::remove_cvref_t<decltype(arg)>;
        if constexpr (std::is_same_v<T, std::optional<formula_res_tab_t>>) {
          auto tab = this->eval_tab(arg);
          res_card_.clear();
          if (tab) {
            for (const auto &row : *tab)
              res_card_.emplace(row, 1);
          }
          return tab;
        } else {
          static_assert(std::is_same_v<T, formula_diff_t>, "unexpected type");
          diff_t diff;
          iterate(arg.pos, [this, &diff](auto &row) {
            if ((++res_card_[row]) == 1)
              diff.pos.emplace(std::move(row));
          });
          iterate(arg.neg, [this, &diff](auto &row) {
            if ((--res_card_[row]) == 0) {
              res_card_.erase(row);
              diff.neg.emplace(std::move(row));
            }
          });
          return diff;
        }
      };
      res.emplace_back(boost::variant2::visit(visitor, rec_tab));
    }
    return res;
  }

  MFormula f_;
  absl::flat_hash_map<ResT, std::size_t> res_card_;
};
