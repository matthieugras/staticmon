#pragma once
#include <boost/mp11.hpp>
#include <mp_helpers.h>
#include <optional>
#include <string>
#include <string_view>
#include <table.h>
#include <tuple>

using namespace boost::mp11;

template<typename L, typename VarL>
using get_var_idx = mp_transform_q<mp_bind<mp_find, L, _1>, VarL>;

template<typename VarId>
struct tvar {
  template<typename L, typename T>
  using var_idx = mp_find<L, VarId>;

  template<typename L, typename T>
  using ResT = mp_at<T, var_idx<L, T>>;

  template<typename L, typename T>
  static ResT<L, T> eval(const T &row) {
    return std::get<var_idx<L, T>::value>(row);
  }
};

template<typename Cst>
struct tcst {
  template<typename L, typename T>
  using ResT = decltype(Cst::value);

  template<typename L, typename T>
  static ResT<L, T> eval(const T &) {
    if constexpr (std::is_same_v<ResT<L, T>, std::string_view>)
      return std::string(Cst::value);
    else
      return Cst::value;
  }
};

template<typename Term>
struct tf2i {
  template<typename L, typename T>
  using ResT = std::int64_t;

  template<typename L, typename T>
  static std::int64_t eval(const T &row) {
    auto rec_res = Term::template eval<L, T>(row);
    static_assert(std::is_same_v<decltype(rec_res), double>,
                  "nested term of f2i must be float");
    return static_cast<std::int64_t>(rec_res);
  }
};

template<typename Term>
struct ti2f {
  template<typename L, typename T>
  using ResT = double;

  template<typename L, typename T>
  static double eval(const T &row) {
    auto rec_res = Term::template eval<L, T>(row);
    static_assert(std::is_same_v<decltype(rec_res), std::int64_t>,
                  "nested term of f2i must be float");
    return static_cast<double>(rec_res);
  }
};

template<typename Term>
struct tuminus {
  template<typename L, typename T>
  using ResT = typename Term::template result_info<L, T>::ResT;

  template<typename L, typename T>
  static ResT<L, T> eval(const T &row) {
    auto rec_res = Term::template eval<L, T>(row);
    static_assert(std::is_same_v<decltype(rec_res), ResT<L, T>>,
                  "nested term does not match computed type");
    return -rec_res;
  }
};

template<typename Term1, typename Term2>
struct tplus {
  template<typename L, typename T>
  using t1_res_t = typename Term1::template result_info<L, T>::ResT;

  template<typename L, typename T>
  using t2_res_t = typename Term2::template result_info<L, T>::ResT;

  template<typename L, typename T>
  using ResT = t1_res_t<L, T>;

  template<typename L, typename T>
  static ResT<L, T> eval(const T &row) {
    static_assert(std::is_same_v<t1_res_t<L, T>, t2_res_t<L, T>>,
                  "computed types are not equal");
    auto rec_res1 = Term1::template eval<L, T>(row);
    auto rec_res2 = Term2::template eval<L, T>(row);
    static_assert(std::is_same_v<decltype(rec_res1), t1_res_t<L, T>>,
                  "rec_res1 has wrong type");
    static_assert(std::is_same_v<decltype(rec_res2), t2_res_t<L, T>>,
                  "rec_res2 has wrong type");
    return rec_res1 + rec_res2;
  }
};

template<typename Term1, typename Term2>
struct tminus {
  template<typename L, typename T>
  using t1_res_t = typename Term1::template result_info<L, T>::ResT;

  template<typename L, typename T>
  using t2_res_t = typename Term2::template result_info<L, T>::ResT;

  template<typename L, typename T>
  using ResT = t1_res_t<L, T>;

  template<typename L, typename T>
  static ResT<L, T> eval(const T &row) {
    static_assert(std::is_same_v<t1_res_t<L, T>, t2_res_t<L, T>>,
                  "computed types are not equal");
    auto rec_res1 = Term1::template eval<L, T>(row);
    auto rec_res2 = Term2::template eval<L, T>(row);
    static_assert(std::is_same_v<decltype(rec_res1), t1_res_t<L, T>>,
                  "rec_res1 has wrong type");
    static_assert(std::is_same_v<decltype(rec_res2), t2_res_t<L, T>>,
                  "rec_res2 has wrong type");
    return rec_res1 - rec_res2;
  }
};

template<typename Term1, typename Term2>
struct tmult {
  template<typename L, typename T>
  using t1_res_t = typename Term1::template result_info<L, T>::ResT;

  template<typename L, typename T>
  using t2_res_t = typename Term2::template result_info<L, T>::ResT;

  template<typename L, typename T>
  using ResT = t1_res_t<L, T>;

  template<typename L, typename T>
  static ResT<L, T> eval(const T &row) {
    static_assert(std::is_same_v<t1_res_t<L, T>, t2_res_t<L, T>>,
                  "computed types are not equal");
    auto rec_res1 = Term1::template eval<L, T>(row);
    auto rec_res2 = Term2::template eval<L, T>(row);
    static_assert(std::is_same_v<decltype(rec_res1), t1_res_t<L, T>>,
                  "rec_res1 has wrong type");
    static_assert(std::is_same_v<decltype(rec_res2), t2_res_t<L, T>>,
                  "rec_res2 has wrong type");
    return rec_res1 * rec_res2;
  }
};

template<typename Term1, typename Term2>
struct tmod {
  template<typename L, typename T>
  using t1_res_t = typename Term1::template result_info<L, T>::ResT;

  template<typename L, typename T>
  using t2_res_t = typename Term2::template result_info<L, T>::ResT;

  template<typename L, typename T>
  using ResT = t1_res_t<L, T>;

  template<typename L, typename T>
  static ResT<L, T> eval(const T &row) {
    static_assert(std::is_same_v<t1_res_t<L, T>, t2_res_t<L, T>>,
                  "computed types are not equal");
    auto rec_res1 = Term1::template eval<L, T>(row);
    auto rec_res2 = Term2::template eval<L, T>(row);
    static_assert(std::is_same_v<decltype(rec_res1), t1_res_t<L, T>>,
                  "rec_res1 has wrong type");
    static_assert(std::is_same_v<decltype(rec_res2), t2_res_t<L, T>>,
                  "rec_res2 has wrong type");
    return rec_res1 % rec_res2;
  }
};

template<typename Term1, typename Term2>
struct tdiv {
  template<typename L, typename T>
  using t1_res_t = typename Term1::template result_info<L, T>::ResT;

  template<typename L, typename T>
  using t2_res_t = typename Term2::template result_info<L, T>::ResT;

  template<typename L, typename T>
  using ResT = t1_res_t<L, T>;

  template<typename L, typename T>
  static ResT<L, T> eval(const T &row) {
    static_assert(std::is_same_v<t1_res_t<L, T>, t2_res_t<L, T>>,
                  "computed types are not equal");
    auto rec_res1 = Term1::template eval<L, T>(row);
    auto rec_res2 = Term2::template eval<L, T>(row);
    static_assert(std::is_same_v<decltype(rec_res1), t1_res_t<L, T>>,
                  "rec_res1 has wrong type");
    static_assert(std::is_same_v<decltype(rec_res2), t2_res_t<L, T>>,
                  "rec_res2 has wrong type");
    return rec_res1 / rec_res2;
  }
};

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
  using t1_res_t = typename Term1::template result_info<L, T>::ResT;

  template<typename L, typename T>
  using t2_res_t = typename Term2::template result_info<L, T>::ResT;

  template<typename L, typename T>
  static std::optional<T> eval(T row) {
    static_assert(std::is_same_v<t1_res_t<L, T>, t2_res_t<L, T>>,
                  "terms in constraints must have same type");
    constexpr bool is_neg = IsNeg::value;
    auto res1 = Term1::template eval<L, T>(row);
    auto res2 = Term2::template eval<L, T>(row);
    static_assert(std::is_same_v<decltype(res1), t1_res_t<L, T>>,
                  "term does not match computed type");
    static_assert(std::is_same_v<decltype(res1), t2_res_t<L, T>>,
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
    using ResT = mp_push_back<T, typename Term::template ResT<L, T>>;
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
    using ResL = typename Op::template result_info<L, T>::ResL;
    using ResT = typename Op::template result_info<L, T>::ResT;
  };

  template<typename L, typename T>
  static std::optional<typename result_info<L, T>::ResT> evals(T row) {
    return Op::template eval<L, T>(std::move(row));
  }
};

template<typename Op1, typename Op2, typename... Ops>
struct simpleops<Op1, Op2, Ops...> {
  template<typename L, typename T>
  using Op1ResL = typename Op1::template result_info<L, T>::ResL;
  template<typename L, typename T>
  using Op1ResT = typename Op1::template result_info<L, T>::ResT;

  template<typename L, typename T>
  struct result_info {
    using ResL = typename simpleops<Op2, Ops...>::template result_info<
      Op1ResL<L, T>, Op1ResT<L, T>>::ResL;
    using ResT = typename simpleops<Op2, Ops...>::template result_info<
      Op1ResL<L, T>, Op1ResT<L, T>>::ResT;
  };

  template<typename L, typename T>
  static std::optional<typename result_info<L, T>::ResT> evals(T row) {
    std::optional<Op1ResT<L, T>> op1_res =
      Op1::template eval<L, T>(std::move(row));
    if (op1_res)
      return simpleops<Op2, Ops...>::template eval<Op1ResL<L, T>,
                                                   Op1ResT<L, T>>(
        std::move(op1_res));
    else
      return std::nullopt;
  }
};

template<typename Ops, typename Formula>
struct mfusedsimpleop {};
