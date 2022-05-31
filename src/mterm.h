#pragma once
#include <boost/mp11.hpp>
#include <cstdint>
#include <mp_helpers.h>
#include <string>
#include <string_view>

using namespace boost::mp11;

template<typename L, typename T, typename Term>
using t_res_t = typename Term::template ResT<L, T>;

template<typename VarId>
struct tvar {
  template<typename L>
  using var_idx = mp_find<L, VarId>;

  template<typename L, typename T>
  using ResT = mp_at<T, var_idx<L>>;

  template<typename L, typename T>
  static ResT<L, T> eval(const T &row) {
    return std::get<var_idx<L>::value>(row);
  }
};

template<typename Cst>
struct tcst {
  using value_type = typename Cst::value_type;

  template<typename L, typename T>
  using ResT = mp_if_c<std::is_same_v<value_type, std::string_view>,
                       std::string, value_type>;

  template<typename L, typename T>
  static ResT<L, T> eval(const T &) {
    static_assert(
      std::disjunction_v<std::is_same<value_type, double>,
                         std::is_same<value_type, std::int64_t>,
                         std::is_same<value_type, std::string_view>>,
      "unknown constant type");
    if constexpr (std::is_same_v<ResT<L, T>, std::string_view>)
      return std::string(Cst::value);
    else {
      return Cst::value;
    }
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
  using ResT = t_res_t<L, T, Term1>;

  template<typename L, typename T>
  static ResT<L, T> eval(const T &row) {
    using t1_res_t = t_res_t<L, T, Term1>;
    using t2_res_t = t_res_t<L, T, Term2>;
    static_assert(std::is_same_v<t1_res_t, t2_res_t>,
                  "computed types are not equal");
    auto rec_res1 = Term1::template eval<L, T>(row);
    auto rec_res2 = Term2::template eval<L, T>(row);
    static_assert(std::is_same_v<decltype(rec_res1), t1_res_t>,
                  "rec_res1 has wrong type");
    static_assert(std::is_same_v<decltype(rec_res2), t2_res_t>,
                  "rec_res2 has wrong type");
    return rec_res1 + rec_res2;
  }
};

template<typename Term1, typename Term2>
struct tminus {
  template<typename L, typename T>
  using ResT = t_res_t<L, T, Term1>;

  template<typename L, typename T>
  static ResT<L, T> eval(const T &row) {
    using t1_res_t = t_res_t<L, T, Term1>;
    using t2_res_t = t_res_t<L, T, Term2>;
    static_assert(std::is_same_v<t1_res_t, t2_res_t>,
                  "computed types are not equal");
    auto rec_res1 = Term1::template eval<L, T>(row);
    auto rec_res2 = Term2::template eval<L, T>(row);
    static_assert(std::is_same_v<decltype(rec_res1), t1_res_t>,
                  "rec_res1 has wrong type");
    static_assert(std::is_same_v<decltype(rec_res2), t2_res_t>,
                  "rec_res2 has wrong type");
    return rec_res1 - rec_res2;
  }
};

template<typename Term1, typename Term2>
struct tmult {
  template<typename L, typename T>
  using ResT = t_res_t<L, T, Term1>;

  template<typename L, typename T>
  static ResT<L, T> eval(const T &row) {
    using t1_res_t = t_res_t<L, T, Term1>;
    using t2_res_t = t_res_t<L, T, Term2>;
    static_assert(std::is_same_v<t1_res_t, t2_res_t>,
                  "computed types are not equal");
    auto rec_res1 = Term1::template eval<L, T>(row);
    auto rec_res2 = Term2::template eval<L, T>(row);
    static_assert(std::is_same_v<decltype(rec_res1), t1_res_t>,
                  "rec_res1 has wrong type");
    static_assert(std::is_same_v<decltype(rec_res2), t2_res_t>,
                  "rec_res2 has wrong type");
    return rec_res1 * rec_res2;
  }
};

template<typename Term1, typename Term2>
struct tmod {
  template<typename L, typename T>
  using ResT = t_res_t<L, T, Term1>;

  template<typename L, typename T>
  static ResT<L, T> eval(const T &row) {
    using t1_res_t = t_res_t<L, T, Term1>;
    using t2_res_t = t_res_t<L, T, Term2>;
    static_assert(std::is_same_v<t1_res_t, t2_res_t>,
                  "computed types are not equal");
    auto rec_res1 = Term1::template eval<L, T>(row);
    auto rec_res2 = Term2::template eval<L, T>(row);
    static_assert(std::is_same_v<decltype(rec_res1), t1_res_t>,
                  "rec_res1 has wrong type");
    static_assert(std::is_same_v<decltype(rec_res2), t2_res_t>,
                  "rec_res2 has wrong type");
    return rec_res1 % rec_res2;
  }
};

template<typename Term1, typename Term2>
struct tdiv {
  template<typename L, typename T>
  using ResT = t_res_t<L, T, Term1>;

  template<typename L, typename T>
  static ResT<L, T> eval(const T &row) {
    using t1_res_t = t_res_t<L, T, Term1>;
    using t2_res_t = t_res_t<L, T, Term2>;
    static_assert(std::is_same_v<t1_res_t, t2_res_t>,
                  "computed types are not equal");
    auto rec_res1 = Term1::template eval<L, T>(row);
    auto rec_res2 = Term2::template eval<L, T>(row);
    static_assert(std::is_same_v<decltype(rec_res1), t1_res_t>,
                  "rec_res1 has wrong type");
    static_assert(std::is_same_v<decltype(rec_res2), t2_res_t>,
                  "rec_res2 has wrong type");
    return rec_res1 / rec_res2;
  }
};
