#pragma once
#include <boost/mp11.hpp>
#include <staticmon/common/mp_helpers.h>

using namespace boost::mp11;

struct inf_bound;

template<std::size_t lower_bound>
struct lower_bound_ops {
  static constexpr bool geq_lower(std::size_t p) { return p >= lower_bound; }

  static constexpr bool lt_lower(std::size_t n) { return n < lower_bound; }
};

template<typename LBound, typename UBound>
struct minterval {
  static_assert(always_false_v<mp_list<LBound, UBound>>, "incorrect bounds");
};

template<std::size_t lower_bound, std::size_t upper_bound>
struct minterval<mp_size_t<lower_bound>, mp_size_t<upper_bound>>
    : lower_bound_ops<lower_bound> {
  static constexpr bool is_infinite = false;

  static constexpr bool contains(std::size_t p) {
    return p >= lower_bound && p <= upper_bound;
  }
};

template<std::size_t lower_bound>
struct minterval<mp_size_t<lower_bound>, inf_bound>
    : lower_bound_ops<lower_bound> {
  static constexpr bool is_infinite = true;

  static constexpr bool contains(std::size_t p) { return p >= lower_bound; }

  static constexpr bool leq_upper(std::size_t) { return true; }

  static constexpr bool gt_upper(std::size_t) { return false; }
};
