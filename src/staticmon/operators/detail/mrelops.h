#pragma once
#include <cstdint>
#include <staticmon/common/mp_helpers.h>
#include <staticmon/common/table.h>
#include <staticmon/operators/detail/mterm.h>
#include <staticmon/operators/detail/operator_types.h>
#include <tuple>
#include <type_traits>

template<typename T>
auto make_empty_tabs(std::size_t n) {
  using res_tab_t = table_util::tab_t_of_row_t<T>;
  std::vector<std::optional<res_tab_t>> res;
  res.reserve(n);
  for (std::size_t i = 0; i < n; ++i)
    res.emplace_back();
  return res;
}

inline auto make_unit_tabs(std::size_t n) {
  using res_tab_t = table_util::table<>;
  std::vector<std::optional<res_tab_t>> res;
  res.reserve(n);
  for (std::size_t i = 0; i < n; ++i)
    res.emplace_back(table_util::unit_table());
  return res;
}

template<typename T>
auto make_singleton_table(const T &val, std::size_t n) {
  using res_tab_t = table_util::table<T>;
  std::vector<std::optional<res_tab_t>> res;
  res.reserve(n);
  for (std::size_t i = 0; i < n; ++i) {
    T val_cp = val;
    res.emplace_back(table_util::singleton_table(std::move(val_cp)));
  }
  return res;
}

struct memptyrel {
  using ResL = mp_list<>;
  using ResT = std::tuple<>;

  auto eval(database &, const ts_list &ts) {
    return make_empty_tabs<ResT>(ts.size());
  }
};

template<typename MFormula>
struct mneg {
  using ResL = mp_list<>;
  using ResT = std::tuple<>;

  auto eval(database &db, const ts_list &ts) {
    auto rec_tabs = f_.eval(db, ts);

    std::vector<std::optional<table_util::table<>>> res_tabs;
    for (const auto &tab : rec_tabs) {
      if (tab) {
        assert(!tab->empty());
        res_tabs.emplace_back();
      } else {
        res_tabs.emplace_back(table_util::unit_table());
      }
    }
    return res_tabs;
  }

  MFormula f_;
};

template<typename Term1, typename Term2>
struct mequal {
  static_assert(always_false_v<mp_list<Term1, Term2>>,
                "invalid terms for mequal");
};

template<typename Cst1, typename Cst2>
struct mequal<tcst<Cst1>, tcst<Cst2>> {
  static constexpr auto cst1 = Cst1::value;
  static constexpr auto cst2 = Cst2::value;

  static_assert(std::is_same_v<decltype(cst1), decltype(cst2)>,
                "type mismatch");

  using ResL = mp_list<>;
  using ResT = std::tuple<>;

  auto eval(database &, const ts_list &ts) {
    if constexpr (cst1 == cst2)
      return make_unit_tabs(ts.size());
    else
      return make_empty_tabs<ResT>(ts.size());
  }
};

template<std::size_t var, typename Cst>
struct mequal<tvar<var>, tcst<Cst>> {
  static constexpr auto cst = Cst::value;

  using ResL = mp_list_c<std::size_t, var>;
  using ResT = std::tuple<std::remove_cv_t<decltype(cst)>>;

  auto eval(database &, const ts_list &ts) {
    return make_singleton_table(cst, ts.size());
  }
};

template<std::size_t var, typename Cst>
struct mequal<tcst<Cst>, tvar<var>> : mequal<tvar<var>, tcst<Cst>> {};
