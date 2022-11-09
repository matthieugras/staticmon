#pragma once
#include <absl/hash/hash.h>
#include <boost/mp11.hpp>
#include <boost/variant2.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <immer/set.hpp>
#include <immer/set_transient.hpp>
#include <optional>
#include <staticmon/common/table.h>
#include <staticmon/common/util.h>
#include <type_traits>

using namespace boost::mp11;

using mem_policy =
  immer::memory_policy<immer::default_heap_policy,
                       immer::unsafe_refcount_policy, immer::no_lock_policy>;
template<typename T>
using persistent_table =
  immer::set<T, absl::Hash<T>, std::equal_to<T>, mem_policy>;

template<typename Tab>
struct table_diff {
  Tab pos;
  Tab neg;

  void swap(table_diff<Tab> &other) {
    pos.swap(other.pos);
    neg.swap(other.neg);
  }

  void clear() {
    pos.clear();
    neg.clear();
  }
};

template<typename Tab>
struct fmt::formatter<table_diff<Tab>> : trivial_parser {
  template<typename FormatContext>
  auto format
    [[maybe_unused]] (const table_diff<Tab> &arg, FormatContext &ctx) const
    -> decltype(auto) {
    return fmt::format_to(ctx.out(), "(table_diff (pos:{}, neg:{}))", arg.pos,
                          arg.neg);
  }
};

template<typename Tab>
using maybe_table_diff =
  boost::variant2::variant<std::optional<Tab>, table_diff<Tab>>;

template<typename Tab>
struct fmt::formatter<maybe_table_diff<Tab>> : trivial_parser {
  template<typename FormatContext>
  auto format [[maybe_unused]] (const maybe_table_diff<Tab> &arg,
                                FormatContext &ctx) const -> decltype(auto) {
    auto visitor = [&ctx](const auto &arg) {
      using T = std::remove_cvref_t<decltype(arg)>;
      if constexpr (std::is_same_v<T, std::optional<Tab>>) {
        return fmt::format_to(ctx.out(), "(table: {})", arg);
      } else {
        static_assert(std::is_same_v<T, table_diff<Tab>>);
        return fmt::format_to(ctx.out(), "{}", arg);
      }
    };
    return boost::variant2::visit(visitor, arg);
  }
};

template<typename MFormula>
using return_type_selector =
  mp_if_c<MFormula::DiffRes,
          maybe_table_diff<table_util::tab_t_of_row_t<typename MFormula::ResT>>,
          std::optional<table_util::tab_t_of_row_t<typename MFormula::ResT>>>;

template<typename Tab>
void advance_tab(Tab &t, table_diff<Tab> &diff) {
  for (auto &r : diff.pos)
    t.emplace(std::move(r));
  for (const auto &r : diff.neg)
    t.erase(r);
}

template<typename T>
persistent_table<T>
advance_persistent_tab(persistent_table<T> t,
                       table_diff<table_util::tab_t_of_row_t<T>> &diff) {
  auto new_tab_trans = std::move(t).transient();
  for (auto &row : diff.pos)
    new_tab_trans.insert(std::move(row));
  for (const auto &row : diff.neg)
    new_tab_trans.erase(row);
  return std::move(new_tab_trans).persistent();
}

template<typename Tab>
table_diff<Tab> compute_diff(const Tab &t0, const Tab &t1) {
  table_diff<Tab> res;
  for (const auto &row : t0) {
    if (!t1.contains(row))
      res.neg.insert(row);
  }

  for (const auto &row : t1) {
    if (!t0.contains(row))
      res.pos.insert(row);
  }

  return res;
}

template<typename T>
struct singleton_diff_buf {
  using res_tab_t = table_util::tab_t_of_row_t<T>;

  void apply_diff(boost::variant2::variant<std::optional<res_tab_t>,
                                           table_diff<res_tab_t>> &diff) {
    auto visitor = [this](auto &arg) {
      using T2 = std::remove_cvref_t<decltype(arg)>;
      if constexpr (std::is_same_v<T2, table_diff<res_tab_t>>) {
        if (!last_tab_)
          last_tab_.emplace();
        advance_tab(*last_tab_, arg);
        if (last_tab_->empty())
          last_tab_.reset();
      } else {
        static_assert(std::is_same_v<T2, std::optional<res_tab_t>>,
                      "unknown type");
        last_tab_ = std::move(arg);
      }
    };
    boost::variant2::visit(visitor, diff);
  }

  const std::optional<res_tab_t> &get_last_tab() const { return last_tab_; }

  std::optional<res_tab_t> last_tab_;
};
