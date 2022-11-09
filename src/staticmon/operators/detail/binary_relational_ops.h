#pragma once
#include <absl/container/flat_hash_map.h>
#include <boost/container/devector.hpp>
#include <boost/mp11.hpp>
#include <boost/variant2.hpp>
#include <cassert>
#include <cstdint>
#include <iterator>
#include <staticmon/common/table.h>
#include <staticmon/common/table_diff.h>
#include <staticmon/operators/detail/binary_buffer.h>
#include <staticmon/operators/detail/operator_types.h>
#include <type_traits>
#include <utility>
#include <vector>

using namespace boost::mp11;

struct and_not_tag;
struct and_tag;
struct or_tag;

template<typename MFormula1, typename MFormula2>
struct mor_base {
  using L1 = typename MFormula1::ResL;
  using L2 = typename MFormula2::ResL;
  using T1 = typename MFormula1::ResT;
  using T2 = typename MFormula2::ResT;
  using ResT = T1;
  using ResL = L1;

  using tab1_t = table_util::tab_t_of_row_t<T1>;
  using tab2_t = table_util::tab_t_of_row_t<T2>;

  std::optional<tab1_t> eval_tab(std::optional<tab1_t> &opt_tab1,
                                 std::optional<tab2_t> &opt_tab2) {
    tab1_t tab1;
    tab2_t tab2;
    if (!opt_tab1 && !opt_tab2)
      return std::nullopt;
    else if (!opt_tab2)
      return std::move(opt_tab1);
    else if (opt_tab1)
      tab1 = std::move(*opt_tab1);
    tab2 = std::move(*opt_tab2);
    tab1.reserve(tab1.size() + tab2.size());
    if constexpr (std::is_same_v<L1, L2>) {
      static_assert(std::is_same_v<T1, T2>, "layouts same but not row types");
      tab1.merge(tab2);
    } else {
      using reorder_mask = table_util::get_reorder_mask<L2, L1>;
      static_assert(std::is_same_v<mp_apply_idxs<T2, reorder_mask>, T1>,
                    "internal error, reorder mask incorrect");
      for (auto &row : tab2)
        tab1.emplace(project_row<reorder_mask>(std::move(row)));
    }
    return tab1;
  }
};

template<typename MFormula2, bool not_diff_r>
struct mor_last_buffer {};

template<typename MFormula2>
struct mor_last_buffer : mor_last_buffer<MFormula2, true> {
  using L2 = typename MFormula2::ResL;
  using tab2_t = table_util::tab_t_of_row_t<T2>;

  std::optional<tab2_t> last_r_;
};

template<typename MFormula1, typename MFormula2, bool not_diff_r>
struct mor_diff_base : mor_last_buffer<MFormula2, not_diff_r> {
  using L1 = typename MFormula1::ResL;
  using L2 = typename MFormula2::ResL;
  using T1 = typename MFormula1::ResT;
  using T2 = typename MFormula2::ResT;
  using ResT = T1;
  using ResL = L1;

  using tab1_t = table_util::tab_t_of_row_t<T1>;
  using tab2_t = table_util::tab_t_of_row_t<T2>;

  void add_row1(const T1 &row, table_diff<tab1_t> &diff) {
    auto &e = last_res_[row];
    assert(!e.first);
    e.first = true;
    if (!e.second)
      diff.pos.emplace(row);
  }

  void add_row2(const T1 &row, table_diff<tab1_t> &diff) {
    auto &e = last_res_[row];
    assert(!e.second);
    e.second = true;
    if (!e.first)
      diff.pos.emplace(row);
  }

  void remove_row1(const T1 &row, table_diff<tab1_t> &diff) {
    auto it = last_res_.find(row);
    assert(it != last_res_.end() && it->second.first == true);
    if (!it->second.second) {
      diff.neg.emplace(diff.first);
      last_res_.erase(it);
    } else {
      it->second.first = false;
    }
  }

  void remove_row2(const T1 &row, table_diff<tab1_t> &diff) {
    auto it = last_res_.find(row);
    assert(it != last_res_.end() && it->second.second == true);
    if (!it->second.first) {
      diff.neg.emplace(diff.first);
      last_res_.erase(it);
    } else {
      it->second.second = false;
    }
  }

  tab1_t convert_tab2(tab2_t &tab2) {
    if constexpr (std::is_same_v<L1, L2>) {
      return std::move(tab2);
    } else {
      using reorder_mask = table_util::get_reorder_mask<L2, L1>;
      static_assert(std::is_same_v<mp_apply_idxs<T2, reorder_mask>, T1>,
                    "internal error, reorder mask incorrect");
      tab1_t tab2_proj;
      tab2_proj.reserve(tab2->size());
      for (auto &row : tab2_proj)
        tab2_proj.emplace(project_row<reorder_mask>(std::move(row)));
      return tab2_proj;
    }
  }

  tab1_t add_two_tabs(std::optional<tab1_t> &tab1,
                      std::optional<tab2_t> &tab2) {
    last_res_.clear();
    if (!tab1 && !tab2)
      return std::nullopt;
    tab1_t res;
    if (tab1) {
      for (const auto &row : *tab1)
        last_res_[row].first = true;
      res = std::move(*tab1);
    }
    this->last_r_.reset();
    if (tab2) {
      auto tab2_proj = convert_tab2(*tab2);
      if constexpr (not_diff_r)
        this->last_r_.emplace(tab2_proj);
      for (const auto &row : tab2_proj)
        last_res_[row].second = true;
      res.merge(tab2_proj);
    }
    return res;
  }

  absl::flat_hash_map<T1, std::pair<bool, bool>> last_res_;
};

template<typename MFormula1, typename MFormula2>
struct mor_mixin : mor_base<MFormula1, MFormula2> {
  static constexpr bool DiffRes = false;
};

template<typename MFormula1, typename MFormula2>
requires(MFormula1::DiffRes &&
         !MFormula2::DiffRes) struct mor_mixin<MFormula1, MFormula2>
    : mor_diff_base<MFormula1, MFormula2, true> {
  using OrBase = typename mor_mixin::mor_base;

  using L1 = typename MFormula1::ResL;
  using L2 = typename MFormula2::ResL;
  using T1 = typename MFormula1::ResT;
  using T2 = typename MFormula2::ResT;
  using ResT = T1;
  using ResL = L1;
  using res_tab_t = maybe_table_diff<table_util::tab_t_of_row_t<ResT>>;
  using tab1_t = table_util::tab_t_of_row_t<T1>;
  using tab2_t = table_util::tab_t_of_row_t<T2>;
  using diff_t =
    boost::variant2::variant<std::optional<tab1_t>, table_diff<tab1_t>>;

  res_tab_t eval_tab(diff_t &data1, std::optional<tab2_t> &tab2) {
    auto visitor = [&tab2, this](auto &tab1) -> diff_t {
      using T = std::remove_cvref_t<decltype(tab1)>;
      if constexpr (std::is_same_v<T, std::optional<tab1_t>>) {
          return add_two_tabs(tab1, tab2);
      } else {
        static_assert(std::is_same_v<T, table_diff<tab1_t>>);
        table_diff<tab1_t> diff;
        for (const auto &row : tab1.pos)
          add_row1(row, diff);
        if (!tab2) {
          for (const auto &row : last_r_)
            remove_row2(row, diff);
          last_r_.clear();
        } else {
          auto tab2_proj = convert_tab2(*tab2);
          auto tab2_diff = compute_diff(last_r_, tab2_proj);
          for (const auto &row : tab2_diff.pos)
            add_row2(row, diff);
          for (const auto &row : tab2_diff.neg)
            remove_row2(row, diff);
          last_r_ = std::move(tab2_proj);
        }
        for (const auto &row : tab1.neg)
          remove_row1(row, diff);
        return diff;
      }
    };
    boost::variant2::visit(visitor, data1);
  }

  absl::flat_hash_map<T1, std::pair<bool, bool>> last_res_;
  tab1_t last_r_;
};

template<typename MFormula1, typename MFormula2>
requires(!MFormula1::DiffRes &&
         MFormula2::DiffRes) struct mor_mixin<MFormula1, MFormula2>
    : mor_mixin<MFormula2, MFormula1> {
};

template<typename MFormula1, typename MFormula2>
requires(MFormula1::DiffRes
           &&MFormula2::DiffRes) struct mor_mixin<MFormula1, MFormula2>
    : mor_diff_base<MFormula1, MFormula2, false>,
      mor_base<MFormula1, MFormula2> {
  using OrBase = typename mor_mixin::mor_base;

  using L1 = typename MFormula1::ResL;
  using L2 = typename MFormula2::ResL;
  using T1 = typename MFormula1::ResT;
  using T2 = typename MFormula2::ResT;
  using ResT = T1;
  using ResL = L1;
  using res_tab_t = maybe_table_diff<table_util::tab_t_of_row_t<ResT>>;
  using tab1_t = table_util::tab_t_of_row_t<T1>;
  using tab2_t = table_util::tab_t_of_row_t<T2>;
  using diff1_t =
    boost::variant2::variant<std::optional<tab1_t>, table_diff<tab1_t>>;
  using diff2_t =
    boost::variant2::variant<std::optional<tab2_t>, table_diff<tab2_t>>;

  res_tab_t eval_tab(diff1_t &data1, diff2_t &data2) {
    using reorder_mask = table_util::get_reorder_mask<L2, L1>;
    static_assert(std::is_same_v<mp_apply_idxs<T2, reorder_mask>, T1>,
                  "internal error, reorder mask incorrect");
    auto visitor = [this](auto &tab1, auto &tab2) {
      using S1 = std::remove_cvref_t<decltype(tab1)>;
      using S2 = std::remove_cvref_t<decltype(tab2)>;
      if constexpr (std::is_same_v<S1, std::optional<tab1_t>> &&
                    std::is_same_v<S2, std::optional<tab2_t>>) {
          return add_two_tabs(tab1, tab2);
      } else if constexpr (!std::is_same_v<S1, std::optional<tab1_t>> &&
                           std::is_same_v<S2, std::optional<tab2_t>>) {
        static_assert(std::is_same_v<S1, table_diff<tab1_t>>);
        table_diff<tab1_t> diff;
        for (const auto &row : tab1.pos)
          add_row1(row, diff);
        for (auto &row : tab2.pos)
          add_row2(project_row<reorder_mask>(std::move(row)), diff);
        for (const auto &row : tab1.neg)
          remove_row1(row, diff);
        for (auto &row : tab2.neg)
          remove_row2(project_row<reorder_mask>(std::move(row)), diff);
        return diff;
      } else if constexpr (std::is_same_v<S1, std::optional<tab1_t>> &&
                           !std::is_same_v<S2, std::optional<tab2_t>>) {
        static_assert(std::is_same_v<S2, table_diff<tab2_t>>);
        table_diff<tab1_t> diff;
        for (const auto &row : tab1.pos)
          add_row1(row, diff);
        for (auto &row : tab2.pos)
          add_row2(project_row<reorder_mask>(std::move(row)), diff);
        for (const auto &row : tab1.neg)
          remove_row1(row, diff);
        for (auto &row : tab2.neg)
          remove_row2(project_row<reorder_mask>(std::move(row)), diff);
        return diff;
      } else {
        static_assert(std::is_same_v<S1, table_diff<tab1_t>> &&
                      std::is_same_v<S2, table_diff<tab2_t>>);
        table_diff<tab1_t> diff;
        for (const auto &row : tab1.pos)
          add_row1(row, diff);
        for (auto &row : tab2.pos)
          add_row2(project_row<reorder_mask>(std::move(row)), diff);
        for (const auto &row : tab1.neg)
          remove_row1(row, diff);
        for (auto &row : tab2.neg)
          remove_row2(project_row<reorder_mask>(std::move(row)), diff);
        return diff;
      }
    };
  }
};

template<typename MFormula1, typename MFormula2>
struct mor_bla {
  using tab1_t = return_type_selector<MFormula1>;
  using tab2_t = return_type_selector<MFormula2>;

  auto eval(database &db, const ts_list &ts) {}

  MFormula1 f1_, f2_;
  bin_op_buffer<tab1_t, tab2_t> buf_;
};

template<typename Tag, typename MFormula1, typename MFormula2>
struct bin_rel_op {
  using T1 = typename MFormula1::ResT;
  using L1 = typename MFormula1::ResL;

  using T2 = typename MFormula2::ResT;
  using L2 = typename MFormula2::ResL;

  using OpRes =
    mp_if<std::is_same<Tag, and_tag>,
          typename table_util::join_result_info<L1, L2, T1, T2>,
          mp_if<std::is_same<Tag, or_tag>,
                typename table_util::union_result_info<L1, L2, T1, T2>,
                mp_if<std::is_same<Tag, and_not_tag>,
                      table_util::anti_join_info<L1, L2, T1, T2>, void>>>;

  static_assert(!std::is_void_v<OpRes>, "invalid tag");

  using ResL = typename OpRes::ResL;
  using ResT = typename OpRes::ResT;

  using res_tab_t = table_util::tab_t_of_row_t<ResT>;
  using rec_tab1_t = table_util::tab_t_of_row_t<T1>;
  using rec_tab2_t = table_util::tab_t_of_row_t<T2>;

  std::vector<std::optional<res_tab_t>> eval(database &db, const ts_list &ts) {
    auto rec_res1 = f1_.eval(db, ts);
    auto rec_res2 = f2_.eval(db, ts);
    if constexpr (std::is_same_v<Tag, and_tag>) {
      return buf_.update_and_reduce(
        rec_res1, rec_res2,
        [](std::optional<rec_tab1_t> &tab1,
           std::optional<rec_tab2_t> &tab2) -> std::optional<res_tab_t> {
          if (!tab1 || !tab2)
            return std::nullopt;
          assert(!tab1->empty() && !tab2->empty());
          auto joined = table_util::table_join<L1, L2>(*tab1, *tab2);
          if (joined.empty())
            return std::nullopt;
          else
            return joined;
        });
    } else if constexpr (std::is_same_v<Tag, or_tag>) {
      return buf_.update_and_reduce(
        rec_res1, rec_res2,
        [](std::optional<rec_tab1_t> &tab1,
           std::optional<rec_tab2_t> &tab2) -> std::optional<res_tab_t> {
          if (!tab2)
            return std::move(tab1);
          assert(!tab2->empty());
          res_tab_t unioned;
          if (!tab1) {
            unioned = table_util::table_union<L1, L2>(rec_tab1_t(), *tab2);
          } else {
            assert(!tab1->empty());
            unioned = table_util::table_union<L1, L2>(std::move(*tab1), *tab2);
          }
          if (unioned.empty())
            return std::nullopt;
          else
            return unioned;
        });
    } else if constexpr (std::is_same_v<Tag, and_not_tag>) {
      return buf_.update_and_reduce(
        rec_res1, rec_res2,
        [](std::optional<rec_tab1_t> &tab1,
           std::optional<rec_tab2_t> &tab2) -> std::optional<res_tab_t> {
          if (!tab1)
            return std::nullopt;
          assert(!tab1->empty());
          if (!tab2)
            return std::move(*tab1);
          assert(!tab2->empty());
          auto ajoined = table_util::table_anti_join<L1, L2>(*tab1, *tab2);
          if (ajoined.empty())
            return std::nullopt;
          else
            return ajoined;
        });
    } else {
      static_assert(always_false_v<Tag>, "cannot happen");
    }
  }

  bin_op_buffer<std::optional<rec_tab1_t>, std::optional<rec_tab2_t>> buf_;
  MFormula1 f1_;
  MFormula2 f2_;
};


template<bool is_anti_join, typename MFormula1, typename MFormula2>
struct mand;

template<typename MFormula1, typename MFormula2>
struct mand<true, MFormula1, MFormula2>
    : bin_rel_op<and_not_tag, MFormula1, MFormula2> {};

template<typename MFormula1, typename MFormula2>
struct mand<false, MFormula1, MFormula2>
    : bin_rel_op<and_tag, MFormula1, MFormula2> {};

template<typename MFormula1, typename MFormula2>
struct mor : bin_rel_op<or_tag, MFormula1, MFormula2> {};
