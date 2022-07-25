#pragma once

#include <absl/container/flat_hash_map.h>
#include <algorithm>
#include <boost/container/devector.hpp>
#include <boost/mp11.hpp>
#include <cstdint>
#include <staticmon/common/mp_helpers.h>
#include <staticmon/common/table.h>
#include <staticmon/operators/detail/binary_buffer.h>
#include <staticmon/operators/detail/minterval.h>
#include <staticmon/operators/detail/operator_types.h>
#include <vector>

template<typename L2, typename T2, typename Interval>
struct until_base {
  using a2_elem_t = absl::flat_hash_map<T2, std::size_t>;
  using a2_map_t = boost::container::devector<a2_elem_t>;
  using ts_buf_t = boost::container::devector<std::size_t>;
  using t2_tab_t = table_util::tab_t_of_row_t<T2>;

  auto eval(std::size_t new_ts) {
    shift(new_ts);
    std::vector<t2_tab_t> ret;
    swap(ret, res_acc_);
    return ret;
  }

  t2_tab_t table_from_map(const a2_elem_t &mapping) {
    t2_tab_t res_tab;
    res_tab.reserve(mapping.size());
    for (const auto &entry : mapping)
      res_tab.emplace(entry.first);
    return res_tab;
  }

  void combine_max(a2_elem_t &mapping1, a2_elem_t &mapping2) {
    for (const auto &entry : mapping1) {
      auto mapping2_it = mapping2.find(entry.first);
      if (mapping2_it == mapping2.end())
        mapping2.insert(entry);
      else
        mapping2_it->second = std::max(mapping2_it->second, entry.second);
    }
  }

  void update_a2_inner_map(std::size_t idx, const T2 &e,
                           std::size_t new_ts_tp) {
    assert(idx < a2_map_.size());
    auto &nest_map = a2_map_[idx];
    auto nest_it = nest_map.find(e);
    if (nest_it == nest_map.end())
      nest_map.emplace(e, new_ts_tp);
    else
      nest_it->second = std::max(nest_it->second, new_ts_tp);
  }

  void shift(std::size_t new_ts) {
    for (; !ts_buf_.empty();
         ts_buf_.pop_front(), a2_map_.pop_front(), fst_tp_++) {
      std::size_t old_ts = ts_buf_.front();
      assert(new_ts >= old_ts);
      if (Interval::leq_upper(new_ts - old_ts))
        break;
      assert(a2_map_.size() >= 2);
      assert(curr_tp_ >= ts_buf_.size());
      auto erase_cond = [this, old_ts](const auto &entry) {
        std::size_t tstp = entry.second;
        if constexpr (Interval::contains(0))
          return fst_tp_ > tstp;
        else
          return old_ts >= tstp;
      };
      absl::erase_if(a2_map_[0], erase_cond);
      res_acc_.push_back(table_from_map(a2_map_[0]));
      combine_max(a2_map_[0], a2_map_[1]);
    }
  }

  std::size_t fst_tp_ = 0, curr_tp_ = 0;
  ts_buf_t ts_buf_;
  a2_map_t a2_map_ = {{}};
  std::vector<t2_tab_t> res_acc_;
};

template<bool left_negated, typename L1, typename L2, typename T1, typename T2,
         typename Interval>
struct until_impl : until_base<L2, T2, Interval> {
  using Base = typename until_impl::until_base;
  using t1_tab_t = table_util::tab_t_of_row_t<T1>;
  using t2_tab_t = typename Base::t2_tab_t;
  using a1_map_t = absl::flat_hash_map<T1, std::size_t>;
  using project_idxs = table_util::comp_common_idx<L2, L1>;
  static constexpr bool contains_zero = Interval::contains(0);

  void add_tables(t1_tab_t &tab_l, t2_tab_t &tab_r, std::size_t new_ts) {
    auto &ts_buf = this->ts_buf_;
    assert(ts_buf.empty() || new_ts >= ts_buf.back());
    this->shift(new_ts);
    update_a2_map(new_ts, tab_r);
    update_a1_map(tab_l);
    ts_buf.push_back(new_ts);
    this->curr_tp_++;
  }

  void update_a2_map(std::size_t new_ts, const t2_tab_t &tab_r) {
    std::size_t new_ts_tp;
    if constexpr (contains_zero)
      new_ts_tp = this->curr_tp_;
    else
      new_ts_tp = new_ts - std::min((Interval::lower() - 1), new_ts);
    assert(this->curr_tp_ >= this->ts_buf_.size());
    for (const auto &e : tab_r) {
      if constexpr (contains_zero) {
        assert(!this->a2_map_.empty());
        this->update_a2_inner_map(this->a2_map_.size() - 1, e, new_ts_tp);
      }
      auto a1_it = a1_map_.find(project_row<project_idxs>(e));
      std::size_t override_idx;
      if (a1_it == a1_map_.end()) {
        if constexpr (left_negated)
          override_idx = 0;
        else
          continue;
      } else {
        if constexpr (left_negated)
          override_idx = a1_it->second + 1 <= this->fst_tp_
                           ? 0
                           : a1_it->second + 1 - this->fst_tp_;
        else
          override_idx =
            a1_it->second <= this->fst_tp_ ? 0 : a1_it->second - this->fst_tp_;
      }
      this->update_a2_inner_map(override_idx, e, new_ts_tp);
    }
    this->a2_map_.emplace_back();
  }

  void update_a1_map(const t1_tab_t &tab_l) {
    if constexpr (left_negated) {
      for (const auto &e : tab_l)
        a1_map_.insert_or_assign(e, this->curr_tp_);
    } else {
      auto erase_cond = [&tab_l](const auto &entry) {
        return !tab_l.contains(entry.first);
      };
      absl::erase_if(a1_map_, erase_cond);
      for (const auto &e : tab_l) {
        if (a1_map_.contains(e))
          continue;
        a1_map_.emplace(e, this->curr_tp_);
      }
    }
  }

  a1_map_t a1_map_;
};


template<typename L2, typename T2, typename Interval>
struct eventually_impl : until_base<L2, T2, Interval> {
  using Base = typename eventually_impl::until_base;
  using t2_tab_t = typename Base::t2_tab_t;
  static constexpr bool contains_zero = Interval::contains(0);

  void add_right_table(t2_tab_t &tab_r, std::size_t new_ts) {
    assert(this->ts_buf_.empty() || new_ts >= this->ts_buf_.back());
    this->shift(new_ts);
    update_a2_map(new_ts, tab_r);
    this->ts_buf_.push_back(new_ts);
    this->curr_tp_++;
  }

  void update_a2_map(std::size_t new_ts, const t2_tab_t &tab_r) {
    size_t new_ts_tp;
    if constexpr (contains_zero)
      new_ts_tp = this->curr_tp_;
    else
      new_ts_tp = new_ts - std::min((Interval::lower() - 1), new_ts);
    assert(this->curr_tp_ >= this->ts_buf_.size());
    for (const auto &e : tab_r) {
      assert(!this->a2_map_.empty());
      if (contains_zero)
        this->update_a2_inner_map(this->a2_map_.size() - 1, e, new_ts_tp);
      this->update_a2_inner_map(0, e, new_ts_tp);
    }
    this->a2_map_.emplace_back();
  }
};

template<typename LBound, typename UBound, typename MFormula>
struct meventually {
  using interval = minterval<LBound, UBound>;
  using L2 = typename MFormula::ResL;
  using T2 = typename MFormula::ResT;
  using res_tab_t = table_util::tab_t_of_row_t<T2>;

  using ResL = L2;
  using ResT = T2;

  auto eval(database &db, const ts_list &ts) {
    ts_buf_.insert(ts_buf_.end(), ts.begin(), ts.end());
    auto rec_tabs = f_.eval(db, ts);
    std::vector<res_tab_t> res_tabs;
    res_tabs.reserve(rec_tabs.size());
    for (auto &tab : rec_tabs) {
      assert(!ts_buf_.empty());
      size_t new_ts = ts_buf_.front();
      ts_buf_.pop_front();
      impl_.add_right_table(tab, new_ts);
      new_ts = ts_buf_.empty() ? new_ts : ts_buf_.front();
      auto res_tabs_part = impl_.eval(new_ts);
      res_tabs.insert(res_tabs.end(),
                      std::make_move_iterator(res_tabs_part.begin()),
                      std::make_move_iterator(res_tabs_part.end()));
    }
    return res_tabs;
  }

  MFormula f_;
  eventually_impl<L2, T2, interval> impl_;
  boost::container::devector<std::size_t> ts_buf_;
};


template<bool left_negated, typename LBound, typename RBound,
         typename MFormula1, typename MFormula2>
struct muntil {
  using interval = minterval<LBound, RBound>;
  using L1 = typename MFormula1::ResL;
  using L2 = typename MFormula2::ResL;
  using T1 = typename MFormula1::ResT;
  using T2 = typename MFormula2::ResT;

  using rec_tab1_t = table_util::tab_t_of_row_t<T1>;
  using rec_tab2_t = table_util::tab_t_of_row_t<T2>;
  using res_tab_t = table_util::tab_t_of_row_t<T2>;

  using ResL = L2;
  using ResT = T2;

  auto eval(database &db, const ts_list &ts) {
    ts_buf_.insert(ts_buf_.end(), ts.begin(), ts.end());
    auto rec_res1 = f1_.eval(db, ts);
    auto rec_res2 = f2_.eval(db, ts);
    auto res = bin_buf_.update_and_reduce(
      rec_res1, rec_res2, [this](rec_tab1_t &tab1, rec_tab2_t &tab2) {
        assert(!ts_buf_.empty());
        std::size_t new_ts = ts_buf_.front();
        ts_buf_.pop_front();
        impl_.add_tables(tab1, tab2, new_ts);
        new_ts = ts_buf_.empty() ? new_ts : ts_buf_.front();
        return impl_.eval(new_ts);
      });
    std::vector<res_tab_t> res_tabs;
    std::for_each(res.begin(), res.end(), [&res_tabs](auto &ltabs) {
      res_tabs.insert(res_tabs.end(), std::make_move_iterator(ltabs.begin()),
                      std::make_move_iterator(ltabs.end()));
    });
    return res_tabs;
  }

  MFormula1 f1_;
  MFormula2 f2_;
  until_impl<left_negated, L1, L2, T1, T2, interval> impl_;
  bin_op_buffer<rec_tab1_t, rec_tab2_t> bin_buf_;
  boost::container::devector<std::size_t> ts_buf_;
};
