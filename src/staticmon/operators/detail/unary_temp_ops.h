#pragma once
#include <boost/container/devector.hpp>
#include <boost/mp11.hpp>
#include <optional>
#include <staticmon/common/mp_helpers.h>
#include <staticmon/common/table.h>
#include <staticmon/operators/detail/minterval.h>
#include <staticmon/operators/detail/operator_types.h>
#include <tuple>

using namespace boost::mp11;

template<typename LBound, typename UBound, typename MFormula>
struct mprev {
  using ResL = typename MFormula::ResL;
  using ResT = typename MFormula::ResT;
  using interval = minterval<LBound, UBound>;
  using rec_tab_t = table_util::tab_t_of_row_t<ResT>;

  auto eval(database &db, const ts_list &ts) {
    auto rec_tabs = f_->eval(db, ts);
    static_assert(std::is_same_v<decltype(rec_tabs), std::vector<rec_tab_t>>,
                  "unexpected table type");
    past_ts_.insert(past_ts_.end(), ts.begin(), ts.end());
    if (rec_tabs.empty()) {
      return rec_tabs;
    }
    std::vector<rec_tab_t> res_tabs;
    res_tabs.reserve(rec_tabs.size() + 1);
    auto tabs_it = rec_tabs.begin();
    if (is_first_) {
      // res_tabs.emplace_back(std::nullopt);
      res_tabs.emplace_back();
      is_first_ = false;
    }

    if (past_ts_.size() >= 2) {
      if (buf_) {
        std::optional<rec_tab_t> taken_val;
        buf_.swap(taken_val);
        assert(past_ts_[1] >= past_ts_[0]);
        if (interval::contains(past_ts_[1] - past_ts_[0]))
          res_tabs.push_back(std::move(*taken_val));
        else
          res_tabs.emplace_back();
        // res_tabs.emplace_back(std::nullopt);
        past_ts_.pop_front();
      }
      for (auto rec_tabs_end = rec_tabs.end();
           past_ts_.size() >= 2 && tabs_it != rec_tabs_end;
           past_ts_.pop_front(), ++tabs_it) {
        assert(past_ts_[1] >= past_ts_[0]);
        if (interval::contains(past_ts_[1] - past_ts_[0]))
          res_tabs.push_back(std::move(*tabs_it));
        else
          res_tabs.emplace_back();
        // res_tabs.push_back(std::nullopt);
      }
    }

    if (tabs_it != rec_tabs.end()) {
      assert(!buf_);
      buf_.emplace(std::move(*tabs_it));
      tabs_it++;
    }

    assert(tabs_it == rec_tabs.end());
    return res_tabs;
  }

  MFormula f_;
  boost::container::devector<std::size_t> past_ts_;
  std::optional<rec_tab_t> buf_;
  bool is_first_ = true;
};

template<typename LBound, typename UBound, typename MFormula>
struct mnext {
  using ResL = typename MFormula::ResL;
  using ResT = typename MFormula::ResT;
  using interval = minterval<LBound, UBound>;
  using rec_tab_t = table_util::tab_t_of_row_t<ResT>;

  auto eval(database &db, const ts_list &ts) {
    auto rec_tabs = f_->eval(db, ts);
    static_assert(std::is_same_v<decltype(rec_tabs), std::vector<rec_tab_t>>,
                  "unexpected table type");
    past_ts_.insert(past_ts_.end(), ts.begin(), ts.end());
    if (rec_tabs.empty())
      return rec_tabs;
    auto tabs_it = rec_tabs.begin();
    if (is_first_) {
      tabs_it++;
      is_first_ = false;
    }
    std::vector<rec_tab_t> res_tabs;
    res_tabs.reserve(rec_tabs.size());
    for (; tabs_it != rec_tabs.end(); past_ts_.pop_front(), ++tabs_it) {
      assert(past_ts_.size() > 1 && past_ts_[0] <= past_ts_[1]);
      if (interval::contains(past_ts_[1] - past_ts_[0]))
        res_tabs.push_back(std::move(*tabs_it));
      else
        res_tabs.emplace_back();
      // res_tabs.emplace_back(std::nullopt);
    }
    assert(!past_ts_.empty() && tabs_it == rec_tabs.end());
    return res_tabs;
  }

  MFormula f_;
  boost::container::devector<std::size_t> past_ts_;
  bool is_first_ = true;
};
