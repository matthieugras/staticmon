#pragma once
#include <absl/container/flat_hash_map.h>
#include <boost/container/devector.hpp>
#include <boost/mp11.hpp>
#include <staticmon/common/mp_helpers.h>
#include <staticmon/common/table.h>
#include <staticmon/operators/detail/binary_buffer.h>
#include <staticmon/operators/detail/minterval.h>
#include <staticmon/operators/detail/operator_types.h>
#include <vector>

using namespace boost::mp11;

template<typename... Args>
using tuple_buf = absl::flat_hash_map<std::tuple<Args...>, std::size_t>;

template<typename T>
using table_buf = boost::container::devector<
  std::pair<std::size_t, table_util::tab_t_of_row_t<T>>>;

struct no_aggregation;

template<typename SharedBase, typename TemporalAggregation, typename T2>
struct aggregation_mixin {
  static_assert(always_false_v<T2>, "not implemented");
};

template<typename SharedBase, typename T2>
struct aggregation_mixin<SharedBase, no_aggregation, T2> {
  auto produce_result() const {
    const auto &tuple_in = static_cast<const SharedBase *>(this)->tuple_in;
    using res_tab_t = table_util::tab_t_of_row_t<T2>;
    res_tab_t tab;
    tab.reserve(tuple_in.size());
    for (auto it = tuple_in.cbegin(); it != tuple_in.cend(); ++it)
      tab.emplace(it->first);
    return tab;
  }

  void tuple_in_update(const T2 &e, size_t ts) {
    auto &tuple_in = static_cast<SharedBase *>(this)->tuple_in;
    tuple_in.insert_or_assign(e, ts);
  }

  template<typename TupleInIt>
  void tuple_in_erase(TupleInIt it) {
    auto &tuple_in = static_cast<SharedBase *>(this)->tuple_in;
    tuple_in.erase(it);
  }

  template<typename Pred>
  void tuple_in_erase_if(Pred pred) {
    auto &tuple_in = static_cast<SharedBase *>(this)->tuple_in;
    absl::erase_if(tuple_in, pred);
  }
};

template<typename SharedBase, typename Interval, typename T2>
struct interval_bnd_mixin {};

template<typename SharedBase, typename Interval, typename T2>
requires(
  !Interval::is_infinite) struct interval_bnd_mixin<SharedBase, Interval, T2> {
  using table_buf_t = table_buf<T2>;
  using tuple_buf_t = mp_rename<T2, tuple_buf>;

  void drop_tuple_from_all_data(const T2 &e) {
    auto &tuple_since = static_cast<SharedBase *>(this)->tuple_since;
    auto all_dat_it = all_data_counted.find(e);
    assert(all_dat_it != all_data_counted.end() && all_dat_it->second > 0);
    if ((--all_dat_it->second) == 0) {
      tuple_since.erase(all_dat_it->first);
      all_data_counted.erase(all_dat_it);
    }
  }

  void drop_too_old(std::size_t ts) {
    auto *as_base = static_cast<SharedBase *>(this);
    for (; !data_in.empty(); data_in.pop_front()) {
      auto old_ts = data_in.front().first;
      assert(ts >= old_ts);
      if (Interval::leq_upper(ts - old_ts))
        break;
      auto &tab = data_in.front();
      for (const auto &e : tab.second) {
        auto in_it = as_base->tuple_in.find(e);
        if (in_it != as_base->tuple_in.end() && in_it->second == tab.first)
          as_base->tuple_in_erase(in_it);
        drop_tuple_from_all_data(e);
      }
    }
    for (; !as_base->data_prev.empty() &&
           Interval::gt_upper(ts - as_base->data_prev.front().first);
         as_base->data_prev.pop_front()) {
      for (const auto &e : as_base->data_prev.front().second)
        drop_tuple_from_all_data(e);
    }
  }
  table_buf_t data_in;
  tuple_buf_t all_data_counted;
};

template<typename TemporalAggregation, typename Interval, typename T2>
struct base_mixin
    : interval_bnd_mixin<base_mixin<TemporalAggregation, Interval, T2>,
                         Interval, T2>,
      aggregation_mixin<base_mixin<TemporalAggregation, Interval, T2>,
                        TemporalAggregation, T2> {

  using table_buf_t = table_buf<T2>;
  using tuple_buf_t = mp_rename<T2, tuple_buf>;
  using tab2_t = table_util::tab_t_of_row_t<T2>;

  void add_new_ts(size_t ts) {
    if constexpr (!Interval::is_infinite)
      this->drop_too_old(ts);

    for (; !data_prev.empty(); data_prev.pop_front()) {
      auto &latest = data_prev.front();
      size_t old_ts = latest.first;
      assert(old_ts <= ts);
      if (Interval::lt_lower(ts - old_ts))
        break;
      for (const auto &e : latest.second) {
        auto since_it = tuple_since.find(e);
        if (since_it != tuple_since.end() && since_it->second <= old_ts)
          this->tuple_in_update(e, old_ts);
      }
      if constexpr (!Interval::is_infinite) {
        assert(this->data_in.empty() || old_ts >= this->data_in.back().first);
        this->data_in.push_back(std::move(latest));
      }
    }
  }

  void add_new_table(tab2_t &tab_r, std::size_t ts) {
    for (const auto &e : tab_r) {
      tuple_since.try_emplace(e, ts);
      if constexpr (!Interval::is_infinite)
        this->all_data_counted[e]++;
    }
    if constexpr (Interval::contains(0)) {
      for (const auto &e : tab_r)
        this->tuple_in_update(e, ts);
      if constexpr (!Interval::is_infinite) {
        assert(this->data_in.empty() || ts >= this->data_in.back().first);
        this->data_in.emplace_back(ts, std::move(tab_r));
      }
    } else {
      assert(data_prev.empty() || ts >= data_prev.back().first);
      data_prev.emplace_back(ts, std::move(tab_r));
    }
  }

  table_buf_t data_prev;
  tuple_buf_t tuple_in, tuple_since;
};

template<bool left_negated, typename L1, typename L2, typename T1, typename T2,
         typename TemporalAggregation, typename Interval>
struct since_impl : base_mixin<TemporalAggregation, Interval, T2> {
  using tab1_t = table_util::tab_t_of_row_t<T1>;
  using tab2_t = table_util::tab_t_of_row_t<T2>;
  using project_idxs = table_util::comp_common_idx<L2, L1>;

  tab2_t eval(tab1_t &tab1, tab2_t &tab2, std::size_t new_ts) {
    this->add_new_ts(new_ts);
    this->join(tab1);
    this->add_new_table(tab2, new_ts);
    return this->produce_result();
  }

  void join(tab1_t &tab1) {
    auto erase_cond = [&tab1](const auto &tup) {
      if constexpr (left_negated)
        return tab1.contains(project_row<project_idxs>(tup.first));
      else
        return !tab1.contains(project_row<project_idxs>(tup.first));
      return true;
    };
    absl::erase_if(this->tuple_since, erase_cond);
    this->tuple_in_erase_if(erase_cond);
  }
};

template<typename L2, typename T2, typename TemporalAggregation,
         typename Interval>
struct once_impl : base_mixin<TemporalAggregation, Interval, T2> {
  using tab2_t = table_util::tab_t_of_row_t<T2>;

  tab2_t eval(tab2_t &tab_r, std::size_t new_ts) {
    this->add_new_ts(new_ts);
    this->add_new_table(tab_r, new_ts);
    return this->produce_result();
  }
};

template<typename LBound, typename UBound, typename MFormula>
struct monce {
  using interval = minterval<LBound, UBound>;
  using L2 = typename MFormula::ResL;
  using T2 = typename MFormula::ResT;
  using rec_tab_t = table_util::tab_t_of_row_t<T2>;

  using ResL = L2;
  using ResT = T2;

  auto eval(database &db, const ts_list &ts) {
    ts_buf_.insert(ts_buf_.end(), ts.begin(), ts.end());
    auto rec_tabs = f_.eval(db, ts);
    static_assert(std::is_same_v<decltype(rec_tabs), std::vector<rec_tab_t>>,
                  "unexpected table type");
    std::vector<rec_tab_t> res;
    res.reserve(rec_tabs.size());
    for (auto &tab : rec_tabs) {
      assert(!ts_buf_.empty());
      size_t new_ts = ts_buf_.front();
      ts_buf_.pop_front();
      auto ret = impl_.eval(tab, new_ts);
      static_assert(std::is_same_v<decltype(ret), rec_tab_t>,
                    "unexpected table type");
      res.emplace_back(std::move(ret));
    }
    return res;
  }

  MFormula f_;
  once_impl<L2, T2, no_aggregation, interval> impl_;
  boost::container::devector<std::size_t> ts_buf_;
};

template<bool left_negated, typename LBound, typename UBound,
         typename MFormula1, typename MFormula2>
struct msince {
  using interval = minterval<LBound, UBound>;
  using L1 = typename MFormula1::ResL;
  using L2 = typename MFormula2::ResL;
  using T1 = typename MFormula1::ResT;
  using T2 = typename MFormula2::ResT;
  using res_tab_t = table_util::tab_t_of_row_t<T2>;
  using rec_tab1_t = table_util::tab_t_of_row_t<T1>;
  using rec_tab2_t = table_util::tab_t_of_row_t<T2>;

  using ResL = L2;
  using ResT = T2;

  auto eval(database &db, const ts_list &ts) {
    ts_buf_.insert(ts_buf_.end(), ts.begin(), ts.end());
    std::vector<res_tab_t> res;
    auto rec_res1 = f1_.eval(db, ts);
    auto rec_res2 = f2_.eval(db, ts);
    static_assert(std::is_same_v<decltype(rec_res1), std::vector<rec_tab1_t>>,
                  "unexpected table type");
    static_assert(std::is_same_v<decltype(rec_res2), std::vector<rec_tab2_t>>,
                  "unexpected table type");
    return bin_buf_.update_and_reduce(
      rec_res1, rec_res2, [this](rec_tab1_t &tab1, rec_tab2_t &tab2) {
        assert(!ts_buf_.empty());
        size_t new_ts = ts_buf_.front();
        // fmt::print("at ts:{}\nleft table:{}\nright table:{}\n\n", new_ts,
        // tab1,
        //            tab2);
        ts_buf_.pop_front();
        auto ret = impl_.eval(tab1, tab2, new_ts);
        static_assert(std::is_same_v<decltype(ret), rec_tab2_t>,
                      "unexpected table type");
        return ret;
      });
  }

  MFormula1 f1_;
  MFormula2 f2_;
  since_impl<left_negated, L1, L2, T1, T2, no_aggregation, interval> impl_;
  bin_op_buffer<rec_tab1_t, rec_tab2_t> bin_buf_;
  boost::container::devector<std::size_t> ts_buf_;
};
