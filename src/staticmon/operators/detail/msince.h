#pragma once
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <boost/container/devector.hpp>
#include <boost/mp11.hpp>
#include <staticmon/common/mp_helpers.h>
#include <staticmon/common/table.h>
#include <staticmon/operators/detail/aggregations.h>
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

template<typename ResVar, typename AggTag, typename AggVar, typename GroupVars>
struct agg_info;

template<bool is_no_remove, typename AggInfo, typename L2, typename T2>
struct aggregation_mixin {
  static_assert(always_false_v<mp_list<mp_bool<is_no_remove>, AggInfo, L2, T2>>,
                "invalid aggregation mixin");
};

template<typename L2, typename T2, typename... AggVals>
struct aggregation_mixin<true, agg_info<AggVals...>, L2, T2> {
  using Aggregation = addonlyaggregation<L2, T2, AggVals...>;
  using tuple_buf_t = mp_rename<T2, tuple_buf>;

  using ResL = typename Aggregation::ResL;
  using ResT = typename Aggregation::ResT;

  auto produce_result() { return agg_.template finalize_table<false>(); }

  void tuple_in_update(const T2 &e, size_t) {
    if (rows_in_result.emplace(e).second) {
      agg_.add_row(e);
      return;
    }
  }

  Aggregation agg_;
  absl::flat_hash_set<T2> rows_in_result;
};

template<typename L2, typename T2, typename... AggVals>
struct aggregation_mixin<false, agg_info<AggVals...>, L2, T2> {
  using Aggregation = aggregationwithremove<L2, T2, AggVals...>;
  using tuple_buf_t = mp_rename<T2, tuple_buf>;

  using ResL = typename Aggregation::ResL;
  using ResT = typename Aggregation::ResT;

  auto produce_result() { return agg_.template finalize_table<false>(); }

  void tuple_in_update(const T2 &e, size_t ts) {
    if (tuple_in.insert_or_assign(e, ts).second) {
      agg_.add_row(e);
      return;
    }
  }

  template<typename TupleInIt>
  void tuple_in_erase(TupleInIt it) {
    agg_.remove_row(it->first);
    tuple_in.erase(it);
  }

  template<typename Pred>
  void tuple_in_erase_if(Pred pred) {
    absl::erase_if(tuple_in, [this, pred](const auto &tup) {
      if (pred(tup)) {
        agg_.remove_row(tup.first);
        return true;
      } else {
        return false;
      }
    });
  }

  Aggregation agg_;
  tuple_buf_t tuple_in;
};

template<bool inf_intv, typename L2, typename T2>
struct aggregation_mixin<inf_intv, no_aggregation, L2, T2> {
  using ResL = L2;
  using ResT = T2;
  using tuple_buf_t = mp_rename<T2, tuple_buf>;
  using res_tab_t = table_util::tab_t_of_row_t<T2>;

  res_tab_t produce_result() {
    res_tab_t tab;
    tab.reserve(tuple_in.size());
    for (auto it = tuple_in.cbegin(); it != tuple_in.cend(); ++it)
      tab.emplace(it->first);
    return tab;
  }

  void tuple_in_update(const T2 &e, size_t ts) {
    tuple_in.insert_or_assign(e, ts);
  }

  template<typename TupleInIt>
  void tuple_in_erase(TupleInIt it) {
    tuple_in.erase(it);
  }

  template<typename Pred>
  void tuple_in_erase_if(Pred pred) {
    absl::erase_if(tuple_in, pred);
  }

  tuple_buf_t tuple_in;
};

template<bool is_once, typename AggInfo, typename Interval, typename L2,
         typename T2>
struct interval_bnd_mixin : aggregation_mixin<is_once, AggInfo, L2, T2> {
  using table_buf_t = table_buf<T2>;
  using tuple_buf_t = mp_rename<T2, tuple_buf>;

  table_buf_t data_prev;
  tuple_buf_t tuple_since;
};

template<bool is_once, typename AggInfo, typename Interval, typename L2,
         typename T2>
requires(!Interval::is_infinite) struct interval_bnd_mixin<is_once, AggInfo,
                                                           Interval, L2, T2>
    : aggregation_mixin<false, AggInfo, L2, T2> {

  using table_buf_t = table_buf<T2>;
  using tuple_buf_t = mp_rename<T2, tuple_buf>;

  void drop_tuple_from_all_data(const T2 &e) {
    auto all_dat_it = all_data_counted.find(e);
    assert(all_dat_it != all_data_counted.end() && all_dat_it->second > 0);
    if ((--all_dat_it->second) == 0) {
      tuple_since.erase(all_dat_it->first);
      all_data_counted.erase(all_dat_it);
    }
  }

  void drop_too_old(std::size_t ts) {
    for (; !data_in.empty(); data_in.pop_front()) {
      auto old_ts = data_in.front().first;
      assert(ts >= old_ts);
      if (Interval::leq_upper(ts - old_ts))
        break;
      auto &tab = data_in.front();
      for (const auto &e : tab.second) {
        auto in_it = this->tuple_in.find(e);
        if (in_it != this->tuple_in.end() && in_it->second == tab.first)
          this->tuple_in_erase(in_it);
        drop_tuple_from_all_data(e);
      }
    }
    for (;
         !data_prev.empty() && Interval::gt_upper(ts - data_prev.front().first);
         data_prev.pop_front()) {
      for (const auto &e : data_prev.front().second)
        drop_tuple_from_all_data(e);
    }
  }

  table_buf_t data_prev, data_in;
  tuple_buf_t all_data_counted, tuple_since;
};

template<bool is_once, typename AggInfo, typename Interval, typename L2,
         typename T2>
struct base_mixin : interval_bnd_mixin<is_once, AggInfo, Interval, L2, T2> {
  using Base = typename base_mixin::interval_bnd_mixin;
  using table_buf_t = table_buf<T2>;
  using tuple_buf_t = mp_rename<T2, tuple_buf>;
  using tab2_t = table_util::tab_t_of_row_t<T2>;

  void add_new_ts(size_t ts) {
    if constexpr (!Interval::is_infinite)
      this->drop_too_old(ts);

    for (; !this->data_prev.empty(); this->data_prev.pop_front()) {
      auto &latest = this->data_prev.front();
      size_t old_ts = latest.first;
      assert(old_ts <= ts);
      if (Interval::lt_lower(ts - old_ts))
        break;
      for (const auto &e : latest.second) {
        auto since_it = this->tuple_since.find(e);
        if (since_it != this->tuple_since.end() && since_it->second <= old_ts)
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
      this->tuple_since.try_emplace(e, ts);
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
      assert(this->data_prev.empty() || ts >= this->data_prev.back().first);
      this->data_prev.emplace_back(ts, std::move(tab_r));
    }
  }
};

template<typename AggInfo, bool left_negated, typename Interval,
         typename MFormula1, typename MFormula2>
struct since_impl
    : base_mixin<false, AggInfo, Interval, typename MFormula2::ResL,
                 typename MFormula2::ResT> {
  using Base = typename since_impl::base_mixin;

  using L1 = typename MFormula1::ResL;
  using L2 = typename MFormula2::ResL;
  using T1 = typename MFormula1::ResT;
  using T2 = typename MFormula2::ResT;
  using rec_tab1_t = table_util::tab_t_of_row_t<T1>;
  using rec_tab2_t = table_util::tab_t_of_row_t<T2>;

  using ResL = typename Base::ResL;
  using ResT = typename Base::ResT;
  using res_tab_t = table_util::tab_t_of_row_t<ResT>;
  using tab1_t = table_util::tab_t_of_row_t<T1>;
  using tab2_t = table_util::tab_t_of_row_t<T2>;
  using project_idxs = table_util::comp_common_idx<L2, L1>;

  res_tab_t eval_tab(tab1_t &tab1, tab2_t &tab2, std::size_t new_ts) {
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

  std::vector<res_tab_t> eval(database &db, const ts_list &ts) {
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
        std::size_t new_ts = ts_buf_.front();
        ts_buf_.pop_front();
        return eval_tab(tab1, tab2, new_ts);
      });
  }

  MFormula1 f1_;
  MFormula2 f2_;
  bin_op_buffer<rec_tab1_t, rec_tab2_t> bin_buf_;
  boost::container::devector<std::size_t> ts_buf_;
};

template<typename AggInfo, typename Interval, typename MFormula>
struct once_impl : base_mixin<true, AggInfo, Interval, typename MFormula::ResL,
                              typename MFormula::ResT> {
  using Base = typename once_impl::base_mixin;

  using L2 = typename MFormula::ResL;
  using T2 = typename MFormula::ResT;
  using rec_tab_t = table_util::tab_t_of_row_t<T2>;
  using tab2_t = table_util::tab_t_of_row_t<T2>;

  using ResL = typename Base::ResL;
  using ResT = typename Base::ResT;
  using res_tab_t = table_util::tab_t_of_row_t<ResT>;

  res_tab_t eval_tab(tab2_t &tab_r, std::size_t new_ts) {
    this->add_new_ts(new_ts);
    this->add_new_table(tab_r, new_ts);
    return this->produce_result();
  }

  std::vector<res_tab_t> eval(database &db, const ts_list &ts) {
    ts_buf_.insert(ts_buf_.end(), ts.begin(), ts.end());
    auto rec_tabs = f_.eval(db, ts);
    static_assert(std::is_same_v<decltype(rec_tabs), std::vector<rec_tab_t>>,
                  "unexpected table type");
    std::vector<res_tab_t> res;
    res.reserve(rec_tabs.size());
    for (auto &tab : rec_tabs) {
      assert(!ts_buf_.empty());
      size_t new_ts = ts_buf_.front();
      ts_buf_.pop_front();
      auto ret = eval_tab(tab, new_ts);
      static_assert(std::is_same_v<decltype(ret), res_tab_t>,
                    "unexpected table type");
      res.emplace_back(std::move(ret));
    }
    return res;
  }

  MFormula f_;
  boost::container::devector<std::size_t> ts_buf_;
};

template<typename LBound, typename UBound, typename MFormula>
struct monce : once_impl<no_aggregation, minterval<LBound, UBound>, MFormula> {
};

template<std::size_t ResVar, typename AggTag, std::size_t AggVar,
         typename GroupVars, typename LBound, typename UBound,
         typename MFormula>
struct monceagg
    : once_impl<
        agg_info<mp_size_t<ResVar>, AggTag, mp_size_t<AggVar>, GroupVars>,
        minterval<LBound, UBound>, MFormula> {};

template<bool left_negated, typename LBound, typename UBound,
         typename MFormula1, typename MFormula2>
struct msince : since_impl<no_aggregation, left_negated,
                           minterval<LBound, UBound>, MFormula1, MFormula2> {};

template<std::size_t ResVar, typename AggTag, std::size_t AggVar,
         typename GroupVars, bool left_negated, typename LBound,
         typename UBound, typename MFormula1, typename MFormula2>
struct msinceagg
    : since_impl<
        agg_info<mp_size_t<ResVar>, AggTag, mp_size_t<AggVar>, GroupVars>,
        left_negated, minterval<LBound, UBound>, MFormula1, MFormula2> {};
