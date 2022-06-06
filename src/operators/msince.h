#pragma once
#include <absl/container/flat_hash_map.h>
#include <boost/container/devector.hpp>
#include <boost/mp11.hpp>
#include <helpers/binary_buffer.h>
#include <minterval.h>
#include <mp_helpers.h>
#include <operator_types.h>
#include <table.h>
#include <vector>

using namespace boost::mp11;

template<typename... Args>
using tuple_buf = absl::flat_hash_map<std::tuple<Args...>, std::size_t>;

struct shared_no_agg {
  template<typename... Args>
  auto produce_result(const tuple_buf<Args...> &tuple_in) {
    using res_tab_t = table_util::table<Args...>;
    res_tab_t tab;
    tab.reserve(tuple_in.size());
    for (auto it = tuple_in.cbegin(); it != tuple_in.cend(); ++it)
      tab.add_row(it->first);
    return tab;
  }

  template<typename... Args>
  void tuple_in_update(tuple_buf<Args...> &tuple_in,
                       const std::tuple<Args...> &e, size_t ts) {
    tuple_in->insert_or_assign(e, ts);
  }

  template<typename... Args>
  void tuple_in_erase(tuple_buf<Args...> &tuple_in,
                      typename tuple_buf<Args...>::iterator it) {
    tuple_in.erase(it);
  }

  template<typename... Args, typename Pred>
  void tuple_in_erase_if(tuple_buf<Args...> &tuple_in, Pred pred) {
    absl::erase_if(tuple_in, pred);
  }
};

template<typename Interval, typename T2, typename SharedBase>
struct unbounded_interval_helper {};

template<typename Interval, typename T2, typename SharedBase>
requires(!Interval::is_infinite) struct unbounded_interval_helper<Interval, T2,
                                                                  SharedBase> {
  using table_buf_t = boost::container::devector<
    std::pair<size_t, table_util::tab_t_of_row_t<T2>>>;
  using tuple_buf_t =
    absl::flat_hash_map<mp_rename<T2, tuple_buf>, std::size_t>;

  void drop_tuple_from_all_data(const T2 &e) {
    auto &tuple_since = static_cast<SharedBase *>(this)->tuple_since;
    auto all_dat_it = all_data_counted.find(e);
    assert(all_dat_it != all_data_counted.end() && all_dat_it->second > 0);
    if ((--all_dat_it->second) == 0) {
      tuple_since.erase(all_dat_it->first);
      all_data_counted.erase(all_dat_it);
    }
  }

  void drop_too_old(size_t ts) {
    auto &data_prev = static_cast<SharedBase *>(this)->data_prev;
    auto &tuple_in = static_cast<SharedBase *>(this)->tuple_in;
    for (; !data_in.empty(); data_in.pop_front()) {
      auto old_ts = data_in.front().first;
      assert(ts >= old_ts);
      if (Interval::leq_upper(ts - old_ts))
        break;
      auto &tab = data_in.front();
      if (tab.second) {
        for (const auto &e : *tab.second) {
          auto in_it = tuple_in.find(e);
          if (in_it != tuple_in.end() && in_it->second == tab.first)
            tuple_in_erase(tuple_in, in_it);
          drop_tuple_from_all_data(e);
        }
      }
    }
    for (;
         !data_prev.empty() && Interval::gt_upper(ts - data_prev.front().first);
         data_prev.pop_front()) {
      if (data_prev.front().second) {
        for (const auto &e : *data_prev.front().second)
          drop_tuple_from_all_data(e);
      }
    }
  }
  table_buf_t data_in;
  tuple_buf_t all_data_counted;
};

template<typename AggBase, typename Interval, typename T2>
struct shared_base
    : AggBase,
      unbounded_interval_helper<Interval, T2,
                                shared_base<AggBase, Interval, T2>> {

  using table_buf_t = boost::container::devector<
    std::pair<size_t, table_util::tab_t_of_row_t<T2>>>;
  using tuple_buf_t =
    absl::flat_hash_map<mp_rename<T2, tuple_buf>, std::size_t>;

  void add_new_ts(size_t ts) {
    if constexpr (!Interval::is_infinite)
      this->drop_too_old(ts);

    for (; !data_prev.empty(); data_prev.pop_front()) {
      auto &latest = data_prev.front();
      size_t old_ts = latest.first;
      assert(old_ts <= ts);
      if (Interval::lt_lower(ts - old_ts))
        break;
      if (latest.second) {
        for (const auto &e : *latest.second) {
          auto since_it = tuple_since.find(e);
          if (since_it != tuple_since.end() && since_it->second <= old_ts)
            tuple_in_update(tuple_in, e, old_ts);
        }
      }
      if constexpr (!Interval::is_infinite) {
        assert(this->data_in.empty() || old_ts >= this->data_in.back().first);
        this->data_in.push_back(std::move(latest));
      }
    }
  }

  template<typename... Args>
  void add_new_table(table_util::table<Args...> &tab_r, std::size_t ts) {
    for (const auto &e : tab_r) {
      tuple_since.try_emplace(e, ts);
      if constexpr (!Interval::is_infinite)
        this->all_data_counted[e]++;
    }
    if constexpr (Interval::contains(0)) {
      for (const auto &e : tab_r)
        tuple_in_update(tuple_in, e, ts);
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
         typename AggBase, typename Interval>
struct since_impl : shared_base<AggBase, Interval, T2> {
  using tab1_t = table_util::tab_t_of_row_t<T1>;
  using tab2_t = table_util::tab_t_of_row_t<T2>;
  using l2_common = typename table_util::join_info<L1, L2, T1, T2>::l2_common;

  tab2_t eval(tab1_t &tab1, tab2_t &tab2, std::size_t new_ts) {
    this->add_new_ts(new_ts);
    this->join(tab1);
    this->add_new_table(tab2, new_ts);
    return this->produce_result();
  }

  void join(tab1_t &tab1) {
    auto erase_cond = [&tab1](const auto &tup) {
      if constexpr (left_negated)
        return tab1.contains(project_row<l2_common>(tup.first));
      else
        return !tab1.contains(project_row<l2_common>(tup.first));
      return true;
    };
    absl::erase_if(this->tuple_since, erase_cond);
    this->tuple_in_erase_if(erase_cond);
  }
};

template<typename L2, typename T2, typename AggBase, typename Interval>
struct once_impl : shared_base<AggBase, Interval, T2> {
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

  auto eval(database &db, const ts_list &ts) {
    ts_buf_.insert(ts_buf_.end(), ts.begin(), ts.end());
    auto rec_tabs = f_->eval(db, ts);
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
  once_impl<L2, T2, shared_no_agg, interval> impl_;
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

  auto eval(database &db, const ts_list &ts) {
    ts_buf_.insert(ts_buf_.end(), ts.begin(), ts.end());
    std::vector<res_tab_t> res;
    auto rec_res1 = f1_.eval(db, ts);
    auto rec_res2 = f2_.eval(db, ts);
    static_assert(std::is_same_v<decltype(rec_res1), std::vector<rec_tab1_t>>,
                  "unexpected table type");
    static_assert(std::is_same_v<decltype(rec_res2), std::vector<rec_tab2_t>>,
                  "unexpected table type");
    bin_buf_.update_and_reduce(
      std::move(rec_res1), std::move(rec_res2),
      [this](const rec_tab1_t &tab1, const rec_tab2_t &tab2) {
        assert(!ts_buf_.empty());
        size_t new_ts = ts_buf_.front();
        ts_buf_.pop_front();
        auto ret = impl_.eval(tab1, tab2, new_ts);
        return ret;
      });
  }

  MFormula1 f1_;
  MFormula2 f2_;
  since_impl<left_negated, L1, L2, T1, T2, shared_no_agg, interval> impl_;
  bin_op_buffer<rec_tab1_t, rec_tab2_t> bin_buf_;
  boost::container::devector<std::size_t> ts_buf_;
};
