#pragma once
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <algorithm>
#include <boost/container/devector.hpp>
#include <boost/mp11.hpp>
#include <boost/variant2.hpp>
#include <functional>
#include <immer/memory_policy.hpp>
#include <immer/set.hpp>
#include <immer/set_transient.hpp>
#include <iterator>
#include <staticmon/common/mp_helpers.h>
#include <staticmon/common/table.h>
#include <staticmon/common/table_diff.h>
#include <staticmon/operators/detail/aggregations.h>
#include <staticmon/operators/detail/binary_buffer.h>
#include <staticmon/operators/detail/minterval.h>
#include <staticmon/operators/detail/operator_types.h>
#include <vector>

using namespace boost::mp11;

template<typename... Args>
using tuple_buf = absl::flat_hash_map<std::tuple<Args...>, std::size_t>;

template<typename T>
struct table_buf {
  using tab_t = table_util::tab_t_of_row_t<T>;
  using data_t = std::optional<tab_t>;

  std::optional<tab_t> &front_tab() { return tabs_.front().second; }

  std::size_t front_ts() const { return tabs_.front().first; }

  bool empty() { return tabs_.empty(); }

  template<typename D>
  void add_ts_table(std::size_t ts, D &&data) {
    tabs_.emplace_back(ts, std::forward<D>(data));
  }

  void pop_front() {
    assert(!tabs_.empty());
    tabs_.pop_front();
  }

  boost::container::devector<std::pair<std::size_t, data_t>> tabs_;
};

template<typename T>
struct persistent_table_buf {
  using persist_tab_t = persistent_table<T>;
  using tab_t = table_util::tab_t_of_row_t<T>;
  using diff_t = table_diff<tab_t>;

  std::optional<persist_tab_t> front_tab() const {
    assert(!tabs_.empty());
    if (tabs_.front().second.empty())
      return std::nullopt;
    else
      return tabs_.front().second;
  }

  std::size_t front_ts() const {
    assert(!tabs_.empty());
    return tabs_.front().first;
  }

  bool empty() { return tabs_.empty(); }

  template<typename D>
  std::optional<persist_tab_t> apply_diff(D &&diff) {
    auto visitor = [this](auto &&arg) -> std::optional<persist_tab_t> {
      using A = std::remove_cvref_t<decltype(arg)>;
      if constexpr (std::is_same_v<A, std::optional<tab_t>>) {
        if (arg) {
          persist_tab_t new_tab;
          auto new_tab_trans = std::move(new_tab).transient();
          for (auto &&row : *arg)
            new_tab_trans.insert(std::move(row));
          return std::move(new_tab_trans).persistent();
        } else {
          return std::nullopt;
        }
      } else {
        static_assert(std::is_same_v<A, diff_t>, "unexpected type");
        assert(arg.pos.empty() || !tabs_.empty());
        persist_tab_t new_tab;
        if (last_tab_)
          new_tab = advance_persistent_tab(*last_tab_, arg);
        else if (tabs_.empty())
          new_tab = advance_persistent_tab(persist_tab_t(), arg);
        else
          new_tab = advance_persistent_tab(tabs_.back().second, arg);
        if (new_tab.empty())
          return std::nullopt;
        else
          return new_tab;
      }
    };
    return boost::variant2::visit(visitor, std::forward<D>(diff));
  }

  void add_ts_table(std::size_t ts, std::optional<persist_tab_t> tab) {
    if (tab)
      tabs_.emplace_back(ts, std::move(*tab));
    else
      tabs_.emplace_back();
    last_tab_.reset();
  }

  void pop_front() {
    assert(!tabs_.empty());
    if (tabs_.size() == 1)
      last_tab_.emplace(tabs_.front().second);
    tabs_.pop_front();
  }

  boost::container::devector<std::pair<std::size_t, persist_tab_t>> tabs_;
  std::optional<persist_tab_t> last_tab_;
};

template<typename T, bool diff_input, typename Interval>
using select_table_buf =
  mp_if_c<!diff_input, table_buf<T>,
          mp_if_c<Interval::contains(0) && Interval::is_infinite,
                  singleton_diff_buf<T>, persistent_table_buf<T>>>;

struct no_aggregation;

template<typename ResVar, typename AggTag, typename AggVar, typename GroupVars>
struct agg_info;

template<bool is_no_remove, typename AggInfo, typename L2, typename T2>
struct aggregation_mixin {
  static_assert(always_false_v<mp_list<mp_bool<is_no_remove>, AggInfo, L2, T2>>,
                "invalid aggregation mixin");
};

template<typename Aggregation>
struct aggregation_mixin_base {
  using ResL = typename Aggregation::ResL;
  using ResT = typename Aggregation::ResT;
  static constexpr bool DiffRes = false;
  using res_tab_t = typename Aggregation::res_tab_t;

  std::optional<res_tab_t> produce_result() {
    auto tab = agg_.template finalize_table<false>();
    if (tab.empty())
      return std::nullopt;
    else
      return std::move(tab);
  }

  Aggregation agg_;
};

template<typename L2, typename T2, typename... AggVals>
struct aggregation_mixin<true, agg_info<AggVals...>, L2, T2>
    : aggregation_mixin_base<addonlyaggregation<L2, T2, AggVals...>> {
  using tuple_buf_t = mp_rename<T2, tuple_buf>;

  void tuple_in_update(const T2 &e, size_t) {
    if (rows_in_result.emplace(e).second) {
      this->agg_.add_row(e);
      return;
    }
  }

  absl::flat_hash_set<T2> rows_in_result;
};

template<typename L2, typename T2, typename... AggVals>
struct aggregation_mixin<false, agg_info<AggVals...>, L2, T2>
    : aggregation_mixin_base<aggregationwithremove<L2, T2, AggVals...>> {
  using tuple_buf_t = mp_rename<T2, tuple_buf>;

  void tuple_in_update(const T2 &e, size_t ts) {
    if (tuple_in.insert_or_assign(e, ts).second) {
      this->agg_.add_row(e);
      return;
    }
  }

  template<typename TupleInIt>
  void tuple_in_erase(TupleInIt it) {
    this->agg_.remove_row(it->first);
    tuple_in.erase(it);
  }

  template<typename Pred>
  void tuple_in_erase_if(Pred pred) {
    absl::erase_if(tuple_in, [this, pred](const auto &tup) {
      if (pred(tup)) {
        this->agg_.remove_row(tup.first);
        return true;
      } else {
        return false;
      }
    });
  }

  void tuple_in_clear() {
    tuple_in.clear();
    this->agg_.clear();
  }

  tuple_buf_t tuple_in;
};

template<bool is_no_remove, typename L2, typename T2>
struct aggregation_mixin<is_no_remove, no_aggregation, L2, T2> {
  using ResL = L2;
  using ResT = T2;
  static constexpr bool DiffRes = true;
  using tuple_buf_t = mp_rename<T2, tuple_buf>;
  using res_tab_t = table_util::tab_t_of_row_t<T2>;
  using diff_t = table_diff<res_tab_t>;
  using res_t = boost::variant2::variant<std::optional<res_tab_t>, diff_t>;

  res_t produce_result() {
    if (return_diff_ && diff_.pos.size() + diff_.neg.size() < tuple_in.size()) {
      diff_t ret_diff;
      ret_diff.swap(diff_);
      return ret_diff;
    } else {
      diff_.clear();
      return_diff_ = true;
      res_tab_t tab;
      tab.reserve(tuple_in.size());
      for (auto it = tuple_in.cbegin(); it != tuple_in.cend(); ++it)
        tab.emplace(it->first);
      if (tab.empty())
        return std::nullopt;
      else
        return tab;
    }
  }

  void tuple_in_update(const T2 &e, size_t ts) {
    if (tuple_in.insert_or_assign(e, ts).second)
      diff_.pos.emplace(e);
  }

  template<typename TupleInIt>
  void tuple_in_erase(TupleInIt it) {
    diff_.neg.emplace(it->first);
    tuple_in.erase(it);
  }

  template<typename Pred>
  void tuple_in_erase_if(Pred pred) {
    double s_pre = static_cast<double>(tuple_in.size());
    absl::erase_if(tuple_in, [this, pred](const auto &tup) {
      bool p_ret = pred(tup);
      if (p_ret)
        diff_.neg.emplace(tup.first);
      return p_ret;
    });
    std::size_t s_post = tuple_in.size();
    if (static_cast<std::size_t>(s_pre * 0.1) > s_post)
      return_diff_ = false;
  }

  void tuple_in_clear() {
    tuple_in.clear();
    return_diff_ = false;
  }

  tuple_buf_t tuple_in;
  diff_t diff_;
  bool return_diff_ = true;
};

template<bool is_once, bool diff_input, typename T2, typename Interval,
         typename Super>
struct lower_bnd_mixin {
  using table_buf_t = select_table_buf<T2, diff_input, Interval>;

  table_buf_t data_prev;
};

template<bool is_once, bool diff_input, typename T2, typename Interval,
         typename Super>
requires(!Interval::contains(0) &&
         !Interval::is_infinite) struct lower_bnd_mixin<is_once, diff_input, T2,
                                                        Interval, Super> {
  using table_buf_t = select_table_buf<T2, diff_input, Interval>;

  void drop_too_old_prev(std::size_t ts) {
    auto self = static_cast<Super *>(this);
    for (; !data_prev.empty() && Interval::gt_upper(ts - data_prev.front_ts());
         data_prev.pop_front()) {
      if constexpr (!is_once) {
        decltype(auto) tab = data_prev.front_tab();
        if (tab) {
          for (const auto &e : *tab)
            self->drop_tuple_from_all_data(e);
        }
      }
    }
  }

  table_buf_t data_prev;
};

template<bool is_once, bool diff_input, typename T2, typename Interval,
         typename Super>
requires(Interval::contains(0) &&
         Interval::is_infinite) struct lower_bnd_mixin<is_once, diff_input, T2,
                                                       Interval, Super> {
  using table_buf_t = select_table_buf<T2, diff_input, Interval>;

  table_buf_t diff_buf;
};

template<bool is_once, typename T2, typename Super>
struct finite_bnd_mixin {
  // is_once == false
};

template<typename T2, typename Super>
struct finite_bnd_mixin<false, T2, Super> {
  using tuple_buf_t = mp_rename<T2, tuple_buf>;

  void drop_tuple_from_all_data(const T2 &e) {
    auto self = static_cast<Super *>(this);
    auto all_dat_it = all_data_counted.find(e);
    assert(all_dat_it != all_data_counted.end() && all_dat_it->second > 0);
    if ((--all_dat_it->second) == 0) {
      self->tuple_since.erase(all_dat_it->first);
      all_data_counted.erase(all_dat_it);
    }
  }

  tuple_buf_t all_data_counted;
};

template<bool is_once, bool diff_input, typename T2, typename Interval,
         typename Super>
struct upper_bnd_mixin
    : lower_bnd_mixin<
        is_once, diff_input, T2, Interval,
        upper_bnd_mixin<is_once, diff_input, T2, Interval, Super>> {};

template<bool is_once, bool diff_input, typename T2, typename Interval,
         typename Super>
requires(!Interval::is_infinite) struct upper_bnd_mixin<is_once, diff_input, T2,
                                                        Interval, Super>
    : lower_bnd_mixin<
        is_once, diff_input, T2, Interval,
        upper_bnd_mixin<is_once, diff_input, T2, Interval, Super>>,
      finite_bnd_mixin<is_once, T2, Super> {
  using table_buf_t = select_table_buf<T2, diff_input, Interval>;

  void maybe_drop_from_tuple_in(std::size_t ts, const T2 &e) {
    auto self = static_cast<Super *>(this);
    auto in_it = self->tuple_in.find(e);
    if (in_it != self->tuple_in.end() && in_it->second == ts)
      self->tuple_in_erase(in_it);
    if constexpr (!is_once)
      this->drop_tuple_from_all_data(e);
  }

  void drop_too_old_in(std::size_t ts) {
    for (; !data_in.empty(); data_in.pop_front()) {
      auto old_ts = data_in.front_ts();
      assert(ts >= old_ts);
      if (Interval::leq_upper(ts - old_ts))
        break;
      decltype(auto) tab = data_in.front_tab();
      if (tab) {
        for (const auto &e : *tab)
          maybe_drop_from_tuple_in(old_ts, e);
      }
    }
  }

  void drop_too_old(std::size_t ts) {
    drop_too_old_in(ts);
    if constexpr (!Interval::contains(0))
      this->drop_too_old_prev(ts);
  }

  table_buf_t data_in;
};

template<bool is_once, bool diff_input, typename AggInfo, typename Interval,
         typename L2, typename T2, typename Super>
struct base_mixin
    : aggregation_mixin<is_once && Interval::is_infinite, AggInfo, L2, T2>,
      upper_bnd_mixin<is_once, diff_input, T2, Interval, Super> {
  using tab2_t = table_util::tab_t_of_row_t<T2>;
  using add_table_param_t = mp_if_c<
    !diff_input, std::optional<tab2_t> &,
    boost::variant2::variant<std::optional<tab2_t>, table_diff<tab2_t>> &>;
  using add_table_impl_param_t =
    mp_if_c<!diff_input, std::optional<tab2_t> &,
            mp_if_c<Interval::contains(0) && Interval::is_infinite,
                    std::optional<tab2_t> const &,
                    std::optional<persistent_table<T2>>>>;

  void update_ts_buf(const ts_list &ts) {
    ts_buf_.insert(ts_buf_.end(), ts.begin(), ts.end());
  }

  bool maybe_add_new_ts() {
    if (ts_buf_.empty()) {
      return false;
    } else {
      add_new_ts();
      return true;
    }
  }

  void add_new_ts() {
    auto self = static_cast<Super *>(this);
    assert(!ts_buf_.empty());
    std::size_t ts = ts_buf_.front();
    ts_buf_.pop_front();
    nts_ = ts;
    if constexpr (!Interval::is_infinite) {
      this->drop_too_old(ts);
    }

    if constexpr (!Interval::contains(0)) {
      for (; !this->data_prev.empty(); this->data_prev.pop_front()) {
        std::size_t old_ts = this->data_prev.front_ts();
        decltype(auto) tab = this->data_prev.front_tab();
        assert(old_ts <= ts);
        if (Interval::lt_lower(ts - old_ts))
          break;
        if (tab) {
          for (const auto &e : *tab) {
            if constexpr (is_once)
              this->tuple_in_update(e, old_ts);
            else
              self->add_tuple_in_if_since(old_ts, e);
          }
        }
        if constexpr (!Interval::is_infinite) {
          // assert(this->data_in.empty() || old_ts >=
          // this->data_in.back().first);
          this->data_in.add_ts_table(old_ts, std::move(tab));
        }
      }
    }
  }

  void add_new_table_impl(add_table_impl_param_t tab_r) {
    auto self = static_cast<Super *>(this);
    if constexpr (!is_once) {
      if (tab_r) {
        for (const auto &e : *tab_r)
          self->add_to_tuple_since(e);
      }
    }
    if constexpr (Interval::contains(0)) {
      if (tab_r) {
        for (const auto &e : *tab_r)
          this->tuple_in_update(e, nts_);
      }
      if constexpr (!Interval::is_infinite) {
        // assert(this->data_in.empty() || nts_ >= this->data_in.back().first);
        this->data_in.add_ts_table(nts_, std::move(tab_r));
      }
    } else {
      // assert(this->data_prev.empty() || nts_ >=
      // this->data_prev.back().first);
      this->data_prev.add_ts_table(nts_, std::move(tab_r));
    }
  }

  void add_new_table(add_table_param_t data_r) {
    if constexpr (!diff_input)
      add_new_table_impl(data_r);
    else if constexpr (!Interval::contains(0))
      add_new_table_impl(this->data_prev.apply_diff(data_r));
    else if constexpr (!Interval::is_infinite)
      add_new_table_impl(this->data_in.apply_diff(data_r));
    else {
      this->diff_buf.apply_diff(data_r);
      add_new_table_impl(this->diff_buf.get_last_tab());
    }
  }

  std::size_t nts_;
  boost::container::devector<std::size_t> ts_buf_;
};

template<typename AggInfo, bool left_negated, typename Interval,
         typename MFormula1, typename MFormula2>
struct since_impl
    : base_mixin<
        false, MFormula2::DiffRes, AggInfo, Interval, typename MFormula2::ResL,
        typename MFormula2::ResT,
        since_impl<AggInfo, left_negated, Interval, MFormula1, MFormula2>> {
  using Base = typename since_impl::base_mixin;

  using L1 = typename MFormula1::ResL;
  using L2 = typename MFormula2::ResL;
  using T1 = typename MFormula1::ResT;
  using T2 = typename MFormula2::ResT;

  using ResL = typename Base::ResL;
  using ResT = typename Base::ResT;
  using tab1_t = table_util::tab_t_of_row_t<T1>;
  using tab2_t = return_type_selector<MFormula2>;
  using project_idxs = table_util::comp_common_idx<L2, L1>;
  using tuple_buf_t = mp_rename<T2, tuple_buf>;
  using res_t =
    std::remove_cvref_t<decltype(std::declval<Base>().produce_result())>;

  void add_tuple_in_if_since(std::size_t old_ts, const T2 &e) {
    auto since_it = tuple_since.find(e);
    if (since_it != tuple_since.end() && since_it->second <= old_ts)
      this->tuple_in_update(e, old_ts);
  }

  void add_to_tuple_since(const T2 &e) {
    tuple_since.try_emplace(e, this->nts_);
    if constexpr (!Interval::is_infinite)
      this->all_data_counted[e]++;
  }

  void join(const std::optional<tab1_t> &tab1) {
    if (!tab1) {
      if constexpr (!left_negated) {
        this->tuple_since.clear();
        this->tuple_in_clear();
      }
      return;
    }
    assert(!tab1->empty());
    auto erase_cond = [&tab1](const auto &tup) {
      if constexpr (left_negated)
        return tab1->contains(project_row<project_idxs>(tup.first));
      else
        return !tab1->contains(project_row<project_idxs>(tup.first));
    };
    absl::erase_if(this->tuple_since, erase_cond);
    this->tuple_in_erase_if(erase_cond);
  }

  std::vector<res_t> eval(database &db, const ts_list &ts) {
    auto rec_res1 = f1_.eval(db, ts);
    auto rec_res2 = f2_.eval(db, ts);
    // check_no_some_empty_tab(rec_res1);
    // check_no_some_empty_tab(rec_res2);
    this->update_ts_buf(ts);
    std::vector<res_t> res;
    f1_buf_.insert(f1_buf_.end(), std::make_move_iterator(rec_res1.begin()),
                   std::make_move_iterator(rec_res1.end()));
    f2_buf_.insert(f2_buf_.end(), std::make_move_iterator(rec_res2.begin()),
                   std::make_move_iterator(rec_res2.end()));
    for (;;) {
      bool has_l = !f1_buf_.empty(), has_r = !f2_buf_.empty();
      if (has_l && !has_r) {
        if constexpr (Interval::contains(0))
          break;
        if (!skew_) {
          this->add_new_ts();
          this->join(f1_buf_.front());
          f1_buf_.pop_front();
          res.emplace_back(this->produce_result());
          skew_ = true;
        }
        break;
      } else if (has_r && skew_) {
        this->add_new_table(f2_buf_.front());
        f2_buf_.pop_front();
        skew_ = false;
      } else if (has_l && has_r && !skew_) {
        this->add_new_ts();
        this->join(f1_buf_.front());
        f1_buf_.pop_front();
        this->add_new_table(f2_buf_.front());
        f2_buf_.pop_front();
        res.emplace_back(this->produce_result());
      } else {
        break;
      }
    }
    return res;
  }

  MFormula1 f1_;
  MFormula2 f2_;
  boost::container::devector<std::optional<tab1_t>> f1_buf_;
  boost::container::devector<tab2_t> f2_buf_;
  tuple_buf_t tuple_since;
  bool skew_ = false;
};

template<typename AggInfo, typename Interval, typename MFormula>
struct once_impl : base_mixin<true, MFormula::DiffRes, AggInfo, Interval,
                              typename MFormula::ResL, typename MFormula::ResT,
                              once_impl<AggInfo, Interval, MFormula>> {
  using Base = typename once_impl::base_mixin;
  using ResL = typename Base::ResL;
  using ResT = typename Base::ResT;
  using res_t =
    std::remove_cvref_t<decltype(std::declval<Base>().produce_result())>;

  std::vector<res_t> eval(database &db, const ts_list &ts) {
    auto rec_tabs = f_.eval(db, ts);
    // check_no_some_empty_tab(rec_tabs);
    this->update_ts_buf(ts);
    std::vector<res_t> res;
    res.reserve(rec_tabs.size());
    auto it = rec_tabs.begin(), eit = rec_tabs.end();
    if constexpr (!Interval::contains(0)) {
      if (it != eit && skew_) {
        this->add_new_table(*it);
        skew_ = false;
        ++it;
      }
    }
    for (; it != eit; ++it) {
      this->add_new_ts();
      this->add_new_table(*it);
      res.emplace_back(this->produce_result());
    }
    if constexpr (!Interval::contains(0)) {
      if (!skew_ && this->maybe_add_new_ts()) {
        res.emplace_back(this->produce_result());
        skew_ = true;
      }
    }
    return res;
  }

  MFormula f_;
  bool skew_ = false;
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
