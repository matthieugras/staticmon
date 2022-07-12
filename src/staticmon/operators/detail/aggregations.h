#pragma once
#include <absl/container/btree_set.h>
#include <absl/container/flat_hash_map.h>
#include <boost/mp11.hpp>
#include <cstdint>
#include <staticmon/common/mp_helpers.h>
#include <staticmon/common/table.h>
#include <staticmon/operators/detail/operator_types.h>
#include <type_traits>
#include <vector>

using namespace boost::mp11;

struct max_agg_op;
struct min_agg_op;
struct avg_agg_op;
struct sum_agg_op;
struct cnt_agg_op;

template<typename AggVarT>
struct simple_combiner {
  using ResT = AggVarT;

  simple_combiner(AggVarT &&first_event) : res_(std::forward(first_event)) {}

  ResT finalize_group() const { return res_; }

  AggVarT res_;
};

template<typename AggTag, typename AggVarT>
struct agg_group {
  static_assert(always_false_v<AggTag>, "unknown aggregation type");
};

template<typename AggVarT>
struct agg_group<max_agg_op, AggVarT> : simple_combiner<AggVarT> {
  agg_group<max_agg_op, AggVarT>(AggVarT &&first_event)
      : simple_combiner<AggVarT>(std::forward<AggVarT>(first_event)) {}

  void add_event(AggVarT &&val) {
    if (val > this->res_)
      this->res_ = std::forward<AggVarT>(val);
  }
};

template<typename AggVarT>
struct agg_group<min_agg_op, AggVarT> : simple_combiner<AggVarT> {
  agg_group<min_agg_op, AggVarT>(AggVarT &&first_event)
      : simple_combiner<AggVarT>(std::forward<AggVarT>(first_event)) {}

  void add_event(const AggVarT &val) {
    if (val < this->res_)
      this->res_ = std::move(val);
  }
};

template<typename AggVarT>
struct agg_group<sum_agg_op, AggVarT> : simple_combiner<AggVarT> {
  static_assert(is_number_type<AggVarT>,
                "sum aggregation only for number types");

  agg_group<sum_agg_op, AggVarT>(AggVarT first_event)
      : simple_combiner<AggVarT>(first_event) {}

  void add_event(AggVarT val) { this->res_ += val; }
};

template<typename AggVarT>
struct agg_group<cnt_agg_op, AggVarT> {
  using ResT = std::int64_t;

  agg_group<cnt_agg_op, AggVarT>(AggVarT &&) : counter_(1) {}

  void add_event(AggVarT &&) { counter_++; }

  ResT finalize_group() const { return counter_; }

  std::int64_t counter_;
};

template<typename AggVarT>
struct agg_group<avg_agg_op, AggVarT> {
  static_assert(is_number_type<AggVarT>,
                "average aggregtion only for number types");

  using ResT = double;

  agg_group<avg_agg_op, AggVarT>(AggVarT first_event)
      : sum_(first_event), counter_(1) {}

  void add_event(AggVarT val) {
    sum_ += val;
    counter_++;
  }

  ResT finalize_group() const {
    return static_cast<double>(sum_) / static_cast<double>(counter_);
  }

  AggVarT sum_;
  std::size_t counter_;
};

template<typename GroupType, typename GroupVarT, typename AggVarT>
struct grouped_state {
  using ResVarT = typename GroupType::ResT;
  using ResT = mp_push_back<GroupVarT, ResVarT>;
  using res_tab_t = table_util::tab_t_of_row_t<ResT>;

  void add_result(GroupVarT &&group, AggVarT &&val) {
    auto it = groups_.template find(group);
    if (it == groups_.end())
      groups_.emplace(std::forward<GroupVarT>(group),
                      GroupType(std::forward<AggVarT>(val)));
    else
      it->second.add_event(std::forward<AggVarT>(val));
  }

  res_tab_t finalize_table(bool clear_groups = true) {
    res_tab_t res;
    for (auto &[group, group_state] : groups_)
      res.add_row(std::tuple_cat(
        group, std::forward_as_tuple(group_state.finalize_group())));
    if (clear_groups)
      groups_.clear();
    return res;
  }

  absl::flat_hash_map<GroupVarT, GroupType> groups_;
};

template<typename AggVarT, typename Compare>
struct min_max_group {
  using ResT = AggVarT;

  min_max_group(AggVarT &&first_event) {
    group_state_.insert(std::forward<AggVarT>(first_event));
  }

  void add_event(AggVarT &&val) {
    group_state_.insert(std::forward<AggVarT>(val));
  }

  void remove_event(const AggVarT &val) {
    auto it = group_state_.find(val);
    assert(it != group_state_.end());
    group_state_.erase(it);
  }

  ResT finalize_group() const {
    assert(!group_state_.empty());
    return *group_state_.begin();
  }

  absl::btree_multiset<AggVarT, Compare> group_state_;
};

template<typename AggTag, typename AggVarT>
struct temporal_agg_group {
  static_assert(always_false_v<AggTag>, "unknown temporal aggregation type");
};

template<typename AggVarT>
struct temporal_agg_group<max_agg_op, AggVarT>
    : min_max_group<AggVarT, std::greater<>> {};

template<typename AggVarT>
struct temporal_agg_group<min_agg_op, AggVarT>
    : min_max_group<AggVarT, std::less<>> {};

template<typename AggVarT>
struct temporal_agg_group<sum_agg_op, AggVarT>
    : agg_group<sum_agg_op, AggVarT> {
  void remove_event(const AggVarT &val) { this->res_ -= val; }
};

template<typename AggTag, typename AggVarT>
struct counted_group {
  using GroupType = temporal_agg_group<AggTag, AggVarT>;
  using ResT = typename GroupType::ResT;

  counted_group(AggVarT &&first_event)
      : nested_group_(GroupType(std::forward<AggVarT>(first_event))),
        counter_(1) {}

  bool empty() const { return counter_ == 0; }

  void add_event(AggVarT &&val) {
    nested_group_.add_event(std::forward<AggVarT>(val));
    counter_++;
  }

  void remove_event(const AggVarT &val) {
    assert(counter_ > 0);
    nested_group_.remove_event(val);
    counter_--;
  }

  ResT finalize_group() const {
    assert(!empty());
    return nested_group_.finalize_group();
  }

  GroupType nested_group_;
  std::size_t counter_;
};

template<typename AggVarT>
struct counted_group<cnt_agg_op, AggVarT> : agg_group<cnt_agg_op, AggVarT> {

  bool empty() const { return this->counter_ == 0; }

  void remove_event(const AggVarT &) {
    assert(this->counter_ > 0);
    this->counter_--;
  }
};

template<typename AggVarT>
struct counted_group<avg_agg_op, AggVarT> : agg_group<avg_agg_op, AggVarT> {

  bool empty() const { return this->counter_ == 0; }

  void remove_event(AggVarT val) {
    assert(this->counter_ > 0);
    this->sum_ -= val;
    this->counter_--;
  }
};

template<typename GroupType, typename GroupVarT, typename AggVarT>
struct temporal_grouped_state : grouped_state<GroupType, GroupVarT, AggVarT> {
  void remove_result(const event &group, const AggVarT &val) {
    auto it = this->groups_.find(group);
    assert(it != this->groups_.end());
    it->second.remove_event(val);
    if (it->second.empty())
      this->groups_.erase(it);
  }
};

template<bool temporal_agg, typename L, typename T, std::size_t ResVar,
         typename AggTag, std::size_t AggVar, typename GroupVars>
struct get_agg_info {
  using agg_var_idx = mp_find_partial<L, mp_size_t<AggVar>>;
  using group_var_idxs =
    mp_transform_q<mp_bind_front<mp_find_partial, L>, GroupVars>;
  using GroupVarT = mp_apply_idxs<T, group_var_idxs>;
  using AggVarT = mp_at<T, agg_var_idx>;
  using GroupType = mp_if_c<temporal_agg, counted_group<AggTag, AggVarT>,
                            agg_group<AggTag, AggVarT>>;
  using grouped_state_t =
    mp_if_c<temporal_agg, temporal_grouped_state<AggTag, GroupVarT, AggVarT>,
            grouped_state<AggTag, GroupVarT, AggVarT>>;
};

template<bool temporal_agg, typename L, typename T, std::size_t ResVar,
         typename AggTag, std::size_t AggVar, typename GroupVars>
struct aggregation_impl : get_agg_info<temporal_agg, L, T, ResVar, AggTag,
                                       AggVar, GroupVars>::grouped_state_t {
  using Base =
    get_agg_info<temporal_agg, L, T, ResVar, AggTag, AggVar, GroupVars>;
  using group_var_idxs = typename Base::group_var_idxs;
  using GroupVarT = typename Base::GroupVarT;
  using grouped_state_t = typename Base::grouped_state_t;
  using agg_var_idx = typename Base::agg_var_idx;
  using AggVarT = typename Base::AggVarT;

  using ResL =
    mp_push_back<mp_apply_idxs<L, group_var_idxs>, mp_size_t<ResVar>>;
  using ResT = mp_push_back<GroupVarT, typename grouped_state_t::ResVarT>;
  using res_tab_t = table_util::tab_t_of_row_t<ResT>;

  void add_row(T &row) {
    if constexpr (is_number_type<AggVarT>) {
      AggVarT agg_var = std::get<agg_var_idx>(row);
      add_result(project_row<group_var_idxs>(row), agg_var);
    } else {
      add_result(project_row<group_var_idxs>(row), std::get<agg_var_idx>(row));
    }
  }
};


template<std::size_t ResVar, typename AggTag, std::size_t AggVar,
         typename GroupVars, typename MFormula>
struct maggregation
    : aggregation_impl<false, typename MFormula::ResL, typename MFormula::ResT,
                       ResVar, AggTag, AggVar, GroupVars> {
  using Base =
    aggregation_impl<false, typename MFormula::ResL, typename MFormula::ResT,
                     ResVar, AggTag, AggVar, GroupVars>;
  using res_tab_t = table_util::tab_t_of_row_t<typename Base::ResT>;

  auto eval(database &db, const ts_list &ts) {
    auto rec_tabs = f_.eval(db, ts);
    std::vector<res_tab_t> res;
    res.reserve(rec_tabs.size());
    for (auto &tab : rec_tabs) {
      for (auto &row : tab)
        add_row(row);
      res.emplace_back(this->gstate_.finalize_table());
    }
    return res;
  }

  MFormula f_;
};

template<typename L, typename T, std::size_t ResVar, typename AggTag,
         std::size_t AggVar, typename GroupVars>
struct mtemporalaggregation
    : aggregation_impl<true, L, T, ResVar, AggTag, AggVar, GroupVars> {
  using Base = aggregation_impl<true, L, T, ResVar, AggTag, AggVar, GroupVars>;
  using group_var_idxs = typename Base::group_var_idxs;
  using agg_var_idx = typename Base::agg_var_idx;

  void remove_row(const T &row) {
    this->gstate_.remove_result(project_row<group_var_idxs>(row),
                                std::get<agg_var_idx>(row));
  }
};
