#pragma once
#include <boost/mp11.hpp>
#include <cstdint>
#include <optional>
#include <staticmon/common/mp_helpers.h>
#include <staticmon/common/table.h>
#include <staticmon/operators/detail/operator_types.h>
#include <string>
#include <vector>

using namespace boost::mp11;

template<typename VarTy, std::size_t VarId>
struct pvar {
  using type = clean_monitor_cst_ty<VarTy>;
  using var = mp_size_t<VarId>;
};

template<typename Cst>
struct pcst {
  using cst = Cst;
  using type = clean_monitor_cst_ty<typename Cst::value_type>;
  using var = void;
};

template<typename... Args>
struct predicate_idx_computation {
  using pred_args = mp_list<Args...>;
  using n_args = mp_size<pred_args>;
  using arg_tys = mp_list<typename Args::type...>;
  using arg_vars = mp_list<typename Args::var...>;

  static_assert(mp_all_of_q<arg_tys, mp_bind<is_not_void, _1>>::value,
                "invalid type in predicate");

  template<typename I, typename T>
  using size_less = mp_less<mp_size<T>, I>;

  using tmp_zip = mp_zip<mp_iota<n_args>, arg_vars>;
  using tmp_two =
    mp_partition_q<tmp_zip, mp_bind<std::is_void, mp_bind<mp_second, _1>>>;

  using cst_idxs = mp_transform_q<mp_bind<mp_first, _1>, mp_first<tmp_two>>;
  using var_groups = mp_group_by_snd<mp_second<tmp_two>>;
  using var_non_trivial_groups =
    mp_remove_if_q<mp_project_c<var_groups, 1>,
                   mp_bind_front<size_less, mp_size_t<2>>>;
  using var_idxs = mp_project_c<mp_project_c<var_groups, 1>, 0>;
  using cst_tys = mp_rename<mp_apply_idxs<arg_tys, cst_idxs>, std::tuple>;
  using csts = mp_apply_idxs<pred_args, cst_idxs>;

  using ResT = mp_rename<mp_apply_idxs<arg_tys, var_idxs>, std::tuple>;
  using ResL = mp_apply_idxs<arg_vars, var_idxs>;
  using res_tab_t = mp_rename<ResT, table_util::table>;
};

struct mtp_tag;
struct mts_tag;
struct mtpts_tag;

template<typename Tag>
struct builtin_pred_impl {};

template<>
struct builtin_pred_impl<mtp_tag> {
  auto eval(std::size_t) {
    auto ret = std::tuple(static_cast<std::int64_t>(curr_tp_));
    curr_tp_ += 1;
    return ret;
  }

  std::size_t curr_tp_ = 0;
};

template<>
struct builtin_pred_impl<mts_tag> {
  auto eval(std::size_t ts) {
    curr_ts_ = ts;
    auto ret = std::tuple(static_cast<std::int64_t>(curr_ts_));
    return ret;
  }

  std::size_t curr_ts_ = 0;
};

template<>
struct builtin_pred_impl<mtpts_tag> {
  auto eval(std::size_t ts) {
    curr_ts_ = ts;
    auto ret = std::tuple(static_cast<std::int64_t>(curr_tp_),
                          static_cast<std::int64_t>(curr_ts_));
    curr_tp_ += 1;
    return ret;
  }

  std::size_t curr_tp_ = 0;
  std::size_t curr_ts_ = 0;
};

template<typename Tag, typename... Args>
struct builtin_pred {
  using PredComp = predicate_idx_computation<Args...>;
  using res_tab_t = typename PredComp::res_tab_t;
  using cst_idxs = typename PredComp::cst_idxs;
  using var_idxs = typename PredComp::var_idxs;
  using var_non_trivial_groups = typename PredComp::var_non_trivial_groups;
  using csts = typename PredComp::csts;
  using ResL = typename PredComp::ResL;
  using ResT = typename PredComp::ResT;

  template<typename Row, typename... VarIdxs>
  ResT project_event_vars(const Row &row, mp_list<VarIdxs...>) {
    return std::tuple(std::get<VarIdxs::value>(row)...);
  }

  template<typename Row, typename... CstIdxs, typename... Csts>
  bool sat_cst_constraints(const Row &row, mp_list<CstIdxs...>,
                           mp_list<Csts...>) {
    return (true && ... && (std::get<CstIdxs::value>(row) == Csts::cst::value));
  }

  template<typename Row, typename FstVarIdx, typename... VarIdxs>
  bool sat_var_group(const Row &row, mp_list<FstVarIdx, VarIdxs...>) {
    const auto &fst = std::get<FstVarIdx::value>(row);
    return (true && ... && (std::get<VarIdxs::value>(row) == fst));
  }

  template<typename Row, typename... Groups>
  bool sat_var_constraints(const Row &row, mp_list<Groups...>) {
    return (true && ... && (sat_var_group(row, Groups{})));
  }

  std::vector<std::optional<res_tab_t>> eval(database &, const ts_list &ts) {
    std::vector<std::optional<res_tab_t>> res;
    res.reserve(ts.size());
    for (std::size_t t : ts) {
      auto row = impl_.eval(t);
      if (sat_cst_constraints(row, cst_idxs{}, csts{}) &&
          sat_var_constraints(row, var_non_trivial_groups{})) {
        res.emplace_back(
          table_util::single_row_table(project_event_vars(row, var_idxs{})));
      } else {
        res.emplace_back();
      }
    }
    return res;
  }

  builtin_pred_impl<Tag> impl_;
};

template<typename... Args>
struct mtp : builtin_pred<mtp_tag, Args...> {};

template<typename... Args>
struct mts : builtin_pred<mts_tag, Args...> {};

template<typename... Args>
struct mtpts : builtin_pred<mtpts_tag, Args...> {};

template<std::size_t PredId, typename... Args>
struct mpredicate {
  using PredComp = predicate_idx_computation<Args...>;
  using res_tab_t = typename PredComp::res_tab_t;
  using cst_tys = typename PredComp::cst_tys;
  using cst_idxs = typename PredComp::cst_idxs;
  using var_idxs = typename PredComp::var_idxs;
  using var_non_trivial_groups = typename PredComp::var_non_trivial_groups;
  using csts = typename PredComp::csts;
  using ResL = typename PredComp::ResL;
  using ResT = typename PredComp::ResT;

  template<typename Event, typename... VarTys, typename... VarIdxs>
  ResT project_event_vars(const Event &e, std::tuple<VarTys...>,
                          mp_list<VarIdxs...>) {
    return std::tuple(std::get<VarTys>(e[VarIdxs::value])...);
  }

  template<typename Event, typename... CstTys, typename... CstIdxs,
           typename... Csts>
  bool sat_cst_constraints(const Event &e, std::tuple<CstTys...>,
                           mp_list<CstIdxs...>, mp_list<Csts...>) {
    return (true && ... &&
            (std::get<CstTys>(e[CstIdxs::value]) == Csts::cst::value));
  }

  template<typename Event, typename FstVarIdx, typename... VarIdxs>
  bool sat_var_group(const Event &e, mp_list<FstVarIdx, VarIdxs...>) {
    const auto &fst = e[FstVarIdx::value];
    return (true && ... && (e[VarIdxs::value] == fst));
  }

  template<typename Event, typename... Groups>
  bool sat_var_constraints(const Event &e, mp_list<Groups...>) {
    return (true && ... && (sat_var_group(e, Groups{})));
  }

  std::vector<std::optional<res_tab_t>> eval(database &db, const ts_list &ts) {
    std::size_t n = ts.size();
    const auto &cdb = db;
    auto it = cdb.find(PredId);
    if (it == cdb.end())
      return std::vector<std::optional<res_tab_t>>(n);
    std::vector<std::optional<res_tab_t>> res;
    res.reserve(n);
    const auto &evll = it->second;
    for (const auto &evl : evll) {
      res_tab_t tab;
      for (const auto &ev : evl) {
        if (sat_cst_constraints(ev, cst_tys{}, cst_idxs{}, csts{}) &&
            sat_var_constraints(ev, var_non_trivial_groups{}))
          tab.emplace(project_event_vars(ev, ResT{}, var_idxs{}));
      }
      if (tab.empty())
        res.emplace_back();
      else
        res.emplace_back(std::move(tab));
    }
    return res;
  }
};
