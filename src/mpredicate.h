#pragma once
#include <boost/mp11.hpp>
#include <cstdint>
#include <monitor_types.h>
#include <mp_helpers.h>
#include <string>
#include <table.h>
#include <vector>

using namespace boost::mp11;

template<typename VarTy, typename VarId>
struct pvar {
  using type = VarTy;
  using var = VarId;
};

template<typename Cst>
struct pcst {
  using cst = Cst;
  using value_type = typename Cst::value_type;
  using type = mp_if<
    std::is_same<value_type, std::string_view>, std::string,
    mp_if<std::is_same<value_type, double>, double,
          mp_if<std::is_same<value_type, std::int64_t>, std::int64_t, void>>>;
  using var = void;
};

template<typename PredId, typename... Args>
struct mpredicate {
  using pred_args = mp_list<Args...>;
  using n_args = mp_size<pred_args>;
  using arg_tys = mp_list<typename Args::type...>;
  using arg_vars = mp_list<typename Args::var...>;

  static_assert(mp_all_of_q<arg_tys, mp_bind<is_not_void, _1>>::value,
                "invalid type in predicate");

  using tmp_zip = mp_zip<mp_iota<n_args>, arg_vars>;
  using tmp_two =
    mp_partition_q<tmp_zip, mp_bind<std::is_void, mp_bind<mp_second, _1>>>;

  using cst_idxs = mp_transform_q<mp_bind<mp_first, _1>, mp_first<tmp_two>>;
  using var_idxs = mp_transform_q<mp_bind<mp_first, _1>, mp_second<tmp_two>>;
  using cst_tys = mp_rename<mp_apply_idxs<arg_tys, cst_idxs>, std::tuple>;
  using csts = mp_apply_idxs<pred_args, cst_idxs>;
  using res_row_l = mp_apply_idxs<arg_vars, var_idxs>;
  using res_row_t = mp_rename<mp_apply_idxs<arg_tys, var_idxs>, std::tuple>;

  template<typename L, typename T>
  struct result_info {
    using ResL = res_row_l;
    using ResT = res_row_t;
  };

  template<typename Event, typename... VarTys, typename... VarIdxs>
  auto project_event_vars(const Event &e, std::tuple<VarTys...>,
                          mp_list<VarIdxs...>) {
    return std::tuple(std::get<VarTys>(e[VarIdxs::value])...);
  }

  template<typename Event, typename... CstTys, typename... CstIdxs,
           typename... Csts>
  bool sat_constraint(const Event &e, std::tuple<CstTys...>,
                      mp_list<CstIdxs...>, mp_list<Csts...>) {
    return (true && ... &&
            (std::get<CstTys>(e[CstIdxs::value]) == Csts::cst::value));
  }

  template<typename L, typename T>
  std::vector<mp_rename<typename result_info<L, T>::ResT, table_util::table>>
  eval(database &db, const ts_list &ts) {
    using res_row_t = typename result_info<L, T>::ResT;
    using res_tab_t = mp_rename<res_row_t, table_util::table>;
    const auto &cdb = db;
    auto it = cdb.find(PredId::value);
    if (it == cdb.cend())
      return std::vector<res_tab_t>(ts.size());
    if constexpr (mp_empty<var_idxs>::value && mp_empty<cst_idxs>::value) {
      return std::vector<res_tab_t>(ts.size(), res_tab_t({res_row_t()}));
    } else {
      std::vector<res_tab_t> res;
      res.reserve(ts.size());
      const auto &evll = it->second;
      for (const auto &evl : evll) {
        res_tab_t tab;
        for (const auto &ev : evl) {
          if (sat_constraint(ev, cst_tys{}, cst_idxs{}, csts{}))
            tab.emplace(project_event_vars(ev, res_row_t{}, var_idxs{}));
        }
        res.emplace_back(std::move(tab));
      }
      return res;
    }
  }
};
