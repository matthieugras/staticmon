#pragma once
#include <cstdint>
#include <staticmon/common/mp_helpers.h>
#include <staticmon/common/table.h>
#include <staticmon/operators/detail/operator_types.h>

template<std::size_t pred_id, typename PredL, typename MFormula1,
         typename MFormula2>
struct mlet {
  using ResL = typename MFormula2::ResL;
  using ResT = typename MFormula2::ResT;
  using res_tab_t = table_util::tab_t_of_row_t<ResT>;
  using reorder_mask =
    table_util::get_reorder_mask<typename MFormula1::ResL, PredL>;

  std::vector<res_tab_t> eval(database &db, const ts_list &ts) {
    auto l_tabs = f1_.eval(db, ts);

    std::optional<database::mapped_type> old_db_ent;
    auto matching_idx = db.find(pred_id);
    if (matching_idx != db.end()) {
      old_db_ent.emplace(std::move(matching_idx->second));
      db.erase(matching_idx);
    }

    database::mapped_type db_ent;
    const size_t n_l_tabs = l_tabs.size();
    db_ent.reserve(n_l_tabs);
    for (const auto &l_tab : l_tabs) {
      database_table new_tab;
      new_tab.reserve(l_tab.size());
      for (const auto &e : l_tab)
        new_tab.emplace_back(event_from_tuple(project_row<reorder_mask>(e)));
      db_ent.emplace_back(std::move(new_tab));
    }
    db.emplace(pred_id, std::move(db_ent));
    auto res = f2_.eval(db, ts);
    db.erase(pred_id);
    if (old_db_ent)
      db.emplace(pred_id, std::move(old_db_ent.value()));
    return res;
  }

  MFormula1 f1_;
  MFormula2 f2_;
};
