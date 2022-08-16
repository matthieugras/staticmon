#pragma once
#include <boost/variant2.hpp>
#include <cstdint>
#include <optional>
#include <staticmon/common/mp_helpers.h>
#include <staticmon/common/table.h>
#include <staticmon/operators/detail/operator_types.h>

template<typename ReorderMask, typename Tab>
database_table tab_to_db_tab(const Tab &tab) {
  database_table new_tab;
  new_tab.reserve(tab.size());
  for (const auto &e : tab) {
    new_tab.emplace_back(event_from_tuple(project_row<ReorderMask>(e)));
  }
  return new_tab;
}

template<typename ReorderMask, typename Tab>
database::mapped_type tabs_to_db_tabs(const std::vector<Tab> &tabs) {
  database::mapped_type res;
  res.reserve(tabs.size());
  for (const auto &tab : tabs) {
    auto visitor = [&res](auto &&tab) {
      using T = std::remove_cvref_t<decltype(tab)>;
      if constexpr (std::is_same_v<T, std::size_t>) {
        for (std::size_t i = 0; i < tab; ++i) {
          res.emplace_back();
        }
      } else {
        res.emplace_back(tab_to_db_tab<ReorderMask>(tab));
      }
    };
    boost::variant2::visit(visitor, tab);
  }
  return res;
}

template<std::size_t pred_id, typename PredL, typename MFormula1,
         typename MFormula2>
struct mlet {
  using ResL = typename MFormula2::ResL;
  using ResT = typename MFormula2::ResT;
  using opt_res_tab_t = table_util::opt_tab_t_of_row_t<ResT>;
  using reorder_mask =
    table_util::get_reorder_mask<typename MFormula1::ResL, PredL>;

  std::vector<opt_res_tab_t> eval(database &db, const ts_list &ts) {
    auto l_tabs = f1_.eval(db, ts);
    auto db_ent = tabs_to_db_tabs<reorder_mask>(l_tabs);
    // predicate ids assumed to be unique
    db.emplace(pred_id, std::move(db_ent));
    auto res = f2_.eval(db, ts);
    db.erase(pred_id);
    return res;
  }

  MFormula1 f1_;
  MFormula2 f2_;
};

template<std::size_t pred_id, typename PredL, typename MFormula1,
         typename MFormula2>
struct mletpast {
  using ResL = typename MFormula2::ResL;
  using ResT = typename MFormula2::ResT;
  using tab1_t = table_util::tab_t_of_row_t<typename MFormula1::ResT>;
  using res_tab_t = table_util::tab_t_of_row_t<ResT>;
  using reorder_mask =
    table_util::get_reorder_mask<typename MFormula1::ResL, PredL>;

  database::mapped_type db_ent_of_buf() {
    if (!buf_) {
      return database::mapped_type();
    } else {
      auto ret = make_vector(tab_to_db_tab<reorder_mask>(*buf_));
      buf_.reset();
      return ret;
    }
  }

  std::vector<tab1_t> eval_left(database &db, const ts_list &ts,
                                database::mapped_type &buf) {
    std::vector<tab1_t> res;
    bool fst_it = true;
    database::mapped_type rec_buf;
    for (;;) {
      std::vector<tab1_t> rec_tabs;
      if (fst_it) {
        fst_it = false;
        db.emplace(pred_id, std::move(buf));
        rec_tabs = f1_.eval(db, ts);
      } else {
        database rec_db;
        rec_db.emplace(pred_id, std::move(rec_buf));
        rec_tabs = f1_.eval(rec_db, ts_list());
      }
      tp1_ += rec_tabs.size();
      if (rec_tabs.empty()) {
        buf_.reset();
        return res;
      } else {
        if (tp1_ + 1 >= curr_tp_) {
          buf_.emplace(rec_tabs[0]);
          for (auto &tab : rec_tabs)
            res.emplace_back(std::move(tab));
          return res;
        } else {
          rec_buf = tabs_to_db_tabs<reorder_mask>(rec_tabs);
          for (auto &tab : rec_tabs)
            res.emplace_back(std::move(tab));
        }
      }
    }
  }

  std::vector<res_tab_t> eval(database &db, const ts_list &ts) {
    assert(db.find(pred_id) == db.end());
    auto db_ent = db_ent_of_buf();
    auto left_tabs = eval_left(db, ts, db_ent);
    db.insert_or_assign(pred_id, tabs_to_db_tabs<reorder_mask>(left_tabs));
    auto ret = f2_.eval(db, ts);
    db.erase(pred_id);
    curr_tp_ += ts.size();
    return ret;
  }

  MFormula1 f1_;
  MFormula2 f2_;
  std::optional<tab1_t> buf_;
  std::size_t tp1_ = 0;
  std::size_t curr_tp_ = 0;
};
