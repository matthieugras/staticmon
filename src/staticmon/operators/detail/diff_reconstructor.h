#pragma once
#include <boost/variant2.hpp>
#include <staticmon/common/table.h>
#include <staticmon/common/table_diff.h>
#include <staticmon/operators/detail/operator_types.h>
#include <type_traits>

template<typename MFormula>
struct diff_reconstructor {
  using ResT = typename MFormula::ResT;
  using ResL = typename MFormula::ResL;
  static constexpr bool DiffRes = false;

  using res_tab_t = table_util::tab_t_of_row_t<ResT>;

  std::vector<std::optional<res_tab_t>> eval(database &db, const ts_list &ts) {
    auto rec_tabs = f_.eval(db, ts);
    std::vector<std::optional<res_tab_t>> res;
    res.reserve(rec_tabs.size());
    for (auto &t : rec_tabs) {
      buf_.apply_diff(t);
      res.emplace_back(buf_.get_last_tab());
    }
    return res;
  }

  MFormula f_;
  singleton_diff_buf<ResT> buf_;
};
