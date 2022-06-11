#pragma once
#include <cstdint>
#include <formula.h>
#include <operator_types.h>
#include <table.h>
#include <type_traits>
#include <vector>
#include <verdict_printer.h>

inline constexpr size_t MAXIMUM_TIMESTAMP = std::numeric_limits<size_t>::max();

struct monitor {
  using T = typename input_formula::ResT;
  using L = typename input_formula::ResL;
  using rec_tab_t = table_util::tab_t_of_row_t<T>;
  using ResT = typename table_util::reorder_info<L, free_variables, T>::ResT;
  using reorder_mask = table_util::get_reorder_mask<L, free_variables>;

  monitor(verdict_printer printer);

  std::vector<ResT> make_verdicts(rec_tab_t &tab);

  void step(database &db, const ts_list &ts);

  void last_step();

  input_formula f_;
  std::vector<std::size_t> output_var_permutation_;
  absl::flat_hash_map<size_t, size_t> tp_ts_map_;
  std::size_t curr_tp_ = 0;
  std::size_t max_tp_ = 0;
  verdict_printer printer_;
};
