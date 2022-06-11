#include <monitor.h>

monitor::monitor(verdict_printer printer) : printer_(std::move(printer)) {}

std::vector<monitor::ResT> monitor::make_verdicts(rec_tab_t &tab) {
  std::vector<ResT> verdicts;
  verdicts.reserve(tab.size());
  for (auto &row : tab)
    verdicts.emplace_back(project_row<reorder_mask>(row));
  return verdicts;
}

void monitor::step(database &db, const ts_list &ts) {
  for (std::size_t t : ts) {
    tp_ts_map_.emplace(max_tp_, t);
    max_tp_++;
  }
  auto sats = f_.eval(db, ts);
  static_assert(std::is_same_v<decltype(sats), std::vector<rec_tab_t>>,
                "unexpected table type");
  std::size_t new_curr_tp = curr_tp_;
  std::size_t n = sats.size();
  for (std::size_t i = 0; i < n; ++i, ++new_curr_tp) {
    auto output_tab = make_verdicts(sats[i]);
    auto it = tp_ts_map_.find(new_curr_tp);
    if (it->second < MAXIMUM_TIMESTAMP)
      printer_.print_verdict(it->second, new_curr_tp, output_tab);
    tp_ts_map_.erase(it);
  }
  curr_tp_ = new_curr_tp;
}

void monitor::last_step() {
  database db;
  step(db, make_vector(static_cast<size_t>(MAXIMUM_TIMESTAMP)));
}
