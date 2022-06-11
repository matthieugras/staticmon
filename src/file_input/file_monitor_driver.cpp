#include <file_monitor_driver.h>

file_monitor_driver::file_monitor_driver(
  const std::filesystem::path &log_path,
  std::optional<std::string> verdict_path)
    : log_(std::ifstream(log_path)), monitor_(verdict_printer(verdict_path)) {}
void file_monitor_driver::do_monitor() {
  std::string db_str;
  while (std::getline(log_, db_str)) {
    auto [ts, db] = parser_.parse_database(db_str);
    monitor_.step(db, make_vector(ts));
  }
  monitor_.last_step();
}
