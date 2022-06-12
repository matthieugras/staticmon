#pragma once
#include <filesystem>
#include <fstream>
#include <optional>
#include <staticmon/file_input/parser/trace_parser.h>
#include <staticmon/monitor/monitor.h>
#include <staticmon/monitor/monitor_driver.h>
#include <staticmon/operators/operators.h>
#include <string>

class file_monitor_driver : public monitor_driver {
public:
  file_monitor_driver(const std::filesystem::path &log_path,
                      std::optional<std::string> verdict_path)
      : log_(std::ifstream(log_path)), monitor_(verdict_printer(verdict_path)) {
  }
  ~file_monitor_driver() noexcept override = default;
  void do_monitor() override {
    std::string db_str;
    while (std::getline(log_, db_str)) {
      auto [ts, db] = parser_.parse_database(db_str);
      monitor_.step(db, make_vector(ts));
    }
    monitor_.last_step();
  }

private:
  parse::trace_parser parser_;
  std::ifstream log_;
  monitor monitor_;
};
