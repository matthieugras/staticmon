#pragma once
#include <filesystem>
#include <fstream>
#include <monitor.h>
#include <monitor_driver.h>
#include <optional>
#include <string>
#include <parser/trace_parser.h>
#include <verdict_printer.h>

class file_monitor_driver : public monitor_driver {
public:
  file_monitor_driver(const std::filesystem::path &log_path,
                      std::optional<std::string> verdict_path);
  ~file_monitor_driver() noexcept override = default;
  void do_monitor() override;

private:
  parse::trace_parser parser_;
  std::ifstream log_;
  monitor monitor_;
};
