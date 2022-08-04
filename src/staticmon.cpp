#include <absl/flags/flag.h>
#include <absl/flags/marshalling.h>
#include <absl/flags/parse.h>
#include <absl/flags/usage.h>
#include <absl/strings/string_view.h>
#include <config.h>
#include <memory>
#include <staticmon/monitor/monitor_driver.h>
#include <stdexcept>
#include <string>

ABSL_FLAG(bool, use_socket, false,
          "use the unix domain socket monitoring interface");

#ifdef ENABLE_SOCKET_INPUT
#include <uds_monitor_driver.h>
ABSL_FLAG(std::string, socket_path, "cppmon_uds",
          "path of the unix socket to create");
#endif

#ifdef ENABLE_FILE_INPUT
#include <staticmon/file_input/file_monitor_driver.h>
ABSL_FLAG(std::string, log, "log", "path to log in monpoly format");
ABSL_FLAG(std::string, vpath, "", "output file of the monitor's verdicts");
#endif

extern "C" __attribute__((visibility("default"))) const char *
__asan_default_options() {
  return "detect_leaks=false";
}

int main(int argc, char *argv[]) {
  absl::SetProgramUsageMessage("Explicit MFOTL monitor written in C++");
  absl::ParseCommandLine(argc, argv);
  std::unique_ptr<monitor_driver> driver;

#ifdef ENABLE_SOCKET_INPUT
  if (absl::GetFlag(FLAGS_use_socket)) {
    driver.reset(new uds_monitor_driver(
      absl::GetFlag(FLAGS_formula), absl::GetFlag(FLAGS_sig),
      absl::GetFlag(FLAGS_socket_path), std::move(vpath)));
#endif

#ifdef ENABLE_FILE_INPUT
    if (!absl::GetFlag(FLAGS_use_socket)) {
      std::optional<std::string> vpath =
        absl::GetFlag(FLAGS_vpath) == ""
          ? std::nullopt
          : std::optional(absl::GetFlag(FLAGS_vpath));
      driver.reset(
        new file_monitor_driver(absl::GetFlag(FLAGS_log), std::move(vpath)));
    }
#endif
    if (!driver)
      throw std::runtime_error("selected input mode not compiled");
    driver->do_monitor();
  }
