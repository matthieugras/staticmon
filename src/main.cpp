#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <monitor_types.h>
#include <mpredicate.h>
#include <string>
#include <table.h>
#include <tuple>
#include <typename.h>

int main() {
  using Formula = mpredicate<mp_size_t<2>>;
  Formula f;
  event ev1 = {event_data(std::int64_t{1}), event_data(std::int64_t{2})};
  event ev2 = {event_data(std::int64_t{2}), event_data(std::int64_t{8})};
  database db1 = {{2, {{ev1, ev2}}}};
  ts_list ts = {0};

  fmt::print("{}\n", f.eval<void, void>(db1, ts));
}
