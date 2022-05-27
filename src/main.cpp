#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <table.h>
#include <tuple>
#include <typename.h>
#include <string>

int main() {
  using namespace table_util;
  // using L1 = mp_list_c<std::size_t, 0, 1, 2, 3>;
  // using L2 = mp_list_c<std::size_t, 1, 3, 2, 5>;
  // using T1 = table<L1, int, float, double, int>;
  // using T2 = table<L2, float, int, double, std::string>;
  // T1 bla;
  // bla.emplace(std::make_tuple(5,3.0,2.0,5));
  // T2 bla2;
  // bla2.emplace(std::make_tuple(5.0,5,2.0,"roflcopter"));
  // fmt::print("{}\n", table_join(bla, bla2));
  using L1 = mp_list_c<std::size_t, 0>;
  using L2 = mp_list_c<std::size_t, 1>;
  using T1 = mp_list<int>;
  using T2 = mp_list<int>;
  using Tab1 = table<L1, int>;
  using Tab2 = table<L2, int>;
  Tab1 bla;
  bla.emplace(std::make_tuple(2));
  Tab2 bla2;
  bla2.emplace(std::make_tuple(3));
  // using LAYOUT = typename table_util::detail::get_join_layout_impl<L1, L2, T1, T2>;
  // fmt::print("{}\n", TypePrinter<LAYOUT>::print_type());
  fmt::print("{}\n", table_join(bla, bla2));
}
