#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <table.h>
#include <tuple>
#include <typename.h>

int main() {
  // using L1 = mp_list<mp_size_t<1>, mp_size_t<0>, mp_size_t<2>>;
  // using L2 = mp_list<mp_size_t<5>, mp_size_t<2>, mp_size_t<1>>;
  // using T = compute_projection_idxs<mp_list<L1, L2>>;
  using L1 = mp_list<mp_size_t<2>, mp_size_t<0>>;
  using L2 = mp_list<mp_size_t<0>, mp_size_t<1>>;
  auto row1 = std::tuple(1,2,3,4);
  auto row2 = std::tuple(5,6,7,8);
  // 3 0 5 6
  fmt::print("{}\n", project_row_mult<mp_list<L1, L2>>(row1, row2));
}
