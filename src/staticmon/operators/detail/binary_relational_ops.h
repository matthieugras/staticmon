#pragma once
#include <boost/container/devector.hpp>
#include <boost/mp11.hpp>
#include <cassert>
#include <cstdint>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <iterator>
#include <staticmon/common/table.h>
#include <staticmon/operators/detail/binary_buffer.h>
#include <staticmon/operators/detail/operator_types.h>
#include <type_traits>
#include <utility>
#include <vector>

using namespace boost::mp11;

struct and_not_tag;
struct and_tag;
struct or_tag;

template<typename Tag, typename MFormula1, typename MFormula2>
struct bin_rel_op {
  using T1 = typename MFormula1::ResT;
  using L1 = typename MFormula1::ResL;

  using T2 = typename MFormula2::ResT;
  using L2 = typename MFormula2::ResL;

  using OpRes =
    mp_if<std::is_same<Tag, and_tag>,
          typename table_util::join_result_info<L1, L2, T1, T2>,
          mp_if<std::is_same<Tag, or_tag>,
                typename table_util::union_result_info<L1, L2, T1, T2>,
                mp_if<std::is_same<Tag, and_not_tag>,
                      table_util::anti_join_info<L1, L2, T1, T2>, void>>>;

  static_assert(!std::is_void_v<OpRes>, "invalid tag");

  using ResL = typename OpRes::ResL;
  using ResT = typename OpRes::ResT;

  using res_tab_t = table_util::tab_t_of_row_t<ResT>;
  using rec_tab1_t = table_util::tab_t_of_row_t<T1>;
  using rec_tab2_t = table_util::tab_t_of_row_t<T2>;

  std::vector<res_tab_t> eval(database &db, const ts_list &ts) {
    auto rec_res1 = f1_.eval(db, ts);
    auto rec_res2 = f2_.eval(db, ts);
    static_assert(std::is_same_v<decltype(rec_res1), std::vector<rec_tab1_t>>,
                  "left subformula has unexpected type");
    static_assert(std::is_same_v<decltype(rec_res2), std::vector<rec_tab2_t>>,
                  "left subformula has unexpected type");
    if constexpr (std::is_same_v<Tag, and_tag>) {
      return buf_.update_and_reduce(
        rec_res1, rec_res2, [](rec_tab1_t &tab1, rec_tab2_t &tab2) {
          auto joined = table_util::table_join<L1, L2>(tab1, tab2);
          static_assert(std::is_same_v<decltype(joined), res_tab_t>,
                        "unexpected join return type");
          return joined;
        });
    } else if constexpr (std::is_same_v<Tag, or_tag>) {
      return buf_.update_and_reduce(
        rec_res1, rec_res2, [](rec_tab1_t &tab1, rec_tab2_t &tab2) {
          auto unioned = table_util::table_union<L1, L2>(std::move(tab1), tab2);
          static_assert(std::is_same_v<decltype(unioned), res_tab_t>,
                        "unexpected union return type");
          return unioned;
        });
    } else if constexpr (std::is_same_v<Tag, and_not_tag>) {
      return buf_.update_and_reduce(
        rec_res1, rec_res2, [](const rec_tab1_t &tab1, const rec_tab2_t &tab2) {
          auto ajoined = table_util::table_anti_join<L1, L2>(tab1, tab2);
          static_assert(std::is_same_v<decltype(ajoined), res_tab_t>,
                        "unexpected antijoin return type");
          return ajoined;
        });
    } else {
      static_assert(always_false_v<Tag>, "cannot happen");
    }
  }

  bin_op_buffer<rec_tab1_t, rec_tab2_t> buf_;
  MFormula1 f1_;
  MFormula2 f2_;
};


template<bool is_anti_join, typename MFormula1, typename MFormula2>
struct mand;

template<typename MFormula1, typename MFormula2>
struct mand<true, MFormula1, MFormula2>
    : bin_rel_op<and_not_tag, MFormula1, MFormula2> {};

template<typename MFormula1, typename MFormula2>
struct mand<false, MFormula1, MFormula2>
    : bin_rel_op<and_tag, MFormula1, MFormula2> {};

template<typename MFormula1, typename MFormula2>
struct mor : bin_rel_op<or_tag, MFormula1, MFormula2> {};
