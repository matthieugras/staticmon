#pragma once
#include <boost/container/devector.hpp>
#include <boost/mp11.hpp>
#include <cassert>
#include <cstdint>
#include <iterator>
#include <monitor_types.h>
#include <table.h>
#include <type_traits>
#include <utility>
#include <vector>

template<typename T1, typename T2>
class bin_op_buffer {
public:
  template<typename F>
  using ResT = std::invoke_result_t<F, std::add_lvalue_reference_t<T1>,
                                    std::add_lvalue_reference_t<T2>>;

  template<typename F>
  std::vector<ResT<F>> update_and_reduce(std::vector<T1> &new_l,
                                         std::vector<T2> &new_r, F f) {
    assert(bufl_.empty() || bufr_.empty());
    if (bufl_.empty())
      return update_and_reduce_impl<true>(bufl_, bufr_, new_l, new_r, f);
    else
      return update_and_reduce_impl<false>(bufr_, bufl_, new_r, new_l, f);
  }

private:
  template<bool fst_is_left, typename Typ1, typename Typ2, typename F>
  std::vector<ResT<F>>
  update_and_reduce_impl(boost::container::devector<Typ1> &buf1,
                         boost::container::devector<Typ2> &buf2,
                         std::vector<Typ1> &new1, std::vector<Typ2> &new2,
                         F f) {
    auto it1 = new1.begin(), eit1 = new1.end();
    auto it2 = new2.begin(), eit2 = new2.end();
    std::vector<ResT<F>> res;
    std::size_t n_match_buf = std::min(buf2.size(), new1.size());
    std::size_t l_left = new1.size() - n_match_buf;
    res.reserve(n_match_buf + std::min(l_left, new2.size()));
    for (; !buf2.empty() && it1 != eit1; ++it1, buf2.pop_back()) {
      if constexpr (fst_is_left)
        res.emplace_back(f(*it1, buf2.back()));
      else
        res.emplace_back(f(buf2.back(), *it1));
    }
    for (; it1 != eit1 && it2 != eit2; ++it1, ++it2) {
      if constexpr (fst_is_left)
        res.emplace_back(f(*it1, *it2));
      else
        res.emplace_back(f(*it2, *it1));
    }
    if (it1 == eit1)
      buf2.insert(buf2.end(), std::make_move_iterator(it2),
                  std::make_move_iterator(eit2));
    else
      buf1.insert(buf1.end(), std::make_move_iterator(it1),
                  std::make_move_iterator(eit1));
    return res;
  }

  boost::container::devector<T1> bufl_;
  boost::container::devector<T2> bufr_;
};

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
        rec_res1, rec_res2, [](const rec_tab1_t &tab1, const rec_tab2_t &tab2) {
          auto joined = table_util::table_join<L1, L2>(tab1, tab2);
          static_assert(std::is_same_v<decltype(joined), res_tab_t>,
                        "unexpected join return type");
          return table_util::table_join<L1, L2>(tab1, tab2);
        });
    } else if constexpr (std::is_same_v<Tag, or_tag>) {
      return buf_.update_and_reduce(
        rec_res1, rec_res2, [](const rec_tab1_t &tab1, const rec_tab2_t &tab2) {
          auto unioned = table_util::table_union<L1, L2>(tab1, tab2);
          static_assert(std::is_same_v<decltype(unioned), res_tab_t>,
                        "unexpected union return type");
        });
    } else if constexpr (std::is_same_v<Tag, and_not_tag>) {
      return buf_.update_and_reduce(
        rec_res1, rec_res2, [](const rec_tab1_t &tab1, const rec_tab2_t &tab2) {
          auto ajoined = table_util::table_anti_join<L1, L2>(tab1, tab2);
          static_assert(std::is_same_v<decltype(ajoined), res_tab_t>,
                        "unexpected antijoin return type");
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
