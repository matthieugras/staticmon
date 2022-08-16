#pragma once
#include <algorithm>
#include <boost/container/devector.hpp>
#include <staticmon/operators/detail/operator_types.h>
#include <type_traits>
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
    assert(buf1.empty() || buf2.empty());
    auto it1 = new1.begin(), eit1 = new1.end();
    auto it2 = new2.begin(), eit2 = new2.end();
    std::vector<ResT<F>> res;
    std::size_t n_match_buf = std::min(buf2.size(), new1.size());
    std::size_t l_left = new1.size() - n_match_buf;
    res.reserve(n_match_buf + std::min(l_left, new2.size()));
    for (; !buf2.empty() && it1 != eit1; ++it1, buf2.pop_front()) {
      if constexpr (fst_is_left)
        res.emplace_back(f(*it1, buf2.front()));
      else
        res.emplace_back(f(buf2.front(), *it1));
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
    assert(buf1.empty() || buf2.empty());
    return res;
  }

  boost::container::devector<T1> bufl_;
  boost::container::devector<T2> bufr_;
};
