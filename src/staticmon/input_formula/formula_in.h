using input_formula =
  mand<false,
         mpredicate<mp_size_t<4>, pvar<std::int64_t, mp_size_t<2>>,
                      pvar<std::int64_t, mp_size_t<3>>>,
         mpredicate<mp_size_t<5>>>;
using free_variables =
  mp_list<mp_size_t<2>, mp_size_t<3>>;
inline static const pred_map_t input_predicates =
  {{"E", {5, {}}}, {"A", {4, {INT_TYPE, INT_TYPE}}}};
