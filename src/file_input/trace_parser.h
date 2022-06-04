#pragma once
#include <absl/container/flat_hash_map.h>
#include <boost/serialization/strong_typedef.hpp>
#include <charconv>
#include <fmt/core.h>
#include <fmt/ranges.h>
#include <iterator>
#include <lexy/action/scan.hpp>
#include <lexy/callback.hpp>
#include <lexy/dsl.hpp>
#include <monitor_types.h>
#include <optional>
#include <string>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

namespace parse {
#define RULE static constexpr auto rule =
#define VALUE static constexpr auto value =

namespace dsl = lexy::dsl;

struct parse_state {
  const pred_map_t &pred_map;
};

class trace_parser {
public:
  trace_parser() = default;
  timestamped_database parse_database(std::string_view db);

private:
  struct string_arg : lexy::token_production {
    using res_type = std::string;
    RULE dsl::quoted(dsl::code_point - dsl::ascii::control,
                     dsl::backslash_escape.capture(dsl::lit_c<'"'> /
                                                   dsl::lit_c<'\\'>));
    VALUE lexy::as_string<std::string>;
  };

  using arg_tup_ty = std::pair<pred_id_t, database_elem>;

  struct unknown_pred {
    static constexpr auto name = "unknown predicate";
  };

  struct not_enough_args {
    static constexpr auto name =
      "predicate has wrong arity - not enough arguments";
  };

  struct too_many_args {
    static constexpr auto name =
      "predicate has wrong arity - too many arguments";
  };
  struct invalid_double {
    static constexpr auto name = "invalid double value";
  };
  struct invalid_integer {
    static constexpr auto name = "invalid integer value";
  };

  struct discard_arg : lexy::token_production {
    RULE dsl::peek(dsl::lit_c<'\"'>) >> dsl::p<string_arg> |
      dsl::else_
        >> dsl::while_(dsl::ascii::character - dsl::comma - dsl::lit_c<')'>);
    VALUE lexy::callback<int>([](auto &&) { return 0; });
  };

  struct discard_tuple {
    RULE dsl::parenthesized.opt_list(dsl::p<discard_arg>, dsl::sep(dsl::comma));
    VALUE lexy::callback<int>([](auto &&) { return 0; });
  };

  struct discard_tup_list {
    // static constexpr auto whitespace = dsl::ascii::blank /
    // dsl::ascii::newline;
    RULE dsl::list(dsl::peek_not(dsl::ascii::alpha_digit_underscore /
                                 dsl::semicolon) >>
                   dsl::p<discard_tuple>);
    VALUE lexy::callback<int>([](auto &&) { return 0; });
  };

  struct pred_name {
    using res_type = std::string;
    RULE dsl::identifier(dsl::ascii::alpha_digit_underscore);
    VALUE lexy::as_string<res_type>;
  };

  struct named_arg_tup_list
      : lexy::scan_production<std::optional<arg_tup_ty>> /*,
         lexy::token_production*/
  {

    template<typename Context, typename Reader>
    static void parse_event(lexy::rule_scanner<Context, Reader> &scanner,
                            std::vector<event_data> &tup, arg_types ty) {
      if (ty == INT_TYPE || ty == FLOAT_TYPE) {
        auto maybe_lexeme = scanner.capture(
          dsl::while_(dsl::ascii::digit / dsl::period / dsl::lit_c<'e'> /
                      dsl::lit_c<'E'> / dsl::lit_c<'-'>));
        if (!scanner)
          return;
        auto lexeme = maybe_lexeme.value();
        if (ty == INT_TYPE) {
          const auto *fst = lexeme.data(), *lst = fst + lexeme.size();
          std::int64_t int_val;
          auto [new_ptr, ec] = std::from_chars(fst, lst, int_val);
          if (ec != std::errc() /* || new_ptr != lst */) {
            scanner.fatal_error(invalid_integer{},
                                scanner.position() - lexeme.size(),
                                scanner.position());
            return;
          }
          tup.emplace_back(int_val);
        } else {
          const auto *fst = lexeme.data(), *lst = fst + lexeme.size();
          double double_val;
          auto [new_ptr, ec] = std::from_chars(fst, lst, double_val);
          if (ec != std::errc() /* || new_ptr != lst */) {
            scanner.fatal_error(invalid_double{},
                                scanner.position() - lexeme.size(),
                                scanner.position());
            return;
          }
          tup.emplace_back(double_val);
        }
      } else {
        lexy::scan_result<std::string> string_val;
        scanner.parse(string_val, dsl::p<string_arg>);
        if (!scanner)
          return;
        tup.emplace_back(string_val.value());
      }
    }

    template<typename Context, typename Reader>
    static void parse_tuple(lexy::rule_scanner<Context, Reader> &scanner,
                            std::vector<event_data> &tup,
                            const std::vector<arg_types> &tys) {
      scanner.parse(dsl::lit_c<'('>);
      if (!scanner)
        return;
      if (scanner.branch(dsl::lit_c<')'>)) {
        if (!tys.empty())
          scanner.fatal_error(not_enough_args{}, scanner.begin(),
                              scanner.position());
        return;
      }

      auto it = tys.cbegin();
      do {
        if (!scanner)
          return;
        if (it == tys.cend()) {
          scanner.fatal_error(too_many_args{}, scanner.begin(),
                              scanner.position());
          return;
        }
        parse_event(scanner, tup, *it);
        if (!scanner)
          return;
        ++it;
      } while (scanner.branch(dsl::lit_c<','>));
      if (!scanner)
        return;
      if (it != tys.cend()) {
        scanner.fatal_error(not_enough_args{}, scanner.begin(),
                            scanner.position());
        return;
      }
      scanner.parse(dsl::lit_c<')'>);
    }

    template<typename Context, typename Reader>
    static scan_result scan(lexy::rule_scanner<Context, Reader> &scanner,
                            const parse_state &state) {
      lexy::scan_result<std::string> p_name_res;
      scanner.parse(p_name_res, dsl::p<pred_name>);
      if (!scanner)
        return lexy::scan_failed;
      // TODO: only const
      std::string p_name(p_name_res.value());
      auto it = state.pred_map.find(p_name);
      if (it == state.sig.cend()) {
        scanner.fatal_error(unknown_pred{}, scanner.begin(),
                            scanner.position());
        return lexy::scan_failed;
      }
      size_t n_args = it->second.size();
      auto p_key = std::pair(std::move(p_name), n_args);
      auto p_it = state.pred_map.find(p_key);
      if (p_it == state.pred_map.end()) {
        scanner.parse(dsl::p<discard_tup_list>);
        if (!scanner)
          return lexy::scan_failed;
        return std::nullopt;
      }
      // Parse everything
      auto p_id = p_it->second;
      database_elem tup_list;
      while (scanner.branch(
        dsl::peek_not(dsl::ascii::alpha_digit_underscore / dsl::semicolon))) {
        if (!scanner)
          return lexy::scan_failed;
        std::vector<event_data> tup;
        tup.reserve(n_args);
        parse_tuple(scanner, tup, it->second);
        if (!scanner)
          return lexy::scan_failed;
        tup_list.emplace_back(std::move(tup));
      }
      if (!scanner)
        return lexy::scan_failed;
      return std::pair(p_id, std::move(tup_list));
    }
  };

  struct db_parse {
    RULE dsl::list(dsl::peek(dsl::ascii::alpha_digit_underscore) >>
                   dsl::p<named_arg_tup_list>);
    static constexpr auto fold_fn = [](database &db,
                                       std::optional<arg_tup_ty> &&elem) {
      if (!elem)
        return;
      auto it = db.find(elem->first);
      if (it == db.end())
        db.insert(std::move(*elem));
      else
        it->second.insert(it->second.end(),
                          std::make_move_iterator(elem->second.begin()),
                          std::make_move_iterator(elem->second.end()));
    };
    VALUE
    lexy::fold_inplace<database>(std::initializer_list<database::init_type>{},
                                 fold_fn);
  };

  struct ts_parse : lexy::token_production {
    RULE dsl::capture(dsl::digits<>);
    VALUE lexy::callback<size_t>([](auto lexeme) -> size_t {
      const auto *fst = lexeme.data(), *lst = fst + lexeme.size();
      size_t ts;
      auto [new_ptr, ec] = std::from_chars(fst, lst, ts);
      if (ec != std::errc() || new_ptr != lst)
        throw std::runtime_error("invalid integer");
      return ts;
    });
  };

  struct ts_db_parse {
    static constexpr auto whitespace = dsl::ascii::blank / dsl::ascii::newline;

    RULE dsl::at_sign + dsl::p<ts_parse> +
      dsl::opt(dsl::peek_not(dsl::semicolon) >> dsl::p<db_parse>) +
      dsl::semicolon + dsl::if_(dsl::ascii::newline) + dsl::eof;
    VALUE lexy::callback<timestamped_database>(
      [](size_t &&ts, std::optional<database> &&db) -> timestamped_database {
        if (db)
          return std::make_pair(ts, std::move(*db));
        else
          return std::make_pair(ts, database());
      });
  };
#undef RULE
#undef VALUE
};
}// namespace parse
