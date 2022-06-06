#include <trace_parser.h>
#include <fmt/core.h>
#include <lexy/action/parse.hpp>
#include <lexy/input/string_input.hpp>
#include <lexy_ext/report_error.hpp>
#include <string_view>
#include <formula.h>

namespace parse {
timestamped_database trace_parser::parse_database(std::string_view db) {
  auto st = parse_state{input_predicates};
  auto input = lexy::string_input<lexy::ascii_encoding>(db);
  auto parsed_db =
    lexy::parse<trace_parser::ts_db_parse>(input, st, lexy_ext::report_error);
  if (!parsed_db || (parsed_db.error_count() > 0))
    throw std::runtime_error("failed to parse timestamped database");
  assert(parsed_db.has_value());
  return parsed_db.value();
}

database monitor_db_from_parser_db(parser_database &&db) {
  database ret_db;
  ret_db.reserve(db.size());
  for (auto &entry : db) {
    if (entry.second.empty())
      continue;
    ret_db.emplace(entry.first, make_vector(std::move(entry.second)));
  }
  return ret_db;
}
}// namespace parse
