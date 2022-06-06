#include <fmt/format.h>
#include <fstream>
#include <stdexcept>
#include <util.h>

std::string read_file(const std::filesystem::path &path) {
  if (!std::filesystem::exists(path))
    throw std::runtime_error(
      fmt::format("path {} does not exist", path.string()));
  if (!std::filesystem::is_regular_file(path))
    throw std::runtime_error(
      fmt::format("path {} does not describe a regular file", path.string()));
  std::ifstream file(path);
  std::stringstream buf;
  buf << file.rdbuf();
  return buf.str();
}
