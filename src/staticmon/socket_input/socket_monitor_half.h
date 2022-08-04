#pragma once

#include <absl/container/flat_hash_map.h>
#include <array>
#include <boost/asio/buffer.hpp>
#include <boost/asio/buffered_read_stream.hpp>
#include <boost/asio/local/stream_protocol.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/write.hpp>
#include <boost/system/error_code.hpp>
#include <cstdint>
#include <cstring>
#include <optional>
#include <staticmon/socket_input/socket_types/c_event_types.h>
#include <staticmon/socket_input/socket_types/serialization_types.h>
#include <staticmon/input_formula/formula.h>
#include <staticmon/operators/operators.h>
#include <string>
#include <utility>

class socket_monitor_half {
public:
  socket_monitor_half(std::string socket_path)
      : path_(std::move(socket_path)), acceptor_(ctx_), sock_(ctx_, 5000) {
    ::unlink(path_.c_str());
    acceptor_.open();
    acceptor_.bind(path_.c_str());
    acceptor_.listen();
    acceptor_.accept(sock_.lowest_layer());
  }

  std::optional<timestamped_database> read_database() {
    while (true) {
      std::optional<timestamped_database> opt_db;
      auto nxt_ctrl = read_primitive<control_bits>();
      if (nxt_ctrl == CTRL_EOF) {
        return opt_db;
      } else if (nxt_ctrl == CTRL_NEW_DATABASE) {
        timestamped_database db;
        db.first = static_cast<std::size_t>(read_primitive<std::int64_t>());
        read_tuple_list(db.second);
        opt_db.emplace(db);
        return opt_db;
      } else if (nxt_ctrl == CTRL_LATENCY_MARKER) {
        std::int64_t lm = read_primitive<std::int64_t>();
        send_latency_marker(lm);
      } else {
        throw std::runtime_error("expected database");
      }
    }
  }

  void send_eof() {
    send_primitive(CTRL_EOF);
    sock_.next_layer().shutdown(boost::asio::socket_base::shutdown_send);
  }

  ~socket_monitor_half() { ::unlink(path_.c_str()); }

private:
  void send_latency_marker(std::int64_t lm) {
    send_primitive(CTRL_LATENCY_MARKER);
    send_primitive(lm);
  }
  template<typename T>
  T read_primitive() {
    T t{};
    std::array<char, sizeof(T)> buf;
    boost::asio::read(sock_, boost::asio::buffer(buf));
    std::memcpy(&t, buf.data(), sizeof(T));
    return t;
  }

  template<typename T>
  void send_primitive(T t) {
    char snd_buf[sizeof(T)];
    std::memcpy(snd_buf, &t, sizeof(T));
    boost::asio::write(sock_, boost::asio::const_buffer(snd_buf, sizeof(T)));
  }

  std::string read_string() {
    auto str_len = static_cast<std::size_t>(read_primitive<int32_t>());
    std::string res;
    res.insert(0, str_len, ' ');
    boost::asio::read(sock_, boost::asio::buffer(res.data(), str_len));
    return res;
  }

  event_data read_event_data() {
    auto ev_ty = read_primitive<c_ev_ty>();
    if (ev_ty == TY_INT) {
      return event_data(read_primitive<std::int64_t>());
    } else if (ev_ty == TY_FLOAT) {
      return event_data(read_primitive<double>());
    } else if (ev_ty == TY_STRING) {
      return event_data(read_string());
    } else
      throw std::runtime_error("invalid type, expected int|float|string");
  }

  void read_tuple(database &db) {
    auto event_name = read_string();
    auto arity = static_cast<std::size_t>(read_primitive<int32_t>());
    auto p_it = input_predicates.find(event_name);
    if (p_it == input_predicates.end()) {
      for (std::size_t i = 0; i < arity; ++i)
        read_event_data();
    } else {
      assert(p_it->second.second.size() == arity);
      event ev;
      ev.reserve(arity);
      for (std::size_t i = 0; i < arity; ++i)
        ev.push_back(read_event_data());
      std::size_t pred_id = p_it->second.first;
      auto it = db.find(pred_id);
      if (it == db.end())
        db.emplace(pred_id, make_vector(make_vector(std::move(ev))));
      else
        it->second[0].push_back(std::move(ev));
    }
  }

  void read_tuple_list(database &db) {
    control_bits nxt_ctrl;
    while ((nxt_ctrl = read_primitive<control_bits>()) == CTRL_NEW_EVENT) {
      read_tuple(db);
    }
    if (nxt_ctrl != CTRL_END_DATABASE) {
      throw std::runtime_error("expected end of database");
    }
  }

  std::string path_;
  boost::asio::io_context ctx_;
  boost::asio::local::stream_protocol::acceptor acceptor_;
  boost::asio::buffered_read_stream<boost::asio::local::stream_protocol::socket>
    sock_;
};
