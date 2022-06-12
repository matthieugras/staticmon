#pragma once

class monitor_driver {
public:
  virtual void do_monitor() = 0;
  virtual ~monitor_driver() noexcept {};
};
