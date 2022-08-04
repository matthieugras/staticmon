#pragma once
enum control_bits {
  CTRL_NEW_EVENT = 0x1,
  CTRL_NEW_DATABASE,
  CTRL_END_DATABASE,
  CTRL_LATENCY_MARKER,
  CTRL_EOF
};
