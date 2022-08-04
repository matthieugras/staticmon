#pragma once

extern "C" {
#include <stdint.h>
typedef enum {
  TY_INT = 0x1,
  TY_FLOAT,
  TY_STRING
} c_ev_ty;

typedef union {
  int64_t i;
  double d;
  char *s;
} c_ev_data;
}
