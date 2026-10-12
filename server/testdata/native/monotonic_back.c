/*
 * Preloaded into the native server by native_config_test.sh: makes CLOCK_MONOTONIC
 * read one second when the process starts, which is what the server sees on a
 * machine that booted a moment ago, and so after whichever machine built the
 * image.
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <time.h>

int clock_gettime(clockid_t id, struct timespec *ts) {
  static int (*real)(clockid_t, struct timespec *);
  static struct timespec base;
  static int based;
  if (!real) {
    real = dlsym(RTLD_NEXT, "clock_gettime");
  }
  int result = real(id, ts);
  if (result == 0 && id == CLOCK_MONOTONIC) {
    if (!based) {
      base = *ts;
      based = 1;
    }
    ts->tv_sec -= base.tv_sec - 1;
  }
  return result;
}
