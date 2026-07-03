#include "fdw/modify_state.h"

extern "C" {
#include "postgres.h"
#include "fmgr.h"

PG_MODULE_MAGIC;

void _PG_init(void) { pgiceberg::fdw::RegisterTransactionCallbacks(); }
}
