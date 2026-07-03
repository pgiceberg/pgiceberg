#include "fdw/modify_state.h"

extern "C" {
#include "postgres.h"
#include "fmgr.h"

PG_MODULE_MAGIC;

// Transaction callbacks must be registered at module load time, not from an
// FDW executor callback, because a backend can run multiple statements in one
// PostgreSQL transaction and all of them have to share the same pending
// Iceberg commit state.
void _PG_init(void) { pgiceberg::fdw::RegisterTransactionCallbacks(); }
}
