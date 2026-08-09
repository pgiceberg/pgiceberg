// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <memory>

#include <iceberg/type_fwd.h>

extern "C" {
#include "postgres.h"
#include "nodes/pg_list.h"
}

namespace pgiceberg::fdw {

// Translate PostgreSQL scan clauses into an Iceberg filter expression for
// scan-time file pruning (partition values, manifest and column metrics).
//
// The translation is strictly best-effort: clauses that cannot be expressed
// as an equivalent-or-weaker Iceberg predicate are skipped.  PostgreSQL keeps
// evaluating every clause locally, so a pushed filter must never exclude a
// row the original clause would accept; it never has to filter exactly.
//
// Returns nullptr when no clause could be translated.  Never raises.
std::shared_ptr<iceberg::Expression> TranslateQualsForPushdown(
    List* quals, Index varno, Oid relation_oid, const iceberg::Schema& schema);

}  // namespace pgiceberg::fdw
