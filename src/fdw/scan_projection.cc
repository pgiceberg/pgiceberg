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

#include "fdw/scan_projection.h"

#include <set>
#include <vector>

extern "C" {
#include "nodes/pathnodes.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "nodes/value.h"
#include "optimizer/optimizer.h"
}

namespace pgiceberg::fdw {
namespace {

void AddReferencedAttributes(Node* node, Index relid, bool* whole_row,
                             std::set<int>* attributes) {
  if (node == nullptr) {
    return;
  }

  List* vars = pull_var_clause(
      node, PVC_RECURSE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS | PVC_RECURSE_PLACEHOLDERS);
  ListCell* cell = nullptr;
  foreach (cell, vars) {
    auto* var = castNode(Var, lfirst(cell));
    if (var->varno != relid || var->varlevelsup != 0) {
      continue;
    }
    if (var->varattno == InvalidAttrNumber) {
      *whole_row = true;
    } else if (var->varattno > 0) {
      attributes->insert(var->varattno);
    }
  }
  list_free(vars);
}

std::vector<int> AttributeVector(RelOptInfo* baserel, bool whole_row,
                                 const std::set<int>& attributes) {
  std::vector<int> projected;
  if (whole_row) {
    for (AttrNumber attnum = 1; attnum <= baserel->max_attr; attnum++) {
      projected.push_back(attnum);
    }
    return projected;
  }

  projected.reserve(attributes.size());
  for (const auto attnum : attributes) {
    projected.push_back(attnum);
  }
  return projected;
}

}  // namespace

List* BuildFdwScanProjectionPrivate(RelOptInfo* baserel, List* target_list,
                                    List* scan_clauses) {
  bool whole_row = false;
  std::set<int> attributes;
  AddReferencedAttributes(reinterpret_cast<Node*>(baserel->reltarget->exprs),
                          baserel->relid, &whole_row, &attributes);
  AddReferencedAttributes(reinterpret_cast<Node*>(target_list), baserel->relid,
                          &whole_row, &attributes);
  AddReferencedAttributes(reinterpret_cast<Node*>(scan_clauses), baserel->relid,
                          &whole_row, &attributes);

  List* projected = NIL;
  for (const auto attnum : AttributeVector(baserel, whole_row, attributes)) {
    projected = lappend(projected, makeInteger(attnum));
  }
  return projected;
}

std::vector<int> FdwScanProjectionFromPlan(const ForeignScan* plan) {
  std::vector<int> projected;
  ListCell* cell = nullptr;
  foreach (cell, plan->fdw_private) {
    projected.push_back(intVal(lfirst(cell)));
  }
  return projected;
}

}  // namespace pgiceberg::fdw
