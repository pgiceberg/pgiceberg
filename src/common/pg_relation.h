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

extern "C" {
#include "postgres.h"
#include "access/table.h"
#include "utils/rel.h"
}

namespace pgiceberg {

class RelationLockGuard {
 public:
  RelationLockGuard(Relation relation, LOCKMODE lockmode)
      : relation_(relation), lockmode_(lockmode) {}

  ~RelationLockGuard() {
    if (relation_ != nullptr) {
      table_close(relation_, lockmode_);
    }
  }

  RelationLockGuard(const RelationLockGuard&) = delete;
  RelationLockGuard& operator=(const RelationLockGuard&) = delete;

 private:
  Relation relation_ = nullptr;
  LOCKMODE lockmode_ = NoLock;
};

}  // namespace pgiceberg
