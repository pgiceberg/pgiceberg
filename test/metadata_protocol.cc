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

#include "metadata/protocol.h"

#include <cstdio>
#include <cstdlib>
#include <vector>

using namespace pgiceberg::metadata;

void Check(bool condition) {
  if (!condition) {
    std::fputs("metadata wire protocol check failed\n", stderr);
    std::abort();
  }
}

int main() {
  std::vector<char> message(kHeaderSize + kMaxPayload);
  MessageHeader header{MessageKind::kEcho,    ReplyStatus::kOk,      42,
                       0x0102030405060708ULL, 0x1112131415161718ULL, kMaxPayload};
  EncodeHeader(message.data(), header);
  Check(message[0] == 'P' && message[1] == 'G' && message[2] == 'I' && message[3] == 'M');
  Check(message[20] == 1 && message[27] == 8 && message[28] == 0x11 &&
        message[35] == 0x18);
  MessageHeader decoded{};
  Check(DecodeHeader(message.data(), message.size(), &decoded));
  Check(decoded.database == 42 && decoded.request == header.request &&
        decoded.generation == header.generation && decoded.payload_size == kMaxPayload);
  Check(!DecodeHeader(message.data(), kHeaderSize - 1, &decoded));
  Check(!DecodeHeader(message.data(), message.size() - 1, &decoded));
  // Reject incompatible versions and bogus lengths before inspecting payloads.
  message[7] = 2;
  Check(!DecodeHeader(message.data(), message.size(), &decoded));
  message[7] = 1;
  message[0] = 'X';
  Check(!DecodeHeader(message.data(), message.size(), &decoded));
  message[0] = 'P';
  WriteUint32(message.data() + 36, kMaxPayload + 1);
  message.push_back(0);
  Check(!DecodeHeader(message.data(), message.size(), &decoded));
  Check(!DecodeHeader(nullptr, 0, &decoded));
}
