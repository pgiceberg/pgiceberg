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

#include <cstddef>
#include <cstdint>

namespace pgiceberg::metadata {

constexpr std::uint32_t kProtocolMagic = 0x5047494d;  // PGIM
constexpr std::uint32_t kProtocolVersion = 1;
constexpr std::size_t kHeaderSize = 40;
constexpr std::size_t kMaxPayload = std::size_t{64} * 1024;

// Keep wire enums wide: narrowing on decode would accept invalid high bits.
// NOLINTNEXTLINE(performance-enum-size)
enum class MessageKind : std::uint32_t { kInvalid = 0, kPing = 1, kEcho = 2 };
// NOLINTNEXTLINE(performance-enum-size)
enum class ReplyStatus : std::uint32_t { kOk = 0, kInvalidRequest = 1 };

struct MessageHeader {
  MessageKind kind;
  ReplyStatus status;
  std::uint32_t database;
  std::uint64_t request;
  std::uint64_t generation;
  std::uint32_t payload_size;
};

inline void WriteUint32(char* out, std::uint32_t value) {
  for (int i = 3; i >= 0; --i) {
    out[i] = static_cast<char>(value & 0xff);
    value >>= 8;
  }
}

inline std::uint32_t ReadUint32(const char* in) {
  std::uint32_t value = 0;
  for (int i = 0; i < 4; ++i) {
    value = (value << 8) | static_cast<unsigned char>(in[i]);
  }
  return value;
}

inline void WriteUint64(char* out, std::uint64_t value) {
  WriteUint32(out, static_cast<std::uint32_t>(value >> 32));
  WriteUint32(out + 4, static_cast<std::uint32_t>(value));
}

inline std::uint64_t ReadUint64(const char* in) {
  return (static_cast<std::uint64_t>(ReadUint32(in)) << 32) | ReadUint32(in + 4);
}

inline void EncodeHeader(char* out, const MessageHeader& header) {
  WriteUint32(out, kProtocolMagic);
  WriteUint32(out + 4, kProtocolVersion);
  WriteUint32(out + 8, static_cast<std::uint32_t>(header.kind));
  WriteUint32(out + 12, static_cast<std::uint32_t>(header.status));
  WriteUint32(out + 16, header.database);
  WriteUint64(out + 20, header.request);
  WriteUint64(out + 28, header.generation);
  WriteUint32(out + 36, header.payload_size);
}

inline bool DecodeHeader(const void* data, std::size_t size, MessageHeader* header) {
  if (size < kHeaderSize || size > kHeaderSize + kMaxPayload) {
    return false;
  }
  const auto* in = static_cast<const char*>(data);
  if (ReadUint32(in) != kProtocolMagic || ReadUint32(in + 4) != kProtocolVersion) {
    return false;
  }
  header->kind = static_cast<MessageKind>(ReadUint32(in + 8));
  header->status = static_cast<ReplyStatus>(ReadUint32(in + 12));
  header->database = ReadUint32(in + 16);
  header->request = ReadUint64(in + 20);
  header->generation = ReadUint64(in + 28);
  header->payload_size = ReadUint32(in + 36);
  return header->payload_size == size - kHeaderSize;
}

}  // namespace pgiceberg::metadata
