/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * ubs-io is licensed under the Mulan PSL v2.
 */
#ifndef UBSIO_KVC_REMOTE_DRAM_OBJECT_H
#define UBSIO_KVC_REMOTE_DRAM_OBJECT_H

#include <cstddef>
#include <cstdint>
#include <string>

namespace ock {
namespace ubsio {
namespace remote_dram {

constexpr uint32_t CHUNK_BYTES = 1024U * 1024U;
int32_t Put(const std::string &key, const void *value, size_t length);
int32_t Get(const std::string &key, void *value, size_t capacity);
int32_t BatchGet(const std::string &key, void *value, size_t capacity);
int32_t Delete(const std::string &key);
int32_t Length(const std::string &key, uint32_t &length);
int32_t Exist(const std::string &key);
// Call only after draining operations at SDK shutdown; remote payload is disposable.
void ResetDirectory();

} // namespace remote_dram
} // namespace ubsio
} // namespace ock
#endif
