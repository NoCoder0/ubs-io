/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

 * ubs-io is licensed under the Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *      http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */
#ifndef BIO_KVC_CONFIG_VALUES_H
#define BIO_KVC_CONFIG_VALUES_H

#include <arpa/inet.h>
#include <cstdint>
#include <limits>
#include <set>
#include <string>
#include <utility>

namespace ock {
namespace bio {
const auto KVC_MODE = std::make_pair("ubsio.kvc.mode", "standalone");
const auto KVC_TENANT_ID = std::make_pair("ubsio.kvc.tenant_id", "1");
const auto KVC_REMOTE_IPS = std::make_pair("ubsio.kvc.remote_ips", "");
const auto KVC_READ_WORKERS = std::make_pair("ubsio.kvc.read_workers", "2");
const auto KVC_READ_BLOCK_LIMIT = std::make_pair("ubsio.kvc.read_block_limit", "64");
const auto KVC_MANIFEST_ONE_GET = std::make_pair("ubsio.kvc.manifest_one_get", "false");
constexpr uint32_t KVC_DEFAULT_WORKERS = 2;
constexpr uint32_t KVC_MAX_READ_WORKERS = 16;
constexpr uint32_t KVC_DEFAULT_READ_BLOCK_LIMIT = 64;
constexpr uint32_t KVC_MIN_READ_BLOCK_LIMIT = 16;
constexpr uint64_t KVC_STANDALONE_TENANT = 1;

struct KvcConfigValues {
    bool remote = false;
    uint64_t tenantId = KVC_STANDALONE_TENANT;
    uint32_t readWorkers = KVC_DEFAULT_WORKERS;
    uint32_t readBlockLimit = KVC_DEFAULT_READ_BLOCK_LIMIT;
    bool manifestOneGet = false;
    std::set<std::string> remoteIps;
};

inline bool KvcReadBudgetFitsPool(uint32_t blockLimit, uint64_t poolBytes, uint64_t blockBytes)
{
    return blockLimit > 0 && blockBytes > 0 && blockLimit <= poolBytes / blockBytes;
}

inline bool ParseKvcUint64(const std::string &text, uint64_t &out)
{
    if (text.empty()) return false;
    uint64_t value = 0;
    constexpr uint64_t decimalBase = 10;
    for (char digit : text) {
        if (digit < '0' || digit > '9') return false;
        const auto number = static_cast<uint64_t>(digit - '0');
        if (value > (std::numeric_limits<uint64_t>::max() - number) / decimalBase) return false;
        value = value * decimalBase + number;
    }
    out = value;
    return true;
}

inline bool ParseKvcIps(const std::string &list, std::set<std::string> &out)
{
    if (list.empty()) return true;
    for (size_t start = 0; start <= list.size();) {
        const size_t separator = list.find(',', start);
        std::string ip = list.substr(start, separator == std::string::npos ? separator : separator - start);
        const auto first = ip.find_first_not_of(" \t\r\n");
        const auto last = ip.find_last_not_of(" \t\r\n");
        if (first == std::string::npos) return false;
        ip = ip.substr(first, last - first + 1);
        in_addr address{};
        if (inet_pton(AF_INET, ip.c_str(), &address) != 1 || !out.insert(ip).second) return false;
        if (separator == std::string::npos) break;
        start = separator + 1;
    }
    return true;
}

inline bool ParseKvcValues(const std::string &mode, const std::string &tenant, const std::string &ips,
    const std::string &workers, const std::string &manifest, const std::string &readBlockLimit,
    KvcConfigValues &out, std::string &error)
{
    uint64_t parsedWorkers = 0;
    uint64_t parsedBlockLimit = 0;
    if (mode != "standalone" && mode != "remote_dram_tcp") {
        error = KVC_MODE.first;
    } else if (!ParseKvcUint64(tenant, out.tenantId) ||
        (mode == "standalone" ? out.tenantId != KVC_STANDALONE_TENANT : out.tenantId <= KVC_STANDALONE_TENANT)) {
        error = KVC_TENANT_ID.first;
    } else if (!ParseKvcIps(ips, out.remoteIps) || (mode == "remote_dram_tcp" && out.remoteIps.empty())) {
        error = KVC_REMOTE_IPS.first;
    } else if (!ParseKvcUint64(workers, parsedWorkers) || parsedWorkers == 0 ||
        parsedWorkers > KVC_MAX_READ_WORKERS) {
        error = KVC_READ_WORKERS.first;
    } else if (!ParseKvcUint64(readBlockLimit, parsedBlockLimit) || parsedBlockLimit < KVC_MIN_READ_BLOCK_LIMIT ||
        parsedBlockLimit > std::numeric_limits<uint32_t>::max()) {
        error = KVC_READ_BLOCK_LIMIT.first;
    } else if (manifest != "true" && manifest != "false") {
        error = KVC_MANIFEST_ONE_GET.first;
    } else {
        out.remote = mode == "remote_dram_tcp";
        out.readWorkers = static_cast<uint32_t>(parsedWorkers);
        out.readBlockLimit = static_cast<uint32_t>(parsedBlockLimit);
        out.manifestOneGet = manifest == "true";
        return true;
    }
    return false;
}
}
}
#endif
