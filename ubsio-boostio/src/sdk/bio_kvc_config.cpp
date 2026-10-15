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
#include "bio_kvc_config.h"
#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <sys/stat.h>
#include <unistd.h>
#include "bio_config_instance.h"
#include "bio_config_kv_reader.h"
#include "bio_log.h"
#include "bio_client_log.h"

namespace ock {
namespace bio {
namespace {
std::mutex gConfigMutex;
std::shared_ptr<const BioKvcSnapshot> gSnapshot;
std::atomic<bool> gSummaryLogged{false};

std::string Item(KVReader &reader, const char *key, const char *fallback)
{
    std::string value;
    return reader.GetItem(key, value) ? value : fallback;
}

bool Invalid(const std::string &key, const char *reason)
{
    // Prepare precedes SDK logger initialization; diagnostics must also work without spdlog.
    Logger::LogToStdErr(BIOLOG_LEVEL_ERROR, "Invalid KVC startup configuration, key:" + key + ", reason:" + reason);
    return false;
}

bool ReadablePath(const std::string &path, bool directory = false)
{
    struct stat info{};
    return !path.empty() && path.front() == '/' && stat(path.c_str(), &info) == 0 &&
        (directory ? S_ISDIR(info.st_mode) : S_ISREG(info.st_mode)) && access(path.c_str(), R_OK) == 0;
}

bool TlsPath(KVReader &reader, const char *key, char *target, size_t capacity, bool required, bool directory = false)
{
    const std::string value = Item(reader, key, "");
    if (value.empty() && !required) return true;
    if (value.size() >= capacity || !ReadablePath(value, directory)) {
        return Invalid(key, "requires a readable absolute path within ABI capacity");
    }
    std::memcpy(target, value.c_str(), value.size() + 1);
    return true;
}

bool Parse(KVReader &reader, BioKvcSnapshot &snapshot)
{
    std::string error;
    if (!ParseKvcValues(Item(reader, KVC_MODE.first, KVC_MODE.second),
        Item(reader, KVC_TENANT_ID.first, KVC_TENANT_ID.second),
        Item(reader, KVC_REMOTE_IPS.first, KVC_REMOTE_IPS.second),
        Item(reader, KVC_READ_WORKERS.first, KVC_READ_WORKERS.second),
        Item(reader, KVC_MANIFEST_ONE_GET.first, KVC_MANIFEST_ONE_GET.second), snapshot.values, error)) {
        return Invalid(error, "invalid value or inconsistent mode/tenant/IP list");
    }
    const auto protocol = Item(reader, NET_DATA_PROTOCOL.first, NET_DATA_PROTOCOL.second);
    if ((protocol != "tcp" && protocol != "rdma") || (snapshot.values.remote && protocol != "tcp")) {
        return Invalid(NET_DATA_PROTOCOL.first, "remote_dram_tcp requires tcp");
    }
    uint64_t timeout = 0;
    constexpr uint64_t minTimeout = 16;
    constexpr uint64_t maxTimeout = 300;
    if (!ParseKvcUint64(Item(reader, WORK_NET_TIMEOUT.first, "20"), timeout) ||
        timeout < minTimeout || timeout > maxTimeout) {
        return Invalid(WORK_NET_TIMEOUT.first, "requires an integer in [16,300]");
    }
    std::string tls;
    const bool hasTlsSwitch = reader.GetItem(NET_TLS_ENABLE_SWITCH.first, tls);
    if (!hasTlsSwitch && snapshot.values.remote) {
        return Invalid(NET_TLS_ENABLE_SWITCH.first, "remote mode requires an explicit TLS switch");
    }
    if (!hasTlsSwitch) tls = NET_TLS_ENABLE_SWITCH.second;
    if (tls != "true" && tls != "false") return Invalid(NET_TLS_ENABLE_SWITCH.first, "requires true or false");
    snapshot.tls.enable = tls == "true";
    if (!snapshot.tls.enable) return true;
    return TlsPath(reader, NET_TLS_CA_CERT_PATH.first, snapshot.tls.caCerPath, sizeof(snapshot.tls.caCerPath), true) &&
        TlsPath(reader, NET_TLS_SERVER_CERT_PATH.first, snapshot.tls.certificationPath,
            sizeof(snapshot.tls.certificationPath), true) &&
        TlsPath(reader, NET_TLS_SERVER_KEY_PATH.first, snapshot.tls.privateKeyPath,
            sizeof(snapshot.tls.privateKeyPath), true) &&
        TlsPath(reader, NET_TLS_CA_CRL_PATH.first, snapshot.tls.caCrlPath, sizeof(snapshot.tls.caCrlPath), false) &&
        TlsPath(reader, NET_TLS_SERVER_KEY_PASS_PATH.first, snapshot.tls.privateKeyPassword,
            sizeof(snapshot.tls.privateKeyPassword), false) &&
        TlsPath(reader, NET_TLS_SERVER_DECRYPTER_PATH.first, snapshot.tls.decrypterLibPath,
            sizeof(snapshot.tls.decrypterLibPath), false) &&
        TlsPath(reader, NET_TLS_SERVER_SSL_LIB_DIR.first, snapshot.tls.opensslLibDir,
            sizeof(snapshot.tls.opensslLibDir), false, true);
}

void CopyOut(const BioKvcSnapshot &snapshot, BioKvcConfigV1 &out, ClientOptionsConfig &options)
{
    out = {BIO_KVC_CONFIG_VERSION_1,
        snapshot.values.remote ? BIO_KVC_MODE_REMOTE_DRAM_TCP : BIO_KVC_MODE_STANDALONE,
        snapshot.values.tenantId, snapshot.values.readWorkers, snapshot.values.manifestOneGet ? 1U : 0U};
    // ClientOptionsConfig layout stays unchanged. Do not overwrite caller logging fields.
    constexpr size_t tlsOffset = offsetof(ClientOptionsConfig, enable);
    std::memcpy(reinterpret_cast<char *>(&options) + tlsOffset,
        reinterpret_cast<const char *>(&snapshot.tls) + tlsOffset, sizeof(options) - tlsOffset);
}
}

std::shared_ptr<const BioKvcSnapshot> GetBioKvcSnapshot()
{
    return std::atomic_load(&gSnapshot);
}

CResult LoadBioKvcConfig(const char *path, uint32_t size, BioKvcConfigV1 *out, ClientOptionsConfig *tlsOptions)
{
    if (out == nullptr || tlsOptions == nullptr || size != sizeof(BioKvcConfigV1) ||
        out->version != BIO_KVC_CONFIG_VERSION_1) {
        Invalid("BioKvcConfigV1", "invalid size/version/output arguments");
        return RET_CACHE_EPERM;
    }
    std::lock_guard<std::mutex> guard(gConfigMutex);
    auto snapshot = GetBioKvcSnapshot();
    // EnsurePrepared after an explicit prepare uses the published snapshot, even if the file disappeared.
    if (snapshot && (path == nullptr || *path == '\0' || path == snapshot->path || path == snapshot->requestedPath)) {
        CopyOut(*snapshot, *out, *tlsOptions);
        return RET_CACHE_OK;
    }
    const char *selected = path;
    if (selected == nullptr || *selected == '\0') selected = std::getenv("UBSIO_CONFIG_PATH");
    if (selected == nullptr || *selected == '\0') selected = "/etc/ubsio/ubsio.conf";
    if (*selected != '/') {
        Invalid("UBSIO_CONFIG_PATH", "requires an absolute regular file");
        return RET_CACHE_EPERM;
    }
    char canonical[PATH_MAX]{};
    if (realpath(selected, canonical) == nullptr || !ReadablePath(canonical)) {
        Invalid("UBSIO_CONFIG_PATH", "file is missing, unreadable or not regular");
        return RET_CACHE_EPERM;
    }
    if (snapshot) {
        if (snapshot->path != canonical) {
            Invalid("UBSIO_CONFIG_PATH", "a different file was already prepared");
            return RET_CACHE_CONFLICT;
        }
        CopyOut(*snapshot, *out, *tlsOptions);
        return RET_CACHE_OK;
    }
    auto candidate = std::make_shared<BioKvcSnapshot>();
    candidate->path = canonical;
    candidate->requestedPath = selected;
    KVReader reader;
    if (!reader.FromFile(candidate->path)) {
        Invalid("UBSIO_CONFIG_PATH", "failed to read configuration");
        return RET_CACHE_EPERM;
    }
    if (!Parse(reader, *candidate)) return RET_CACHE_EPERM;
    std::shared_ptr<const BioKvcSnapshot> published = candidate;
    std::atomic_store(&gSnapshot, published);
    CopyOut(*candidate, *out, *tlsOptions);
    return RET_CACHE_OK;
}

void LogBioKvcConfigReady()
{
    const auto snapshot = GetBioKvcSnapshot();
    if (!snapshot || gSummaryLogged.exchange(true)) return;
    CLIENT_LOG_INFO("KVC startup configuration path:" << snapshot->path << ", mode:"
        << (snapshot->values.remote ? "remote_dram_tcp" : "standalone") << ", tenant:"
        << snapshot->values.tenantId << ", nodes:" << snapshot->values.remoteIps.size() << ", workers:"
        << snapshot->values.readWorkers << ", manifestOneGet:" << snapshot->values.manifestOneGet);
}
}
}
