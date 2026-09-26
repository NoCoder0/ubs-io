/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.

 * ubs-io is licensed under the Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *      http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include <fcntl.h>
#include <sys/mman.h>
#include <cstring>
#include <algorithm>
#include <vector>
#include <memory>
#include <condition_variable>
#include <limits>
#include "bio_c.h"
#include "bio_err.h"
#include "ubsio_kvc_log.h"
#include "ubsio_kvc_err.h"
#include "dl_biosdk_api.h"
#include "kv_operation.h"
#include "remote_dram_object.h"

namespace ock {
namespace ubsio {

KvOperation *KvOperation::gInstance = nullptr;
std::mutex KvOperation::gLock;
constexpr uint32_t BATCH_LIMIT = 1000;

namespace {
int32_t MapBioResult(CResult result)
{
    switch (result) {
        case RET_CACHE_OK: return UBSIO_KVC_OK;
        case RET_CACHE_NOT_FOUND:
        case RET_CACHE_MISS: return UBSIO_KVC_NOT_FOUND;
        case RET_CACHE_EPERM: return UBSIO_KVC_INVALID_PARAM;
        case RET_CACHE_BUSY:
        case RET_CACHE_NEED_RETRY:
        case RET_CACHE_NOT_READY:
        case RET_CACHE_NO_SPACE:
        case RET_CACHE_UNAVAILABLE:
        case RET_CACHE_EXCEED_QUOTA:
        case RET_CACHE_PT_FAULT: return UBSIO_KVC_UNAVAILABLE;
        default: return UBSIO_KVC_BIO_ERR;
    }
}

int32_t MapBioBatchResult(int32_t result)
{
    switch (result) {
        case bio::BIO_OK: return UBSIO_KVC_OK;
        case bio::BIO_NOT_EXISTS: return UBSIO_KVC_NOT_FOUND;
        case bio::BIO_CRC_ERR: return UBSIO_KVC_CORRUPT;
        case bio::BIO_INVALID_PARAM: return UBSIO_KVC_INVALID_PARAM;
        case bio::BIO_NOT_READY:
        case bio::BIO_ALLOC_FAIL:
        case bio::BIO_CHECK_PT_FAIL:
        case bio::BIO_INNER_RETRY:
        case bio::BIO_NET_RETRY:
        case bio::BIO_QUOTA_NOT_ENOUGH:
        case bio::BIO_QUOTA_TIMEOUT: return UBSIO_KVC_UNAVAILABLE;
        default: return UBSIO_KVC_BIO_ERR;
    }
}
}

int32_t KvOperation::Initialize(const std::string &path)
{
    std::lock_guard<std::mutex> guard(mMutex);
    if (mInited) {
        LOG_INFO("Kv operation has been initialized");
        return UBSIO_KVC_OK;
    }
    mInited = true;
    return UBSIO_KVC_OK;
}

void KvOperation::UnInitialize(void)
{
    {
        std::lock_guard<std::mutex> guard(mMutex);
        if (!mInited) {
            LOG_INFO("KvOperation has not been initialized");
            return;
        }
        mInited = false;
    }
    ExitKvExecutor();
}

int32_t KvOperation::KvPutData(const std::string &key, void *value, size_t len)
{
    if (DlBioSdkApi::RemoteMode()) return remote_dram::Put(key, value, len);
    ObjLocation location;
    CResult status = DlBioSdkApi::CalcLocation(DlBioSdkApi::TenantId(),
        static_cast<uint64_t>(std::hash<std::string>{}(key)), &location);
    if (UNLIKELY(status != CResult::RET_CACHE_OK)) {
        LOG_ERROR("Calc location failed, status:" << status);
        return UBSIO_KVC_ERR;
    }
    return MapBioResult(DlBioSdkApi::Put(DlBioSdkApi::TenantId(), key.c_str(),
        reinterpret_cast<char*>(value), len, location));
}

int32_t KvOperation::KvGetData(const std::string &key, void *value, size_t len)
{
    if (DlBioSdkApi::RemoteMode()) return remote_dram::Get(key, value, len);
    ObjLocation location;
    CResult status = DlBioSdkApi::CalcLocation(DlBioSdkApi::TenantId(),
        static_cast<uint64_t>(std::hash<std::string>{}(key)), &location);
    if (UNLIKELY(status != CResult::RET_CACHE_OK)) {
        LOG_ERROR("CalcLocation failed with returned status " << status);
        return UBSIO_KVC_ERR;
    }
    uint64_t outLength = 0;
    int32_t result = MapBioResult(DlBioSdkApi::Get(DlBioSdkApi::TenantId(), key.c_str(), 0, len,
        location, reinterpret_cast<char*>(value), &outLength));
    return result == UBSIO_KVC_OK && outLength != len ? UBSIO_KVC_CORRUPT : result;
}

int32_t KvOperation::KvDeleteKey(const std::string &key)
{
    if (DlBioSdkApi::RemoteMode()) return remote_dram::Delete(key);
    ObjLocation location;
    CResult status = DlBioSdkApi::CalcLocation(DlBioSdkApi::TenantId(),
        static_cast<uint64_t>(std::hash<std::string>{}(key)), &location);
    if (UNLIKELY(status != CResult::RET_CACHE_OK)) {
        LOG_ERROR("CalcLocation failed with returned status " << status);
        return UBSIO_KVC_ERR;
    }
    return MapBioResult(DlBioSdkApi::Delete(DlBioSdkApi::TenantId(), key.c_str(), location));
}

bool KvOperation::KvExistKey(const std::string &key)
{
    if (DlBioSdkApi::RemoteMode()) return remote_dram::Exist(key) == UBSIO_KVC_OK;
    ObjLocation location;
    CResult status = DlBioSdkApi::CalcLocation(DlBioSdkApi::TenantId(),
        static_cast<uint64_t>(std::hash<std::string>{}(key)), &location);
    if (UNLIKELY(status != CResult::RET_CACHE_OK)) {
        LOG_ERROR("CalcLocation failed with returned status " << status);
        return false;
    }
    ObjStat stat;
    stat.size = 0;
    auto ret = DlBioSdkApi::Stat(DlBioSdkApi::TenantId(), key.c_str(), location, &stat);
    if ((ret != CResult::RET_CACHE_OK) || (stat.size == 0)) {
        return false;
    }
    return true;
}

int32_t KvOperation::KvGetLengthKey(const std::string &key, uint32_t &length)
{
    if (DlBioSdkApi::RemoteMode()) return remote_dram::Length(key, length);
    ObjLocation location;
    CResult status = DlBioSdkApi::CalcLocation(DlBioSdkApi::TenantId(),
        static_cast<uint64_t>(std::hash<std::string>{}(key)), &location);
    if (UNLIKELY(status != CResult::RET_CACHE_OK)) {
        LOG_ERROR("CalcLocation failed with returned status " << status);
        return UBSIO_KVC_ERR;
    }
    ObjStat stat;
    stat.size = 0;
    auto ret = DlBioSdkApi::Stat(DlBioSdkApi::TenantId(), key.c_str(), location, &stat);
    if ((ret != CResult::RET_CACHE_OK) || (stat.size == 0)) {
        LOG_ERROR("Exist data from meta failed, ret: " << ret);
        return ret == RET_CACHE_OK ? UBSIO_KVC_CORRUPT : MapBioResult(ret);
    }
    length = stat.size;
    return UBSIO_KVC_OK;
}

int32_t KvOperation::BatchKvGetData(const std::vector<std::string> &key, void **bufs,
    std::vector<size_t> &lengths, std::vector<int> &results)
{
    if (DlBioSdkApi::RemoteMode()) {
        if (bufs == nullptr || lengths.size() != key.size() || results.size() < key.size()) {
            return UBSIO_KVC_INVALID_PARAM;
        }
        for (size_t i = 0; i < key.size(); ++i) {
            results[i] = KvGetData(key[i], bufs[i], lengths[i]);
        }
        return UBSIO_KVC_OK;
    }
    uint32_t keysCount = key.size();
    std::vector<ObjLocation> locationVec(keysCount);
    std::vector<uint64_t> realLength(keysCount);
    std::vector<uint64_t> offsets(keysCount, 0);
    std::vector<const char*> keys(keysCount);
    for (size_t i = 0; i < keysCount; i++) {
        ObjLocation location;
        CResult status = DlBioSdkApi::CalcLocation(DlBioSdkApi::TenantId(),
            static_cast<uint64_t>(std::hash<std::string>{}(key[i])), &location);
        if (UNLIKELY(status != CResult::RET_CACHE_OK)) {
            LOG_ERROR("Calc location failed, status:" << status);
            return UBSIO_KVC_ERR;
        }
        locationVec[i] = location;
        keys[i] = key[i].c_str();
    }

    int32_t ret = MapBioResult(DlBioSdkApi::BatchGet(DlBioSdkApi::TenantId(), keys.data(), keysCount,
        offsets.data(), lengths.data(), locationVec.data(), reinterpret_cast<uintptr_t *>(bufs),
        realLength.data(), results.data()));
    for (size_t i = 0; i < keysCount; ++i) {
        results[i] = MapBioBatchResult(results[i]);
        if (results[i] == UBSIO_KVC_OK && realLength[i] != lengths[i]) results[i] = UBSIO_KVC_CORRUPT;
    }
    return ret;
}

int32_t KvOperation::BatchKvExistKey(const std::vector<std::string> &key, bool *results)
{
    if (DlBioSdkApi::RemoteMode()) {
        for (size_t i = 0; i < key.size(); ++i) {
            int32_t result = remote_dram::Exist(key[i]);
            if (result != UBSIO_KVC_OK && result != UBSIO_KVC_NOT_FOUND) return result;
            results[i] = result == UBSIO_KVC_OK;
        }
        return UBSIO_KVC_OK;
    }
    uint32_t keysCount = key.size();
    std::vector<ObjLocation> locationVec(keysCount);
    std::vector<const char *> keys(keysCount);
    for (size_t i = 0; i < keysCount; i++) {
        ObjLocation location;
        CResult status = DlBioSdkApi::CalcLocation(DlBioSdkApi::TenantId(),
            static_cast<uint64_t>(std::hash<std::string>{}(key[i])), &location);
        if (UNLIKELY(status != CResult::RET_CACHE_OK)) {
            LOG_ERROR("Calc location failed, status:" << status);
            return UBSIO_KVC_ERR;
        }
        locationVec[i] = location;
        keys[i] = key[i].c_str();
    }

    int ret = DlBioSdkApi::BatchExist(DlBioSdkApi::TenantId(), keys.data(), locationVec.data(), keysCount, results);
    if (ret != UBSIO_KVC_OK) {
        LOG_ERROR("BioBatchExist failed with returned status " << ret);
        return UBSIO_KVC_ERR;
    }
    return UBSIO_KVC_OK;
}

int32_t KvOperation::RunBatch(size_t count, std::vector<int> &results,
                              const std::function<int32_t(size_t)> &task)
{
    std::lock_guard<std::mutex> executorGuard(mBatchMutex);
    if (mKvExecutor == nullptr || results.size() < count) return UBSIO_KVC_INVALID_PARAM;
    for (size_t start = 0; start < count; start += BATCH_LIMIT) {
        std::mutex mutex;
        std::condition_variable completed;
        size_t pending = 0;
        bool submitFailed = false;
        const size_t end = std::min(start + BATCH_LIMIT, count);
        for (size_t index = start; index < end; ++index) {
            {
                std::lock_guard<std::mutex> guard(mutex);
                ++pending;
            }
            bool accepted = mKvExecutor->Execute([&, index]() {
                try {
                    results[index] = task(index);
                } catch (...) {
                    LOG_ERROR("Batch task threw, index:" << index);
                    results[index] = UBSIO_KVC_ERR;
                }
                {
                    std::lock_guard<std::mutex> guard(mutex);
                    --pending;
                }
                completed.notify_one();
            });
            if (!accepted) {
                {
                    std::lock_guard<std::mutex> guard(mutex);
                    --pending;
                }
                results[index] = UBSIO_KVC_UNAVAILABLE;
                for (size_t rest = index + 1; rest < count; ++rest) results[rest] = UBSIO_KVC_UNAVAILABLE;
                submitFailed = true;
                break;
            }
        }
        std::unique_lock<std::mutex> guard(mutex);
        completed.wait(guard, [&pending]() { return pending == 0; });
        if (submitFailed) {
            LOG_ERROR("Batch executor rejected task after partial submission, start:" << start);
            return UBSIO_KVC_UNAVAILABLE;
        }
    }
    return UBSIO_KVC_OK;
}

int32_t KvOperation::BatchKvPutData(const std::vector<std::string> &key, std::vector<void*> &value,
    std::vector<size_t> &lengths, std::vector<int> &results)
{
    if (key.size() != value.size() || key.size() != lengths.size()) return UBSIO_KVC_INVALID_PARAM;
    return RunBatch(key.size(), results, [&](size_t index) {
        return KvPutData(key[index], value[index], lengths[index]);
    });
}

int32_t KvOperation::BatchKvDeleteKey(const std::vector<std::string> &key, std::vector<int> &results)
{
    return RunBatch(key.size(), results, [&](size_t index) { return KvDeleteKey(key[index]); });
}

int32_t KvOperation::BatchGetLengthKey(const std::vector<std::string> &key, std::vector<uint32_t> &lengths,
                                       std::vector<int> &results)
{
    if (lengths.size() < key.size()) return UBSIO_KVC_INVALID_PARAM;
    return RunBatch(key.size(), results, [&](size_t index) {
        return KvGetLengthKey(key[index], lengths[index]);
    });
}
}; // namespace ubsio
} // namespace ock
