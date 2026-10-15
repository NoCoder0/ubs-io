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
#include <atomic>
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
    std::lock_guard<std::mutex> guard(mMutex);
    if (!mInited) {
        LOG_INFO("KvOperation has not been initialized");
        return;
    }
    ExitKvExecutor();
    mInited = false;
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
    return DlBioSdkApi::Put(DlBioSdkApi::TenantId(), key.c_str(),
        reinterpret_cast<char*>(value), len, location);
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
    return DlBioSdkApi::Get(DlBioSdkApi::TenantId(), key.c_str(), 0, len,
        location, reinterpret_cast<char*>(value), &outLength);
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
    return DlBioSdkApi::Delete(DlBioSdkApi::TenantId(), key.c_str(), location);
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
        return UBSIO_KVC_ERR;
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
        return RunRemoteReads(key.size(), results, [&](size_t index) {
            return remote_dram::BatchGet(key[index], bufs[index], lengths[index]);
        });
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

    // BIO owns these item statuses; unknown or unexecuted entries start as failure.
    std::fill_n(results.begin(), keysCount, bio::BIO_ERR);
    CResult ret = DlBioSdkApi::BatchGet(DlBioSdkApi::TenantId(), keys.data(), keysCount,
        offsets.data(), lengths.data(), locationVec.data(), reinterpret_cast<uintptr_t *>(bufs),
        realLength.data(), results.data());
    for (size_t i = 0; i < keysCount; ++i) {
        results[i] = results[i] == bio::BIO_OK ? UBSIO_KVC_OK : UBSIO_KVC_ERR;
        // The SDK may mark an entry OK before sending it. A completed read must
        // also report the requested length; untouched/short entries stay failed.
        if (results[i] == UBSIO_KVC_OK && realLength[i] != lengths[i]) results[i] = UBSIO_KVC_ERR;
    }
    return ret == RET_CACHE_OK ? UBSIO_KVC_OK : UBSIO_KVC_ERR;
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

int32_t KvOperation::RunRemoteReads(size_t count, std::vector<int> &results,
                                    const std::function<int32_t(size_t)> &task)
{
    if (results.size() < count) return UBSIO_KVC_INVALID_PARAM;
    std::fill_n(results.begin(), count, UBSIO_KVC_UNAVAILABLE);
    if (count == 0) return UBSIO_KVC_OK;
    ExecutorServicePtr executor;
    {
        std::lock_guard<std::mutex> guard(mBatchMutex);
        if (mKvExecutor == nullptr || mReadExecutorClosing) return UBSIO_KVC_UNAVAILABLE;
        executor = mKvExecutor;
        ++mActiveReadBatches;
    }
    // Keep the executor alive and forbid Stop (which prepends stop tasks) until
    // every accepted worker completes. Do not hold the common batch mutex here.
    struct ReadLease {
        KvOperation &operation;
        ~ReadLease()
        {
            std::lock_guard<std::mutex> guard(operation.mBatchMutex);
            --operation.mActiveReadBatches;
            operation.mReadBatchesDone.notify_all();
        }
    } lease{*this};
    std::atomic<size_t> next{0};
    std::atomic<bool> abort{false};
    std::mutex mutex;
    std::condition_variable completed;
    size_t pending = 0;
    bool submitFailed = false;
    const size_t workers = std::min<size_t>(DlBioSdkApi::ReadWorkers(), count);
    for (size_t worker = 0; worker < workers; ++worker) {
        {
            std::lock_guard<std::mutex> guard(mutex);
            ++pending;
        }
        bool accepted = false;
        try {
            accepted = executor->Execute([&]() {
                while (!abort.load()) {
                    const size_t index = next.fetch_add(1);
                    if (index >= count) break;
                    try {
                        results[index] = task(index);
                    } catch (...) {
                        LOG_ERROR("Remote read task threw, index:" << index);
                        results[index] = UBSIO_KVC_ERR;
                    }
                }
                std::lock_guard<std::mutex> guard(mutex);
                --pending;
                // Notify while holding the completion mutex: the waiter must
                // not destroy this condition variable before the final notify.
                completed.notify_all();
            });
        } catch (...) {
            LOG_ERROR("Remote read worker submission threw");
        }
        if (!accepted) {
            abort.store(true);
            std::lock_guard<std::mutex> guard(mutex);
            --pending;
            submitFailed = true;
            break;
        }
    }
    std::unique_lock<std::mutex> guard(mutex);
    completed.wait(guard, [&pending] { return pending == 0; });
    return submitFailed ? UBSIO_KVC_UNAVAILABLE : UBSIO_KVC_OK;
}

int32_t KvOperation::BatchKvPutData(const std::vector<std::string> &key, std::vector<void*> &value,
    std::vector<size_t> &lengths, std::vector<int> &results)
{
    auto totalSize = key.size();
    std::fill_n(results.begin(), totalSize, UBSIO_KVC_ERR);
    for (uint32_t start = 0; start < totalSize; start += BATCH_LIMIT) {
        sem_t sem;
        sem_init(&sem, 0, 0);
        uint32_t end = std::min(start + BATCH_LIMIT, (uint32_t)totalSize);
        uint32_t batchSize = end - start;
        auto keySize = std::make_shared<std::atomic<uint32_t>>(batchSize);
        for (uint32_t i = start; i < end; i++) {
            auto index = i;
            auto keyCopy = key[i];
            auto valueCopy = value[i];
            auto lengthCopy = lengths[i];
            std::function<void()> func = [this, index, keyCopy, valueCopy, lengthCopy,
                &results, keySize, &sem, totalSize]() {
                auto ret = KvPutData(keyCopy, valueCopy, lengthCopy);
                if (ret != UBSIO_KVC_OK) {
                    LOG_ERROR("Batch put data failed, ret: " << ret
                        << " batch num: " << totalSize << " i:" << index);
                }
                results[index] = ret;
                if (keySize->fetch_sub(1) == 1) {
                    sem_post(&sem);
                }
            };

            auto result = mKvExecutor->Execute(func);
            if (!result) {
                LOG_ERROR("Execute batch put data, batch num: " << totalSize << " i:" << i);
                sem_destroy(&sem);
                return UBSIO_KVC_ERR;
            }
        }
        sem_wait(&sem);
        sem_destroy(&sem);
    }
    return UBSIO_KVC_OK;
}

int32_t KvOperation::BatchKvDeleteKey(const std::vector<std::string> &key, std::vector<int> &results)
{
    auto totalSize = key.size();
    std::fill_n(results.begin(), totalSize, UBSIO_KVC_ERR);
    for (uint32_t start = 0; start < totalSize; start += BATCH_LIMIT) {
        sem_t sem;
        sem_init(&sem, 0, 0);
        uint32_t end = std::min(start + BATCH_LIMIT, (uint32_t)totalSize);
        uint32_t batchSize = end - start;
        auto keySize = std::make_shared<std::atomic<uint32_t>>(batchSize);
        for (uint32_t i = start; i < end; i++) {
            auto index = i;
            auto keyCopy = key[i];
            std::function<void()> func = [this, index, keyCopy, &results,
                keySize, &sem, totalSize]() {
                auto ret = KvDeleteKey(keyCopy);
                if (ret != UBSIO_KVC_OK) {
                    LOG_ERROR("Batch delete key failed, ret: " << ret
                        << " batch num: " << totalSize << " i:" << index);
                }
                results[index] = ret;
                if (keySize->fetch_sub(1) == 1) {
                    sem_post(&sem);
                }
            };

            auto result = mKvExecutor->Execute(func);
            if (!result) {
                LOG_ERROR("Execute batch delete key, batch num: " << totalSize << " i:" << i);
                sem_destroy(&sem);
                return UBSIO_KVC_ERR;
            }
        }
        sem_wait(&sem);
        sem_destroy(&sem);
    }
    return UBSIO_KVC_OK;
}

int32_t KvOperation::BatchGetLengthKey(const std::vector<std::string> &key, std::vector<uint32_t> &lengths,
                                       std::vector<int> &results)
{
    auto totalSize = key.size();
    std::fill_n(results.begin(), totalSize, UBSIO_KVC_ERR);
    for (uint32_t start = 0; start < totalSize; start += BATCH_LIMIT) {
        sem_t sem;
        sem_init(&sem, 0, 0);
        uint32_t end = std::min(start + BATCH_LIMIT, (uint32_t)totalSize);
        uint32_t batchSize = end - start;
        auto keySize = std::make_shared<std::atomic<uint32_t>>(batchSize);
        for (uint32_t i = start; i < end; i++) {
            auto index = i;
            auto keyCopy = key[i];
            std::function<void()> func = [this, index, keyCopy, &lengths, &results,
                keySize, &sem, totalSize]() {
                auto ret = KvGetLengthKey(keyCopy, lengths[index]);
                if (ret != UBSIO_KVC_OK) {
                    LOG_ERROR("Batch get length failed, ret: " << ret
                        << " batch num: " << totalSize << " i:" << index);
                }
                results[index] = ret;
                if (keySize->fetch_sub(1) == 1) {
                    sem_post(&sem);
                }
            };

            auto result = mKvExecutor->Execute(func);
            if (!result) {
                LOG_ERROR("Execute batch get length, batch num: " << totalSize << " i:" << i);
                sem_destroy(&sem);
                return UBSIO_KVC_ERR;
            }
        }
        sem_wait(&sem);
        sem_destroy(&sem);
    }
    return UBSIO_KVC_OK;
}
}; // namespace ubsio
} // namespace ock
