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

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <vector>
#include "ubsio_kvc_log.h"
#include <ubsio_kvc_err.h>
#include "dl_biosdk_api.h"
#include "ubsio_kvc_operation.h"
#include "ubsio_kvc_instance.h"
#include "ubsio_kvc.h"

using namespace ock::ubsio;

namespace {
constexpr int MAX_KEY_LENGTH = 255;
constexpr int MAX_BATCH_OP_COUNT = 16 << 10; // 16K
constexpr int MAX_KV_LAYER_NUM = 2 * 512; // k layer + v layer
constexpr int64_t MAX_KV_LAYER_LENGTH = 2 * 1024 * 1024 * 1024LL; // 2G
}

UBSIO_API int32_t UbsioKvCacheInit(int32_t devId)
{
    if (UNLIKELY(devId < -1)) {
        LOG_ERROR("Invalid device id: " << devId);
        return UBSIO_KVC_INVALID_PARAM;
    }
    int32_t ret = KvcInstance::Instance().Initialize(devId);
    if (UNLIKELY(ret != UBSIO_KVC_OK)) {
        LOG_ERROR("init kvc instance failed, device id: " << devId << ", ret: " << ret);
        return UBSIO_KVC_ERR;
    }
    return KvcOperationInit(devId);
}

UBSIO_API int32_t UbsioGetResourceInfo(UbsioResourceInfo *info)
{
    if (UNLIKELY(info == nullptr)) {
        LOG_ERROR("Invalid resource info pointer.");
        return UBSIO_KVC_INVALID_PARAM;
    }
    *info = {};

    int32_t ret = DlBioSdkApi::LoadLibrary();
    if (UNLIKELY(ret != UBSIO_KVC_OK)) {
        LOG_ERROR("Load bio sdk failed, ret:" << ret << ".");
        return UBSIO_KVC_ERR;
    }

    CacheResourcesDesc localResource{};
    CResult bioRet = DlBioSdkApi::ShowLocalCacheResource(&localResource);
    if (UNLIKELY(bioRet != RET_CACHE_OK)) {
        LOG_ERROR("Query local write cache resource failed, ret:" << bioRet << ".");
        return UBSIO_KVC_ERR;
    }

    info->diskCap = localResource.wCacheDiskCapacity;
    info->diskUsed = localResource.wCacheDiskUsedSize;
    info->memCap = localResource.wCacheMemCapacity;
    info->memUsed = localResource.wCacheMemUsedSize;
    info->diskNum = std::min<uint32_t>(localResource.diskNum, UBSIO_RESOURCE_MAX_DISK_NUM);
    for (uint32_t diskIndex = 0; diskIndex < info->diskNum; ++diskIndex) {
        const auto &source = localResource.disks[diskIndex];
        auto &target = info->disks[diskIndex];
        target.status = source.status;
        target.readBandwidth = source.readBandwidth;
        target.writeBandwidth = source.writeBandwidth;
        target.totalBandwidth = source.totalBandwidth;
        target.bandwidthValid = source.bandwidthValid;
        std::snprintf(target.path, sizeof(target.path), "%s", source.path);
        if (target.status != 0) {
            ++info->faultDiskNum;
        }
    }
    return UBSIO_KVC_OK;
}

UBSIO_API int32_t UbsioKvCacheRegisterMetaEventCallback(UbsioMetaEventCallbackC callback, void *context)
{
    int32_t ret = DlBioSdkApi::LoadLibrary();
    if (ret != UBSIO_KVC_OK) {
        LOG_ERROR("load bio sdk failed, ret: " << ret);
        return UBSIO_KVC_ERR;
    }
    return DlBioSdkApi::RegisterMetaEventCallback(callback, context) == RET_CACHE_OK ? UBSIO_KVC_OK : UBSIO_KVC_ERR;
}

UBSIO_API void UbsioKvCacheExit(void) 
{ 
    KvcExit();
}

UBSIO_API int32_t UbsioKvCachePut(const char *key, void *buf, size_t length, uint32_t flags)
{
    if (UNLIKELY(key == nullptr || buf == nullptr || length == 0)) {
        LOG_ERROR("Invalid parma, length:" << length);
        return UBSIO_KVC_INVALID_PARAM;
    }
    if (UNLIKELY(strlen(key) > MAX_KEY_LENGTH || strlen(key) < 1)) {
        LOG_ERROR("Invalid parma");
        return UBSIO_KVC_INVALID_PARAM;
    }
    return KvcPutData(key, buf, length, flags);
}

UBSIO_API int32_t UbsioKvCacheBatchPut(const char **keys, 
                                        uint32_t keysCount, 
                                        void **bufs, 
                                        size_t *lengths, 
                                        int *results, 
                                        uint32_t flags) 
 { 
    if (keys == nullptr || bufs == nullptr || lengths == nullptr || results == nullptr) { 
        LOG_ERROR("Invalid params, keysCount: " << keysCount); 
        return UBSIO_KVC_INVALID_PARAM; 
    } 
    if (keysCount > MAX_BATCH_OP_COUNT || keysCount < 1) { 
        LOG_ERROR("Invalid params, keysCount: " << keysCount); 
        return UBSIO_KVC_INVALID_PARAM; 
    } 
    std::vector<std::string> key_vector; 
    std::vector<void *> bufs_vector; 
    std::vector<int> batchResult(keysCount, UBSIO_KVC_ERR); 
    std::vector<size_t> lengths_vector; 
    bufs_vector.reserve(keysCount); 
    key_vector.reserve(keysCount); 
    lengths_vector.reserve(keysCount); 
 
    for (size_t i = 0; i < keysCount; i++) { 
        if (keys[i] == nullptr) { 
            LOG_ERROR("Get invalid key nullptr on idx [" << i << "]"); 
            return UBSIO_KVC_INVALID_PARAM; 
        } 
        if (strlen(keys[i]) > MAX_KEY_LENGTH || strlen(keys[i]) < 1) { 
            LOG_ERROR("Get invalid key length [" << i << "]"); 
            return UBSIO_KVC_INVALID_PARAM; 
        } 
        if (bufs == nullptr || bufs[i] == nullptr || lengths == nullptr || lengths[i] == 0) { 
            LOG_ERROR("Get invalid bufs address [" << i << "]"); 
            return UBSIO_KVC_INVALID_PARAM; 
        } 
        key_vector.emplace_back(keys[i]); 
        bufs_vector.emplace_back(bufs[i]); 
        lengths_vector.emplace_back(lengths[i]); 
    } 
 
    auto ret = KvcBatchPutData(key_vector, bufs_vector, lengths_vector, batchResult, flags); 
    if (ret != UBSIO_KVC_OK) { 
        LOG_ERROR("batch put failed, keysCount: " << keysCount); 
    } 
    for (uint32_t i = 0; i < keysCount; ++i) { 
        results[i] = batchResult[i]; 
    } 
    return ret;
}

UBSIO_API int32_t UbsioKvCacheGet(const char *key, void *buf, size_t length, uint32_t flags) 
{ 
    if (key == nullptr || buf == nullptr || length == 0) { 
        LOG_ERROR("Invalid params"); 
        return UBSIO_KVC_INVALID_PARAM; 
    } 
    if (strlen(key) > MAX_KEY_LENGTH || strlen(key) < 1) { 
        LOG_ERROR("Invalid params"); 
        return UBSIO_KVC_INVALID_PARAM; 
    } 
    return KvcGetData(key, buf, length, flags); 
}

UBSIO_API bool UbsioKvCacheExist(const char *key, uint32_t flags) 
{ 
    if (key == nullptr || strlen(key) > MAX_KEY_LENGTH || strlen(key) < 1) { 
        LOG_ERROR("Invalid params"); 
        return false; 
    } 
    return KvcExistKey(key, flags); 
}

UBSIO_API int32_t UbsioKvCacheDelete(const char *key, uint32_t flags) 
{ 
    if (key == nullptr || strlen(key) > MAX_KEY_LENGTH || strlen(key) < 1) { 
        LOG_ERROR("Invalid parma"); 
        return UBSIO_KVC_INVALID_PARAM; 
    } 
    return KvcDeleteKey(key, flags); 
}

UBSIO_API int32_t UbsioKvCacheBatchDelete(const char **keys, uint32_t keysCount, int32_t *results, uint32_t flags) 
{ 
    if (keys == nullptr || results == nullptr) { 
        LOG_ERROR("Invalid parma"); 
        return UBSIO_KVC_INVALID_PARAM; 
    } 
    if (keysCount > MAX_BATCH_OP_COUNT || keysCount < 1) { 
        LOG_ERROR("Invalid parma"); 
        return UBSIO_KVC_INVALID_PARAM; 
    } 
    std::vector<std::string> key_vector; 
    std::vector<int> delete_results_vector(keysCount, UBSIO_KVC_ERR); 
    key_vector.reserve(keysCount); 

    for (size_t i = 0; i < keysCount; i++) { 
        if (keys[i] == nullptr) { 
            LOG_ERROR("Get invalid key nullptr on idx [" << i << "]"); 
            return UBSIO_KVC_INVALID_PARAM; 
        } 
        if (strlen(keys[i]) > MAX_KEY_LENGTH || strlen(keys[i]) < 1) { 
            LOG_ERROR("Get invalid key length [" << i << "]"); 
            return UBSIO_KVC_INVALID_PARAM; 
        } 
        key_vector.emplace_back(keys[i]); 
    } 

    auto ret = KvcBatchDeleteKey(key_vector, delete_results_vector, flags); 
    if (ret != UBSIO_KVC_OK) { 
        LOG_ERROR("Kvc batch delete failed"); 
    } 
    for (uint32_t i = 0; i < keysCount; ++i) { 
        results[i] = delete_results_vector[i]; 
    } 
    return ret;
}

UBSIO_API int32_t UbsioKvCacheGetLength(const char *key, size_t *length, uint32_t flags) 
{ 
    if (key == nullptr || length == nullptr || strlen(key) > MAX_KEY_LENGTH || strlen(key) < 1) { 
        LOG_ERROR("Invalid params"); 
        return UBSIO_KVC_INVALID_PARAM; 
    } 
    uint32_t size = 0; 
    auto ret = KvcGetKeyLength(key, size, flags); 
    if (ret != UBSIO_KVC_OK) { 
        LOG_ERROR("KvcGetKeyLength failed"); 
        return ret; 
    } 
    *length = static_cast<size_t>(size); 
    return UBSIO_KVC_OK; 
}

UBSIO_API int32_t UbsioKvCacheBatchGetLength(const char **keys, 
                                            uint32_t keysCount, 
                                            size_t *lengths, 
                                            int32_t *results, 
                                            uint32_t flags) 
{ 
    if (keys == nullptr || lengths == nullptr || results == nullptr) { 
        LOG_ERROR("Invalid params, keysCount: " << keysCount); 
        return UBSIO_KVC_INVALID_PARAM; 
    } 
    if (keysCount > MAX_BATCH_OP_COUNT || keysCount < 1) { 
        LOG_ERROR("Invalid params, keysCount: " << keysCount); 
        return UBSIO_KVC_INVALID_PARAM; 
    } 
    std::vector<std::string> key_vector; 
    std::vector<int> length_results_vector(keysCount, UBSIO_KVC_ERR); 
    std::vector<uint32_t> get_lengths; 
    key_vector.reserve(keysCount); 
    get_lengths.resize(keysCount);


    for (size_t i = 0; i < keysCount; i++) { 
        if (keys[i] == nullptr) { 
            LOG_ERROR("Get invalid key nullptr on idx [" << i << "]"); 
            return UBSIO_KVC_INVALID_PARAM; 
        } 
        if (strlen(keys[i]) > MAX_KEY_LENGTH || strlen(keys[i]) < 1) { 
            LOG_ERROR("Get invalid key length [" << i << "]"); 
            return UBSIO_KVC_INVALID_PARAM; 
        } 
        key_vector.emplace_back(keys[i]); 
    } 

    auto ret = KvcBatchGetLengthKey(key_vector, get_lengths, length_results_vector, flags); 
    if (ret != UBSIO_KVC_OK) { 
        LOG_ERROR("Kvc batch get length failed"); 
    } 
    for (uint32_t i = 0; i < keysCount; ++i) { 
        lengths[i] = static_cast<size_t>(get_lengths[i]); 
        results[i] = length_results_vector[i]; 
    } 
    return ret;
}

UBSIO_API int32_t UbsioKvCacheBatchGet(const char **keys,
                                       uint32_t keysCount,
                                       void **bufs,
                                       size_t *lengths,
                                       int *results,
                                       uint32_t flags)
{
    if (UNLIKELY(keys == nullptr || bufs == nullptr || lengths == nullptr || results == nullptr)) {
        LOG_ERROR("Invalid params, keysCount: " << keysCount);
        return UBSIO_KVC_INVALID_PARAM;
    }
    if (UNLIKELY(keysCount > MAX_BATCH_OP_COUNT || keysCount < 1)) {
        LOG_ERROR("Invalid params, keysCount: " << keysCount);
        return UBSIO_KVC_INVALID_PARAM;
    }

    std::vector<std::string> keyVector(keysCount);
    std::vector<int> batchResult(keysCount, UBSIO_KVC_ERR);
    std::vector<size_t> lengthsVector(keysCount);
    for (size_t i = 0; i < keysCount; i++) {
        if (UNLIKELY(keys[i] == nullptr)) {
            LOG_ERROR("Get invalid key nullptr on idx [" << i << "]");
            return UBSIO_KVC_INVALID_PARAM;
        }
        if (UNLIKELY(strlen(keys[i]) > MAX_KEY_LENGTH || strlen(keys[i]) < 1)) {
            LOG_ERROR("Get invalid key length [" << i << "]");
            return UBSIO_KVC_INVALID_PARAM;
        }
        if (UNLIKELY(lengths[i] == 0)) {
            LOG_ERROR("Get invalid lengths [" << i << "]");
            return UBSIO_KVC_INVALID_PARAM;
        }
        keyVector[i] = keys[i];
        lengthsVector[i] = lengths[i];
    }

    auto ret = KvcBatchGetData(keyVector, bufs, lengthsVector, batchResult, flags);
    if (UNLIKELY(ret != UBSIO_KVC_OK)) {
        LOG_ERROR("Kvc batch get failed, ret:" << ret);
    }
    for (uint32_t i = 0; i < keysCount; ++i) {
        results[i] = batchResult[i];
    }
    return ret;
}

int32_t UbsioKvCacheBatchGetDirect(const char **keys,
                                   uint32_t keysCount,
                                   void ***bufs,
                                   size_t **lengths,
                                   uint32_t lengthsRows,
                                   uint32_t lengthsCols,
                                   int *results,
                                   uint32_t flags)
{
    
    LOG_ERROR("Interface reserved; currently not support directly get data to hbm, please use UbsioKvCacheBatchGet instead.");
    return UBSIO_KVC_ERR;
}

UBSIO_API int32_t UbsioKvCacheBatchExist(const char **keys, uint32_t keysCount, bool *results, uint32_t flags)
{
    if (UNLIKELY(keys == nullptr || results == nullptr)) {
        LOG_ERROR("Invalid params, keysCount: " << keysCount);
        return UBSIO_KVC_INVALID_PARAM;
    }
    if (UNLIKELY(keysCount > MAX_BATCH_OP_COUNT || keysCount < 1)) {
        LOG_ERROR("Invalid params, keysCount: " << keysCount);
        return UBSIO_KVC_INVALID_PARAM;
    }

    std::vector<std::string> keyVector(keysCount);
    for (size_t i = 0; i < keysCount; i++) {
        if (UNLIKELY(keys[i] == nullptr)) {
            LOG_ERROR("Get invalid key nullptr on idx [" << i << "]");
            return UBSIO_KVC_INVALID_PARAM;
        }
        if (UNLIKELY(strlen(keys[i]) > MAX_KEY_LENGTH || strlen(keys[i]) < 1)) {
            LOG_ERROR("Get invalid key length[" << i << "]");
            return UBSIO_KVC_INVALID_PARAM;
        }
        keyVector[i] = keys[i];
    }

    auto ret = KvcBatchExistKey(keyVector, results, flags);
    if (UNLIKELY(ret != UBSIO_KVC_OK)) {
        LOG_ERROR("Kvc batch exist failed, ret:" << ret);
        return ret;
    }
    return UBSIO_KVC_OK;
}

UBSIO_API int32_t UbsioKvCacheBatchFree(void **bufs, uint32_t keysCount)
{
    if (UNLIKELY(bufs == nullptr)) {
        LOG_ERROR("Invalid params, keysCount: " << keysCount);
        return UBSIO_KVC_INVALID_PARAM;
    }
    if (UNLIKELY(keysCount > MAX_BATCH_OP_COUNT || keysCount < 1)) {
        LOG_ERROR("Invalid params, keysCount: " << keysCount);
        return UBSIO_KVC_INVALID_PARAM;
    }

    int ret = KvcBatchFreeGetAddress(bufs, keysCount);
    if (UNLIKELY(ret != UBSIO_KVC_OK)) {
        LOG_ERROR("Kvc batch free address failed, ret:" << ret);
        return UBSIO_KVC_ERR;
    }
    return UBSIO_KVC_OK;
}
