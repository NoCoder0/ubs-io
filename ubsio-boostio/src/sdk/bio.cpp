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

#include <iostream>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include "securec.h"
#include "message.h"
#include "bio_functions.h"
#include "bio_client_agent.h"
#include "bio_client_log.h"
#include "bio_trace.h"
#include "bio_client.h"
#include "bio.h"
#include "bio_kvc_config.h"

namespace ock {
namespace bio {
BioClientPtr gClient = BioClient::Instance();

inline static CResult ToCResult(const BResult ret)
{
    switch (ret) {
        case BIO_OK:
            return RET_CACHE_OK;
        case BIO_ERR:
        case BIO_INNER_ERR:
        case BIO_NOT_READY:
            return RET_CACHE_ERROR;
        case BIO_INVALID_PARAM:
            return RET_CACHE_EPERM;
        case BIO_ALLOC_FAIL:
            return RET_CACHE_NEED_RETRY;
        case BIO_NOT_INITIALIZED:
            return RET_CACHE_NOT_READY;
        case BIO_NOT_EXISTS:
            return RET_CACHE_NOT_FOUND;
        case BIO_CHECK_PT_FAIL:
            return RET_CACHE_PT_FAULT;
        case BIO_READ_EXCEED:
            return RET_CACHE_READ_EXCEED;
        case BIO_KEY_CONFLICT:
            return RET_CACHE_CONFLICT;
        case BIO_DISK_IOERR:
            return RET_CACHE_DISK_FAULT;
        case BIO_UFS_IOERR:
            return RET_CACHE_UFS_FAULT;
        default:
            return RET_CACHE_NEED_RETRY;
    }
}

inline static void StatisticPutIoSize(uint64_t length)
{
    if (length <= IO_SIZE_4K) {
        BIO_TRACE_START(SDK_TRACE_W_S_1_4K);
        BIO_TRACE_END(SDK_TRACE_W_S_1_4K, BIO_OK);
    } else if (length <= IO_SIZE_8K) {
        BIO_TRACE_START(SDK_TRACE_W_S_4_8K);
        BIO_TRACE_END(SDK_TRACE_W_S_4_8K, BIO_OK);
    } else if (length <= IO_SIZE_64K) {
        BIO_TRACE_START(SDK_TRACE_W_S_8_64K);
        BIO_TRACE_END(SDK_TRACE_W_S_8_64K, BIO_OK);
    } else if (length <= IO_SIZE_128K) {
        BIO_TRACE_START(SDK_TRACE_W_S_64_128K);
        BIO_TRACE_END(SDK_TRACE_W_S_64_128K, BIO_OK);
    } else if (length <= IO_SIZE_256K) {
        BIO_TRACE_START(SDK_TRACE_W_S_128_256K);
        BIO_TRACE_END(SDK_TRACE_W_S_128_256K, BIO_OK);
    } else if (length <= IO_SIZE_1M) {
        BIO_TRACE_START(SDK_TRACE_W_S_256K_1M);
        BIO_TRACE_END(SDK_TRACE_W_S_256K_1M, BIO_OK);
    } else if (length <= IO_SIZE_2M) {
        BIO_TRACE_START(SDK_TRACE_W_S_1_2M);
        BIO_TRACE_END(SDK_TRACE_W_S_1_2M, BIO_OK);
    } else if (length <= IO_SIZE_4M) {
        BIO_TRACE_START(SDK_TRACE_W_S_2_4M);
        BIO_TRACE_END(SDK_TRACE_W_S_2_4M, BIO_OK);
    } else {
        BIO_TRACE_START(SDK_TRACE_W_S_4M);
        BIO_TRACE_END(SDK_TRACE_W_S_4M, BIO_OK);
    }
}

inline static void StatisticGetIoSize(uint64_t length)
{
    if (length <= IO_SIZE_4K) {
        BIO_TRACE_START(SDK_TRACE_R_S_1_4K);
        BIO_TRACE_END(SDK_TRACE_R_S_1_4K, BIO_OK);
    } else if (length <= IO_SIZE_8K) {
        BIO_TRACE_START(SDK_TRACE_R_S_4_8K);
        BIO_TRACE_END(SDK_TRACE_R_S_4_8K, BIO_OK);
    } else if (length <= IO_SIZE_64K) {
        BIO_TRACE_START(SDK_TRACE_R_S_8_64K);
        BIO_TRACE_END(SDK_TRACE_R_S_8_64K, BIO_OK);
    } else if (length <= IO_SIZE_128K) {
        BIO_TRACE_START(SDK_TRACE_R_S_64_128K);
        BIO_TRACE_END(SDK_TRACE_R_S_64_128K, BIO_OK);
    } else if (length <= IO_SIZE_256K) {
        BIO_TRACE_START(SDK_TRACE_R_S_128_256K);
        BIO_TRACE_END(SDK_TRACE_R_S_128_256K, BIO_OK);
    } else if (length <= IO_SIZE_1M) {
        BIO_TRACE_START(SDK_TRACE_R_S_256K_1M);
        BIO_TRACE_END(SDK_TRACE_R_S_256K_1M, BIO_OK);
    } else if (length <= IO_SIZE_2M) {
        BIO_TRACE_START(SDK_TRACE_R_S_1_2M);
        BIO_TRACE_END(SDK_TRACE_R_S_1_2M, BIO_OK);
    } else if (length <= IO_SIZE_4M) {
        BIO_TRACE_START(SDK_TRACE_R_S_2_4M);
        BIO_TRACE_END(SDK_TRACE_R_S_2_4M, BIO_OK);
    } else {
        BIO_TRACE_START(SDK_TRACE_R_S_4M);
        BIO_TRACE_END(SDK_TRACE_R_S_4M, BIO_OK);
    }
}

inline static bool KeyValid(const char *key)
{
    if (UNLIKELY(key == nullptr)) {
        CLIENT_LOG_ERROR("Parameter key is nullptr.");
        return false;
    }
    size_t len = strlen(key);
    if (UNLIKELY(len == 0 || len >= KEY_MAX_SIZE)) {
        CLIENT_LOG_ERROR("Invalid parameter key, len:" << len << ".");
        return false;
    }
    return true;
}

CResult Bio::CalculateLocation(uint64_t objectId, ObjLocation &location)
{
    if (UNLIKELY(!gClient->Ready())) {
        return RET_CACHE_NOT_READY;
    }
    return ToCResult(gClient->CalculateLocation(objectId, mAffinity, location));
}

CResult Bio::Put(const char *key, const char *value, uint64_t length, const ObjLocation &location)
{
    if (UNLIKELY(!gClient->Ready())) {
        return RET_CACHE_NOT_READY;
    }

    if (UNLIKELY(!KeyValid(key) || value == nullptr || length == 0)) {
        CLIENT_LOG_ERROR("Invalid put parameter, key or value pointers is nullptr, length:" << length << "(Mb).");
        return RET_CACHE_EPERM;
    }

    StatisticPutIoSize(length);
    BIO_TRACE_START(SDK_TRACE_PUT);
    MirrorClient::MirrorPut param = { { mTenantId, mAffinity, mStrategy }, const_cast<char *>(key),
        const_cast<char *>(value), length, location, 0 };
    BResult ret = gClient->Put(param);
    BIO_TRACE_END(SDK_TRACE_PUT, ret);
    CResult cRet = ToCResult(ret);
    if (UNLIKELY(ret != BIO_OK)) {
        CLIENT_LOG_ERROR("Put value failed, ret:" << ret << ", key:" << key << ", length:" << length <<
            ", location0:" << location.location[0] << ", location1:" << location.location[1] << ".");
    } else {
        CLIENT_LOG_DEBUG("Put value success, key:" << key << ", length:" << length << ", location0:" <<
            location.location[0] << ", location1:" << location.location[1] << ".");
    }
    return cRet;
}

CResult Bio::AsyncPut(const char *key, const char *value, uint64_t length, const ObjLocation &location,
    AsyncPutParam &cbParam)
{
    if (UNLIKELY(!gClient->Ready())) {
        return RET_CACHE_NOT_READY;
    }

    if (UNLIKELY(!KeyValid(key) || value == nullptr || length == 0)) {
        CLIENT_LOG_ERROR("Invalid put parameter, key or value pointers is nullptr, length:" << length << "(Mb).");
        return RET_CACHE_EPERM;
    }

    StatisticPutIoSize(length);
    BIO_TRACE_START(SDK_TRACE_ASYNC_PUT);
    MirrorClient::MirrorPut param = { { mTenantId, mAffinity, mStrategy }, const_cast<char *>(key),
                                      const_cast<char *>(value), length, location, 0 };
    BResult ret = gClient->AsyncPut(param, cbParam.callback, cbParam.context);
    BIO_TRACE_END(SDK_TRACE_ASYNC_PUT, ret);
    if (UNLIKELY(ret != BIO_OK)) {
        CLIENT_LOG_ERROR("Async put value failed, ret:" << ret << ", key:" << key << ", length:" << length <<
            ", location0:" << location.location[0] << ", location1:" << location.location[1] << ".");
    } else {
        CLIENT_LOG_DEBUG("Async put value success, key:" << key << ", length:" << length << ", location0:" <<
            location.location[0] << ", location1:" << location.location[1] << ".");
    }
    return ToCResult(ret);
}

CResult Bio::Put(const char *key, CacheSpaceDesc &spaceInfo)
{
    if (UNLIKELY(!gClient->Ready())) {
        return RET_CACHE_NOT_READY;
    }

    if (UNLIKELY(!KeyValid(key) || spaceInfo.addressNum == 0 || spaceInfo.descriptorSize == 0) ||
        UINT32_MAX - spaceInfo.address[0].size < spaceInfo.address[1].size) {
        return RET_CACHE_EPERM;
    }

    // 计算本次写的总数据大小
    uint32_t length = spaceInfo.address[0].size + spaceInfo.address[1].size;
    if (UNLIKELY(length > IO_SIZE_4M)) {
        return RET_CACHE_EPERM;
    }
    CLIENT_LOG_TRACE("Put value with space key:" << key << ", location0:" << spaceInfo.loc.location[0] <<
        ", location1:" << spaceInfo.loc.location[1] << ", addr num:" << spaceInfo.addressNum << ", addr0 size:" <<
        spaceInfo.address[0].size << ", addr1 size:" << spaceInfo.address[1].size << ", length:" << length << ".");

    StatisticPutIoSize(length);
    MirrorClient::MirrorPut param = { { mTenantId, mAffinity, mStrategy }, const_cast<char *>(key),
        nullptr, length, spaceInfo.loc, 0 };
    BIO_TRACE_START(SDK_TRACE_PUT);
    BResult ret = gClient->Put(param, spaceInfo);
    BIO_TRACE_END(SDK_TRACE_PUT, ret);
    CResult cRet = ToCResult(ret);
    if (UNLIKELY(ret != BIO_OK)) {
        CLIENT_LOG_ERROR("Put copy free value failed, ret:" << ret << ", key:" << key << ", length:" << length <<
            ", location0:" << spaceInfo.loc.location[0] << ", location1:" << spaceInfo.loc.location[1] << ".");
    } else {
        CLIENT_LOG_DEBUG("Put copy free value success, key:" << key << ", length:" << length << ", location0:" <<
            spaceInfo.loc.location[0] << ", location1:" << spaceInfo.loc.location[1] << ".");
    }
    return cRet;
}

CResult Bio::BatchGetKeyDiskAddr(const char **keys, ObjLocation *locations, const uint32_t count, KeyAddrInfo *infos)
{
    if (UNLIKELY(!gClient->Ready())) {
        return RET_CACHE_NOT_READY;
    }

    if (keys == nullptr || locations == nullptr || infos == nullptr) {
        return RET_CACHE_EPERM;
    }

    for (uint32_t i = 0; i < count; i++) {
        if (UNLIKELY(!KeyValid(keys[i]))) {
            CLIENT_LOG_ERROR("Invalid get parameter, key or value pointers is nullptr.");
            return RET_CACHE_EPERM;
        }
    }
    MirrorClient::MirrorBatchGetKeyAddr param{ count, keys, locations, infos };
    BResult ret = gClient->BatchGetKeyDiskAddr(param);
    return ToCResult(ret);
}

CResult Bio::Get(const char *key, uint64_t offset, uint64_t length, const ObjLocation &location, char *value,
    uint64_t &realLength)
{
    if (UNLIKELY(!gClient->Ready())) {
        return RET_CACHE_NOT_READY;
    }

    if (UNLIKELY(!KeyValid(key) || value == nullptr || length == 0)) {
        CLIENT_LOG_ERROR("Invalid get parameter, key or value pointers is nullptr, length:" << length << ".");
        return RET_CACHE_EPERM;
    }

    StatisticGetIoSize(length);
    BIO_TRACE_START(SDK_TRACE_GET);
    MirrorClient::MirrorGet param{ { mTenantId, mAffinity, mStrategy }, key, value, offset, length, location };
    realLength = 0;
    BResult ret = gClient->Get(param, realLength);
    BIO_TRACE_END(SDK_TRACE_GET, ret);
    CResult cRet = ToCResult(ret);
    if (UNLIKELY(ret != BIO_OK)) {
        CLIENT_LOG_ERROR("Get value failed, ret:" << ret << ", key:" << key << ", offset:" << offset << ", length:" <<
            length << ", location0:" << location.location[0] << ", location1:" << location.location[1] << ".");
    } else {
        CLIENT_LOG_DEBUG("Get value success, key:" << key << ", offset:" << offset << ", length:" << length <<
            ", realLen:" << realLength << ", location0:" << location.location[0] << ", location1:" <<
            location.location[1] << ".");
    }
    return cRet;
}

CResult Bio::BatchGet(const char **keys, const uint32_t count, uint64_t *offsets, uint64_t *lengths,
                      ObjLocation *locations, uintptr_t *valueAddrs,
                      uint64_t *realLengths, int32_t *results)
{
    if (UNLIKELY(!gClient->Ready())) {
        return RET_CACHE_NOT_READY;
    }
    if (UNLIKELY(keys == nullptr || lengths == nullptr || offsets == nullptr ||
        locations == nullptr || valueAddrs == nullptr || realLengths == nullptr || results == nullptr)) {
        return RET_CACHE_EPERM;
    }
    for (uint32_t i = 0; i < count; i++) {
        if (UNLIKELY(!KeyValid(keys[i]) || lengths[i] == 0)) {
            CLIENT_LOG_ERROR("Invalid get parameter, key or value pointers is nullptr, length:" << lengths[i] << ", index:" << i);
            return RET_CACHE_EPERM;
        }
    }

    BIO_TRACE_START(SDK_TRACE_BATCH_GET);
    BResult ret = gClient->BatchGet({ mTenantId, mAffinity, mStrategy }, keys, count, offsets, lengths, locations,
                                    valueAddrs, realLengths, results);
    BIO_TRACE_END(SDK_TRACE_BATCH_GET, ret);
    CResult cRet = ToCResult(ret);
    if (UNLIKELY(ret != BIO_OK)) {
        CLIENT_LOG_ERROR("Batch get value failed, ret:" << ret << ", key count:" << count << ".");
    } else {
        CLIENT_LOG_DEBUG("Batch get value success, key count:" << count << ".");
    }
    return cRet;
}

void Bio::BatchGetFree(uintptr_t *valueAddrs, const uint32_t count)
{
    BIO_TRACE_START(SDK_TRACE_BATCH_GET_FREE);
    gClient->BatchGetFree(valueAddrs, count);
    BIO_TRACE_END(SDK_TRACE_BATCH_GET_FREE, BIO_OK);
}

CResult Bio::AsyncGet(AsyncGetParam param, const ObjLocation &location, AsyncOpParam &opParam)
{
    if (UNLIKELY(!gClient->Ready())) {
        return RET_CACHE_NOT_READY;
    }
    const char *key = param.key;
    char *value = param.value;
    uint64_t offset = param.offset;
    uint64_t length = param.length;
    if (UNLIKELY(!KeyValid(key) || value == nullptr || length == 0)) {
        CLIENT_LOG_ERROR("Invalid get parameter, key or value pointers is nullptr, length:" << length << ".");
        return RET_CACHE_EPERM;
    }

    StatisticGetIoSize(length);
    BIO_TRACE_START(SDK_TRACE_ASYNC_GET);
    MirrorClient::MirrorGet getParam{{mTenantId, mAffinity, mStrategy}, key, value, offset, length, location};
    BResult ret = gClient->AsyncGet(getParam, opParam);
    BIO_TRACE_END(SDK_TRACE_ASYNC_GET, ret);
    if (UNLIKELY(ret != BIO_OK)) {
        CLIENT_LOG_ERROR("Async get value failed, ret:" << ret << ", key:" << key << ", offset:" << offset << ", length:" <<
            length << ", location0:" << location.location[0] << ", location1:" << location.location[1] << ".");
    } else {
        CLIENT_LOG_DEBUG("Async get value success, key:" << key << ", offset:" << offset << ", length:" << length <<
            ", location0:" << location.location[0] << ", location1:" << location.location[1] << ".");
    }
    return ToCResult(ret);
}

CResult Bio::Delete(const char *key, const ObjLocation &location)
{
    if (UNLIKELY(!gClient->Ready())) {
        return RET_CACHE_NOT_READY;
    }

    if (UNLIKELY(!KeyValid(key))) {
        CLIENT_LOG_ERROR("Invalid delete parameter, key:" << key << ".");
        return RET_CACHE_EPERM;
    }

    BIO_TRACE_START(SDK_TRACE_DELETE);
    BResult ret = gClient->DeleteKey(key, location);
    BIO_TRACE_END(SDK_TRACE_DELETE, ret);
    if (UNLIKELY(ret != BIO_OK)) {
        CLIENT_LOG_ERROR("Delete key failed, ret:" << ret << ", key:" << key << ".");
    } else {
        CLIENT_LOG_DEBUG("Delete key success, key:" << key << ", location0:" << location.location[0] <<
            ", location1:" << location.location[1] << ".");
    }
    return ToCResult(ret);
}

CResult Bio::Load(LoadPara &para, const ObjLocation location, const BioLoadCallback callback, void *context)
{
    if (UNLIKELY(!gClient->Ready())) {
        return RET_CACHE_NOT_READY;
    }

    if (UNLIKELY(!KeyValid(para.key) || context == nullptr || para.offset != 0 || para.length == 0)) {
        CLIENT_LOG_ERROR("Invalid load parameter, key:" << para.key << ".");
        return RET_CACHE_EPERM;
    }

    LoadCallback cb = [location, &para, callback](void *context, BResult result) {
        if (result != BIO_OK) {
            CLIENT_LOG_ERROR("Load failed, ret:" << result << ", key:" << para.key << ", location0:" <<
                location.location[0] << ", location1:" << location.location[1] << ".");
        } else {
            CLIENT_LOG_DEBUG("Load success, key:" << para.key << ", location0:" << location.location[0]
                             << ", location1:" << location.location[1] << ".");
        }
        if (callback != nullptr && context != nullptr) {
            callback(context, ToCResult(result));
        }
    };

    BIO_TRACE_START(SDK_TRACE_LOAD);
    BResult ret = gClient->Load(para, location, cb, context);
    BIO_TRACE_END(SDK_TRACE_LOAD, ret);
    if (UNLIKELY(ret != BIO_OK)) {
        CLIENT_LOG_ERROR("Load failed, ret:" << ret << ", key:" << para.key << ", offset:" << para.offset
                         << ", length:" << para.length << ", location:" << location.location[0] << ".");
    }
    return ToCResult(ret);
}

CResult Bio::ListAll(const char *prefix, std::unordered_map<std::string, ObjStat> &objs)
{
    if (UNLIKELY(!gClient->Ready())) {
        return RET_CACHE_NOT_READY;
    }

    if (UNLIKELY(!KeyValid(prefix))) {
        CLIENT_LOG_ERROR("Invalid list parameter, prefix:" << prefix << ".");
        return RET_CACHE_EPERM;
    }

    BIO_TRACE_START(SDK_TRACE_LISTALL);
    BResult ret = gClient->ListAll(prefix, objs);
    BIO_TRACE_END(SDK_TRACE_LISTALL, ret);
    if (UNLIKELY(ret != BIO_OK)) {
        CLIENT_LOG_ERROR("List all failed, ret:" << ret << ", prefix:" << prefix << ".");
    } else {
        CLIENT_LOG_DEBUG("List all success, prefix:" << prefix << ", num:" << objs.size() << ".");
    }
    return ToCResult(ret);
}

CResult Bio::Stat(const char *key, const ObjLocation &location, ObjStat &stat)
{
    if (UNLIKELY(!gClient->Ready())) {
        return RET_CACHE_NOT_READY;
    }

    if (UNLIKELY(!KeyValid(key))) {
        CLIENT_LOG_ERROR("Invalid Stat parameter, key:" << key << ".");
        return RET_CACHE_EPERM;
    }

    BIO_TRACE_START(SDK_TRACE_STAT);
    auto ret = gClient->Stat(key, location, stat);
    BIO_TRACE_END(SDK_TRACE_STAT, ret);
    return ToCResult(ret);
}

CResult Bio::BatchExist(const char *key[], ObjLocation location[], uint32_t count, bool *result)
{
    if (UNLIKELY(!gClient->Ready())) {
        return RET_CACHE_NOT_READY;
    }
    if (UNLIKELY(key == nullptr || result == nullptr || location == nullptr || count == 0)) {
        return RET_CACHE_EPERM;
    }
    for (uint32_t i = 0; i < count; i++) {
        if (UNLIKELY(!KeyValid(key[i]))) {
            CLIENT_LOG_ERROR("Invalid batch exist parameter, key is invalid, index:" << i << ".");
            return RET_CACHE_EPERM;
        }
    }

    BIO_TRACE_START(SDK_TRACE_BATCH_EXIST);
    auto ret = gClient->BatchExist(key, location, count, result);
    BIO_TRACE_END(SDK_TRACE_BATCH_EXIST, ret);
    return ToCResult(ret);
}

CResult Bio::NotifyUpdatePrepare()
{
    if (UNLIKELY(!gClient->Ready())) {
        return RET_CACHE_NOT_READY;
    }

    bool flag = true;
    BIO_TRACE_START(SDK_TRACE_NOTIFYUPDATE);
    auto ret = gClient->NotifyUpdate(flag);
    BIO_TRACE_END(SDK_TRACE_NOTIFYUPDATE, ret);
    if (ret != BIO_OK) {
        CLIENT_LOG_ERROR("Boostio notify upgrade prepare failed, ret:" << ret << ".");
    } else {
        CLIENT_LOG_DEBUG("Boostio notify upgrade prepare success.");
    }
    return ToCResult(ret);
}

CResult Bio::NotifyUpdateFinish()
{
    if (UNLIKELY(!gClient->Ready())) {
        return RET_CACHE_NOT_READY;
    }

    bool flag = false;
    BIO_TRACE_START(SDK_TRACE_UPDATEFINISH);
    auto ret = gClient->NotifyUpdate(flag);
    BIO_TRACE_END(SDK_TRACE_UPDATEFINISH, ret);
    if (ret != BIO_OK) {
        CLIENT_LOG_ERROR("Boostio notify upgrade finish failed, ret:" << ret << ".");
    } else {
        CLIENT_LOG_INFO("Boostio notify upgrade finish success.");
    }
    return ToCResult(ret);
}

CResult Bio::CheckUpdateReady()
{
    if (UNLIKELY(!gClient->Ready())) {
        return RET_CACHE_NOT_READY;
    }

    BIO_TRACE_START(SDK_TRACE_CHECKREADY);
    auto ret = gClient->CheckUpdateReady();
    BIO_TRACE_END(SDK_TRACE_CHECKREADY, ret);
    if (ret != BIO_OK) {
        CLIENT_LOG_WARN("Boostio upgrade check not ready, ret:" << ret << ".");
    } else {
        CLIENT_LOG_DEBUG("Boostio upgrade check is ready.");
    }
    return ToCResult(ret);
}

CResult Bio::AllocSpace(uint64_t objectId, uint64_t length, CacheSpaceDesc &spaceInfo)
{
    if (UNLIKELY(!gClient->Ready())) {
        return RET_CACHE_NOT_READY;
    }

    ObjLocation location;
    if (spaceInfo.allocLoc != 0) {
        auto ret = gClient->CalculateLocation(objectId, LOCAL_AFFINITY, location);
        if (UNLIKELY(ret != BIO_OK)) {
            return RET_CACHE_NOT_READY;
        }
        spaceInfo.loc = location;
    } else {
        location = spaceInfo.loc;
    }

    MirrorClient::MirrorPut param = { { mTenantId, mAffinity, mStrategy }, nullptr, nullptr, length, location, 0 };
    BIO_TRACE_START(SDK_TRACE_ALLOC_SPACE);
    auto ret = gClient->AllocSpace(param, spaceInfo);
    BIO_TRACE_END(SDK_TRACE_ALLOC_SPACE, ret);
    if (UNLIKELY(ret != BIO_OK)) {
        CLIENT_LOG_ERROR("Alloc space failed, ret:" << ret << ", objectId:" << objectId << ", length:" << length <<
            ", location0:" << location.location[0] << ", location1:" << location.location[1] << ".");
    } else {
        CLIENT_LOG_DEBUG("Alloc space success, objectId:" << objectId << ", length:" << length << ", location0:" <<
            location.location[0] << ", location1:" << location.location[1] << ".");
    }
    return ToCResult(ret);
}

std::shared_ptr<Bio> BioService::CreateCache(const CacheDescriptor &desc)
{
    if (UNLIKELY(desc.affinity >= AFFINITY_BUTT || desc.strategy >= STRATEGY_BUTT ||
        desc.affinity < LOCAL_AFFINITY || desc.strategy < WRITE_BACK)) {
        CLIENT_LOG_ERROR("Invalid cache descriptor, tenantId:" << desc.tenantId << ", affinity:" << desc.affinity <<
            ", strategy:" << desc.strategy << ".");
        return nullptr;
    }

    BIO_TRACE_START(SDK_TRACE_CREATE_CACHE);
    std::shared_ptr<Bio> cache = nullptr;
    try {
        cache = std::make_shared<Bio>(desc.tenantId, desc.affinity, desc.strategy);
    } catch (const std::bad_alloc& e) {
        CLIENT_LOG_ERROR("Malloc bio cache failed.");
        return nullptr;
    }
    BResult ret = gClient->Insert(cache);
    BIO_TRACE_END(SDK_TRACE_CREATE_CACHE, ret);
    if (UNLIKELY(ret != BIO_OK)) {
        CLIENT_LOG_ERROR("Client insert cache instance failed, ret:" << ret << ".");
        return nullptr;
    }

    static std::string affinityStr[] = { "INVALID", "LOCAL_AFFINITY", "GLOBAL_BALANCE", "BUTT" };
    static std::string strategyStr[] = { "INVALID", "WRITE_BACK", "WRITE_THROUGH", "BUTT" };
    CLIENT_LOG_INFO("Create cache instance success, tenantId:" << desc.tenantId << ", affinity:" <<
        affinityStr[desc.affinity] << ", strategy:" << strategyStr[desc.strategy] << ".");
    return cache;
}

CacheDescriptor BioService::GetCache(uint64_t tenantId)
{
    return gClient->Query(tenantId);
}

std::vector<CacheDescriptor> BioService::ListCache()
{
    return gClient->List();
}

void BioService::DestroyCache(uint64_t tenantId)
{
    BIO_TRACE_START(SDK_TRACE_DESTROY_CACHE);
    gClient->Delete(tenantId);
    BIO_TRACE_END(SDK_TRACE_DESTROY_CACHE, BIO_OK);
    CLIENT_LOG_INFO("Destroy cache instance success, tenantId:" << tenantId << ".");
}

CResult BioService::BioShowCacheResource(std::vector<CacheResourcesDesc> &nodeDesc)
{
    if (UNLIKELY(!gClient->Ready())) {
        return RET_CACHE_NOT_READY;
    }
    BResult ret = gClient->QueryCacheResource(nodeDesc);
    if (UNLIKELY(ret != BIO_OK)) {
        CLIENT_LOG_ERROR("query cache resource failed ret: " << ret);
    }

    return ToCResult(ret);
}

CResult BioService::BioShowLocalCacheResource(CacheResourcesDesc &nodeDesc)
{
    if (UNLIKELY(!gClient->Ready())) {
        return RET_CACHE_NOT_READY;
    }
    BResult ret = gClient->QueryLocalCacheResource(nodeDesc);
    if (UNLIKELY(ret != BIO_OK)) {
        CLIENT_LOG_ERROR("Query local cache resource failed ret: " << ret);
    }
    return ToCResult(ret);
}

CResult BioService::BioShowCacheHitRatio(std::unordered_map<uint16_t, CacheHitDesc> &nodeDesc)
{
    if (UNLIKELY(!gClient->Ready())) {
        return RET_CACHE_NOT_READY;
    }
    BResult ret = gClient->GetCacheHitRatio(nodeDesc);
    if (UNLIKELY(ret != BIO_OK)) {
        CLIENT_LOG_ERROR("Get Cache HitRatio failed ret: " << ret);
    }

    return ToCResult(ret);
}

CResult BioService::Initialize(WorkerMode mode, const ClientOptionsConfig &optConf)
{
    auto bioClient = BioClient::Instance();
    if (UNLIKELY(bioClient == nullptr)) {
        CLIENT_LOG_ERROR("Make bio client instance failed.");
        return RET_CACHE_ERROR;
    }
    return ToCResult(bioClient->Start(mode, optConf));
}

void BioService::Exit()
{
    gClient->Exit();
}
}
}

/* ******************************** Boostio api implementation in C language ********************* */
using namespace ock::bio;

static std::unordered_map<uint64_t, std::shared_ptr<Bio>> gBioCacheMap;
static std::mutex g_lock;

static bool CheckClientConfig(const ClientOptionsConfig &optConf)
{
    if (optConf.logType < STDOUT_TYPE || optConf.logType > STDERR_TYPE) {
        return false;
    }
    if (!memchr(optConf.logFilePath, '\0', PATH_MAX)) {
        return false;
    }
    if (!memchr(optConf.certificationPath, '\0', PATH_MAX)) {
        return false;
    }
    if (!memchr(optConf.caCerPath, '\0', PATH_MAX)) {
        return false;
    }
    if (!memchr(optConf.caCrlPath, '\0', PATH_MAX)) {
        return false;
    }
    if (!memchr(optConf.privateKeyPath, '\0', PATH_MAX)) {
        return false;
    }
    if (!memchr(optConf.privateKeyPassword, '\0', PATH_MAX)) {
        return false;
    }
    if (!memchr(optConf.decrypterLibPath, '\0', PATH_MAX)) {
        return false;
    }
    if (!memchr(optConf.opensslLibDir, '\0', PATH_MAX)) {
        return false;
    }
    return true;
}

CResult BioLoadKvcConfig(const char *path, uint32_t size, BioKvcConfigV1 *out, ClientOptionsConfig *tlsOptions)
{
    try {
        return LoadBioKvcConfig(path, size, out, tlsOptions);
    } catch (...) {
        std::fputs("KVC startup configuration failed: internal error\n", stderr);
        return RET_CACHE_ERROR;
    }
}

CResult BioInitialize(WorkerMode mode, ClientOptionsConfig *optConf)
{
    const auto kvcConfig = GetBioKvcSnapshot();
    if (kvcConfig && mode == STANDALONE && setenv("UBSIO_CONFIG_PATH", kvcConfig->path.c_str(), 1) != 0) {
        return RET_CACHE_ERROR;
    }
    if (optConf == nullptr) {
        ClientOptionsConfig config{};
        config.enable = false;
        const CResult result = BioService::Initialize(mode, config);
        if (result == RET_CACHE_OK) LogBioKvcConfigReady();
        return result;
    }

    if (!CheckClientConfig(*optConf)) {
        return RET_CACHE_EPERM;
    }
    const CResult result = BioService::Initialize(mode, *optConf);
    if (result == RET_CACHE_OK) LogBioKvcConfigReady();
    return result;
}

CResult BioRegisterMetaEventCallback(UbsioMetaEventCallbackC callback, void *context)
{
    auto agentPtr = ock::bio::agent::BioClientAgent::Instance();
    if (agentPtr == nullptr) {
        return RET_CACHE_ERROR;
    }
    return agentPtr->RegisterMetaEventCallback(callback, context) == BIO_OK ? RET_CACHE_OK : RET_CACHE_ERROR;
}

void BioExit(void)
{
    BioService::Exit();
}

static void BioCacheResourceCalc(CacheResourcesDesc **nodeDesc, const std::vector<CacheResourcesDesc> &nodeDescription)
{
    uint32_t i = 0;
    for (auto& nodeInfo : nodeDescription) {
        (*nodeDesc)[i].nodeId = nodeInfo.nodeId;
        (*nodeDesc)[i].rCacheMemCapacity = nodeInfo.rCacheMemCapacity;
        (*nodeDesc)[i].rCacheDiskCapacity = nodeInfo.rCacheDiskCapacity;
        (*nodeDesc)[i].wCacheMemCapacity = nodeInfo.wCacheMemCapacity;
        (*nodeDesc)[i].wCacheDiskCapacity = nodeInfo.wCacheDiskCapacity;
        (*nodeDesc)[i].wCacheMemUsedSize = nodeInfo.wCacheMemUsedSize;
        (*nodeDesc)[i].wCacheDiskUsedSize = nodeInfo.wCacheDiskUsedSize;
        (*nodeDesc)[i].rCacheMemUsedSize = nodeInfo.rCacheMemUsedSize;
        (*nodeDesc)[i].rCacheDiskUsedSize = nodeInfo.rCacheDiskUsedSize;
        (*nodeDesc)[i].diskNum = nodeInfo.diskNum > DISK_RESOURCE_MAX_NUM ? DISK_RESOURCE_MAX_NUM : nodeInfo.diskNum;
        for (uint16_t diskIndex = 0; diskIndex < (*nodeDesc)[i].diskNum; ++diskIndex) {
            (*nodeDesc)[i].disks[diskIndex] = nodeInfo.disks[diskIndex];
        }
        i++;
    }
}

CResult BioBatchGetKeyDiskAddr(uint64_t tenantId, const char **keys, ObjLocation *locations,
                               const uint32_t count, KeyAddrInfo *infos)
{
    std::shared_ptr<Bio> bioInstance = nullptr;
    {
        std::unique_lock<std::mutex> locker(g_lock);
        auto iter = gBioCacheMap.find(tenantId);
        if (UNLIKELY(iter == gBioCacheMap.end())) {
            return RET_CACHE_NOT_FOUND;
        }
        bioInstance = iter->second;
    }

    return bioInstance->BatchGetKeyDiskAddr(keys, locations, count, infos);
}

CResult BioShowCacheResource(CacheResourcesDesc **nodeDesc, uint64_t *nodeNum)
{
    if (UNLIKELY(nodeNum == nullptr || nodeDesc == nullptr)) {
        return RET_CACHE_EPERM;
    }
    {
        std::unique_lock<std::mutex> locker(g_lock);
        if (UNLIKELY(gBioCacheMap.empty())) {
            return RET_CACHE_NOT_FOUND;
        }
    }

    std::vector<CacheResourcesDesc> nodeDescription;
    auto ret = BioService::BioShowCacheResource(nodeDescription);
    if (UNLIKELY(ret != RET_CACHE_OK)) {
        return ret;
    }

    *nodeNum = nodeDescription.size();
    *nodeDesc = (CacheResourcesDesc *)malloc(sizeof(CacheResourcesDesc) * (*nodeNum));
    if (*nodeDesc == nullptr) {
        return RET_CACHE_NO_SPACE;
    }

    BioCacheResourceCalc(nodeDesc, nodeDescription);
    return ret;
}

CResult BioShowLocalCacheResource(CacheResourcesDesc *nodeDesc)
{
    if (UNLIKELY(nodeDesc == nullptr)) {
        return RET_CACHE_EPERM;
    }
    {
        std::unique_lock<std::mutex> locker(g_lock);
        if (UNLIKELY(gBioCacheMap.empty())) {
            return RET_CACHE_NOT_FOUND;
        }
    }

    *nodeDesc = {};
    return BioService::BioShowLocalCacheResource(*nodeDesc);
}

static void BioCacheHitRadioCalc(CacheHitFinalDesc *desc, CacheHitFinalDesc **nodeDesc,
                                 std::unordered_map<uint16_t, CacheHitDesc> nodeDescription)
{
    desc->nodeId = INVALID_NID;
    desc->rCacheHitMemCount = 0;
    desc->rCacheHitDiskCount = 0;
    desc->rCacheHitCount = 0;
    desc->wCacheHitMemCount = 0;
    desc->wCacheHitDiskCount = 0;
    desc->wCacheHitCount = 0;
    desc->rCacheTotalCount = 0;
    desc->wCacheTotalCount = 0;
    desc->backendHitCount = 0;
    uint32_t i = 0;
    for (auto& nodeInfo : nodeDescription) {
        (*nodeDesc)[i].nodeId = nodeInfo.first;
        (*nodeDesc)[i].rCacheHitMemCount = nodeInfo.second.rCacheHitMemCount.load();
        (*nodeDesc)[i].rCacheHitDiskCount = nodeInfo.second.rCacheHitDiskCount.load();
        (*nodeDesc)[i].rCacheHitCount = nodeInfo.second.rCacheHitCount.load();
        (*nodeDesc)[i].rCacheTotalCount = nodeInfo.second.rCacheTotalCount.load();
        (*nodeDesc)[i].wCacheHitMemCount = nodeInfo.second.wCacheHitMemCount.load();
        (*nodeDesc)[i].wCacheHitDiskCount = nodeInfo.second.wCacheHitDiskCount.load();
        (*nodeDesc)[i].wCacheHitCount = nodeInfo.second.wCacheHitCount.load();
        (*nodeDesc)[i].wCacheTotalCount = nodeInfo.second.wCacheTotalCount.load();
        (*nodeDesc)[i].backendHitCount = nodeInfo.second.backendHitCount.load();

        desc->rCacheHitMemCount += (*nodeDesc)[i].rCacheHitMemCount;
        desc->rCacheHitDiskCount += (*nodeDesc)[i].rCacheHitDiskCount;
        desc->rCacheHitCount += (*nodeDesc)[i].rCacheHitCount;
        desc->rCacheTotalCount += (*nodeDesc)[i].rCacheTotalCount;
        desc->wCacheHitMemCount += (*nodeDesc)[i].wCacheHitMemCount;
        desc->wCacheHitDiskCount += (*nodeDesc)[i].wCacheHitDiskCount;
        desc->wCacheHitCount += (*nodeDesc)[i].wCacheHitCount;
        desc->wCacheTotalCount += (*nodeDesc)[i].wCacheTotalCount;
        desc->backendHitCount += (*nodeDesc)[i].backendHitCount;
        i++;
    }
}

CResult BioShowCacheHitRatio(CacheHitFinalDesc *desc, CacheHitFinalDesc **nodeDesc, uint64_t *nodeNum)
{
    if (UNLIKELY(nodeNum == nullptr || desc == nullptr || nodeDesc == nullptr)) {
        return RET_CACHE_EPERM;
    }
    {
        std::unique_lock<std::mutex> locker(g_lock);
        if (UNLIKELY(gBioCacheMap.empty())) {
            return RET_CACHE_NOT_FOUND;
        }
    }

    std::unordered_map<uint16_t, CacheHitDesc> nodeDescription;
    auto ret = BioService::BioShowCacheHitRatio(nodeDescription);
    if (UNLIKELY(ret != RET_CACHE_OK)) {
        return ret;
    }

    *nodeNum = nodeDescription.size();
    *nodeDesc = (CacheHitFinalDesc *)malloc(sizeof(CacheHitFinalDesc) * (*nodeNum));
    if (*nodeDesc == nullptr) {
        return RET_CACHE_NO_SPACE;
    }

    BioCacheHitRadioCalc(desc, nodeDesc, std::move(nodeDescription));
    return ret;
}

void BioFreeCacheResourcePtr(CacheResourcesDesc **nodeDesc, uint64_t nodeNum)
{
    if (nodeNum == 0 || nodeDesc == nullptr || *nodeDesc == nullptr) {
        return;
    }
    free(*nodeDesc);
    *nodeDesc = nullptr;
}

void BioFreeCacheHitPtr(CacheHitFinalDesc **nodeDesc, uint64_t nodeNum)
{
    if (nodeNum == 0 || nodeDesc == nullptr || *nodeDesc == nullptr) {
        return;
    }
    free(*nodeDesc);
    *nodeDesc = nullptr;
}

CResult BioCreateCache(CacheDescriptor desc)
{
    std::unique_lock<std::mutex> locker(g_lock);
    auto iter = gBioCacheMap.find(desc.tenantId);
    if (UNLIKELY(iter != gBioCacheMap.end())) {
        return RET_CACHE_EXISTS;
    }
    auto bioInstance = BioService::CreateCache(desc);
    if (UNLIKELY(bioInstance == nullptr)) {
        return RET_CACHE_EPERM;
    }
    if (gBioCacheMap.size() > NO_1024) {
        return RET_CACHE_NO_SPACE;
    }
    gBioCacheMap.insert({ desc.tenantId, bioInstance });
    return RET_CACHE_OK;
}

CResult BioGetCache(uint64_t tenantId, CacheDescriptor *desc)
{
    if (UNLIKELY(desc == nullptr)) {
        return RET_CACHE_EPERM;
    }
    std::unique_lock<std::mutex> locker(g_lock);
    auto iter = gBioCacheMap.find(tenantId);
    if (UNLIKELY(iter == gBioCacheMap.end())) {
        return RET_CACHE_NOT_FOUND;
    }
    *desc = { iter->second->mTenantId, iter->second->mAffinity, iter->second->mStrategy };
    return RET_CACHE_OK;
}

CResult BioDestroyCache(uint64_t tenantId)
{
    std::unique_lock<std::mutex> locker(g_lock);
    auto iter = gBioCacheMap.find(tenantId);
    if (UNLIKELY(iter == gBioCacheMap.end())) {
        return RET_CACHE_NOT_FOUND;
    }
    BioService::DestroyCache(tenantId);
    gBioCacheMap.erase(iter);
    return RET_CACHE_OK;
}

CResult BioCalcLocation(uint64_t tenantId, uint64_t objectId, ObjLocation *location)
{
    if (UNLIKELY(location == nullptr)) {
        return RET_CACHE_EPERM;
    }
    std::shared_ptr<Bio> bioInstance = nullptr;
    {
        std::unique_lock<std::mutex> locker(g_lock);
        auto iter = gBioCacheMap.find(tenantId);
        if (UNLIKELY(iter == gBioCacheMap.end())) {
            return RET_CACHE_NOT_FOUND;
        }
        bioInstance = iter->second;
    }
    ObjLocation outLocation = { 0 };
    auto ret = bioInstance->CalculateLocation(objectId, outLocation);
    location->location[0] = outLocation.location[0];
    location->location[1] = outLocation.location[1];
    return ret;
}

CResult BioAllocCacheSpace(uint64_t tenantId, uint64_t objectId, uint64_t length, CacheSpaceDesc *space)
{
    if (UNLIKELY(space == nullptr || length > IO_SIZE_4M)) {
        return RET_CACHE_EPERM;
    }
    std::shared_ptr<Bio> bioInstance = nullptr;
    {
        std::unique_lock<std::mutex> locker(g_lock);
        auto iter = gBioCacheMap.find(tenantId);
        if (UNLIKELY(iter == gBioCacheMap.end())) {
            return RET_CACHE_NOT_FOUND;
        }
        bioInstance = iter->second;
    }
    return bioInstance->AllocSpace(objectId, length, *space);
}

CResult BioPutWithCopyFree(uint64_t tenantId, const char *key, CacheSpaceDesc *space)
{
    if (UNLIKELY(space == nullptr)) {
        return RET_CACHE_EPERM;
    }
    std::shared_ptr<Bio> bioInstance = nullptr;
    {
        std::unique_lock<std::mutex> locker(g_lock);
        auto iter = gBioCacheMap.find(tenantId);
        if (UNLIKELY(iter == gBioCacheMap.end())) {
            return RET_CACHE_NOT_FOUND;
        }
        bioInstance = iter->second;
    }
    return bioInstance->Put(key, *space);
}

CResult BioPut(uint64_t tenantId, const char *key, const char *value, uint64_t length, ObjLocation location)
{
    std::shared_ptr<Bio> bioInstance = nullptr;
    {
        std::unique_lock<std::mutex> locker(g_lock);
        auto iter = gBioCacheMap.find(tenantId);
        if (UNLIKELY(iter == gBioCacheMap.end())) {
            return RET_CACHE_NOT_FOUND;
        }
        bioInstance = iter->second;
    }
    return bioInstance->Put(key, value, length, location);
}

CResult BioAsyncPut(uint64_t tenantId, const char *key, const char *value, uint64_t length, ObjLocation location,
    BioAsyncPutCallback callback, void* context)
{
    std::shared_ptr<Bio> bioInstance = nullptr;
    {
        std::unique_lock<std::mutex> locker(g_lock);
        auto iter = gBioCacheMap.find(tenantId);
        if (UNLIKELY(iter == gBioCacheMap.end())) {
            return RET_CACHE_NOT_FOUND;
        }
        bioInstance = iter->second;
    }
    AsyncPutParam cbParam = {callback, context};
    return bioInstance->AsyncPut(key, value, length, location, cbParam);
}

CResult BioGet(uint64_t tenantId, const char *key, uint64_t offset, uint64_t length, ObjLocation location, char *value,
    uint64_t *realLength)
{
    if (UNLIKELY(realLength == nullptr)) {
        return RET_CACHE_EPERM;
    }
    std::shared_ptr<Bio> bioInstance = nullptr;
    {
        std::unique_lock<std::mutex> locker(g_lock);
        auto iter = gBioCacheMap.find(tenantId);
        if (UNLIKELY(iter == gBioCacheMap.end())) {
            return RET_CACHE_NOT_FOUND;
        }
        bioInstance = iter->second;
    }
    uint64_t outLen = 0;
    auto ret = bioInstance->Get(key, offset, length, location, value, outLen);
    *realLength = outLen;
    return ret;
}

CResult BioBatchGet(uint64_t tenantId, const char **keys, const uint32_t count, uint64_t *offsets, uint64_t *lengths,
                    ObjLocation *locations, uintptr_t *valueAddrs,
                    uint64_t *realLengths, int32_t *results)
{
    std::shared_ptr<Bio> bioInstance = nullptr;
    {
        std::unique_lock<std::mutex> locker(g_lock);
        auto iter = gBioCacheMap.find(tenantId);
        if (UNLIKELY(iter == gBioCacheMap.end())) {
            return RET_CACHE_NOT_FOUND;
        }
        bioInstance = iter->second;
    }
    return bioInstance->BatchGet(keys, count, offsets, lengths, locations, valueAddrs, realLengths, results);
}

CResult BioBatchGetFree(uint64_t tenantId, uintptr_t *valueAddrs, const uint32_t count)
{
    std::shared_ptr<Bio> bioInstance = nullptr;
    {
        std::unique_lock<std::mutex> locker(g_lock);
        auto iter = gBioCacheMap.find(tenantId);
        if (UNLIKELY(iter == gBioCacheMap.end())) {
            return RET_CACHE_NOT_FOUND;
        }
        bioInstance = iter->second;
    }
    uint64_t outLen = 0;
    bioInstance->BatchGetFree(valueAddrs, count);
    return RET_CACHE_OK;
}

CResult BioAsyncGet(uint64_t tenantId, const char *key, uint64_t offset, uint64_t length, ObjLocation location,
    char *value, BioGetCallbackFunc callback, void *context)
{
    std::shared_ptr<Bio> bioInstance = nullptr;
    {
        std::unique_lock<std::mutex> locker(g_lock);
        auto iter = gBioCacheMap.find(tenantId);
        if (UNLIKELY(iter == gBioCacheMap.end())) {
            return RET_CACHE_NOT_FOUND;
        }
        bioInstance = iter->second;
    }
    AsyncGetParam param = { key, offset, length, value };
    AsyncOpParam opParam = { callback, context };
    return bioInstance->AsyncGet(param, location, opParam);
}

CResult BioDelete(uint64_t tenantId, const char *key, ObjLocation location)
{
    std::shared_ptr<Bio> bioInstance = nullptr;
    {
        std::unique_lock<std::mutex> locker(g_lock);
        auto iter = gBioCacheMap.find(tenantId);
        if (UNLIKELY(iter == gBioCacheMap.end())) {
            return RET_CACHE_NOT_FOUND;
        }
        bioInstance = iter->second;
    }
    return bioInstance->Delete(key, location);
}

CResult BioLoad(uint64_t tenantId, const char *key, uint64_t offset, uint64_t length, ObjLocation location,
    BioLoadCallback callback, void *context)
{
    std::shared_ptr<Bio> bioInstance = nullptr;
    {
        std::unique_lock<std::mutex> locker(g_lock);
        auto iter = gBioCacheMap.find(tenantId);
        if (UNLIKELY(iter == gBioCacheMap.end())) {
            return RET_CACHE_NOT_FOUND;
        }
        bioInstance = iter->second;
    }

    LoadPara para = {key, offset, length};
    return bioInstance->Load(para, location, callback, context);
}

CResult BioListAll(uint64_t tenantId, const char *prefix, ObjStat **objs, uint64_t *objNum)
{
    if (objs == nullptr) {
        CLIENT_LOG_ERROR("Invalid input parameter, objs is nullptr.");
        return RET_CACHE_EPERM;
    }

    if (UNLIKELY(objNum == nullptr)) {
        return RET_CACHE_EPERM;
    }
    std::shared_ptr<Bio> bioInstance = nullptr;
    {
        std::unique_lock<std::mutex> locker(g_lock);
        auto iter = gBioCacheMap.find(tenantId);
        if (UNLIKELY(iter == gBioCacheMap.end())) {
            return RET_CACHE_NOT_FOUND;
        }
        bioInstance = iter->second;
    }
    std::unordered_map<std::string, ObjStat> listObjs;
    auto ret = bioInstance->ListAll(prefix, listObjs);
    if (UNLIKELY(ret != RET_CACHE_OK)) {
        return ret;
    }

    *objNum = listObjs.size();
    *objs = (ObjStat *)malloc(sizeof(ObjStat) * (*objNum));
    if (*objs == nullptr) {
        return RET_CACHE_NO_SPACE;
    }
    uint32_t index = 0;
    for (auto &obj : listObjs) {
        CopyKey((*objs)[index].key, obj.second.key, MAX_KEY_SIZE);
        (*objs)[index].size = obj.second.size;
        (*objs)[index].time = obj.second.time;
        index++;
    }
    return RET_CACHE_OK;
}

void BioFreeListResources(ObjStat **objs, uint64_t objNum)
{
    if (objNum == 0 || objs == nullptr || *objs == nullptr) {
        return;
    }
    free(*objs);
    *objs = nullptr;
}

CResult BioStat(uint64_t tenantId, const char *key, ObjLocation location, ObjStat *stat)
{
    if (UNLIKELY(key == nullptr || stat == nullptr)) {
        return RET_CACHE_ERROR;
    }
    std::shared_ptr<Bio> bioInstance = nullptr;
    {
        std::unique_lock<std::mutex> locker(g_lock);
        auto iter = gBioCacheMap.find(tenantId);
        if (UNLIKELY(iter == gBioCacheMap.end())) {
            return RET_CACHE_NOT_FOUND;
        }
        bioInstance = iter->second;
    }

    ObjStat statInfo;
    auto ret = bioInstance->Stat(key, location, statInfo);
    if (LIKELY(ret == RET_CACHE_OK)) {
        *stat = statInfo;
    }
    return ret;
}

CResult BioBatchExist(uint64_t tenantId, const char *key[], ObjLocation location[], uint32_t count, bool result[])
{
    std::shared_ptr<Bio> bioInstance = nullptr;
    {
        std::unique_lock<std::mutex> locker(g_lock);
        auto iter = gBioCacheMap.find(tenantId);
        if (UNLIKELY(iter == gBioCacheMap.end())) {
            return RET_CACHE_NOT_FOUND;
        }
        bioInstance = iter->second;
    }

    return bioInstance->BatchExist(key, location, count, result);
}

CResult BioNotifyUpgradePrepare(uint64_t tenantId)
{
    std::shared_ptr<Bio> bioInstance = nullptr;
    {
        std::unique_lock<std::mutex> locker(g_lock);
        auto iter = gBioCacheMap.find(tenantId);
        if (UNLIKELY(iter == gBioCacheMap.end())) {
            return RET_CACHE_NOT_FOUND;
        }
        bioInstance = iter->second;
    }
    return bioInstance->NotifyUpdatePrepare();
}

CResult BioNotifyUpgradeFinish(uint64_t tenantId)
{
    std::shared_ptr<Bio> bioInstance = nullptr;
    {
        std::unique_lock<std::mutex> locker(g_lock);
        auto iter = gBioCacheMap.find(tenantId);
        if (UNLIKELY(iter == gBioCacheMap.end())) {
            return RET_CACHE_NOT_FOUND;
        }
        bioInstance = iter->second;
    }
    return bioInstance->NotifyUpdateFinish();
}

CResult BioCheckUpgradeReady(uint64_t tenantId)
{
    std::shared_ptr<Bio> bioInstance = nullptr;
    {
        std::unique_lock<std::mutex> locker(g_lock);
        auto iter = gBioCacheMap.find(tenantId);
        if (UNLIKELY(iter == gBioCacheMap.end())) {
            return RET_CACHE_NOT_FOUND;
        }
        bioInstance = iter->second;
    }
    return bioInstance->CheckUpdateReady();
}

ReadHook g_readHook = nullptr;
WriteHook g_writeHook = nullptr;
WriteCopyFreeHook g_writeCopyFreeHook = nullptr;

int BioReadHook(uint64_t inode, char *buff, uint64_t count, uint64_t offset, int *readLen)
{
    if (g_readHook == nullptr) {
        CLIENT_LOG_ERROR("g_readHook is nullptr.");
        return RET_CACHE_ERROR;
    }
    return g_readHook(inode, buff, count, offset, readLen);
}

int BioWriteHook(uint64_t inode, char *buff, uint64_t count, uint64_t offset, uint64_t fh)
{
    if (g_writeHook == nullptr) {
        CLIENT_LOG_ERROR("g_writeHook is nullptr.");
        return RET_CACHE_ERROR;
    }
    return g_writeHook(inode, buff, count, offset, fh);
}

int BioWriteCopyFreeHook(uint64_t inode, uint64_t offset, uint64_t count, CacheSpaceDesc *space)
{
    if (g_writeCopyFreeHook == nullptr) {
        CLIENT_LOG_ERROR("g_writeCopyFreeHook is nullptr.");
        return RET_CACHE_ERROR;
    }
    return g_writeCopyFreeHook(inode, offset, count, space);
}

void BioRegisterInterceptorRead(ReadHook rh)
{
    g_readHook = rh;
}

void BioRegisterInterceptorWrite(WriteHook wh)
{
    g_writeHook = wh;
}

void BioRegisterInterceptorWriteCopyFree(WriteCopyFreeHook wh)
{
    g_writeCopyFreeHook = wh;
}

CResult BioConvertLocation(ObjLocation location, ObjLocationDetail *detailLoc)
{
    if (UNLIKELY(detailLoc == nullptr)) {
        CLIENT_LOG_ERROR("Invalid input parameter, detailLoc is nullptr.");
        return RET_CACHE_EPERM;
    }

    if (UNLIKELY(!gClient->Ready())) {
        return RET_CACHE_NOT_READY;
    }

    auto ptId = static_cast<uint16_t>(location.location[0]);
    ock::bio::CmPtInfo info;
    BResult ret = gClient->GetMirror()->GetPtEntry(ptId, info);
    if (ret != BIO_OK) {
        CLIENT_LOG_ERROR("Failed to get pt entry, ptId:" << ptId << ".");
        return RET_CACHE_EPERM;
    }

    FileLocationQueryRsp rsp;
    uint16_t slaveId = 0;
    uint32_t idx = 0;
    for (; idx < info.copys.size(); ++idx) {
        if (info.copys[idx].nodeId != info.masterNodeId) {
            slaveId = info.copys[idx].nodeId;
            break;
        }
    }

    if (idx == info.copys.size()) {
        CLIENT_LOG_ERROR("Failed to get slave node, ptId:" << ptId << ".");
        return RET_CACHE_ERROR;
    }

    ret = gClient->GetMirror()->GetFileLocation(info.masterNodeId, slaveId, rsp);
    if (ret != BIO_OK) {
        CLIENT_LOG_ERROR("Failed get file location, ret:" << ret << ", slaveId:"<< slaveId << ".");
        return RET_CACHE_ERROR;
    }

    memcpy_s(detailLoc->hostMaster, NODE_DESC_SIZE, rsp.hostMaster, NODE_DESC_SIZE);
    detailLoc->hostMaster[NODE_DESC_SIZE - 1] = '\0';
    memcpy_s(detailLoc->hostSlave, NODE_DESC_SIZE, rsp.hostSlave, NODE_DESC_SIZE);
    detailLoc->hostSlave[NODE_DESC_SIZE - 1] = '\0';

    detailLoc->portMaster = rsp.portMaster;
    detailLoc->portSlave = rsp.portSlave;
    return RET_CACHE_OK;
}

CResult BioAddDisk(const char *diskPath)
{
    if (UNLIKELY(diskPath == nullptr || strlen(diskPath) >= FILE_PATH_MAX_LEN)) {
        CLIENT_LOG_ERROR("Invalid input parameter, diskPath: " << diskPath << ".");
        return RET_CACHE_EPERM;
    }

    if (UNLIKELY(!gClient->Ready())) {
        return RET_CACHE_NOT_READY;
    }

    auto ret = gClient->GetMirror()->AddDisk(diskPath);
    if (ret != BIO_OK) {
        CLIENT_LOG_ERROR("Failed to add disk, ret:" << ret << ", diskPath:"<< diskPath << ".");
        return ToCResult(ret);
    }

    CLIENT_LOG_INFO("Add disk sucess! disk path: " << diskPath << ".");
    return RET_CACHE_OK;
}
