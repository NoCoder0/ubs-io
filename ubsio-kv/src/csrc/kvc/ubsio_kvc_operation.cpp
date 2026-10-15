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

#include <vector>
#include "ubsio_kvc_err.h"
#include "ubsio_kvc_log.h"
#include "dl_biosdk_api.h"
#include "dl_acl_api.h"
#include "ubsio_kvc_stream_manager.h"
#include "ubsio_kvc_instance.h"
#include "kv_operation.h"
#include "ubsio_kvc_operation.h"
#include "remote_dram_object.h"

namespace ock {
namespace ubsio {

static uint64_t tenantId = 1;
static KvOperation *g_kvOperation = KvOperation::Instance();

int32_t KvcOperationInit(int32_t devId)
{
    if (UNLIKELY(devId < -1)) {
        LOG_ERROR("Invalid device id: " << devId);
        return UBSIO_KVC_INVALID_PARAM;
    }
    // 1. init acl stream
    if (devId >= 0) {
        if (ACLApi::LoadLibrary() != UBSIO_KVC_OK) {
            return UBSIO_KVC_ERR;
        }
        if (KvcStreamManager::InitAclStream(devId) != UBSIO_KVC_OK) {
            return UBSIO_KVC_ERR;
        }
    }
    
    // 2. dlopen and init boostio
    int ret = DlBioSdkApi::LoadLibrary();
    if (UNLIKELY(ret != UBSIO_KVC_OK)) {
        LOG_ERROR("dlopen boostio library failed, ret:" << ret);
        return UBSIO_KVC_ERR;
    }
    ret = DlBioSdkApi::KvBioInit();
    if (UNLIKELY(ret != UBSIO_KVC_OK)) {
        LOG_ERROR("init boostio failed, ret:" << ret);
        return UBSIO_KVC_ERR;
    }

    // 3. init kv operation executor
    ret = g_kvOperation->InitKvExecutor();
    if (ret != UBSIO_KVC_OK) {
        LOG_ERROR("kv init executor failed");
        return UBSIO_KVC_ERR;
    }

    LOG_INFO("Init ubsio kv cache success, deviceId:" << devId);
    return UBSIO_KVC_OK;
}

void KvcExit(void)
{
    if (g_kvOperation != nullptr) {
        g_kvOperation->ExitKvExecutor();
    }
    remote_dram::ResetDirectory();
    KvcInstance::Instance().UnInitialize();
    KvcStreamManager::DestroyAclStream();
    DlBioSdkApi::Exit();
    DlBioSdkApi::CleanupLibrary();
}

int32_t KvcPutData(const std::string &key, void *value, size_t len, uint32_t flags)
{
    return g_kvOperation->KvPutData(key, value, len);
}

int32_t KvcGetData(const std::string &key, void *value, size_t len, uint32_t flags)
{
    return g_kvOperation->KvGetData(key, value, len);
}

int32_t KvcDeleteKey(const std::string &key, uint32_t flags)
{
    return g_kvOperation->KvDeleteKey(key);
}

bool KvcExistKey(const std::string &key, uint32_t flags)
{
    return g_kvOperation->KvExistKey(key);
}

int32_t KvcGetKeyLength(const std::string &key, uint32_t &length, uint32_t flags)
{
    return g_kvOperation->KvGetLengthKey(key, length);
}

int32_t KvcBatchPutData(const std::vector<std::string> &key,
                        std::vector<void *> &value,
                        std::vector<size_t> &lengths,
                        std::vector<int> &results,
                        uint32_t flags)
{
    return g_kvOperation->BatchKvPutData(key, value, lengths, results);
}

int32_t KvcBatchExistKey(const std::vector<std::string> &key, bool *results, uint32_t flags)
{
    return g_kvOperation->BatchKvExistKey(key, results);
}

int32_t KvcBatchGetData(const std::vector<std::string> &key,
                        void **bufs,
                        std::vector<size_t> &lengths,
                        std::vector<int> &results,
                        uint32_t flags)
{
    return g_kvOperation->BatchKvGetData(key, bufs, lengths, results);
}

int32_t KvcBatchDeleteKey(const std::vector<std::string> &key, std::vector<int> &results, uint32_t flags)
{
    return g_kvOperation->BatchKvDeleteKey(key, results);
}

int32_t KvcBatchGetLengthKey(const std::vector<std::string> &key,
                             std::vector<uint32_t> &lengths,
                             std::vector<int> &results,
                             uint32_t flags)
{
    return g_kvOperation->BatchGetLengthKey(key, lengths, results);
}

int32_t KvcBatchFreeGetAddress(void **bufs, uint32_t keys_count)
{
    if (DlBioSdkApi::RemoteMode()) return UBSIO_KVC_OK; // Remote BatchGet never replaces caller buffers.
    return DlBioSdkApi::BatchGetFree(DlBioSdkApi::TenantId(), reinterpret_cast<uintptr_t*>(bufs), keys_count);
}

} // namespace ubsio
} // namespace ock
