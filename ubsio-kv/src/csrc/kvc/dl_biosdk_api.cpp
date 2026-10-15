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

#include <stdint.h>
#include <cstdio>
#include <string>
#include <dlfcn.h>
#include <iostream>
#include <cstring>
#include <cstdlib>
#include <cerrno>
#include "ubsio_kvc_log.h"
#include "ubsio_kvc_def.h"
#include "dl_biosdk_api.h"

namespace ock {
namespace ubsio {

bool DlBioSdkApi::gLoaded = false;
std::mutex DlBioSdkApi::gMutex;
void *DlBioSdkApi::bioSdkHandle = nullptr;
const std::string DlBioSdkApi::gBioSdkLibName = "libbio_sdk.so";
namespace {
constexpr uint32_t DEFAULT_READ_WORKERS = 2;
constexpr uint32_t D_READ_WORKERS = 4;
constexpr uint64_t STANDALONE_TENANT_ID = 1;
bool gPrepared = false;
BioKvcConfigV1 gConfig{BIO_KVC_CONFIG_VERSION_1, BIO_KVC_MODE_STANDALONE, STANDALONE_TENANT_ID, DEFAULT_READ_WORKERS, 0};
ClientOptionsConfig gTlsOptions{};
}

BioExitFunc DlBioSdkApi::pBioExit = nullptr;
BioInitFunc DlBioSdkApi::pBioInitialize = nullptr;
BioLoadKvcConfigFunc DlBioSdkApi::pBioLoadKvcConfig = nullptr;
BioCreateCacheFunc DlBioSdkApi::pBioCreateCache = nullptr;
BioCalLocationFunc DlBioSdkApi::pBioCalcLocation = nullptr;
BioGetFunc DlBioSdkApi::pBioGet = nullptr;
BioPutFunc DlBioSdkApi::pBioPut = nullptr;
BioStatFunc DlBioSdkApi::pBioStat = nullptr;
BioBatchGetFunc DlBioSdkApi::pBioBatchGet = nullptr;
BioBatchExistFunc DlBioSdkApi::pBioBatchExist = nullptr;
BioBatchFreeFunc DlBioSdkApi::pBioBatchGetFree = nullptr;
BioDeleteFunc DlBioSdkApi::pBioDelete = nullptr;
BioBatchGetKeyDiskAddrFunc DlBioSdkApi::pBioBatchGetKeyDiskAddr = nullptr;
BioRegisterMetaEventCallbackFunc DlBioSdkApi::pBioRegisterMetaEventCallback = nullptr;
BioShowLocalCacheResourceFunc DlBioSdkApi::pBioShowLocalCacheResource = nullptr;

int32_t DlBioSdkApi::LoadLibrary()
{
    std::lock_guard<std::mutex> guard(gMutex);
    if (gLoaded) {
        return 0;
    }

    /* dlopen library */
    bioSdkHandle = dlopen(gBioSdkLibName.c_str(), RTLD_LAZY | RTLD_GLOBAL);
    if (bioSdkHandle == nullptr) {
        LOG_ERROR("Failed to open library [" << gBioSdkLibName << "], error: " << dlerror());
        return -1;
    }

    /* load sym */
    DL_LOAD_SYM(pBioExit, BioExitFunc, bioSdkHandle, "BioExit");
    DL_LOAD_SYM(pBioInitialize, BioInitFunc, bioSdkHandle, "BioInitialize");
    DL_LOAD_SYM(pBioGet, BioGetFunc, bioSdkHandle, "BioGet");
    DL_LOAD_SYM(pBioPut, BioPutFunc, bioSdkHandle, "BioPut");
    DL_LOAD_SYM(pBioStat, BioStatFunc, bioSdkHandle, "BioStat");
    DL_LOAD_SYM(pBioCreateCache, BioCreateCacheFunc, bioSdkHandle, "BioCreateCache");
    DL_LOAD_SYM(pBioCalcLocation, BioCalLocationFunc, bioSdkHandle, "BioCalcLocation");
    DL_LOAD_SYM(pBioBatchGet, BioBatchGetFunc, bioSdkHandle, "BioBatchGet");
    DL_LOAD_SYM(pBioBatchExist, BioBatchExistFunc, bioSdkHandle, "BioBatchExist");
    DL_LOAD_SYM(pBioBatchGetFree, BioBatchFreeFunc, bioSdkHandle, "BioBatchGetFree");
    DL_LOAD_SYM(pBioDelete, BioDeleteFunc, bioSdkHandle, "BioDelete");
    DL_LOAD_SYM(pBioBatchGetKeyDiskAddr, BioBatchGetKeyDiskAddrFunc, bioSdkHandle, "BioBatchGetKeyDiskAddr");
    DL_LOAD_SYM(pBioRegisterMetaEventCallback, BioRegisterMetaEventCallbackFunc, bioSdkHandle,
        "BioRegisterMetaEventCallback");
    DL_LOAD_SYM(pBioShowLocalCacheResource, BioShowLocalCacheResourceFunc, bioSdkHandle,
        "BioShowLocalCacheResource");

    DL_LOAD_SYM(pBioLoadKvcConfig, BioLoadKvcConfigFunc, bioSdkHandle, "BioLoadKvcConfig");

    gLoaded = true;
    return 0;
}

void DlBioSdkApi::CleanupLibrary()
{
    std::lock_guard<std::mutex> guard(gMutex);
    if (!gLoaded) {
        return;
    }

    pBioExit = nullptr;
    pBioInitialize = nullptr;
    pBioLoadKvcConfig = nullptr;
    gPrepared = false;
    pBioGet = nullptr;
    pBioPut = nullptr;
    pBioStat = nullptr;
    pBioCreateCache = nullptr;
    pBioCalcLocation = nullptr;
    pBioBatchGet = nullptr;
    pBioBatchExist = nullptr;
    pBioBatchGetFree = nullptr;
    pBioDelete = nullptr;
    pBioBatchGetKeyDiskAddr = nullptr;
    pBioRegisterMetaEventCallback = nullptr;
    pBioShowLocalCacheResource = nullptr;

    if (bioSdkHandle != nullptr) {
        dlclose(bioSdkHandle);
        bioSdkHandle = nullptr;
    }
    gLoaded = false;
}

int32_t DlBioSdkApi::PrepareConfig(const char *path, uint32_t *mode)
{
    if (mode == nullptr) return UBSIO_KVC_INVALID_PARAM;
    if (LoadLibrary() != UBSIO_KVC_OK) return UBSIO_KVC_ERR;
    std::lock_guard<std::mutex> guard(gMutex);
    BioKvcConfigV1 config{};
    config.version = BIO_KVC_CONFIG_VERSION_1;
    ClientOptionsConfig options{};
    if (pBioLoadKvcConfig(path, sizeof(config), &config, &options) != RET_CACHE_OK ||
        config.version != BIO_KVC_CONFIG_VERSION_1 ||
        (config.mode != BIO_KVC_MODE_STANDALONE && config.mode != BIO_KVC_MODE_REMOTE_DRAM_TCP) ||
        (config.readWorkers != DEFAULT_READ_WORKERS && config.readWorkers != D_READ_WORKERS) ||
        config.manifestOneGet > 1 ||
        (config.mode == BIO_KVC_MODE_STANDALONE ? config.tenantId != STANDALONE_TENANT_ID :
            config.tenantId <= STANDALONE_TENANT_ID)) {
        LOG_ERROR("SDK startup configuration preparation failed or incompatible ABI values");
        return UBSIO_KVC_INVALID_PARAM;
    }
    if (!gPrepared) {
        gConfig = config;
        gTlsOptions = options;
        gPrepared = true;
    }
    *mode = gConfig.mode;
    return UBSIO_KVC_OK;
}

int32_t DlBioSdkApi::KvBioInit()
{
    uint32_t mode = BIO_KVC_MODE_STANDALONE;
    if (PrepareConfig(nullptr, &mode) != UBSIO_KVC_OK) return UBSIO_KVC_INVALID_PARAM;
    LOG_INFO("Start boostio begin...");
    ClientOptionsConfig optConf = gTlsOptions;
    optConf.logType = FILE_TYPE;
    std::string logDir = "/var/log/ubsio";
    std::snprintf(optConf.logFilePath, sizeof(optConf.logFilePath), "%s", logDir.c_str());

    auto ret = Initialize(RemoteMode() ? WorkerMode::SEPARATES : WorkerMode::STANDALONE, &optConf);
    if (ret != 0) {
        LOG_ERROR("boostio initialize failed, ret: " << ret);
        return -1;
    }
    LOG_INFO("Start boostio success.");

    LOG_INFO("boostio createcache...");
    uint64_t tenantId = gConfig.tenantId;
    AffinityStrategy affinity = RemoteMode() ? GLOBAL_BALANCE : LOCAL_AFFINITY;
    WriteStrategy strategy = WRITE_BACK;
    ret = CreateCache({ tenantId, affinity, strategy });
    if (ret == RET_CACHE_EXISTS) {
        LOG_INFO("boostio cache already exist");
        return 0;
    }
    if (ret != 0) {
        LOG_ERROR("boostio createcache failed, ret: " << ret);
        return -1;
    }
    LOG_INFO("boostio createcache success.");

    return 0;
}

bool DlBioSdkApi::RemoteMode()
{
    return gConfig.mode == BIO_KVC_MODE_REMOTE_DRAM_TCP;
}

uint64_t DlBioSdkApi::TenantId()
{
    return gConfig.tenantId;
}

uint32_t DlBioSdkApi::ReadWorkers()
{
    return gConfig.readWorkers;
}

bool DlBioSdkApi::ManifestOneGet()
{
    return gConfig.manifestOneGet != 0;
}

}  // namespace ubsio
}  // namespace ock
