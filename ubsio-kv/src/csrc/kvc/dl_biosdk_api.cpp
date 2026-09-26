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
bool gRemoteMode = false;
uint64_t gTenantId = 1;

bool CopyTlsPath(const char *envName, char *destination, size_t capacity)
{
    const char *value = std::getenv(envName);
    if (value == nullptr || *value == '\0') {
        return false;
    }
    const size_t length = std::strlen(value);
    if (length >= capacity) {
        LOG_ERROR("Remote TLS path is too long for " << envName);
        return false;
    }
    std::memcpy(destination, value, length + 1);
    return true;
}

bool ConfigureRemoteTls(ClientOptionsConfig &options)
{
    const char *ca = std::getenv("UBSIO_KVC_TLS_CA_CERT");
    const char *cert = std::getenv("UBSIO_KVC_TLS_CERT");
    const char *key = std::getenv("UBSIO_KVC_TLS_KEY");
    const bool hasCa = ca != nullptr && *ca != '\0';
    const bool hasCert = cert != nullptr && *cert != '\0';
    const bool hasKey = key != nullptr && *key != '\0';
    if (!hasCa && !hasCert && !hasKey) {
        return true;
    }
    if (!hasCa || !hasCert || !hasKey ||
        !CopyTlsPath("UBSIO_KVC_TLS_CA_CERT", options.caCerPath, sizeof(options.caCerPath)) ||
        !CopyTlsPath("UBSIO_KVC_TLS_CERT", options.certificationPath, sizeof(options.certificationPath)) ||
        !CopyTlsPath("UBSIO_KVC_TLS_KEY", options.privateKeyPath, sizeof(options.privateKeyPath))) {
        LOG_ERROR("Remote TLS requires UBSIO_KVC_TLS_CA_CERT, UBSIO_KVC_TLS_CERT, and UBSIO_KVC_TLS_KEY");
        return false;
    }
    options.enable = 1;
    return true;
}
}

BioExitFunc DlBioSdkApi::pBioExit = nullptr;
BioInitFunc DlBioSdkApi::pBioInitialize = nullptr;
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

int32_t DlBioSdkApi::KvBioInit()
{
    LOG_INFO("Start boostio begin...");
    const char *mode = std::getenv("UBSIO_KVC_MODE");
    gRemoteMode = mode != nullptr && std::strcmp(mode, "remote_dram_tcp") == 0;
    if (mode != nullptr && !gRemoteMode && std::strcmp(mode, "standalone") != 0) {
        LOG_ERROR("Invalid UBSIO_KVC_MODE: " << mode);
        return UBSIO_KVC_INVALID_PARAM;
    }
    gTenantId = 1;
    if (gRemoteMode) {
        const char *tenant = std::getenv("UBSIO_KVC_TENANT_ID");
        if (tenant == nullptr || *tenant == '\0' || *tenant == '-') {
            LOG_ERROR("Remote mode requires UBSIO_KVC_TENANT_ID greater than one");
            return UBSIO_KVC_INVALID_PARAM;
        }
        errno = 0;
        char *end = nullptr;
        unsigned long long parsed = std::strtoull(tenant, &end, 10);
        if (errno != 0 || *end != '\0' || parsed <= 1) {
            LOG_ERROR("Invalid UBSIO_KVC_TENANT_ID: " << tenant);
            return UBSIO_KVC_INVALID_PARAM;
        }
        gTenantId = parsed;
    }
    ClientOptionsConfig optConf{};
    optConf.logType = (LogType)(1);
    optConf.enable = false;
    if (!ConfigureRemoteTls(optConf)) {
        return UBSIO_KVC_INVALID_PARAM;
    }
    std::string logDir = "/var/log/ubsio";
    std::snprintf(optConf.logFilePath, sizeof(optConf.logFilePath), "%s", logDir.c_str());

    auto ret = Initialize(gRemoteMode ? WorkerMode::SEPARATES : WorkerMode::STANDALONE, &optConf);
    if (ret != 0) {
        LOG_ERROR("boostio initialize failed, ret: " << ret);
        return -1;
    }
    LOG_INFO("Start boostio success.");

    LOG_INFO("boostio createcache...");
    uint64_t tenantId = gTenantId;
    AffinityStrategy affinity = gRemoteMode ? GLOBAL_BALANCE : LOCAL_AFFINITY;
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
    return gRemoteMode;
}

uint64_t DlBioSdkApi::TenantId()
{
    return gTenantId;
}

}  // namespace ubsio
}  // namespace ock
