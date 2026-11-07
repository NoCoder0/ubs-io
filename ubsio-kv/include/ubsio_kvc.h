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

#ifndef UBSIO_KVC_H
#define UBSIO_KVC_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define UBSIO_RESOURCE_DISK_PATH_MAX_SIZE (256)
#define UBSIO_RESOURCE_MAX_DISK_NUM (16)

typedef enum {
    UBSIO_KVC_MODE_STANDALONE_C = 0,
    UBSIO_KVC_MODE_REMOTE_DRAM_TCP_C = 1,
} UbsioKvcModeC;

/** Prepare startup configuration before callback registration or initialization.
 * No network/cache is started. NULL/empty path uses UBSIO_CONFIG_PATH or the default.
 * Repeating the same absolute path returns the immutable snapshot; a different path fails.
 * mode is written only on success. This process supports one startup configuration.
 */
int32_t UbsioKvCachePrepareConfig(const char *path, uint32_t *mode);

typedef struct {
    uint16_t status;
    char path[UBSIO_RESOURCE_DISK_PATH_MAX_SIZE];
    uint64_t readBandwidth;
    uint64_t writeBandwidth;
    uint64_t totalBandwidth;
    uint8_t bandwidthValid;
    uint8_t reserved[7];
} UbsioDiskInfo;

typedef struct {
    uint64_t diskCap;
    uint64_t diskUsed;
    uint64_t memCap;
    uint64_t memUsed;
    uint32_t diskNum;
    uint32_t faultDiskNum;
    UbsioDiskInfo disks[UBSIO_RESOURCE_MAX_DISK_NUM];
} UbsioResourceInfo;

#ifndef UBSIO_META_EVENT_C_DEFINED
#define UBSIO_META_EVENT_C_DEFINED
typedef enum {
    UBSIO_META_RECOVER_C = 0,
    UBSIO_META_DELETE_C = 1,
} UbsioMetaEventTypeC;

typedef struct {
    int32_t type;
    const char *key;
    uint32_t keyLen;
} UbsioMetaEventC;

typedef void (*UbsioMetaEventCallbackC)(void *context, const UbsioMetaEventC *events, uint32_t count);
#endif

/** Remote DRAM only. limit=0 initializes journal cursors once, before accepting writes. Positive limit (<=64)
 * polls one batch per remote node and synchronously delivers logical DELETE keys through callback.
 * One consumer per SDK process; the caller serializes calls and drains them before Exit.
 * Callback must not re-enter polling/Exit. Borrowed event/key pointers are valid
 * only inside callback. A nonzero result may accompany successfully delivered batches from other
 * nodes. Journal GAP/restart is sticky; reconcile externally before Exit/reinitialization.
 * No automatic scan or MetaService acknowledgement is provided by this API.
 */
int32_t UbsioKvCachePollMetaEvents(UbsioMetaEventCallbackC callback, void *context, uint32_t limit);

/**
 * @brief Initialize UBS-IO KV Cache
 *
 * @param devId            [in] device id, -1 means no ACL device binding and uses standalone device 0
 * @return 0 if successful
 */
int32_t UbsioKvCacheInit(int32_t devId);

/**
 * @brief Get write cache and disk resource information managed by the local process.
 *
 * Configured block-device bandwidth is sampled for 200 ms from Linux block statistics and is reported in bytes
 * per second. When a partition is configured, only that partition's bandwidth is reported.
 * bandwidthValid distinguishes a valid zero value from a collection failure.
 * Disk status is 0 for normal and 1 for fault.
 *
 * @param info             [out] Write cache capacity/usage and per-disk information
 * @return 0 if successful
 */
int32_t UbsioGetResourceInfo(UbsioResourceInfo *info);

/**
 * @brief Register metadata events for MemCache Master metadata synchronization.
 *
 * The events buffer and key pointers are only valid during the callback; the callee must copy keys if needed.
 * Passing nullptr unregisters the callback.
 */
int32_t UbsioKvCacheRegisterMetaEventCallback(UbsioMetaEventCallbackC callback, void *context);

 /** 
  * @brief Exit UBS-IO KV Cache 
  */ 
 void UbsioKvCacheExit(void);

/**
 * @brief Put data of object with key into UBS-IO KV Cache
 * This data operation default supports async
 *
 * @param key              [in] key of data, less than 256
 * @param buf              [in] data to be put
 * @param length           [in] data size
 * @param flags            [in] optional flags, reserved
 * @return 0 if successful
 */
int32_t UbsioKvCachePut(const char *key, void *buf, size_t length, uint32_t flags);

 /** 
  * @brief Put multiple data objects into UBS-IO KV Cache 
  * This data operation default supports async 
  * 
  * @param keys           [in] Array of keys for the data objects 
  * @param keysCount      [in] Number of keys in the array 
  * @param bufs           [in] Array of data buffers to be put 
  * @param length         [in] Size of data buf in data buffers 
  * @param results        [out] result of data to be put in data buffers 
  * @param flags          [in] Optional flags, reserved 
  * @return 0 if successful 
  */ 
 int32_t UbsioKvCacheBatchPut(const char **keys, 
                              uint32_t keysCount, 
                              void **bufs, 
                              size_t *lengths, 
                              int *results, 
                              uint32_t flags);


 /** 
  * @brief Get data of object by key from UBS-IO KV Cache 
  * This data operation default supports async 
  * 
  * @param key              [in] key of data, less than 256 
  * @param buf              [in] data to be gotten 
  * @param length           [in] data size 
  * @param options          [in] options for get policy 
  * @return 0 if successful 
  */ 
 int32_t UbsioKvCacheGet(const char *key, void *buf, size_t length, uint32_t flags);

/**
 * @brief Get multiple data objects by keys from UBS-IO KV Cache
 * This data operation default supports async
 *
 * @param keys           [in] Array of keys for the data objects
 * @param keysCount      [in] Number of keys in the array
 * @param bufs           [out] Array of data buffers to be get
 * @param length         [in] Size of data buf in data buffers
 * @param results        [out] result of data to be get in data buffers
 * @param flags          [in] Optional flags, reserved
 * @return 0 if successful
 */
int32_t UbsioKvCacheBatchGet(const char **keys,
                             uint32_t keysCount,
                             void **bufs,
                             size_t *lengths,
                             int *results,
                             uint32_t flags);

/**
 * @brief Get multiple data objects by keys from UBS-IO KV Cache
 * This data operation default supports async
 *
 * @param keys           [in] Array of keys for the data objects
 * @param keysCount      [in] Number of keys in the array
 * @param bufs           [out] Array of npu data buffers to be get
    bufs：{
            {key1_npu_buflayer_0, key1_npu_buflayer_1, ..., {key1_npu_buflayer_N}},
            {key2_npu_buflayer_0, key2_npu_buflayer_1},..., {key2_npu_buflayer_N}},
            ...,
            {keyM_npu_buflayer_0, keyM_npu_buflayer_1},..., {keyM_npu_buflayer_N}}
          }
 * @param length         [in] Size of data buf in data buffers
 * @param lengthsRows    [in] Number of rows in the lengths matrix
 * @param lengthsCols    [in] Number of columns in the lengths matrix
 * @param results        [out] result of data to be get in data buffers
 * @param flags          [in] Optional flags, reserved
 * @return 0 if successful
 */
int32_t UbsioKvCacheBatchGetDirect(const char **keys,
                                   uint32_t keysCount,
                                   void ***bufs,
                                   size_t **lengths,
                                   uint32_t lengthsRows,
                                   uint32_t lengthsCols,
                                   int *results,
                                   uint32_t flags);

 /** 
  * @brief Determine whether the key is within the UBS-IO KV Cache 
  * 
  * @param key              [in] key of data, less than 256 
  * @param flags            [in] optional flags, reserved 
  * @return 0 if successfully 
  */ 
 bool UbsioKvCacheExist(const char *key, uint32_t flags);

/**
 * @brief Determine whether the list of keys is within the UBS-IO KV Cache
 *
 * @param keys             [in] keys of data, the length of key is less than 256
 * @param keysCount        [in] Count of keys
 * @param results          [out] existence status list of keys in UBS-IO KV Cache
 * @return 0 if successfully
 */
int32_t UbsioKvCacheBatchExist(const char **keys, uint32_t keysCount, bool *results, uint32_t flags);

 /** 
  * @brief Delete the object with key from UBS-IO KV Cache 
  * 
  * @param key              [in] key of data, less than 256 
  * @param flags            [in] optional flags, reserved 
  * @return  0 if successful 
  */ 
 int32_t UbsioKvCacheDelete(const char *key, uint32_t flags);

 /** 
  * @brief Delete multiple keys from the UBS-IO KV Cache 
  * 
  * @param keys             [in] List of keys to be deteled from the UBS-IO KV Cache 
  * @param keysCount        [in] Count of keys 
  * @param results          [out] Results of each delete operation 
  * @param flags            [in] Flags for the operation 
  * @return 0 if successfully, positive value if error happens 
  */ 
 int32_t UbsioKvCacheBatchDelete(const char **keys, uint32_t keysCount, int32_t *results, uint32_t flags);

 /** 
  * @brief Get the length of object 
  * 
  * @param key              [in]  key of data, less than 256 
  * @param length           [out] size of data 
  * @param flags            [in]  optional flags, reserved 
  * @return  0 if successful 
  */ 
 int32_t UbsioKvCacheGetLength(const char *key, size_t *length, uint32_t flags);

 /** 
  * @brief Get multiple keys length from the UBS-IO KV Cache 
  * 
  * @param keys             [in] List of keys to be deteled from the UBS-IO KV Cache 
  * @param keysCount        [in] Count of keys 
  * @param lengths          [out] length of each get key 
  * @param results          [out] Results of each get length operation 
  * @param flags            [in] Flags for the operation 
  * @return 0 if successfully, positive value if error happens 
  */ 
 int32_t UbsioKvCacheBatchGetLength(const char **keys, uint32_t keysCount, size_t *lengths, int32_t *results, uint32_t flags);

/**
 * @brief Free shm address within the UBS-IO KV Cache
 *
 * @param bufs             [in] Array of data buffers to be free
 * @param keys_count       [in] Count of keys
 * @return 0 if successfully
 */
int32_t UbsioKvCacheBatchFree(void **bufs, uint32_t keysCount);

#ifdef __cplusplus
}
#endif
#endif // UBSIO_KVC_H
