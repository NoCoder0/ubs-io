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

#ifndef BIO_C_H
#define BIO_C_H

#include <stdint.h>
#include <time.h>
#include <limits.h>

#ifdef __cplusplus
extern "C" {
#endif
typedef enum {
    RET_CACHE_OK = 0,            // successful
    RET_CACHE_PROTECTED = 1,     // cache write protected
    RET_CACHE_ERROR = 2,         // unknown error code
    RET_CACHE_EPERM = 3,         // input parameter is incorrect
    RET_CACHE_BUSY = 4,          // cache busy, need outer retry
    RET_CACHE_NEED_RETRY = 5,    // need retry
    RET_CACHE_NOT_READY = 6,     // retry is not required
    RET_CACHE_NOT_FOUND = 7,     // not found this key
    RET_CACHE_CONFLICT = 8,      // key conflict
    RET_CACHE_MISS = 9,          // cache miss
    RET_CACHE_NO_SPACE = 10,     // cache capacity not enough
    RET_CACHE_UNAVAILABLE = 11,  // cache service unavailable
    RET_CACHE_EXCEED_QUOTA = 12, // exceed cache quota limit
    RET_CACHE_PT_FAULT = 13,     // cache partition fault
    RET_CACHE_READ_EXCEED = 14,  // read limit is exceeded
    RET_CACHE_EXISTS = 15,       // cache already exists
    RET_CACHE_DISK_FAULT = 16,   // cache disk fault
    RET_CACHE_UFS_FAULT = 17,    // cache ufs fault
    RET_CACHE_BUTT
} CResult;

typedef enum {
    LOCAL_AFFINITY = 1, // data local affinity
    GLOBAL_BALANCE = 2, // data global balance
    AFFINITY_BUTT
} AffinityStrategy;

typedef enum {
    WRITE_BACK = 1,
    WRITE_THROUGH = 2,
    STRATEGY_BUTT
} WriteStrategy;

typedef enum {
    CONVERGENCE,
    SEPARATES,
    STANDALONE
} WorkerMode;

typedef enum {
    STDOUT_TYPE,
    FILE_TYPE,
    STDERR_TYPE
} LogType;

#define MAX_KEY_SIZE (256)
#define LOCATION_SIZE (2)
#define NODE_DESC_SIZE (16)
#define CACHE_SPACE_ADDRESS_SIZE (2)
#define CACHE_SPACE_DEC_SIZE (64)
#define MAX_TRACE_NAME_LEN (64)
#define TRACE_MAX_NUM (256)
#define DISK_PATH_MAX_SIZE (128)
#define DISK_RESOURCE_MAX_NUM (16)
#define DISK_RESOURCE_PATH_MAX_SIZE (256)
#define CHUNK_ADDR_MAX_SIZE (2)

typedef void (*BioLoadCallback)(void *context, int32_t result);
typedef void (*BioGetCallbackFunc)(void *context, int32_t result, uint32_t realLen);
typedef void (*BioAsyncPutCallback)(void *context, int32_t result);

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

typedef struct {
    char key[MAX_KEY_SIZE];
    uint32_t size;
    time_t time;
} ObjStat;

typedef struct {
    uint64_t location[LOCATION_SIZE];
} ObjLocation;

typedef struct {
    char hostMaster[NODE_DESC_SIZE];
    char hostSlave[NODE_DESC_SIZE];
    uint16_t portMaster;
    uint16_t portSlave;
} ObjLocationDetail;

typedef struct {
    uint64_t tenantId;
    AffinityStrategy affinity;
    WriteStrategy strategy;
} CacheDescriptor;

typedef struct {
    uint64_t address;
    uint32_t size;
} CacheAddress;

typedef struct {
    uint8_t allocLoc;
    uint16_t addressNum;
    uint16_t descriptorSize;
    ObjLocation loc;
    CacheAddress address[CACHE_SPACE_ADDRESS_SIZE];
    char descriptorInfo[CACHE_SPACE_DEC_SIZE];
} CacheSpaceDesc;

typedef struct {
    uint16_t diskId;
    uint16_t status;
    char path[DISK_RESOURCE_PATH_MAX_SIZE];
    uint64_t readBandwidth;
    uint64_t writeBandwidth;
    uint64_t totalBandwidth;
    uint8_t bandwidthValid;
    uint8_t reserved[7];
} DiskResourcesDesc;

typedef struct {
    uint16_t nodeId;
    uint64_t rCacheMemCapacity;
    uint64_t rCacheDiskCapacity;
    uint64_t wCacheMemCapacity;
    uint64_t wCacheDiskCapacity;
    uint64_t rCacheMemUsedSize;
    uint64_t rCacheDiskUsedSize;
    uint64_t wCacheMemUsedSize;
    uint64_t wCacheDiskUsedSize;
    uint16_t diskNum;
    DiskResourcesDesc disks[DISK_RESOURCE_MAX_NUM];
} CacheResourcesDesc;

typedef struct {
    uint16_t nodeId;
    uint64_t rCacheHitMemCount;
    uint64_t rCacheHitDiskCount;
    uint64_t rCacheHitCount;
    uint64_t rCacheTotalCount;
    uint64_t wCacheHitMemCount;
    uint64_t wCacheHitDiskCount;
    uint64_t wCacheHitCount;
    uint64_t wCacheTotalCount;
    uint64_t backendHitCount;
} CacheHitFinalDesc;

typedef struct {
    char path[DISK_PATH_MAX_SIZE];
    uint64_t offset[CHUNK_ADDR_MAX_SIZE];
    uint64_t length[CHUNK_ADDR_MAX_SIZE];
    int32_t result;
    uint8_t count;
} KeyAddrInfo;

typedef struct {
    LogType logType;                   // STDOUT_TYPE/FILE_TYPE/STDERR_TYPE
    char logFilePath[PATH_MAX];        // log file path, if log type use FILE_TYPE, need to set this param
    uint8_t enable;                    // switch
    char certificationPath[PATH_MAX];  // certification path
    char caCerPath[PATH_MAX];          // caCer path
    char caCrlPath[PATH_MAX];          // caCer path
    char privateKeyPath[PATH_MAX];     // private key path
    char privateKeyPassword[PATH_MAX]; // private key password
    char decrypterLibPath[PATH_MAX];   // decrypter lib path
    char opensslLibDir[PATH_MAX];      // openssl lib dir path
} ClientOptionsConfig;

#define BIO_KVC_CONFIG_VERSION_1 (1U)
#define BIO_KVC_MODE_STANDALONE (0U)
#define BIO_KVC_MODE_REMOTE_DRAM_TCP (1U)

typedef struct {
    uint32_t version;
    uint32_t mode;
    uint64_t tenantId;
    uint32_t readWorkers;
    uint32_t manifestOneGet;
} BioKvcConfigV1;

/** Prepare one immutable startup snapshot without starting networking or a cache.
 * out->version must be BIO_KVC_CONFIG_VERSION_1 and size must equal sizeof(*out).
 * path is absolute; NULL/empty selects UBSIO_CONFIG_PATH, then /etc/ubsio/ubsio.conf.
 * Repeating the prepared path returns the snapshot; a different path fails.
 * Only TLS fields in tlsOptions are updated; caller logging options are preserved.
 */
CResult BioLoadKvcConfig(const char *path, uint32_t size, BioKvcConfigV1 *out, ClientOptionsConfig *tlsOptions);

/**
 * @brief: Initialize boostio service
 *
 * @param[in]: mode: working mode
 * @param[in]: option: log options and security options
 * @return: return RETURN_CACHE_OK mean success, others, return non-zero value
 */
CResult BioInitialize(WorkerMode mode, ClientOptionsConfig *optConf);

/**
 * @brief Register a same-process UBS IO metadata event callback.
 *
 * The events buffer and key pointers are only valid during the callback; the callee must copy keys if needed.
 * Passing nullptr unregisters the callback.
 */
CResult BioRegisterMetaEventCallback(UbsioMetaEventCallbackC callback, void *context);

/**
 * @brief: Exit boostio service
 *
 * @return: void
 */
void BioExit(void);

/**
 * @brief: Get the address of an key on the disk
 *
 * @param[in]: tenantId: tenant id
 * @param[in]: keys: key array
 * @param[in]: locations: location info array
 * @param[in]: count: key numbers
 * @param[out]: infos: address of key
 * @return: return RETURN_CACHE_OK mean success, others, return non-zero value
 */
CResult BioBatchGetKeyDiskAddr(uint64_t tenantId, const char **keys, ObjLocation *locations,
                               const uint32_t count, KeyAddrInfo *infos);

/**
 * @brief: Show cache resource information
 *
 * @param[out]: nodeDesc: Cache Resource Description array
 * @param[out]: nodeNum: node num
 * @return: return RETURN_CACHE_OK mean success, others, return non-zero value
 */
CResult BioShowCacheResource(CacheResourcesDesc **nodeDesc, uint64_t *nodeNum);

/**
 * @brief: Show cache and disk resource information managed by the local process
 *
 * @param[out]: nodeDesc: local cache and disk resource description
 * @return: return RETURN_CACHE_OK mean success, others, return non-zero value
 */
CResult BioShowLocalCacheResource(CacheResourcesDesc *nodeDesc);

/**
 * @brief: Free cache hit resources
 *
 * @param[in]: nodeDesc: cache resource pointer
 * @param[in]: nodeNum: node number
 * @return: void
 */
void BioFreeCacheResourcePtr(CacheResourcesDesc **nodeDesc, uint64_t nodeNum);

/**
 * @brief: Show cache hit ratio information
 *
 * @param[out]: desc: Cache hit all node information
 * @param[out]: nodeDesc: Cache hit array
 * @param[out]: nodeNum: node num
 * @return: return RETURN_CACHE_OK mean success, others, return non-zero value
 */
CResult BioShowCacheHitRatio(CacheHitFinalDesc *desc, CacheHitFinalDesc **nodeDesc, uint64_t *nodeNum);

/**
 * @brief: Free cache hit resources
 *
 * @param[in]: nodeDesc: cache hit pointer
 * @param[in]: nodeNum: node number
 * @return: void
 */
void BioFreeCacheHitPtr(CacheHitFinalDesc **nodeDesc, uint64_t nodeNum);

/**
 * @brief: Create cache instance
 *
 * @param[in]: desc: cache descriptor
 * @return: return RETURN_CACHE_OK mean success, others, return non-zero value
 */
CResult BioCreateCache(CacheDescriptor desc);

/**
 * @brief: Get cache instance
 *
 * @param[in]: tenantId: tenant id
 * @param[out]: desc: cache descriptor
 * @return: return RETURN_CACHE_OK mean success, others, return non-zero value
 */
CResult BioGetCache(uint64_t tenantId, CacheDescriptor *desc);

/**
 * @brief: Destroy cache instance
 *
 * @param[in]: tenantId: tenant id
 * @return: return RETURN_CACHE_OK mean success, others, return non-zero value
 */
CResult BioDestroyCache(uint64_t tenantId);

/**
 * @brief: Calculate object location info
 *
 * @param[in]: tenantId: tenant id
 * @param[in]: objectId: object id
 * @param[out]: location: location info
 * @return: return RETURN_CACHE_OK mean success, others, return non-zero value
 */
CResult BioCalcLocation(uint64_t tenantId, uint64_t objectId, ObjLocation *location);

/**
 * @brief: Put value
 *
 * @param[in]: tenantId: tenant id
 * @param[in]: key: key
 * @param[in]: value: value
 * @param[in]: length: value length
 * @param[in]: location: location info
 * @return: return RETURN_CACHE_OK mean success, others, return non-zero value
 */
CResult BioPut(uint64_t tenantId, const char *key, const char *value, uint64_t length, ObjLocation location);

/**
 * @brief: Async Put value
 *
 * @param[in]: tenantId: tenant id
 * @param[in]: key: key
 * @param[in]: value: value
 * @param[in]: length: value length
 * @param[in]: location: location info
 * @param[in]: callback: async put callback function
 * @param[in]: context: callback context
 * @return: return RETURN_CACHE_OK mean success, others, return non-zero value
 */
CResult BioAsyncPut(uint64_t tenantId, const char *key, const char *value, uint64_t length, ObjLocation location,
    BioAsyncPutCallback callback, void* context);

/**
 * @brief: Get value
 *
 * @param[in]: tenantId: tenant id
 * @param[in]: key: key
 * @param[in]: offset: offset of the get value
 * @param[in]: length : length of the get value
 * @param[in]: location : location info
 * @param[out]: value : value
 * @param[out]: realLength : real length
 * @return: return RETURN_CACHE_OK mean success, others, return non-zero value
 */
CResult BioGet(uint64_t tenantId, const char *key, uint64_t offset, uint64_t length, ObjLocation location, char *value,
    uint64_t *realLength);

/**
 * @brief: Batch Get value
 *
 * @param[in]: tenantId: tenant id
 * @param[in]: keys: multiple keys
 * @param[in]: offset: offsets of multiple keys
 * @param[in]: length : lengths of the get values
 * @param[in]: location : location info
 * @param[out]: valueAddrs : address of the values corresponding to multiple keys, need free
 * @param[out]: realLength : real length
 * @param[out]: results : result of getting multiple keys
 * @return: return RETURN_CACHE_OK mean success, others, return non-zero value
 */
CResult BioBatchGet(uint64_t tenantId, const char **keys, const uint32_t count, uint64_t *offsets, uint64_t *lengths,
                    ObjLocation *locations, uintptr_t *valueAddrs,
                    uint64_t *realLengths, int32_t *results);

/**
 * @brief: release the address returned by batchget.
 *
 * @param[in]: tenantId: tenant id
 * @param[in]: valueAddrs: return value of the BioBatchGet method
 * @param[in]: count: number of addresses in valueAddrs
 * @return: return RETURN_CACHE_OK mean success, others, return non-zero value
 */
CResult BioBatchGetFree(uint64_t tenantId, uintptr_t *valueAddrs, const uint32_t count);

/**
 * @brief: Async Get value
 *
 * @param[in]: tenantId: tenant id
 * @param[in]: key: key
 * @param[in]: offset: offset of the get value
 * @param[in]: length : length of the get value
 * @param[in]: location : location info
 * @param[out]: value : value
 * @param[in]: callback : async callback func
 * @param[in]: context : async call context
 * @return: return RETURN_CACHE_OK mean success, others, return non-zero value
 */
CResult BioAsyncGet(uint64_t tenantId, const char *key, uint64_t offset, uint64_t length, ObjLocation location,
    char *value, BioGetCallbackFunc callback, void *context);

/**
 * @brief: Delete object
 *
 * @param[in]: tenantId: tenant id
 * @param[in]: key: key
 * @param[in]: location : location info
 * @return: return RETURN_CACHE_OK mean ok, others, return non-zero value
 */
CResult BioDelete(uint64_t tenantId, const char *key, ObjLocation location);

/**
 * @brief: Load object value
 *
 * @param[in]: tenantId: tenant id
 * @param[in]: key: key
 * @param[in]: offset: offset
 * @param[in]: length: length
 * @param[in]: location: location info
 * @param[in]: callback: load callback function
 * @param[in]: context: callback context
 * @return: return RETURN_CACHE_OK mean success, others, return non-zero value
 */
CResult BioLoad(uint64_t tenantId, const char *key, uint64_t offset, uint64_t length, ObjLocation location,
    BioLoadCallback callback, void *context);

/**
 * @brief: List all object that meets the prefix condition
 *
 * @param[in]: tenantId: tenant id
 * @param[in]: prefix: key prefix
 * @param[out]: objs: object array
 * @param[out]: objNum: object number
 * @return: return RETURN_CACHE_OK mean success, others, return non-zero value
 */
CResult BioListAll(uint64_t tenantId, const char *prefix, ObjStat **objs, uint64_t *objNum);

/**
 * @brief: Free list object resources
 *
 * @param[out]: objs: object list pointer
 * @param[out]: objNum: object number
 * @return: return RETURN_CACHE_OK mean success, others, return non-zero value
 */
void BioFreeListResources(ObjStat **objs, uint64_t objNum);

/**
 * @brief: Stat object
 *
 * @param[in]: tenantId: tenant id
 * @param[in]: key: key
 * @param[in]: location : location info
 * @param[out]: stat: object stat info
 * @return: return RETURN_CACHE_OK mean success, others, return non-zero value
 */
CResult BioStat(uint64_t tenantId, const char *key, ObjLocation location, ObjStat *stat);

/**
 * @brief: Batch exist object
 *
 * @param[in]: tenantId: tenant id
 * @param[in]: key[]: key array
 * @param[in]: location[] : location info array
 * @param[in]: count : key count
 * @param[out]: result[]: exist result
 * @return: return RETURN_CACHE_OK mean success, others, return non-zero value
 */
CResult BioBatchExist(uint64_t tenantId, const char *key[], ObjLocation location[], uint32_t count, bool result[]);

/**
 * @brief: Notify boostio upgrade prepare
 *
 * @param[in]: tenantId: tenant id
 * @return: return RETURN_CACHE_OK mean success, others, return non-zero value
 */
CResult BioNotifyUpgradePrepare(uint64_t tenantId);

/**
 * @brief: Notify boostio upgrade finish
 *
 * @param[in]: tenantId: tenant id
 * @return: return RETURN_CACHE_OK mean success, others, return non-zero value
 */
CResult BioNotifyUpgradeFinish(uint64_t tenantId);

/**
 * @brief: Check whether boostio upgrade is ready
 *
 * @param[in]: tenantId: tenant id
 * @return: return RETURN_CACHE_OK mean success, others, return non-zero value
 */
CResult BioCheckUpgradeReady(uint64_t tenantId);

/**
 * @brief: Alloc cache space for copy free write
 *
 * @param[in]: tenantId: tenant id
 * @param[in]: objectId: object id for generate location
 * @param[in]: length : alloc space length
 * @param[out]: space: cache space describe
 * @return: return RETURN_CACHE_OK mean success, others, return non-zero value
 */
CResult BioAllocCacheSpace(uint64_t tenantId, uint64_t objectId, uint64_t length, CacheSpaceDesc *space);

/**
 * @brief: Put with copy free
 *
 * @param[in]: tenantId: tenant id
 * @param[in]: key: write key
 * @param[in]: space : cache space describe
 * @return: return RETURN_CACHE_OK mean success, others, return non-zero value
 */
CResult BioPutWithCopyFree(uint64_t tenantId, const char *key, CacheSpaceDesc *space);

typedef int (*ReadHook)(uint64_t, char *, uint64_t, uint64_t, int *);
typedef int (*WriteHook)(uint64_t, char *, uint64_t, uint64_t, uint64_t);
typedef int (*WriteCopyFreeHook)(uint64_t, uint64_t, uint64_t, CacheSpaceDesc *);

/**
 * @brief: Interceptor read hook
 *
 * @param[in]: inode: inode number
 * @param[in]: buff: data buffer
 * @param[in]: count : data count
 * @param[in]: offset : read data offset
 * @param[out]: readLen : real read length
 * @return: return RETURN_CACHE_OK mean success, others, return non-zero value
 */
int BioReadHook(uint64_t inode, char *buff, uint64_t count, uint64_t offset, int *readLen);

/**
 * @brief: Interceptor write hook
 *
 * @param[in]: inode: inode number
 * @param[in]: buff: data buffer
 * @param[in]: count : data count
 * @param[in]: offset : read data offset
 * @param[in]: fh : file handler
 * @return: return RETURN_CACHE_OK mean success, others, return non-zero value
 */
int BioWriteHook(uint64_t inode, char *buff, uint64_t count, uint64_t offset, uint64_t fh);

/**
 * @brief: Interceptor write with copy free hook
 *
 * @param[in]: inode: inode number
 * @param[in]: offset : write data offset
 * @param[in]: count : data count
 * @param[in]: space : cache space describe
 * @return: return RETURN_CACHE_OK mean success, others, return non-zero value
 */
int BioWriteCopyFreeHook(uint64_t inode, uint64_t offset, uint64_t count, CacheSpaceDesc *space);

/**
 * @brief: Register interceptor read interface
 *
 * @param[in]: rh: read hook
 * @return: void
 */
void BioRegisterInterceptorRead(ReadHook rh);

/**
 * @brief: Register interceptor write interface
 *
 * @param[in]: wh: write hook
 * @return: void
 */
void BioRegisterInterceptorWrite(WriteHook wh);

/**
 * @brief: Register interceptor write interface with copy free
 *
 * @param[in]: wh: write hook
 * @return: void
 */
void BioRegisterInterceptorWriteCopyFree(WriteCopyFreeHook wh);

/**
 * @brief: Convert location information
 *
 * @param[in]: location: origin location
 * @param[out]: detailLoc: detail location information
 * @return: return RETURN_CACHE_OK mean success, others, return non-zero value
 */
CResult BioConvertLocation(ObjLocation location, ObjLocationDetail *detailLoc);

/**
 * @brief: Add Disk to Bio
 * @param[in]: diskPath: disk path
 * @return: return RETURN_CACHE_OK mean success, others, return non-zero value
 */
CResult BioAddDisk(const char *diskPath);

#ifdef __cplusplus
}
#endif
#endif // BIO_C_H
