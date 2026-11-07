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

#ifndef MESSAGE_H
#define MESSAGE_H

#include <stdint.h>
#include <sys/types.h>
#include <unistd.h>
#include <semaphore.h>
#include "bio_c.h"
#include "bio_config_instance.h"

#ifdef __cplusplus
extern "C" {
#endif

const uint16_t MESSAGE_MAGIC = 0xABCD;
const uint32_t KEY_MAX_SIZE = 256;
const uint32_t KEY_MAX_COUNT = 256;
const uint32_t BATCH_GET_MAX_COUNT = 16 * 1024;
const uint32_t NET_BATCH_GET_MAX_COUNT = KEY_MAX_COUNT;
const uint32_t STANDALONE_BATCH_GET_MAX_COUNT = BATCH_GET_MAX_COUNT;
const uint32_t IP_MAX_SIZE = 32;
const uint32_t DISK_MAX_SIZE = 16;
const uint32_t CLUSTER_NODE_SIZE = 32;
const uint32_t PT_COPY_MAX_SIZE = 3;
const uint32_t PT_SIZE = 64;
const uint32_t SLICE_ADDR_MAX_SIZE = 64;
const uint32_t SLICE_ADDR_SIZE = 4;
const uint32_t MAX_LISTEN_ADDRESS_LENGTH = 32;
const uint32_t FILE_PATH_MAX_LEN = 256;

typedef struct {
    uint16_t magic;
    uint16_t ptId;
    uint64_t ptv;
    uint32_t srcNid;
    pid_t pid;
} RequestComm;

// Node-local, volatile eviction journal. Zero limit reads the initial tail only.
const uint32_t EVICT_LOG_BATCH_MAX = 64;
const uint32_t EVICT_LOG_CAPACITY = 65536;
enum EvictLogStatus : uint32_t { EVICT_LOG_OK = 0, EVICT_LOG_GAP = 1, EVICT_LOG_EPOCH_CHANGED = 2 };
struct EvictLogRecord {
    uint64_t seq;
    char key[KEY_MAX_SIZE];
};
struct EvictLogRequest {
    uint64_t epoch;
    uint64_t afterSeq;
    uint32_t limit;
    uint32_t reserved;
};
struct EvictLogResponse {
    uint64_t epoch;
    uint64_t firstSeq;
    uint64_t nextSeq;
    uint32_t status;
    uint32_t count;
    EvictLogRecord records[EVICT_LOG_BATCH_MAX];
};

/* Shm init */
typedef struct {
    RequestComm comm;
} ShmInitRequest;

typedef struct {
    int32_t memFd;
    int32_t serverPid;
    uint64_t offset;
    uint64_t length;
    uint64_t mKey;
    uint32_t scene;
    uint32_t alignSize;
    uint32_t ioTimeOut;
    uint32_t netTimeOut;
    uint32_t netSegmentSize;
    int32_t logLevel;
    bool enableCrc;
    bool enableCli;
    bool enableHtrace;
    bool enablePrometheus;
    char listenAddress[MAX_LISTEN_ADDRESS_LENGTH];
    uint32_t scrapeIntervalSec;
} ShmInitResponse;

/* Standalone runtime config */
typedef struct {
    int32_t serverPid;
    uint32_t scene;
    uint32_t alignSize;
    uint32_t ioTimeOut;
    uint32_t netTimeOut;
    uint32_t netSegmentSize;
    int32_t logLevel;
    bool enableCrc;
    bool enableCli;
    bool enableHtrace;
    bool enablePrometheus;
    char listenAddress[MAX_LISTEN_ADDRESS_LENGTH];
    uint32_t scrapeIntervalSec;
    uint64_t sdkPoolSize;
    uint64_t dataMsgBlockSize;
} StandaloneRuntimeConfigResponse;

/* Query cache resource quota */
typedef struct {
    RequestComm comm;
} QueryQuotaRequest;

typedef struct {
    bool enable;
    uint64_t preloadSize;
} QueryQuotaResponse;

/* Alloc cache quota */
typedef struct {
    RequestComm comm;
    uint32_t nid;
    uint64_t cid;
    uint64_t allocQuota;
} AllocQuotaRequest;

typedef struct {
    uint64_t exceptQuota;
} AllocQuotaResponse;

/* Free cache quota */
typedef struct {
    RequestComm comm;
    uint32_t nid;
    uint64_t cid;
    uint64_t quota;
} FreeQuotaRequest;

/* Query node view */
typedef struct {
    uint16_t diskId;
    uint16_t diskStatus;
} DiskInfoDesc;

typedef struct {
    uint16_t groupId;
    uint16_t nodeId;
    char ip[IP_MAX_SIZE];
    uint16_t port;
    uint16_t status;
    uint32_t num;
    DiskInfoDesc diskDesc[DISK_MAX_SIZE];
} NodeInfoDesc;

typedef struct {
    RequestComm comm;
    uint32_t bar;
} QueryNodeViewRequest;

typedef struct {
    int32_t flag;
    uint64_t curNodeTimes;
    uint32_t num;
    NodeInfoDesc desc[CLUSTER_NODE_SIZE];
} QueryNodeViewResponse; // size:2456

/* Query pt view */
typedef struct {
    uint16_t nodeId;
    uint16_t diskId;
    uint16_t state;
} PtCopyDesc;

typedef struct {
    uint64_t version;
    uint16_t ptId;
    uint16_t state;
    uint16_t masterNodeId;
    uint16_t masterDiskId;
    PtCopyDesc copys[PT_COPY_MAX_SIZE];
} PtInfoDesc;

typedef struct {
    uint16_t masterPtId;
    uint16_t slavePtId;
} FileLocationQueryReq;

typedef struct {
    char hostMaster[NODE_DESC_SIZE];
    char hostSlave[NODE_DESC_SIZE];
    uint16_t portMaster;
    uint16_t portSlave;
} FileLocationQueryRsp;

typedef struct {
    RequestComm comm;
    uint32_t bar;
} QueryPtViewRequest;

typedef struct {
    int32_t flag;
    uint64_t curPtTimes;
    uint32_t num;
    uint32_t copyNum;
    PtInfoDesc desc[PT_SIZE];
} QueryPtViewResponse; // size:2072

/* Query local node info */
typedef struct {
    RequestComm comm;
} GetLocalNidRequest;

typedef struct {
    uint16_t groupId;
    uint16_t nodeId;
    uint16_t protocol;
} GetLocalNidResponse;

/* Create flow */
typedef struct {
    RequestComm comm;
    uint16_t opType;
    uint64_t flowId;
    bool isDegrade;
} CreateFlowRequest;

typedef struct {
    uint64_t flowId;
    bool isDegrade;
    uint64_t index;
    uint64_t offset;
    bool isNewFlow;
} CreateFlowResponse;

/* Destroy flow */
typedef struct {
    RequestComm comm;
    uint64_t flowId;
} DestroyFlowRequest;

typedef struct {
    uint64_t flowId;
} DestroyFlowResponse;

/* Create data message memory pool */
typedef struct {
    RequestComm comm;
} CreateDataMsgMemPoolRequest;

typedef struct {
    int32_t memFd;
    uint64_t offset;
    uint64_t poolSize;
    uint64_t blockSize;
} CreateDataMsgMemPoolResponse;

/* Get slice */
typedef struct {
    RequestComm comm;
    uint64_t flowId;
    uint64_t flowOffset;
    uint64_t flowIndex;
    uint64_t length;
} GetSliceRequest;

typedef struct {
    uint64_t chunkId;
    uint32_t chunkOffset;
    uint32_t chunkLen;
} SliceAddrDesc;

typedef struct {
    uint64_t addrNum;
    SliceAddrDesc addr[SLICE_ADDR_MAX_SIZE];
    uint64_t addrOffset[SLICE_ADDR_MAX_SIZE];
    uint64_t sliceLen;
    char sliceBuf[0];
} GetSliceResponse;

/* Put */
typedef struct {
    RequestComm comm;
    uint64_t tenantId;
    uint8_t affinity;
    uint8_t strategy;
    char key[KEY_MAX_SIZE];
    uint64_t length;
    uint64_t flowId;
    uint64_t flowOffset;
    uint64_t flowIndex;
    uintptr_t mrAddress;
    uint64_t mrOffset;
    uint64_t mrSize;
    uint64_t mrKey;
    bool memFromServer;
    bool isDegrade;
    bool isAlloc;
    uint32_t ioStrategy;
    uint64_t sliceLen;
    uint64_t quotaNid;
    uint64_t quotaCid;
    uint32_t dataCrc;
    char sliceBuf[0];
} PutRequest;

typedef struct {
    uint32_t ioStrategy;
} PutResponse;

/* Parse key addr */
typedef struct {
    char key[KEY_MAX_SIZE];
    uint16_t ptId;
} BatchKeyInfo;

typedef struct {
    char path[DISK_PATH_MAX_SIZE];
    uint64_t offset[CHUNK_ADDR_MAX_SIZE];
    uint64_t length[CHUNK_ADDR_MAX_SIZE];
    int32_t result;
    uint8_t count;
} BatchKeyAddrInfo;

typedef struct {
    uint16_t magic;
    uint32_t count;
    BatchKeyInfo infos[0];
} BatchParseKeyAddrRequest;

typedef struct {
    BatchKeyAddrInfo infos[0];
} BatchParseKeyAddrResp;

/* Get */
typedef struct {
    RequestComm comm;
    char key[KEY_MAX_SIZE];
    uint16_t ptId;
    uint64_t offset;
    uint64_t length;
    uint8_t isMr;
    uintptr_t address;
    uint64_t size;
    uint64_t mrKey;
    bool enableCrc;
    bool isConvDeploy;
} GetRequest;

typedef struct {
    bool isAlloc;
    uint32_t num;
    uint64_t updateQuota;
    uintptr_t address[SLICE_ADDR_SIZE];
    uint64_t addrOffset[SLICE_ADDR_SIZE];
    uint64_t addrLen[SLICE_ADDR_SIZE];
    uint64_t realLen;
    uint32_t dataCrc;
} GetResponse;

/* Delete */
typedef struct {
    RequestComm comm;
    char key[KEY_MAX_SIZE];
} DeleteRequest;

/* BatchGet */
typedef struct {
    char key[KEY_MAX_SIZE];
    uint64_t offset;
    uint64_t length;
    uintptr_t address;
    uintptr_t addressOffset;
    uint64_t mrKey;
    uint32_t size;
    uint16_t ptId;
    uint64_t *realLength;
    int32_t *result;
} GetKeyInfo;

typedef struct {
    uint32_t count;
    pid_t pid;
    uint16_t srcNid;
    bool isConvDeploy;
    GetKeyInfo keysInfo[0];
} BatchGetRequest;

struct BatchGetPlan {
    uint32_t count;
    uint32_t index;
    uint32_t reqLen;
    BatchGetRequest *req;
    BatchGetPlan() : count(0), index(0), reqLen(0), req(nullptr) {}
    BatchGetPlan(uint32_t countParam, uint32_t indexParam, uint32_t reqLenParam, BatchGetRequest *reqParam) :
        count(countParam), index(indexParam), reqLen(reqLenParam), req(reqParam) {}
};

typedef struct {
    uint64_t realLengths[STANDALONE_BATCH_GET_MAX_COUNT];
    int32_t results[STANDALONE_BATCH_GET_MAX_COUNT];
    uint32_t count;
    uint16_t nodeId;
} BatchGetResponse;

typedef struct {
    uint64_t realLength;
    int32_t result;
    uint32_t reserved;
} BatchGetResultItem;

typedef struct {
    uint32_t count;
    uint16_t nodeId;
    uint16_t reserved;
    BatchGetResultItem items[0];
} BatchGetWireResponse;

/* Stat */
typedef struct {
    RequestComm comm;
    char key[KEY_MAX_SIZE];
} StatRequest;

typedef struct {
    uint32_t size;
    time_t time;
} StatResponse;

/* BatchExist */
typedef struct {
    char key[KEY_MAX_SIZE];
    uint16_t index;
    uint16_t ptVec;
} BatchExistKeyInfo;

typedef struct {
    uint32_t count;
    BatchExistKeyInfo keys[0];
} BatchExistRequest;

typedef struct {
    uint16_t index[KEY_MAX_COUNT];
    bool result[KEY_MAX_COUNT];
    uint16_t count;
} BatchExistResponse;

typedef struct {
    uint32_t count;
    uint64_t ptV;
    uint32_t index;
    uint32_t reqLen;
    BatchExistRequest *req;
} BatchExistPlan;

/* List */
typedef struct {
    RequestComm comm;
    char prefix[KEY_MAX_SIZE];
    bool isListUnderFs;
    uintptr_t address;
    uint64_t size;
    uint64_t mrKey;
} ListRequest;

/* Add disk */
typedef struct {
    RequestComm comm;
    char diskPath[FILE_PATH_MAX_LEN];
} AddDiskRequest;

typedef struct {
    uint32_t result;
} AddDiskResponse;

typedef struct {
    uintptr_t addr;
    uint64_t addrOffset;
    uint32_t num;
    uint32_t buffLen;
    char statBuf[0];
} ListResponse;

/* Load */
typedef struct {
    RequestComm comm;
    char key[KEY_MAX_SIZE];
    uint64_t offset;
    uint64_t length;
} LoadRequest;

/* Sync data */
typedef struct {
    RequestComm comm;
} SyncDataRequest;

/* Evict */
typedef struct {
    RequestComm comm;
    uint64_t flowId;
} GetEvictRequest;

typedef struct {
    RequestComm comm;
    uint64_t flowId;
    bool needDestroy;
    uint64_t index;
    uint64_t offset;
} ProcFlowSyncRequest;

typedef struct {
    uint32_t nodeId;
    bool needDestroy;
} ProcFlowSyncResponse;

typedef struct {
    int32_t result;
    uint32_t quota;
    sem_t sem;
    bool needDestroy;
} ProcBrokenCallbackCtx;

/* Free server memory */
typedef struct {
    RequestComm comm;
    uint32_t num;
    uint8_t memFreeType;
    uintptr_t addr[SLICE_ADDR_SIZE];
} FreeMemRequest;

typedef struct {
    int32_t result;
    uint32_t quota;
    sem_t sem;
    void *resp;
    uint32_t respLen;
} ClientCallbackCtx;

typedef struct {
    int32_t result;
    uint32_t quota;
    void *resp;
    uint32_t respLen;
} AsyncPutCbCtx;

/* Notify Update */
typedef struct {
    RequestComm comm;
    bool flag;
} NotifyUpdateRequest;

/* Check Update Ready */
typedef struct {
    RequestComm comm;
} CheckUpdateReadyRequest;

typedef struct {
    bool flag;
} CheckUpdateReadyResponse;

typedef struct {
    RequestComm comm;
} CheckRemoteUpdateReadyRequest;

typedef struct {
    bool flag;
} CheckRemoteUpdateReadyResponse;

typedef struct {
    uint32_t pid;
    uint64_t length;
    uint64_t startTime;
} InterceptorAllocPageReq;

typedef struct {
    uint32_t pid;
    uint64_t addrOffset[CACHE_SPACE_ADDRESS_SIZE];
    CacheSpaceDesc address;
} InterceptorAllocPageRsp;

typedef struct {
    uint32_t pid;
    int32_t fd;
    uint64_t inode;
    uint64_t nbytes;
    int64_t offset;
    uint64_t startTime;
} InterceptorPreadIn;

typedef struct {
    int32_t ret;
    uint32_t unused;
    uint64_t dataLen;
    char data[];
} InterceptorPreadOut;

typedef struct {
    uint32_t pid;
    uint64_t fd;
    uint64_t inode;
    uint64_t nbytes;
    int64_t offset;
    uint64_t startTime;
    char data[];
} InterceptorPwriteIn;

typedef struct {
    uint32_t pid;
    uint64_t fd;
    uint64_t inode;
    uint64_t nbytes;
    int64_t offset;
    uint64_t startTime;
    CacheSpaceDesc address;
} InterceptorLargePwriteIn;

typedef struct {
    int32_t ret;
    uint32_t unused;
    int64_t dataLen;
} InterceptorPwriteOut;

typedef struct {
    RequestComm comm;
} GetUnderFsConfigRequest;

typedef struct {
    char cfgPath[KEY_MAX_SIZE];
    char cluster[KEY_MAX_SIZE];
    char user[KEY_MAX_SIZE];
    char pool[KEY_MAX_SIZE];
} CephConfigResponse;

typedef struct {
    char nameNode[KEY_MAX_SIZE];
    char workingPath[KEY_MAX_SIZE];
} HdfsConfigResponse;

typedef struct {
    char underFsType[KEY_MAX_SIZE];
    CephConfigResponse cephConfig;
    HdfsConfigResponse hdfsConfig;
} GetUnderFsConfigResponse;

/* Cache Resource */
typedef struct {
    RequestComm comm;
    bool includeDiskInfo;
    uint8_t reserved[7];
} CacheResourceRequest;

typedef struct {
    uint64_t rCacheMemCapacity;
    uint64_t rCacheDiskCapacity;
    uint64_t wCacheMemCapacity;
    uint64_t wCacheDiskCapacity;
    uint64_t rCacheMemUsedSize;
    uint64_t rCacheDiskUsedSize;
    uint64_t wCacheMemUsedSize;
    uint64_t wCacheDiskUsedSize;
    uint16_t nodeId;
    uint16_t diskNum;
    DiskResourcesDesc disks[DISK_RESOURCE_MAX_NUM];
} CacheResourceResponse;

/* Cache Hit */
typedef struct {
    RequestComm comm;
} CacheHitRequest;

typedef struct {
    uint64_t rCacheHitMemCount;
    uint64_t rCacheHitDiskCount;
    uint64_t rCacheHitCount;
    uint64_t rCacheTotalCount;
    uint64_t wCacheHitMemCount;
    uint64_t wCacheHitDiskCount;
    uint64_t wCacheHitCount;
    uint64_t wCacheTotalCount;
    uint64_t backendHitCount;
    uint16_t nodeId;
} CacheHitResponse;

typedef struct {
    uint64_t beginData;
    uint64_t goodEnd;
    uint64_t badEnd;
    uint64_t max;
    uint64_t min;
    uint64_t total;
} TraceMetrics;

typedef struct {
    char traceName[MAX_TRACE_NAME_LEN];
    TraceMetrics metrics;
} TraceData;

typedef struct {
    TraceData traces[TRACE_MAX_NUM];
    int count;
} TraceDatabase;

/* Get Trace Points */
typedef struct {
    uint16_t nodeId;
    RequestComm comm;
} GetTracePointsRequest;

typedef struct {
    TraceDatabase traceDatabase;
} GetTracePointsResponse;

#ifdef __cplusplus
}
#endif
#endif // MESSAGE_H
