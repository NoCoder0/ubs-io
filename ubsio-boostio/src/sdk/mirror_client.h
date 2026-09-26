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

#ifndef MIRROR_CLIENT_H
#define MIRROR_CLIENT_H

#include <semaphore.h>
#include <atomic>
#include <unordered_map>
#include <set>
#include "cm.h"
#include "bio_ref.h"
#include "bio_lock.h"
#include "message.h"
#include "bio.h"
#include "flow_instance.h"
#include "flow.h"
#include "slice.h"
#include "cache_def.h"
#include "cache_slice.h"
#include "cache_slice_operator.h"
#include "bio_qos.h"
#include "bio_client_agent.h"

namespace ock {
namespace bio {
constexpr uint16_t BIO_INIT_TIMEOUT_TIME = 90;
constexpr uint16_t BIO_IO_TIMEOUT_TIME = 60;
constexpr uint16_t BIO_IO_INTERAL_TIME = 2;
constexpr uint16_t BIO_IO_DELAY_TIME = 1;
using UpdateView = std::function<void(void)>;
using FlowInfo = ock::bio::agent::BioClientAgent::FlowInfo;

enum WorkerScene : uint32_t {
    SCENE_NONE = 0,
    SCENE_BIGDATA = 1
};

struct IoStrategy {
    std::atomic<uint64_t> expired;
    std::atomic<uint32_t> strategy;
};

class MirrorClient {
public:
    struct MirrorPut {
        CacheAttr attr;
        char *key;
        char *value;
        uint64_t length;
        ObjLocation location;
        uint64_t flowId;
        uint64_t flowOffset;
        uint64_t flowIndex;
    };

    struct MirrorAsyncPut {
        CacheAttr attr;
        char *key;
        char *value;
        uint64_t length;
        ObjLocation location;
        uint64_t flowId;
        uint64_t flowOffset;
        uint64_t flowIndex;
        BioAsyncPutCallback callback;
        void* context;
    };

    struct MirrorGet {
        CacheAttr attr;
        const char *key;
        char *value;
        uint64_t offset;
        uint64_t length;
        ObjLocation location;
    };

    struct DispathBatchGetResult {
        BResult result;
        uint32_t index;
        uint32_t count;
    };

    struct MirrorBatchGet {
        CacheAttr attr;
        const char **keys;
        uint32_t count;
        uint64_t *offsets;
        uint64_t *lengths;
        ObjLocation *locations;
        uintptr_t *valuesAddr;
        uint64_t *realLengths;
        int32_t *results;
        MirrorBatchGet() : attr(0, AFFINITY_BUTT, STRATEGY_BUTT), keys(nullptr), count(0), offsets(nullptr),
                           lengths(nullptr), locations(nullptr), valuesAddr(nullptr),
                           realLengths(nullptr), results(nullptr) {}
        MirrorBatchGet(CacheAttr attrParam, const char **keysParam, uint32_t countParam,
                       uint64_t *offsetsParam, uint64_t *lengthsParam, ObjLocation *locationsParam,
                       uintptr_t *valuesAddrParam, uint64_t *realLengthsParam, int32_t *resultsParam) : attr(attrParam),
                       keys(keysParam), count(countParam), offsets(offsetsParam),
                       lengths(lengthsParam), locations(locationsParam), valuesAddr(valuesAddrParam),
                       realLengths(realLengthsParam), results(resultsParam) {}
    };

    struct BatchExistSendKeyInfo {
        CmPtInfo ptEntry;
        bool result = true;
        explicit BatchExistSendKeyInfo(CmPtInfo &entry)
        {
            ptEntry = entry;
        }
    };

    struct MirrorBatchGetKeyAddr {
        uint32_t count;
        const char **keys;
        ObjLocation *locations;
        KeyAddrInfo* infos;
    };

    struct UpdateParams {
        uint16_t ptId;
        uint64_t ptv;
        uint64_t flowId;
        bool isDegrade;
        uint64_t index;
        uint64_t offset;
    };

    static constexpr uint32_t DEFAULT_MAX_FLOW_SIZE = 1024;

    BResult Initialize(UpdateView updateView, uint32_t scene, uint32_t alignSize, uint32_t timeOut, bool enableCrc);
    BResult Start();
    BResult RecoverDataMessageMem();

    void Exit();
    void FreeIoStrategy();

    explicit MirrorClient(WorkerMode mode) : mMode(mode), mCurNodeTimes(0), mCurPtTimes(0), mNetProtocol(0) {}
    ~MirrorClient() {}

    uint16_t SelectingPt(uint64_t objectId, AffinityStrategy affinity);

    inline uint16_t ParseLocation(ObjLocation location)
    {
        return static_cast<uint16_t>(location.location[0]);
    }

    BResult Put(MirrorPut &param);

    BResult AsyncPut(MirrorPut &param, BioAsyncPutCallback callback, void *context);

    BResult Put(MirrorPut &param, CacheSpaceDesc &spaceInfo);

    BResult Get(MirrorGet &param, uint64_t &realLen);

    BResult BatchGet(CacheAttr attr, const char **keys, const uint32_t count, uint64_t *offsets,
                     uint64_t *lengths, ObjLocation *locations, uintptr_t *valueAddrs,
                     uint64_t *realLengths, int32_t *results);

    inline void DispathBatchGetRecycleResource(uint32_t parallelNum, DispathBatchGetResult *taskResults,
                                                         uintptr_t *valueAddrs);

    BResult DispathBatchGet(CacheAttr attr, const char **keys, const uint32_t count, uint64_t *offsets,
                                          uint64_t *lengths, ObjLocation *locations, uintptr_t *valueAddrs,
                                          uint64_t *realLengths, int32_t *results);

    void BatchFree(uintptr_t *valueAddrs, const uint32_t count);

    BResult BatchGetKeyDiskAddr(MirrorBatchGetKeyAddr &param);

    BResult AsyncGet(MirrorGet &param, AsyncOpParam &opParam);

    BResult DeleteKey(const char *key, const ObjLocation &location);

    BResult Load(LoadPara &para, const ObjLocation &location, const Bio::LoadCallback &callback, void *context);

    BResult ListAll(const char *prefix, std::unordered_map<std::string, ObjStat> &objs);

    BResult StatObject(const char *key, const ObjLocation &location, ObjStat &stat);

    BResult BatchExist(const char *key[], ObjLocation location[], uint32_t count, bool *result);

    BResult DispathBatchExist(const char *key[], ObjLocation location[], uint32_t count, bool *result);

    BResult GetFileLocation(uint16_t masterPtId, uint16_t slavePtId, FileLocationQueryRsp &fileLocationQueryRsp);

    BResult AllocSpace(MirrorClient::MirrorPut &param, CacheSpaceDesc &spaceInfo);

    BResult NotifyUpdate(bool &flag);

    BResult AddDisk(const char *diskPath);

    BResult CheckUpdateReady();

    BResult GetCacheHitRatioImpl(std::unordered_map<uint16_t, CacheHitDesc> &nodeDesc);

    BResult QueryCacheResourceImpl(std::vector<CacheResourcesDesc> &nodeDesc);

    BResult QueryLocalCacheResourceImpl(CacheResourcesDesc &nodeDesc);

    DEFINE_REF_COUNT_FUNCTIONS

    std::vector<uint16_t> ListLocalAffinityPt();

    inline uint16_t GetNetProtocol() const
    {
        return mNetProtocol;
    }

    inline CmNodeId GetLocalNodeInfo() const
    {
        return mLocalNid;
    }

    inline BioQosPtr &GetQosPtr()
    {
        return mBioQos;
    }

    inline bool CheckIsOnline(uint16_t nodeId, std::string &ip, uint16_t &port)
    {
        CmNodeId node(mLocalNid.groupId, nodeId);
        bool isOnline = false;
        mLock.LockRead();
        if (mNodeView.find(node) != mNodeView.end()) {
            if (mNodeView[node].status == CM_NODE_NORMAL) {
                isOnline = true;
                ip = mNodeView[node].ip;
                port = mNodeView[node].port;
            }
            mLock.UnLock();
            return isOnline;
        }
        mLock.UnLock();
        return false;
    }

    inline std::map<CmNodeId, CmNodeInfo, CmNodeIdCmp> GetNodeView()
    {
        ReadLocker<ReadWriteLock> locker(&mLock);
        return mNodeView;
    }

    inline std::map<uint16_t, CmPtInfo> &GetPtView()
    {
        return mPtView;
    }

    inline void SetScene(WorkerScene s)
    {
        mScene = s;
    }

    BResult RebuildNodeView();

    BResult RebuildPtView();

    BResult GetPtEntry(uint16_t ptId, ock::bio::CmPtInfo &ptEntry);
    BResult InitializeRemotePlacement();
    bool RemotePtValid(uint16_t ptId, const CmPtInfo &ptEntry) const;

    BResult PutAlignSize(const char *value, MirrorPut &param, bool &isAllocMem);

    inline FlowInstancePtr Query(uint16_t ptId)
    {
        BIO_TP_START(SDK_MIRROR_CLIENT_QUERY_FAIL, 0);
        mLock.LockRead();
        auto it = mFlowMap.find(ptId);
        if (LIKELY(it != mFlowMap.end())) {
            mLock.UnLock();
            return it->second;
        }
        mLock.UnLock();
        BIO_TP_END;
        return nullptr;
    }

private:
    bool FailHandler(BResult result, uint64_t startTimeUs, uint64_t timeOutSec);

    uint16_t SelectingPtImpl(uint64_t objectId, AffinityStrategy affinity);
    BResult PreparePutWithSpace(MirrorPut &param, CmPtInfo &ptEntry, CacheSpaceDesc &spaceInfo, PutRequest *&req);
    BResult PutImpl(MirrorPut &param, uint16_t ptId, CmPtInfo &ptEntry);
    BResult AsyncPutImpl(MirrorPut &param, uint16_t ptId, CmPtInfo &ptEntry, BioAsyncPutCallback callback,
        void* context);
    BResult PutImpl(MirrorPut &param, CacheSpaceDesc &spaceInfo);
    BResult PutCheckPtState(CmPtInfo ptEntry);

    BResult GetImpl(MirrorGet &param, uint64_t &realLen);

    BResult BatchGetKeyDiskAddrImpl(MirrorBatchGetKeyAddr &param);

    BResult BatchGetImpl(MirrorBatchGet &param);

    BResult GetImpl(MirrorGet &param, AsyncOpParam &opParam);

    BResult DeleteKeyImpl(const char *key, const ObjLocation &location);

    BResult LoadImpl(LoadPara &para, const ObjLocation &location, const Bio::LoadCallback &callback, void *context);

    BResult ListAllImpl(const char *prefix, std::unordered_map<std::string, ObjStat> &objs);

    BResult StatObjectImpl(const char *key, const ObjLocation &location, ObjStat &stat);

    BResult BatchExistImpl(const char *key[], ObjLocation location[], uint32_t count, bool *result);

    BResult SendNotifyUpdateRequest(bool &flag);

    BResult SendCheckUpdateReadyRequest();

    BResult SendCacheHitRequest(CacheHitRequest &req, std::unordered_map<uint16_t, CacheHitDesc> &nodeDesc);

    BResult SendCacheResourceRequest(CacheResourceRequest &req, std::vector<CacheResourcesDesc> &nodeDesc);

    void GetCacheHitLocal(CacheHitRequest &req, uint16_t localId,
                          std::unordered_map<uint16_t, CacheHitDesc> &nodeDesc);

    void GetCacheHitRemote(CacheHitRequest &req, std::vector<uint16_t> &remoteId,
                           std::unordered_map<uint16_t, CacheHitDesc> &nodeDesc);

    void CalcCacheResourceLocal(CacheResourceRequest &req, uint16_t localId,
                                std::vector<CacheResourcesDesc> &nodeDesc);

    void CalcCacheResourceRemote(CacheResourceRequest &req, std::vector<uint16_t> &remoteId,
                                 std::vector<CacheResourcesDesc> &nodeDesc);

    BResult AllocSpaceImpl(uint16_t ptId, CmPtInfo &ptEntry, MirrorPut &param, CacheSpaceDesc &spaceInfo);
    BResult InitializeBioQos();
    BResult LoadOriginView();
    BResult LoadOriginViewImpl();
    BResult LoadAffinityFlow();

    BResult CreateDataMessageMemLocal();
    BResult CreateDataMessageMemRemote();
    BResult CreateDataMessageMemStandalone();
    BResult CreateDataMessageMem();
    void DestroyDataMessageMem();
    BResult AllocDataMessageBuffer(uint64_t length, uintptr_t &address);
    void FreeDataMessageBuffer(uintptr_t &address);
    void FillBatchGetBufferInfo(GetKeyInfo &keyInfo, uintptr_t address, uint64_t length);
    inline bool IsDataMsgMemAddr(uintptr_t address) const
    {
        return mDataMsgMemAddr != nullptr && mDataMsgMemSize != 0 &&
               address >= reinterpret_cast<uintptr_t>(mDataMsgMemAddr) &&
               address < reinterpret_cast<uintptr_t>(mDataMsgMemAddr) + mDataMsgMemSize;
    }

    void InitCallbackCtx(ClientCallbackCtx &cbCtx, uint32_t quota);
    void InitAsyncPutCbCtx(AsyncPutCbCtx &cbCtx, uint32_t quota);
    uint32_t CalcPtQuota(CmPtInfo &ptEntry);

    BResult AllocPutOffset(uint16_t ptId, uint64_t ptv, uint64_t len, uint64_t &flowId, uint64_t &offset,
        uint64_t &index);
    BResult SendCreateFlowRequestRemote(uint16_t nodeId, CmPtInfo &ptEntry, uint16_t ptId, uint16_t opType,
        FlowInfo &flowInfo);
    BResult SendDestroyFlowRequestRemote(uint16_t nodeId, CmPtInfo &ptEntry, uint16_t ptId, uint64_t flowId);
    BResult CreateFlowImpl(uint16_t nodeId, CmPtInfo &ptEntry, uint16_t ptId, uint16_t opType, FlowInfo &flowInfo);
    BResult DestroyFlowImpl(uint16_t nodeId, CmPtInfo &ptEntry, uint16_t ptId, uint64_t flowId);
    BResult CreateFlow(uint16_t ptId);
    BResult DestroyFlow(uint16_t ptId, uint64_t flowId);

    void ConstructPutReq(PutRequest *req, CmPtInfo &ptEntry, MirrorPut &param, uint64_t flowId, uint64_t flowOffset,
        uint64_t flowIndex, GetSliceResponse *rsp);
    void ConstructPutReq(PutRequest *req, CmPtInfo &ptEntry, MirrorPut &param, uint64_t flowId, uint64_t flowOffset,
        uint64_t flowIndex, NetMrInfo &mr);
    BResult DataCopy(const char *from, uint32_t fromLen, SliceAddrDesc *addr, uint64_t *offset, uint32_t addrNum);
    bool IsExistLocalCopy(CmPtInfo &ptEntry);
    BResult PrepareFromServer(CmPtInfo &ptEntry, MirrorPut &param, PutRequest *&req);
    BResult PrepareFromClient(CmPtInfo &ptEntry, MirrorPut &param, PutRequest *&req);
    BResult Prepare(CmPtInfo &ptEntry, MirrorPut &param, PutRequest *&req);
    void PutRemote(PutRequest *req, CmPtInfo &ptEntry, std::vector<uint32_t> &indexVec, Callback &callback);
    void PutLocal(PutRequest *req, uint32_t localIdx, Callback &callback) const;
    BResult SendPutRequestImpl(CmPtInfo &ptEntry, MirrorPut &param, PutRequest *req);
    BResult SendAsyncPutRequestImpl(CmPtInfo &ptEntry, MirrorPut &param, PutRequest *req,
        BioAsyncPutCallback asyncPutCallback, void *context);
    BResult SendPutRequest(CmPtInfo &ptEntry, MirrorPut &param);
    BResult SendAsyncPutRequest(CmPtInfo &ptEntry, MirrorPut &param, BioAsyncPutCallback callback, void* context);

    BResult GetServerRemote(GetRequest &req, uint16_t dstNid, char *value, uint64_t &realLen);
    BResult GetServerRemote(GetRequest &req, uint16_t masterNid, char *value, Callback callback);
    BResult GetFromServer(GetRequest &req, uint16_t serverNid, char *value, uint64_t &realLen);
    BResult SendGetRequest(CmPtInfo &ptEntry, GetRequest &req, char *value, uint64_t &realLen);
    BResult SendBatchGetKeyDiskAddrRequest(BatchParseKeyAddrRequest *req, uint32_t reqLen, KeyAddrInfo* infos);
    void BatchGetRemote(uint16_t nodeId, uint32_t reqLen, BatchGetRequest *req, Callback &callback);
    BResult SendBatchGetRequest(std::unordered_map<uint16_t, BatchGetPlan> &planSend);
    BResult GetShmDataCallBack(GetResponse *rsp, uint64_t &realLen, const GetRequest &req, char *value);
    BResult GetRpcDataCallBack(GetResponse *rsp, const GetRequest &req, char *value, uint64_t &realLen);
    BResult GetFromServer(GetRequest &req, uint16_t serverNid, char *value, AsyncOpParam &opParam);
    BResult SendGetRequest(CmPtInfo &ptEntry, GetRequest &req, char *value, AsyncOpParam &opParam);

    void DeleteRemote(DeleteRequest &req, CmPtInfo &ptEntry, uint32_t index, Callback &callback);
    void DeleteLocal(DeleteRequest &req, Callback &callback) const;
    BResult SendDeleteRequest(CmPtInfo &ptEntry, DeleteRequest &req);

    BResult StatRemote(uint16_t dstNid, StatRequest &req, ObjStat &objInfo);
    BResult StatLocal(StatRequest &req, ObjStat &objInfo) const;
    BResult SendStatRequest(CmPtInfo &ptEntry, StatRequest &req, ObjStat &objInfo);

    inline void BatchExistRemote(uint16_t nodeId, uint32_t reqLen, BatchExistRequest *req, Callback &callback);
    inline void BatchExistLocal(uint32_t reqLen, BatchExistRequest *req, Callback &callback);
    BResult SendBatchExistRequest(std::unordered_map<uint16_t, BatchExistPlan> &planSend,
                                                std::vector<BatchExistSendKeyInfo> &keysInfo);

    BResult ListRemote(uint16_t nid, ListRequest &req, std::unordered_map<std::string, ObjStat> &objs);
    BResult ListLocal(ListRequest &req, std::unordered_map<std::string, ObjStat> &objs);
    BResult SendListRequest(ListRequest &req, std::unordered_map<std::string, ObjStat> &objs);

    BResult LoadMaster(LoadRequest &req, uint16_t masterNid, const Bio::LoadCallback &callback, void *context);
    BResult SendLoadRequest(CmPtInfo &ptEntry, LoadRequest &req, const Bio::LoadCallback &callback, void *context);
    BResult AddDiskImpl(const char *diskPath);
    BResult SendAddDiskRequest(AddDiskRequest &req);

    inline BResult Insert(uint16_t ptId)
    {
        mLock.LockWrite();
        if (UNLIKELY(mFlowMap.size() > DEFAULT_MAX_FLOW_SIZE)) {
            mLock.UnLock();
            return BIO_ERR;
        }
        auto it = mFlowMap.find(ptId);
        if (it != mFlowMap.end()) {
            mLock.UnLock();
            return BIO_EXISTS;
        }
        FlowInstancePtr instance = MakeRef<FlowInstance>();
        if (instance == nullptr) {
            mLock.UnLock();
            return BIO_ALLOC_FAIL;
        }
        mFlowMap[ptId] = instance;
        mLock.UnLock();
        return BIO_OK;
    }

    inline BResult Update(UpdateParams &para)
    {
        mLock.LockWrite();
        if (UNLIKELY(mFlowMap.size() > DEFAULT_MAX_FLOW_SIZE)) {
            mLock.UnLock();
            return BIO_ERR;
        }
        auto it = mFlowMap.find(para.ptId);
        if (it != mFlowMap.end()) {
            FlowInstancePtr instance = it->second;
            instance->Update(para.flowId, para.ptv, para.isDegrade, para.index, para.offset);
            mLock.UnLock();
            return BIO_OK;
        }
        mLock.UnLock();
        return BIO_NOT_EXISTS;
    }

    inline void Delete(uint16_t ptId, uint64_t flowId)
    {
        sleep(BIO_IO_DELAY_TIME);
        mLock.LockWrite();
        auto it = mFlowMap.find(ptId);
        if (UNLIKELY(it == mFlowMap.end())) {
            mLock.UnLock();
            return;
        }
        if (it->second->FlowId() != flowId) {
            mLock.UnLock();
            return;
        }
        mFlowMap.erase(it);
        mLock.UnLock();

        DestroyFlow(ptId, flowId);
    }

private:
    std::unordered_map<uint16_t, FlowInstancePtr> mFlowMap;
    ReadWriteLock mLock;
    WorkerMode mMode;
    CmNodeId mLocalNid;
    std::map<CmNodeId, CmNodeInfo, CmNodeIdCmp> mNodeView;
    std::map<uint16_t, CmPtInfo> mPtView;
    bool mRemoteDramMode = false;
    std::set<std::string> mRemoteIps;
    std::map<uint16_t, uint16_t> mRemotePtMasters;
    std::vector<uint16_t> mRemotePtIds;
    std::map<uint16_t, IoStrategy *> mIoStrategy;
    uint64_t mCurNodeTimes;
    uint64_t mCurPtTimes;
    uint16_t mNetProtocol;
    UpdateView mUpdateView { nullptr };
    WorkerScene mScene = SCENE_NONE;
    uint32_t mAlignSize = NO_1;
    uint32_t mTimeOut = NO_60;
    bool mEnableCrc { false };
    BioQosPtr mBioQos = nullptr;
    uint8_t *mDataMsgMemAddr = nullptr;
    uint64_t mDataMsgMemSize = 0;
    int32_t mDataMsgMemFd = -1;
    uint64_t mDataMsgMemBlockSize = NO_4096 * NO_1024;
    MemoryRegion mDataMsgMemMr;
    NetBlockPoolPtr mDataMsgMemPool = nullptr;
    ExecutorServicePtr mBatchGetExecutor{ nullptr };
    ExecutorServicePtr mBatchExistExecutor{ nullptr };
    DEFINE_REF_COUNT_VARIABLE
};
using MirrorClientPtr = Ref<MirrorClient>;
}
}
#endif
