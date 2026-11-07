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

#ifndef BOOSTIO_WCACHE_MANAGER_H
#define BOOSTIO_WCACHE_MANAGER_H

#include <atomic>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "bio_err.h"
#include "bio_ref.h"
#include "cache_def.h"
#include "slice.h"
#include "wcache.h"
#include "wcache_index.h"
#include "message.h"
#include "rcache_manager.h"
#include "wcache_statistic.h"

namespace ock {
namespace bio {
constexpr uint64_t WRITE_CACHE_EVICT_PERIOD = 5; // 5秒
// The writer may move sliceRef to keep the acquired WCache lease until an asynchronous read completes.
using WCacheBatchSliceWriter =
    std::function<BResult(const SlicePtr &from, const SlicePtr &to, WCacheSliceRefPtr &sliceRef)>;

class WCacheManager;
using WCacheManagerPtr = Ref<WCacheManager>;
class WCacheManager {
public:
    WCacheManager() = default;

    ~WCacheManager() = default;

    inline static WCacheManagerPtr &Instance()
    {
        static auto instance = MakeRef<WCacheManager>();
        return instance;
    }

    BResult Init(const RCacheManagerPtr &rCacheManager);

    void Exit();

public:
    BResult EvictNegotiateExecutorInit();
    BResult MemoryEvictExecutorInit();
    BResult DiskEvictExecutorInit();
    BResult GcEvictExecutorInit();
    BResult RetryEvictExecutorInit();
    BResult DelayDestroyExecutorInit();
    BResult MetaReportExecutorInit();

    BResult AllocateFlowId(uint16_t ptId, uint64_t ptv, uint64_t &flowId);

    BResult CreateWCache(uint64_t procId, uint64_t flowId, uint16_t ptId, uint64_t ptv,
        uint16_t diskId, bool isDegrade, bool isRecover = false);

    BResult DestroyWCache(uint64_t procId, uint64_t flowId, uint16_t ptId, uint64_t ptv);

    BResult DeleteWCache(uint64_t flowId);

    BResult RecoverCache(FlowPtr metaFlow);

    BResult ServiceUngradeFlush();

    BResult GetWCacheSlice(const SliceKey &sliceKey, WCacheSlicePtr &slice);

    void SetDegradeState(const WCacheSlicePtr &slice, bool flag);

    BResult Put(const Key &key, const WCacheSlicePtr &slice, const SliceReader &sliceReader, CacheAttr &attr,
        bool isDegrade);

    BResult ParseKeyAddr(const Key &key, uint16_t ptId, BatchKeyAddrInfo *info);

    BResult Get(const Key &key, uint64_t offset, const RCacheSlicePtr &slice, const SliceWriter &sliceWriter,
        uint64_t &realLen);

    BResult GetBatch(const Key &key, uint64_t offset, const RCacheSlicePtr &slice,
        const WCacheBatchSliceWriter &sliceWriter, uint64_t &realLen);

    bool IsCrcEnabled() const
    {
        return mEnableCrc;
    }

    BResult Stat(uint16_t ptId, const Key &key, CacheObjStat &cacheObjStat);

    bool Exist(uint16_t ptId, const Key &key);

    BResult List(char *prefix, uint16_t ptId, std::unordered_map<std::string, CacheObjStat> &objs);

    BResult Delete(uint16_t ptId, const Key &key);

    void RegGetLocDiskStatus(GetLocDiskStatus getLocDiskStatus);

    void RegGetGlobEvictOffset(GetGlobEvictOffset evictOffset);

    void RegCheckLocRole(CheckLocRole localRole);

    void RegUbsIoMetaEventCallback(UbsIoMetaEventCallback callback);

    BResult ReadEvictLog(const EvictLogRequest &request, EvictLogResponse &response);

    void AppendMetaEvent(UbsIoMetaEventType type, const Key &key,
        const UbsIoMetaEventBatchPtr &batch = nullptr);

    void AppendMetaEvents(std::vector<UbsIoMetaEvent> &&events);

    void FlushMetaEventBatch(const UbsIoMetaEventBatchPtr &batch);

    BResult GetEvictOffset(uint64_t flowId, uint64_t &flowOffset);

    BResult Flush(uint16_t ptId, uint64_t ptv);

    BResult ExpiredClear(uint16_t ptId, uint64_t ptv);

    BResult HandleProcBroken(uint64_t procId);

    BResult HandleProcBrokenHdl(uint64_t procId);

    BResult GetTruncateIndex(uint64_t flowId, uint64_t &truncateIndex);

    BResult GetReuseFlowId(uint16_t ptId, uint64_t &flowId);

    BResult ProcBrokenSyncOldFlow(uint64_t flowId, uint64_t index, uint64_t offset, bool &needDestroy);

    WCachePtr GetWCache(uint64_t flowId);

    void HandleProcBrokenDestroyFlow(WCachePtr flow, uint32_t localNid, bool *slaveResult);

    BResult SendProcBrokenSyncRequest(WCachePtr flow, CmPtInfo cmPtInfo, uint32_t localNid,
                                      bool *slaveResult, bool &needDestroy);
    void ScanProcCache(uint64_t  procId, std::list<WCache*> &list);
    BResult HandleProcBrokenImpl(uint64_t procId, std::list<WCache*> &oldList);

    DEFINE_REF_COUNT_FUNCTIONS;

private:
    BResult Read(uint64_t offset, const WCacheSlicePtr &srcSlice, const RCacheSlicePtr &destSlice,
        const SliceWriter &sliceWriter, uint64_t &realLen);
    BResult FlushImpl(uint16_t ptId, uint64_t ptv);
    BResult ExpiredClearImpl(uint16_t ptId, uint64_t ptv);
    BResult HandleCacheBrokenHdl(uint64_t procId, uint64_t flowId);
    BResult HandleCacheBrokenImpl(WCachePtr wcache);
    BResult MasterProcBrokenSyncFlow(WCachePtr flow, CmPtInfo ptEntry, uint32_t localNid);
    void InitCallbackCtx(ProcBrokenCallbackCtx &cbCtx, uint32_t quota);

    void ScanUpgradeCache(std::list<WCachePtr> &list);
    BResult ClearUpgradeCache();

    void ScanOldCache(uint16_t ptId, uint64_t ptv, std::list<WCachePtr> &list);
    BResult ClearOldCache(uint16_t ptId, uint64_t ptv);

    BResult ClearProcCache(uint32_t procId);

    void RetryEvictThread();
    void DestroyEvictThread();
    void ScheduleFlushMetaEvents();
    void AppendEvictLog(const Key &key);
    void FlushMetaEvents();

private:
    ReadWriteLock mWCacheManagerLock;
    std::unordered_map<uint64_t, WCachePtr> mWCacheManager;
    std::vector<uint64_t> mRetryManager[MAX_WCACHE_TIER];
    std::unordered_map<uint64_t, uint64_t> mDestroyManager;
    RCacheManagerPtr mRCacheManager;

    bool mRunning = true;
    bool mEnableCrc = false;
    bool mHasDiskCache = true;

    ExecutorServicePtr mEvictService[MAX_WCACHE_TIER]{ nullptr, nullptr };
    ExecutorServicePtr mGcEvictService{ nullptr };
    ExecutorServicePtr mRetryEvictService{ nullptr };
    ExecutorServicePtr mDestroyEvictService{ nullptr };
    ExecutorServicePtr mMetaReportService{ nullptr };
    ExecutorServicePtr mMemoryEvictTransService{ nullptr };
    ExecutorServicePtr mMemoryEvictConsultService{ nullptr };

    std::mutex mMetaReportLock;
    std::vector<UbsIoMetaEvent> mPendingMetaEvents;
    std::mutex mEvictLogLock;
    std::vector<EvictLogRecord> mEvictLog;
    uint64_t mEvictLogEpoch{0};
    uint64_t mEvictLogNextSeq{1};
    std::atomic<bool> mMetaReportScheduled{ false };
    UbsIoMetaEventCallback mMetaEventCallback{ nullptr };

    GetLocDiskStatus mGetLocDiskStatus{ nullptr };
    GetGlobEvictOffset mEvictOffset{ nullptr };
    CheckLocRole mLocRole{ nullptr };

    WCacheIndexPtr mCacheIndex;

    std::unordered_map<uint16_t, std::unordered_set<uint64_t>> mReuseFlows{};
    ReadWriteLock mReuseFlowsLock;

    std::unordered_set<uint32_t> startedProc;
    Lock startedProcLock;
    DEFINE_REF_COUNT_VARIABLE;
};
}
}


#endif // BOOSTIO_WCACHE_MANAGER_H
