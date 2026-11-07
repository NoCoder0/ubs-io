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

#include <gtest/gtest.h>
#include <mockcpp/mockcpp.hpp>
#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <mutex>
#include "bio_server.h"
#include "bio_server_c.h"
#include "bio_mock.h"
#include "bio_config_instance.h"
#include "cache_slice_operator.h"
#include "wcache_manager.h"
#include "bdm_core.h"
#include "flow_task_pool.h"
#include "flow_manager.h"
#include "tracepoint.h"
#include "cache_overload_ctrl.h"
#include "test_wcache.h"

using namespace ock::bio;

static WCacheManagerPtr gWCacheManager = WCacheManager::Instance();

bool TestWCache::gSetup = false;

void TestWCache::SetUp()
{
    if (gSetup) {
        return;
    }
    gSetup = true;
    return;
}

void TestWCache::TearDown()
{
    return;
}

static constexpr uint16_t G_PT_ID = 0;
static constexpr uint16_t G_PT_V = 1;
static uint64_t g_flowId = 0;
static FlowInstance *g_flowInst = nullptr;

namespace {
// Isolate the real journal methods from the live cache/evict executors.
template<typename Tag, typename Tag::Type member>
struct JournalAccessor {
    friend typename Tag::Type Member(Tag) { return member; }
};
struct JournalRing {
    using Type = std::vector<EvictLogRecord> WCacheManager::*;
    friend Type Member(JournalRing);
};
struct JournalEpoch {
    using Type = uint64_t WCacheManager::*;
    friend Type Member(JournalEpoch);
};
struct JournalDisk {
    using Type = bool WCacheManager::*;
    friend Type Member(JournalDisk);
};
struct JournalAppend {
    using Type = void (WCacheManager::*)(const Key &);
    friend Type Member(JournalAppend);
};
template struct JournalAccessor<JournalRing, &WCacheManager::mEvictLog>;
template struct JournalAccessor<JournalEpoch, &WCacheManager::mEvictLogEpoch>;
template struct JournalAccessor<JournalDisk, &WCacheManager::mHasDiskCache>;
template struct JournalAccessor<JournalAppend, &WCacheManager::AppendEvictLog>;

TEST(EvictJournalTest, IndependentCursorsWrapGapAndRestart)
{
    constexpr size_t capacity = 3;
    constexpr uint64_t epoch = 73;
    WCacheManager manager;
    manager.*Member(JournalDisk{}) = false;
    manager.*Member(JournalEpoch{}) = epoch;
    (manager.*Member(JournalRing{})).resize(capacity);
    auto append = [&](const char *value) {
        Key key = const_cast<char *>(value);
        (manager.*Member(JournalAppend{}))(key);
    };
    EvictLogResponse first{};
    ASSERT_EQ(manager.ReadEvictLog({0, 0, 0, 0}, first), BIO_OK);
    EXPECT_EQ(first.epoch, epoch);
    EXPECT_EQ(first.nextSeq, 1U);
    append("one");
    append("two");
    ASSERT_EQ(manager.ReadEvictLog({epoch, 0, 1, 0}, first), BIO_OK);
    ASSERT_EQ(first.count, 1U);
    EXPECT_STREQ(first.records[0].key, "one");
    EvictLogResponse second{};
    ASSERT_EQ(manager.ReadEvictLog({epoch, 0, 2, 0}, second), BIO_OK);
    EXPECT_EQ(second.count, 2U); // Reading does not consume another reader's records.
    append("three");
    append("four");
    ASSERT_EQ(manager.ReadEvictLog({epoch, 0, 2, 0}, first), BIO_OK);
    EXPECT_EQ(first.status, EVICT_LOG_GAP);
    EXPECT_EQ(first.count, 0U);
    EXPECT_EQ(first.firstSeq, 2U);
    ASSERT_EQ(manager.ReadEvictLog({epoch, 1, 3, 0}, second), BIO_OK);
    ASSERT_EQ(second.count, 3U);
    EXPECT_STREQ(second.records[0].key, "two");
    EXPECT_STREQ(second.records[2].key, "four");
    ASSERT_EQ(manager.ReadEvictLog({epoch + 1, 4, 1, 0}, first), BIO_OK);
    EXPECT_EQ(first.status, EVICT_LOG_EPOCH_CHANGED);
    ASSERT_EQ(manager.ReadEvictLog({epoch, second.nextSeq, 1, 0}, first), BIO_OK);
    EXPECT_EQ(first.status, EVICT_LOG_GAP); // A future cursor must not silently succeed.
    EXPECT_EQ(manager.ReadEvictLog({epoch, 0, EVICT_LOG_BATCH_MAX + 1, 0}, first), BIO_INVALID_PARAM);
    EXPECT_EQ(manager.ReadEvictLog({epoch, 0, 0, 1}, first), BIO_INVALID_PARAM);
    manager.*Member(JournalDisk{}) = true;
    EXPECT_EQ(manager.ReadEvictLog({0, 0, 0, 0}, first), BIO_NOT_READY);
}

class DaemonConfigGuard {
public:
    DaemonConfigGuard()
    {
        auto &config = const_cast<BioConfig::DaemonConfig &>(BioConfig::Instance()->GetDaemonConfig());
        mConfig = &config;
        mOrigin = config;
    }

    ~DaemonConfigGuard()
    {
        *mConfig = mOrigin;
    }

    BioConfig::DaemonConfig &Get()
    {
        return *mConfig;
    }

private:
    BioConfig::DaemonConfig *mConfig{ nullptr };
    BioConfig::DaemonConfig mOrigin;
};
}

static auto reader = [](const SlicePtr &from, const SlicePtr &to) -> BResult {
    CacheSliceOperator sliceOperator;
    auto ret = sliceOperator.Copy(from, to);
    EXPECT_EQ(ret, BIO_OK);
    return ret;
};

static auto wWriter = [](const SlicePtr &from, const SlicePtr &to) -> BResult {
    CacheSliceOperator sliceOperator;
    auto ret = sliceOperator.Copy(from, to);
    return ret;
};

static WCacheSlicePtr gWcacheSlice;
static BResult GetSlice(uint64_t flowId, uint64_t flowOffset, uint64_t length)
{
    SliceKey sliceKey(flowId, flowOffset, FLOW_MEMORY, length, 0);
    return gWCacheManager->GetWCacheSlice(sliceKey, gWcacheSlice);
}

struct MetaEventCollector {
    std::mutex lock;
    std::condition_variable cv;
    std::vector<UbsIoMetaEvent> events;
};

static bool WaitMetaEvents(MetaEventCollector &collector, size_t count)
{
    std::unique_lock<std::mutex> lock(collector.lock);
    return collector.cv.wait_for(lock, std::chrono::seconds(5), [&collector, count]() {
        return collector.events.size() >= count;
    });
}

struct CMetaEventCollector {
    std::mutex lock;
    std::condition_variable cv;
    std::vector<UbsioMetaEventC> events;
    std::vector<std::string> keys;
};

static void CollectCMetaEvents(void *context, const UbsioMetaEventC *events, uint32_t count)
{
    auto *collector = static_cast<CMetaEventCollector *>(context);
    if (collector == nullptr || events == nullptr) {
        return;
    }

    std::lock_guard<std::mutex> lock(collector->lock);
    for (uint32_t i = 0; i < count; ++i) {
        collector->events.push_back(events[i]);
        collector->keys.emplace_back(events[i].key, events[i].keyLen);
    }
    collector->cv.notify_one();
}

static bool WaitCMetaEvents(CMetaEventCollector &collector, size_t count)
{
    std::unique_lock<std::mutex> lock(collector.lock);
    return collector.cv.wait_for(lock, std::chrono::seconds(5), [&collector, count]() {
        return collector.events.size() >= count;
    });
}

TEST_F(TestWCache, test_create_flow_return_ok)
{
    LOG_INFO("test_create_flow_return_ok");
    bool isDegrade = false;
    uint64_t cacheId = 123;
    auto ret = Cache::Instance().AllocateFlowId(cacheId, G_PT_ID, G_PT_V, g_flowId);
    EXPECT_EQ(ret, BIO_OK);

    ret = Cache::Instance().CreateWCache(cacheId, G_PT_ID, G_PT_V, g_flowId, isDegrade);
    EXPECT_EQ(ret, BIO_OK);

    ret = Cache::Instance().CreateRCache(G_PT_ID, G_PT_V);
    EXPECT_EQ(ret, BIO_OK);

    g_flowInst = new FlowInstance(g_flowId, G_PT_V, isDegrade);
    EXPECT_NE(g_flowInst, nullptr);
}

TEST_F(TestWCache, test_get_slice_param_invalid)
{
    LOG_INFO("test_get_slice_param_invalid");
    SliceKey sliceKey(NO_MAX_VALUE64, 0, FLOW_MEMORY, NO_1024, 0);
    WCacheSlicePtr wSlice = nullptr;
    auto ret = gWCacheManager->GetWCacheSlice(sliceKey, wSlice);
    EXPECT_EQ(ret, BIO_INVALID_PARAM);
    EXPECT_EQ(wSlice, nullptr);

    sliceKey.flowId = g_flowId;
    sliceKey.flowOffset = NO_MAX_VALUE64;
    sliceKey.length = 0;
    ret = gWCacheManager->GetWCacheSlice(sliceKey, wSlice);
    EXPECT_EQ(ret, BIO_INVALID_PARAM);
    EXPECT_EQ(wSlice, nullptr);

    sliceKey.flowOffset = 0;
    sliceKey.length = NO_MAX_VALUE64;
    ret = gWCacheManager->GetWCacheSlice(sliceKey, wSlice);
    EXPECT_EQ(ret, BIO_INVALID_PARAM);
    EXPECT_EQ(wSlice, nullptr);
}

TEST_F(TestWCache, test_meta_event_batch_flush_and_closed_batch)
{
    LOG_INFO("test_meta_event_batch_flush_and_closed_batch");
    MetaEventCollector collector;
    gWCacheManager->RegUbsIoMetaEventCallback([&collector](const std::vector<UbsIoMetaEvent> &events) {
        std::lock_guard<std::mutex> lock(collector.lock);
        collector.events.insert(collector.events.end(), events.begin(), events.end());
        collector.cv.notify_one();
    });

    auto batch = std::make_shared<UbsIoMetaEventBatch>();
    ASSERT_NE(batch, nullptr);
    gWCacheManager->AppendMetaEvent(UBSIO_META_RECOVER, "test_meta_event_recover", batch);
    gWCacheManager->AppendMetaEvent(UBSIO_META_DELETE, "test_meta_event_delete", batch);
    gWCacheManager->FlushMetaEventBatch(batch);

    // Delayed SetSlice callbacks may run after the worker-local batch is closed.
    // Those events must bypass the closed batch and still be reported through manager pending queue.
    gWCacheManager->AppendMetaEvent(UBSIO_META_DELETE, "test_meta_event_after_close", batch);

    bool gotEvents = WaitMetaEvents(collector, 3);
    gWCacheManager->RegUbsIoMetaEventCallback(nullptr);
    ASSERT_TRUE(gotEvents);

    std::lock_guard<std::mutex> lock(collector.lock);
    auto hasRecover = std::find_if(collector.events.begin(), collector.events.end(),
        [](const UbsIoMetaEvent &event) {
            return event.type == UBSIO_META_RECOVER && event.key == "test_meta_event_recover";
        });
    auto hasDelete = std::find_if(collector.events.begin(), collector.events.end(),
        [](const UbsIoMetaEvent &event) {
            return event.type == UBSIO_META_DELETE && event.key == "test_meta_event_delete";
        });
    auto hasDelayedDelete = std::find_if(collector.events.begin(), collector.events.end(),
        [](const UbsIoMetaEvent &event) {
            return event.type == UBSIO_META_DELETE && event.key == "test_meta_event_after_close";
        });
    EXPECT_NE(hasRecover, collector.events.end());
    EXPECT_NE(hasDelete, collector.events.end());
    EXPECT_NE(hasDelayedDelete, collector.events.end());
}

TEST_F(TestWCache, test_meta_event_c_abi_callback)
{
    LOG_INFO("test_meta_event_c_abi_callback");
    CMetaEventCollector collector;
    auto ret = UbsioRegisterMetaEventCallback(CollectCMetaEvents, &collector);
    EXPECT_EQ(ret, BIO_OK);

    gWCacheManager->AppendMetaEvent(UBSIO_META_RECOVER, "test_c_abi_recover");
    bool gotEvents = WaitCMetaEvents(collector, 1);

    ret = UbsioRegisterMetaEventCallback(nullptr, nullptr);
    EXPECT_EQ(ret, BIO_OK);
    ASSERT_TRUE(gotEvents);

    std::lock_guard<std::mutex> lock(collector.lock);
    ASSERT_GE(collector.events.size(), 1U);
    EXPECT_EQ(collector.events[0].type, UBSIO_META_RECOVER_C);
    EXPECT_EQ(collector.events[0].keyLen, strlen("test_c_abi_recover"));
    EXPECT_EQ(collector.keys[0], "test_c_abi_recover");
}

TEST_F(TestWCache, test_put_case_return_ok)
{
    LOG_INFO("test_put_case_return_ok");
    uint64_t length = NO_1024;
    uint64_t flowIndex = 0;
    uint64_t flowOffset = g_flowInst->AllocOffset(length, flowIndex);
    SliceKey sliceKey(g_flowId, flowOffset, FLOW_MEMORY, length, flowIndex);
    WCacheSlicePtr wSlice = nullptr;
    auto ret = gWCacheManager->GetWCacheSlice(sliceKey, wSlice);
    EXPECT_EQ(ret, BIO_OK);

    Key key = "test_put_case_return_ok";
    CacheAttr attr = { 0, LOCAL_AFFINITY, WRITE_BACK };
    ret = gWCacheManager->Put(key, wSlice, reader, attr, false);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestWCache, test_batch_get_releases_unclaimed_slice_ref)
{
    uint64_t length = NO_1024;
    uint64_t flowIndex = 0;
    uint64_t flowOffset = g_flowInst->AllocOffset(length, flowIndex);
    SliceKey sliceKey(g_flowId, flowOffset, FLOW_MEMORY, length, flowIndex);
    WCacheSlicePtr wSlice = nullptr;
    ASSERT_EQ(gWCacheManager->GetWCacheSlice(sliceKey, wSlice), BIO_OK);

    Key key = "test_batch_get_releases_unclaimed_slice_ref";
    CacheAttr attr = { 0, LOCAL_AFFINITY, WRITE_BACK };
    ASSERT_EQ(gWCacheManager->Put(key, wSlice, reader, attr, false), BIO_OK);

    std::vector<char> value(length);
    MrInfo mrInfo = { reinterpret_cast<uint64_t>(value.data()), static_cast<uint32_t>(value.size()) };
    std::vector<FlowAddr> addrs = { FlowAddr(mrInfo) };
    RCacheSlicePtr dest = MakeRef<RCacheSlice>(G_PT_ID, length, addrs);
    WCacheSliceRefPtr observed = nullptr;
    WCacheBatchSliceWriter writer =
        [&observed](const SlicePtr &from, const SlicePtr &to, WCacheSliceRefPtr &sliceRef) -> BResult {
        observed = sliceRef;
        CacheSliceOperator sliceOperator;
        return sliceOperator.Copy(from, to);
    };

    uint64_t realLen = 0;
    EXPECT_EQ(gWCacheManager->GetBatch(key, 0, dest, writer, realLen), BIO_OK);
    ASSERT_NE(observed, nullptr);
    EXPECT_FALSE(observed->Test());
    EXPECT_EQ(realLen, length);
}

TEST_F(TestWCache, test_batch_get_transfers_slice_ref_to_writer)
{
    uint64_t length = NO_1024;
    uint64_t flowIndex = 0;
    uint64_t flowOffset = g_flowInst->AllocOffset(length, flowIndex);
    SliceKey sliceKey(g_flowId, flowOffset, FLOW_MEMORY, length, flowIndex);
    WCacheSlicePtr wSlice = nullptr;
    ASSERT_EQ(gWCacheManager->GetWCacheSlice(sliceKey, wSlice), BIO_OK);

    Key key = "test_batch_get_transfers_slice_ref_to_writer";
    CacheAttr attr = { 0, LOCAL_AFFINITY, WRITE_BACK };
    ASSERT_EQ(gWCacheManager->Put(key, wSlice, reader, attr, false), BIO_OK);

    std::vector<char> value(length);
    MrInfo mrInfo = { reinterpret_cast<uint64_t>(value.data()), static_cast<uint32_t>(value.size()) };
    std::vector<FlowAddr> addrs = { FlowAddr(mrInfo) };
    RCacheSlicePtr dest = MakeRef<RCacheSlice>(G_PT_ID, length, addrs);
    WCacheSliceRefPtr lease = nullptr;
    WCacheBatchSliceWriter writer =
        [&lease](const SlicePtr &, const SlicePtr &, WCacheSliceRefPtr &sliceRef) -> BResult {
        lease = std::move(sliceRef);
        return BIO_OK;
    };

    uint64_t realLen = 0;
    EXPECT_EQ(gWCacheManager->GetBatch(key, 0, dest, writer, realLen), BIO_OK);
    ASSERT_NE(lease, nullptr);
    EXPECT_TRUE(lease->Test());
    lease->Release();
    EXPECT_FALSE(lease->Test());
}

TEST_F(TestWCache, test_put_without_disk_cache_only_supports_write_back)
{
    LOG_INFO("test_put_without_disk_cache_only_supports_write_back");
    DaemonConfigGuard configGuard;
    auto &config = configGuard.Get();
    config.hasDiskCache = false;
    config.memCap = NO_MAX_VALUE64 / NO_10;
    config.memReadRatio = NO_5;
    config.memWriteRatio = NO_5;
    config.wcacheMemEvictLevel = NO_100;
    config.diskCaps.clear();

    uint64_t flowId = 0;
    auto ret = gWCacheManager->AllocateFlowId(G_PT_ID, G_PT_V, flowId);
    EXPECT_EQ(ret, BIO_OK);
    ASSERT_NE(flowId, 0);
    ret = gWCacheManager->CreateWCache(0, flowId, G_PT_ID, G_PT_V, 0, false);
    EXPECT_EQ(ret, BIO_OK);

    uint64_t flowIndex = 0;
    SliceKey sliceKey(flowId, 0, FLOW_MEMORY, NO_1024, flowIndex);
    WCacheSlicePtr wSlice = nullptr;
    ret = gWCacheManager->GetWCacheSlice(sliceKey, wSlice);
    EXPECT_EQ(ret, BIO_OK);
    ASSERT_NE(wSlice, nullptr);

    Key key = "test_put_without_disk_cache_write_back";
    CacheAttr writeBackAttr = { 0, LOCAL_AFFINITY, WRITE_BACK };
    ret = gWCacheManager->Put(key, wSlice, reader, writeBackAttr, false);
    EXPECT_EQ(ret, BIO_OK);
    EXPECT_EQ(writeBackAttr.ioStrategy, WRITE_MEM_BACK);

    SliceKey writeThroughSliceKey(flowId, NO_1024, FLOW_MEMORY, NO_1024, 1);
    WCacheSlicePtr writeThroughSlice = nullptr;
    ret = gWCacheManager->GetWCacheSlice(writeThroughSliceKey, writeThroughSlice);
    EXPECT_EQ(ret, BIO_OK);
    ASSERT_NE(writeThroughSlice, nullptr);

    Key writeThroughKey = "test_put_without_disk_cache_write_through";
    CacheAttr writeThroughAttr = { 0, LOCAL_AFFINITY, WRITE_THROUGH };
    ret = gWCacheManager->Put(writeThroughKey, writeThroughSlice, reader, writeThroughAttr, false);
    EXPECT_EQ(ret, BIO_INVALID_PARAM);
}

TEST_F(TestWCache, test_put_state_not_normal_case_return_fail)
{
    LOG_INFO("test_put_state_not_normal_case_return_fail");
    NetMrInfo bioMrInfo;
    auto ret = BioServer::Instance()->MemAlloc(NO_1024, bioMrInfo);
    EXPECT_EQ(ret, BIO_OK);
    MrInfo mrInfo = { bioMrInfo.address, static_cast<uint32_t>(bioMrInfo.size) };
    std::vector<FlowAddr> addrVec = { FlowAddr(mrInfo) };
    WCacheSlicePtr wSlice = MakeRef<WCacheSlice>(g_flowId, 0, 1, NO_1024, addrVec);
    EXPECT_NE(wSlice, nullptr);

    Key key = "test_put_state_not_normal_case_return_fail";
    CacheAttr attr = { 0, LOCAL_AFFINITY, WRITE_BACK };
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "WCACHE_STATE_NOT_NORMAL", 0, 1, userParam);
    ret = gWCacheManager->Put(key, wSlice, reader, attr, false);
    EXPECT_EQ(ret, BIO_INNER_RETRY);
    BioHvsDeactiveTracePoint(0, "WCACHE_STATE_NOT_NORMAL");

    ret = gWCacheManager->Delete(G_PT_ID, key);
    EXPECT_EQ(ret, BIO_NOT_EXISTS);
    BioServer::Instance()->MemFree(mrInfo.address);
}

TEST_F(TestWCache, test_put_wcache_put_err_case_return_fail)
{
    LOG_INFO("test_put_wcache_put_err_case_return_fail");
    NetMrInfo bioMrInfo;
    auto ret = BioServer::Instance()->MemAlloc(NO_1024, bioMrInfo);
    EXPECT_EQ(ret, BIO_OK);
    MrInfo mrInfo = { bioMrInfo.address, static_cast<uint32_t>(bioMrInfo.size) };
    std::vector<FlowAddr> addrVec = { FlowAddr(mrInfo) };
    WCacheSlicePtr wSlice = MakeRef<WCacheSlice>(g_flowId, 0, 1, NO_1024, addrVec);
    EXPECT_NE(wSlice, nullptr);

    CacheAttr attr = { 0, LOCAL_AFFINITY, WRITE_BACK };
    Key key = "test_put_wcache_put_err_case_return_fail";
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "WCACHE_PUT_FAIL", 0, 1, userParam);
    ret = gWCacheManager->Put(key, wSlice, reader, attr, false);
    EXPECT_EQ(ret, BIO_ERR);
    BioHvsDeactiveTracePoint(0, "WCACHE_PUT_FAIL");

    ret = gWCacheManager->Delete(G_PT_ID, key);
    EXPECT_EQ(ret, BIO_NOT_EXISTS);
    BioServer::Instance()->MemFree(mrInfo.address);
}

TEST_F(TestWCache, test_put_repeat_case_return_ok)
{
    LOG_INFO("test_put_repeat_case_return_ok");
    uint64_t length = NO_1024;
    uint64_t flowIndex = 0;
    uint64_t flowOffset = g_flowInst->AllocOffset(length, flowIndex);
    SliceKey sliceKey(g_flowId, flowOffset, FLOW_MEMORY, length, flowIndex);
    WCacheSlicePtr wSlice = nullptr;
    auto ret = gWCacheManager->GetWCacheSlice(sliceKey, wSlice);
    EXPECT_EQ(ret, BIO_OK);

    Key key = "test_put_repeat_case_return_ok";
    CacheAttr attr = { 0, LOCAL_AFFINITY, WRITE_BACK };
    ret = gWCacheManager->Put(key, wSlice, reader, attr, false);
    EXPECT_EQ(ret, BIO_OK);

    ret = gWCacheManager->Put(key, wSlice, reader, attr, false);
    EXPECT_EQ(ret, BIO_OK);

    ret = gWCacheManager->Delete(G_PT_ID, key);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestWCache, test_put_nullkey_case_return_fail)
{
    LOG_INFO("test_put_nullkey_case_return_fail");
    CacheAttr attr = { 0, LOCAL_AFFINITY, WRITE_BACK };
    auto ret = gWCacheManager->Put(nullptr, gWcacheSlice, reader, attr, false);
    EXPECT_EQ(ret, BIO_INVALID_PARAM);

    Key key = "test_put_nullkey_case_return_fail";
    ret = gWCacheManager->Put(key, nullptr, reader, attr, false);
    EXPECT_EQ(ret, BIO_INVALID_PARAM);

    ret = gWCacheManager->Put(key, gWcacheSlice, nullptr, attr, false);
    EXPECT_EQ(ret, BIO_INVALID_PARAM);

    ret = gWCacheManager->Delete(G_PT_ID, key);
    EXPECT_EQ(ret, BIO_NOT_EXISTS);
}

TEST_F(TestWCache, test_put_degrate_case_return_ok)
{
    LOG_INFO("test_put_degrate_case_return_ok");
    uint64_t length = NO_1024;
    uint64_t flowIndex = 0;
    uint64_t flowOffset = g_flowInst->AllocOffset(length, flowIndex);
    SliceKey sliceKey(g_flowId, flowOffset, FLOW_MEMORY, length, flowIndex);
    WCacheSlicePtr wSlice = nullptr;
    auto ret = gWCacheManager->GetWCacheSlice(sliceKey, wSlice);
    EXPECT_EQ(ret, BIO_OK);

    gWCacheManager->SetDegradeState(wSlice, true);
    Key key = "test_put_degrate_case_return_ok";
    CacheAttr attr = { 0, LOCAL_AFFINITY, WRITE_BACK };
    ret = gWCacheManager->Put(key, wSlice, reader, attr, true);
    EXPECT_EQ(ret, BIO_OK);
    gWCacheManager->SetDegradeState(wSlice, false);

    ret = gWCacheManager->Delete(G_PT_ID, key);
    EXPECT_EQ(ret, BIO_NOT_EXISTS);
}

TEST_F(TestWCache, test_get_case_return_ok)
{
    LOG_INFO("test_get_case_return_ok");
    uint64_t length = NO_1024;
    uint64_t flowIndex = 0;
    uint64_t flowOffset = g_flowInst->AllocOffset(length, flowIndex);
    SliceKey sliceKey(g_flowId, flowOffset, FLOW_MEMORY, length, flowIndex);
    WCacheSlicePtr wSlice = nullptr;
    auto ret = gWCacheManager->GetWCacheSlice(sliceKey, wSlice);
    EXPECT_EQ(ret, BIO_OK);
    Key key = "test_get_case_return_ok";
    CacheAttr attr = { 0, LOCAL_AFFINITY, WRITE_BACK };
    ret = gWCacheManager->Put(key, wSlice, reader, attr, false);
    EXPECT_EQ(ret, BIO_OK);

    NetMrInfo bioMrInfo;
    ret = BioServer::Instance()->MemAlloc(NO_1024, bioMrInfo);
    EXPECT_EQ(ret, BIO_OK);
    MrInfo mrInfo = { bioMrInfo.address, static_cast<uint32_t>(bioMrInfo.size) };
    std::vector<FlowAddr> addrVec = { FlowAddr(mrInfo) };
    RCacheSlicePtr rcacheSlice = MakeRef<RCacheSlice>(G_PT_ID, NO_1024, addrVec);
    uint64_t realLen = 0;
    ret = gWCacheManager->Get(key, 0, rcacheSlice, wWriter, realLen);
    EXPECT_EQ(ret, BIO_OK);

    ret = gWCacheManager->Delete(G_PT_ID, key);
    EXPECT_EQ(ret, BIO_OK);
    BioServer::Instance()->MemFree(mrInfo.address);
}

TEST_F(TestWCache, test_get_offset_over_case_return_err)
{
    LOG_INFO("test_get_offset_over_case_return_err");
    uint64_t length = NO_1024;
    uint64_t flowIndex = 0;
    uint64_t flowOffset = g_flowInst->AllocOffset(length, flowIndex);
    SliceKey sliceKey(g_flowId, flowOffset, FLOW_MEMORY, length, flowIndex);
    WCacheSlicePtr wSlice = nullptr;
    auto ret = gWCacheManager->GetWCacheSlice(sliceKey, wSlice);
    EXPECT_EQ(ret, BIO_OK);
    Key key = "test_get_offset_over_case_return_err";
    CacheAttr attr = { 0, LOCAL_AFFINITY, WRITE_BACK };
    ret = gWCacheManager->Put(key, wSlice, reader, attr, false);
    EXPECT_EQ(ret, BIO_OK);

    NetMrInfo bioMrInfo;
    ret = BioServer::Instance()->MemAlloc(NO_1024, bioMrInfo);
    EXPECT_EQ(ret, BIO_OK);
    MrInfo mrInfo = { bioMrInfo.address, static_cast<uint32_t>(bioMrInfo.size) };
    std::vector<FlowAddr> addrVec = { FlowAddr(mrInfo) };
    RCacheSlicePtr rcacheSlice = MakeRef<RCacheSlice>(G_PT_ID, NO_1024, addrVec);
    uint64_t realLen = 0;
    ret = gWCacheManager->Get(key, NO_MAX_VALUE64, rcacheSlice, wWriter, realLen);
    EXPECT_EQ(ret, BIO_READ_EXCEED);

    ret = gWCacheManager->Delete(G_PT_ID, key);
    EXPECT_EQ(ret, BIO_OK);
    BioServer::Instance()->MemFree(mrInfo.address);
}

TEST_F(TestWCache, test_get_offset_err_case_return_err)
{
    LOG_INFO("test_get_offset_err_case_return_err");
    uint64_t length = NO_1024;
    uint64_t flowIndex = 0;
    uint64_t flowOffset = g_flowInst->AllocOffset(length, flowIndex);
    SliceKey sliceKey(g_flowId, flowOffset, FLOW_MEMORY, length, flowIndex);
    WCacheSlicePtr wSlice = nullptr;
    auto ret = gWCacheManager->GetWCacheSlice(sliceKey, wSlice);
    EXPECT_EQ(ret, BIO_OK);
    Key key = "test_get_offset_err_case_return_err";
    CacheAttr attr = { 0, LOCAL_AFFINITY, WRITE_BACK };
    ret = gWCacheManager->Put(key, wSlice, reader, attr, false);
    EXPECT_EQ(ret, BIO_OK);

    NetMrInfo bioMrInfo;
    ret = BioServer::Instance()->MemAlloc(NO_1024, bioMrInfo);
    EXPECT_EQ(ret, BIO_OK);
    MrInfo mrInfo = { bioMrInfo.address, static_cast<uint32_t>(bioMrInfo.size) };
    std::vector<FlowAddr> addrVec = { FlowAddr(mrInfo) };
    RCacheSlicePtr rcacheSlice = MakeRef<RCacheSlice>(G_PT_ID, NO_1024, addrVec);
    uint64_t realLen = 0;
    ret = gWCacheManager->Get(key, NO_100, rcacheSlice, wWriter, realLen);
    EXPECT_EQ(ret, BIO_INVALID_PARAM);

    ret = gWCacheManager->Delete(G_PT_ID, key);
    EXPECT_EQ(ret, BIO_OK);
    BioServer::Instance()->MemFree(mrInfo.address);
}

TEST_F(TestWCache, test_cache_get_nullkey_case_return_err)
{
    LOG_INFO("test_cache_get_nullkey_case_return_err");
    uint64_t length = NO_1024;
    uint64_t flowIndex = 0;
    uint64_t flowOffset = g_flowInst->AllocOffset(length, flowIndex);
    SliceKey sliceKey(g_flowId, flowOffset, FLOW_MEMORY, length, flowIndex);
    WCacheSlicePtr wSlice = nullptr;
    auto ret = gWCacheManager->GetWCacheSlice(sliceKey, wSlice);
    EXPECT_EQ(ret, BIO_OK);
    Key key = "test_cache_get_nullkey_case_return_err";
    CacheAttr attr = { 0, LOCAL_AFFINITY, WRITE_BACK };
    ret = gWCacheManager->Put(key, wSlice, reader, attr, false);
    EXPECT_EQ(ret, BIO_OK);

    NetMrInfo bioMrInfo2;
    ret = BioServer::Instance()->MemAlloc(NO_1024, bioMrInfo2);
    EXPECT_EQ(ret, BIO_OK);
    MrInfo mrInfo2 = { bioMrInfo2.address, static_cast<uint32_t>(bioMrInfo2.size) };
    std::vector<FlowAddr> addrVec = { FlowAddr(mrInfo2) };
    RCacheSlicePtr rcacheSlice = MakeRef<RCacheSlice>(G_PT_ID, NO_1024, addrVec);
    uint64_t realLen = 0;
    ret = gWCacheManager->Get(key, 0, rcacheSlice, wWriter, realLen);
    EXPECT_EQ(ret, BIO_OK);
    ret = gWCacheManager->Get(nullptr, 0, rcacheSlice, wWriter, realLen);
    EXPECT_EQ(ret, BIO_INVALID_PARAM);

    ret = gWCacheManager->Delete(G_PT_ID, key);
    EXPECT_EQ(ret, BIO_OK);
    BioServer::Instance()->MemFree(mrInfo2.address);
}

TEST_F(TestWCache, test_cache_get_nullslice_case_return_err)
{
    LOG_INFO("test_cache_get_nullslice_case_return_err");
    uint64_t length = NO_1024;
    uint64_t flowIndex = 0;
    uint64_t flowOffset = g_flowInst->AllocOffset(length, flowIndex);
    SliceKey sliceKey(g_flowId, flowOffset, FLOW_MEMORY, length, flowIndex);
    WCacheSlicePtr wSlice = nullptr;
    auto ret = gWCacheManager->GetWCacheSlice(sliceKey, wSlice);
    EXPECT_EQ(ret, BIO_OK);
    Key key = "test_cache_get_nullslice_case_return_err";
    CacheAttr attr = { 0, LOCAL_AFFINITY, WRITE_BACK };
    ret = gWCacheManager->Put(key, wSlice, reader, attr, false);
    EXPECT_EQ(ret, BIO_OK);

    NetMrInfo bioMrInfo2;
    ret = BioServer::Instance()->MemAlloc(NO_1024, bioMrInfo2);
    EXPECT_EQ(ret, BIO_OK);
    MrInfo mrInfo2 = { bioMrInfo2.address, static_cast<uint32_t>(bioMrInfo2.size) };
    std::vector<FlowAddr> addrVec = { FlowAddr(mrInfo2) };
    RCacheSlicePtr rcacheSlice = MakeRef<RCacheSlice>(G_PT_ID, NO_1024, addrVec);
    uint64_t realLen = 0;
    ret = gWCacheManager->Get(key, 0, rcacheSlice, wWriter, realLen);
    EXPECT_EQ(ret, BIO_OK);
    ret = gWCacheManager->Get(nullptr, 0, nullptr, wWriter, realLen);
    EXPECT_EQ(ret, BIO_INVALID_PARAM);

    ret = gWCacheManager->Delete(G_PT_ID, key);
    EXPECT_EQ(ret, BIO_OK);
    BioServer::Instance()->MemFree(mrInfo2.address);
}

TEST_F(TestWCache, test_rcache_get_rcahceptr_notexist_case_return_fail)
{
    LOG_INFO("test_rcache_get_rcahceptr_notexist_case_return_fail");
    uint64_t length = NO_1024;
    uint64_t flowIndex = 0;
    uint64_t flowOffset = g_flowInst->AllocOffset(length, flowIndex);
    SliceKey sliceKey(g_flowId, flowOffset, FLOW_MEMORY, length, flowIndex);
    WCacheSlicePtr wSlice = nullptr;
    auto ret = gWCacheManager->GetWCacheSlice(sliceKey, wSlice);
    EXPECT_EQ(ret, BIO_OK);
    Key key = "test_rcache_get_rcahceptr_notexist_case_return_fail";
    CacheAttr attr = { 0, LOCAL_AFFINITY, WRITE_BACK };
    ret = gWCacheManager->Put(key, wSlice, reader, attr, false);
    EXPECT_EQ(ret, BIO_OK);

    NetMrInfo bioMrInfo2;
    ret = BioServer::Instance()->MemAlloc(NO_1024, bioMrInfo2);
    EXPECT_EQ(ret, BIO_OK);
    MrInfo mrInfo2 = { bioMrInfo2.address, static_cast<uint32_t>(bioMrInfo2.size) };
    std::vector<FlowAddr> addrVec = { FlowAddr(mrInfo2) };
    RCacheSlicePtr rcacheSlice = MakeRef<RCacheSlice>(G_PT_ID, NO_1024, addrVec);
    uint64_t realLen = 0;
    ret = gWCacheManager->Get(key, 0, rcacheSlice, wWriter, realLen);
    EXPECT_EQ(ret, BIO_OK);
    ret = gWCacheManager->Get(nullptr, 0, nullptr, nullptr, realLen);
    EXPECT_EQ(ret, BIO_INVALID_PARAM);

    ret = gWCacheManager->Delete(G_PT_ID, key);
    EXPECT_EQ(ret, BIO_OK);
    BioServer::Instance()->MemFree(mrInfo2.address);
}

TEST_F(TestWCache, test_rcache_get_flow_offset_err_case_return_fail)
{
    LOG_INFO("test_rcache_get_flow_offset_err_case_return_fail");
    uint64_t length = NO_1024;
    uint64_t flowIndex = 0;
    uint64_t flowOffset = g_flowInst->AllocOffset(length, flowIndex);
    SliceKey sliceKey(g_flowId, flowOffset, FLOW_MEMORY, length, flowIndex);
    WCacheSlicePtr wSlice = nullptr;
    auto ret = gWCacheManager->GetWCacheSlice(sliceKey, wSlice);
    EXPECT_EQ(ret, BIO_OK);
    Key key = "test_rcache_get_flow_offset_err_case_return_fail";
    CacheAttr attr = { 0, LOCAL_AFFINITY, WRITE_BACK };
    ret = gWCacheManager->Put(key, wSlice, reader, attr, false);
    EXPECT_EQ(ret, BIO_OK);

    uint64_t realLen = 0;
    std::vector<FlowAddr> addrVec;
    RCacheSlicePtr slicePtr = MakeRef<RCacheSlice>(G_PT_ID, NO_1024, addrVec);
    sleep(NO_5);
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "WCACHE_FLOW_OFFSET_FAIL", 0, 1, userParam);
    ret = Cache::Instance().Get(key, 0, slicePtr, wWriter, realLen);
    EXPECT_EQ(ret, BIO_OK);
    BioHvsDeactiveTracePoint(0, "WCACHE_FLOW_OFFSET_FAIL");

    ret = gWCacheManager->Delete(G_PT_ID, key);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestWCache, test_cache_get_nullslicewriter_case_return_err)
{
    LOG_INFO("test_cache_get_nullslicewriter_case_return_err");
    uint64_t length = NO_1024;
    uint64_t flowIndex = 0;
    uint64_t flowOffset = g_flowInst->AllocOffset(length, flowIndex);
    SliceKey sliceKey(g_flowId, flowOffset, FLOW_MEMORY, length, flowIndex);
    WCacheSlicePtr wSlice = nullptr;
    auto ret = gWCacheManager->GetWCacheSlice(sliceKey, wSlice);
    EXPECT_EQ(ret, BIO_OK);
    Key key = "test_cache_get_nullslicewriter_case_return_err";
    CacheAttr attr = { 0, LOCAL_AFFINITY, WRITE_BACK };
    ret = gWCacheManager->Put(key, wSlice, reader, attr, false);
    EXPECT_EQ(ret, BIO_OK);

    NetMrInfo bioMrInfo;
    ret = BioServer::Instance()->MemAlloc(NO_1024, bioMrInfo);
    EXPECT_EQ(ret, BIO_OK);
    MrInfo mrInfo = { bioMrInfo.address, static_cast<uint32_t>(bioMrInfo.size) };
    std::vector<FlowAddr> addrVec = { FlowAddr(mrInfo) };
    RCacheSlicePtr rcacheSlice = MakeRef<RCacheSlice>(G_PT_ID, NO_1024, addrVec);
    uint64_t realLen = 0;
    ret = gWCacheManager->Get(key, 0, rcacheSlice, nullptr, realLen);
    EXPECT_EQ(ret, BIO_INVALID_PARAM);

    ret = gWCacheManager->Delete(G_PT_ID, key);
    EXPECT_EQ(ret, BIO_OK);
    BioServer::Instance()->MemFree(mrInfo.address);
}

TEST_F(TestWCache, test_stat_case_return_ok)
{
    LOG_INFO("test_stat_case_return_ok");
    uint64_t length = NO_1024;
    uint64_t flowIndex = 0;
    uint64_t flowOffset = g_flowInst->AllocOffset(length, flowIndex);
    SliceKey sliceKey(g_flowId, flowOffset, FLOW_MEMORY, length, flowIndex);
    WCacheSlicePtr wSlice = nullptr;
    auto ret = gWCacheManager->GetWCacheSlice(sliceKey, wSlice);
    EXPECT_EQ(ret, BIO_OK);
    Key key = "test_stat_case_return_ok";
    CacheAttr attr = { 0, LOCAL_AFFINITY, WRITE_BACK };
    ret = gWCacheManager->Put(key, wSlice, reader, attr, false);
    EXPECT_EQ(ret, BIO_OK);

    CacheObjStat objState;
    ret = gWCacheManager->Stat(G_PT_ID, key, objState);
    EXPECT_EQ(ret, BIO_OK);

    ret = gWCacheManager->Delete(G_PT_ID, key);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestWCache, test_list_prefix_null_case_return_ok)
{
    LOG_INFO("test_list_prefix_null_case_return_ok");
    std::unordered_map<std::string, CacheObjStat> objs;
    CacheObjStat stat;
    for (int i = 0; i < NO_1024; i++) {
        objs.emplace("test" + std::to_string(i), stat);
    }
    auto ret = Cache::Instance().List(nullptr, 0, true, objs);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestWCache, test_list_prefix_null_underfs_fail_case_return_ok)
{
    LOG_INFO("test_list_prefix_null_underfs_fail_case_return_ok");
    std::unordered_map<std::string, CacheObjStat> objs;
    CacheObjStat stat;
    for (int i = 0; i < NO_10; i++) {
        objs.emplace("test" + std::to_string(i), stat);
    }
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "UNDERFS_INIT_FAIL", 0, 1, userParam);
    auto ret = Cache::Instance().List(nullptr, 0, true, objs);
    EXPECT_EQ(ret, BIO_ERR);
    BioHvsDeactiveTracePoint(0, "UNDERFS_INIT_FAIL");
}

TEST_F(TestWCache, test_delete_wcache_flowid_notexist_return_ok)
{
    LOG_INFO("test_delete_wcache_flowid_notexist_return_ok");
    uint64_t invalidFlowId = NO_1024;
    auto ret = gWCacheManager->DeleteWCache(invalidFlowId);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestWCache, test_wcache_alloc_case_return_fail)
{
    LOG_INFO("test_wcache_alloc_case_return_fail");
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "WCACHE_ALLOC_FAIL", 0, 1, userParam);
    uint16_t ptId = 1;
    auto ret = Cache::Instance().CreateWCache(0, ptId, 0, 0, false);
    EXPECT_EQ(ret, BIO_ALLOC_FAIL);
    BioHvsDeactiveTracePoint(0, "WCACHE_ALLOC_FAIL");
}

TEST_F(TestWCache, test_cache_delete_keynull_case_return_err)
{
    LOG_INFO("test_cache_delete_keynull_case_return_err");
    Key key = nullptr;
    auto ret = Cache::Instance().Delete(G_PT_ID, key);
    EXPECT_EQ(ret, BIO_INVALID_PARAM);
}

TEST_F(TestWCache, test_flush_return_err)
{
    LOG_INFO("test_flush_return_err");
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "NO_PROCESS_CLEAR_OLD_CACHE", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_FLUSH", 0, 1, userParam);
    SyncDataRequest req = { { MESSAGE_MAGIC, 1, 1, 1, getpid() } };
    auto ret = MirrorServer::Instance()->SyncData(req);
    EXPECT_EQ(ret, BIO_INNER_ERR);
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_CLEAR_OLD_CACHE");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_FLUSH");
}

TEST_F(TestWCache, test_expired_flush_return_ok)
{
    LOG_INFO("test_expired_flush_return_ok");
    auto ret = Cache::Instance().ExpiredClear(G_PT_ID, G_PT_V);
    EXPECT_EQ(ret, BIO_OK);
    ret = gWCacheManager->ExpiredClear(G_PT_ID, G_PT_V);
}

TEST_F(TestWCache, test_wcache_destroy_case_return_ok)
{
    LOG_INFO("test_wcache_destroy_case_return_ok");
    BioTracepointParam userParam;
    auto ret = Cache::Instance().DestroyWCache(0, G_PT_ID, G_PT_V, g_flowId);
    EXPECT_EQ(ret, BIO_OK);

    ret = Cache::Instance().DestroyRCache(G_PT_ID);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestWCache, test_wcache_destroy_flowid_unexist_return_ok)
{
    LOG_INFO("test_wcache_destroy_flowid_unexist_return_ok");
    auto ret = Cache::Instance().DestroyWCache(0, G_PT_ID, G_PT_V, NO_1024);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestWCache, test_wcache_destroy_flowid_err_return_ok)
{
    LOG_INFO("test_wcache_destroy_flowid_err_return_ok");
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "NO_PROCESS_WCACHE_MANAGER_EMPTY_EVICT", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "WCACHE_HANDLE_BROCK_FLOWID_FAIL", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "HANDLE_CACHE_BROKE_OK", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_DESTROY_EVICT_THREAD", 0, 1, userParam);
    auto ret = Cache::Instance().DestroyWCache(0, 0, 0, g_flowId);
    EXPECT_EQ(ret, BIO_OK);
    sleep(NO_5);
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_WCACHE_MANAGER_EMPTY_EVICT");
    BioHvsDeactiveTracePoint(0, "WCACHE_HANDLE_BROCK_FLOWID_FAIL");
    BioHvsDeactiveTracePoint(0, "HANDLE_CACHE_BROKE_OK");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_DESTROY_EVICT_THREAD");
}

TEST_F(TestWCache, test_wcache_destroy_flush_return_ok)
{
    LOG_INFO("test_wcache_destroy_flush_return_ok");
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "NO_PROCESS_WCACHE_MANAGER_EMPTY_EVICT", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_WCACHE_FLUSH", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_DESTROY_EVICT_THREAD", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "HANDLE_CACHE_BROKE_OK", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "WCACHE_HANDLE_BROCK_FLUSH", 0, 1, userParam);
    auto ret = Cache::Instance().DestroyWCache(0, 0, 0, g_flowId);
    EXPECT_EQ(ret, BIO_OK);
    sleep(NO_5);
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_WCACHE_MANAGER_EMPTY_EVICT");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_WCACHE_FLUSH");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_DESTROY_EVICT_THREAD");
    BioHvsDeactiveTracePoint(0, "HANDLE_CACHE_BROKE_OK");
    BioHvsDeactiveTracePoint(0, "WCACHE_HANDLE_BROCK_FLUSH");
}

TEST_F(TestWCache, test_wcache_destroy_expire_return_ok)
{
    LOG_INFO("test_wcache_destroy_expire_return_ok");
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "NO_PROCESS_WCACHE_MANAGER_EMPTY_EVICT", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_WCACHE_MANAGER_EXPIRED_CLEAR", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_DESTROY_EVICT_THREAD", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "HANDLE_CACHE_BROKE_OK", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "WCACHE_HANDLE_BROCK_EXPIRED_CLEAR", 0, 1, userParam);
    auto ret = Cache::Instance().DestroyWCache(0, 0, 0, g_flowId);
    EXPECT_EQ(ret, BIO_OK);
    sleep(NO_5);
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_WCACHE_MANAGER_EMPTY_EVICT");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_WCACHE_MANAGER_EXPIRED_CLEAR");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_DESTROY_EVICT_THREAD");
    BioHvsDeactiveTracePoint(0, "HANDLE_CACHE_BROKE_OK");
    BioHvsDeactiveTracePoint(0, "WCACHE_HANDLE_BROCK_EXPIRED_CLEAR");
}

TEST_F(TestWCache, test_handle_proc_broken_err)
{
    LOG_INFO("test_handle_proc_broken_err");
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "HANDLE_PROC_BROKEN_FAIL", 0, 1, userParam);
    auto ret = Cache::Instance().HandleProcBroken(NO_1024);
    EXPECT_EQ(ret, BIO_ERR);
    BioHvsDeactiveTracePoint(0, "HANDLE_PROC_BROKEN_FAIL");
}

TEST_F(TestWCache, test_handle_proc_broken_expired_err)
{
    LOG_INFO("test_handle_proc_broken_expired_err");
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "NO_PROCESS_WCACHE_FLUSH", 0, NO_10, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_WCACHE_EXPIRED_CLEAR", 0, NO_10, userParam);
    BioHvsActiveTracePoint(0, "WCACHE_HANDLE_PROC_BROCK_EXPIRED_CLEAR", 0, NO_10, userParam);
    BioHvsActiveTracePoint(0, "HANDLE_PROC_BROKE_OK", 0, NO_10, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_CLEAR_PROC_CACHE", 0, NO_10, userParam);
    auto ret = WCacheManager::Instance()->HandleProcBrokenHdl(0);
    EXPECT_EQ(ret, BIO_OK);
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_WCACHE_EXPIRED_CLEAR");
    BioHvsDeactiveTracePoint(0, "WCACHE_HANDLE_PROC_BROCK_EXPIRED_CLEAR");
    BioHvsDeactiveTracePoint(0, "HANDLE_PROC_BROKE_OK");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_CLEAR_PROC_CACHE");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_WCACHE_FLUSH");
}

TEST_F(TestWCache, test_handle_proc_broken_flush_err)
{
    LOG_INFO("test_handle_proc_broken_flush_err");
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "NO_PROCESS_WCACHE_EXPIRED_CLEAR", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_WCACHE_FLUSH", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "WCACHE_HANDLE_PROC_BROCK_EXPIRED_CLEAR", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "HANDLE_PROC_BROKE_OK", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_CLEAR_PROC_CACHE", 0, 1, userParam);
    auto ret = WCacheManager::Instance()->HandleProcBrokenHdl(0);
    EXPECT_EQ(ret, BIO_OK);
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_WCACHE_FLUSH");
    BioHvsDeactiveTracePoint(0, "WCACHE_HANDLE_PROC_BROCK_EXPIRED_CLEAR");
    BioHvsDeactiveTracePoint(0, "HANDLE_PROC_BROKE_OK");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_CLEAR_PROC_CACHE");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_WCACHE_EXPIRED_CLEAR");
}

TEST_F(TestWCache, test_handle_proc_broken_role_err)
{
    LOG_INFO("test_handle_proc_broken_role_err");
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "NO_PROCESS_WCACHE_EXPIRED_CLEAR", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_WCACHE_FLUSH", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "WCACHE_HANDLE_PROC_BROCK_ROLE_ERR", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "HANDLE_PROC_BROKE_OK", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_CLEAR_PROC_CACHE", 0, 1, userParam);
    auto ret = WCacheManager::Instance()->HandleProcBrokenHdl(0);
    EXPECT_EQ(ret, BIO_OK);
    BioHvsDeactiveTracePoint(0, "WCACHE_HANDLE_PROC_BROCK_ROLE_ERR");
    BioHvsDeactiveTracePoint(0, "HANDLE_PROC_BROKE_OK");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_CLEAR_PROC_CACHE");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_WCACHE_FLUSH");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_WCACHE_EXPIRED_CLEAR");
}

TEST_F(TestWCache, test_start_pool_threadnum_incorrect_return_fail)
{
    LOG_INFO("test_start_pool_threadnum_incorrect_return_fail");
    FlowTaskPoolPtr flowTaskPool = MakeRef<FlowTaskPool>("testflow");
    auto ret = flowTaskPool->Start(0, NO_16);
    EXPECT_EQ(ret, BIO_ALLOC_FAIL);
}

TEST_F(TestWCache, test_start_pool_return_ok)
{
    LOG_INFO("test_start_pool_return_ok");
    FlowTaskPoolPtr flowTaskPool = MakeRef<FlowTaskPool>("testflow");
    auto ret = flowTaskPool->Start(NO_8, NO_16);
    EXPECT_EQ(ret, BIO_OK);
    ret = flowTaskPool->Start(NO_8, NO_16);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestWCache, test_service_update_return_retry)
{
    LOG_INFO("test_service_update_return_retry");
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "NO_PROCESS_UPGRADE_FLUSH", 0, 1, userParam);
    auto ret = gWCacheManager->ServiceUngradeFlush();
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_UPGRADE_FLUSH");
    EXPECT_EQ(ret, BIO_OK);
}

void TestWCache::Stub()
{
    MOCKER_CPP(&WCache::IsEmptyEvict, bool (*)()).stubs().will(returnValue(false));
}

static int32_t BdmGetNextUsedChunkIdStub(uint32_t bdmId, uint64_t *chunkId, uint64_t *chunkSize, uint64_t *bucketId,
    uint64_t *bucketOffset)
{
    *chunkId = 0;
    *chunkSize = NO_4194304;
    *bucketId = g_flowId;
    *bucketOffset = 0;
    return BIO_OK;
}

void TestWCache::RecoverStub()
{
    MOCKER(BdmGetNextUsedChunkId).stubs().will(invoke(BdmGetNextUsedChunkIdStub));
    MOCKER_CPP(&FlowTaskPool::Start, BResult(*)(uint32_t coreThreadNum, uint32_t queueSize))
        .stubs()
        .will(returnValue(BIO_OK));
}

TEST_F(TestWCache, test_get_evict_offset_return_ok)
{
    LOG_INFO("test_get_evict_offset_return_ok");
    uint64_t flowOffset = 0;
    auto ret = Cache::Instance().GetEvictOffset(g_flowId, flowOffset);
    EXPECT_EQ(ret, BIO_NOT_EXISTS);
}

TEST_F(TestWCache, test_get_slice_wcache_flow_offset_err_return_fail)
{
    LOG_INFO("test_get_slice_wcache_flow_offset_err_return_fail");
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "WCACHE_FLOW_OFFSET_FAIL", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "WCACHE_STATE_NORMAL", 0, 1, userParam);
    auto ret = GetSlice(g_flowId, 0, NO_1024);
    BioHvsDeactiveTracePoint(0, "WCACHE_STATE_NORMAL");
    BioHvsDeactiveTracePoint(0, "WCACHE_FLOW_OFFSET_FAIL");
    EXPECT_EQ(ret, BIO_ERR);
}

TEST_F(TestWCache, test_get_slice_wcache_hold_wait_err_return_fail)
{
    LOG_INFO("test_get_slice_wcache_hold_wait_err_return_fail");
    GetSliceRequest req = { { MESSAGE_MAGIC, 1, 1, 1, getpid() }, 1, 0, 1, NO_128 };
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "WCACHE_HOLD_WAIT_FAIL", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "WCACHE_STATE_NORMAL", 0, 1, userParam);
    auto ret = GetSlice(g_flowId, 0, NO_MAX_VALUE64-1);
    BioHvsDeactiveTracePoint(0, "WCACHE_STATE_NORMAL");
    BioHvsDeactiveTracePoint(0, "WCACHE_HOLD_WAIT_FAIL");
    EXPECT_EQ(ret, BIO_ERR);
}

TEST_F(TestWCache, test_bio_server_put_write_slice_null_reply_ok)
{
    LOG_INFO("test_bio_server_put_write_slice_null_reply_ok");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    PutRequest req;
    req.comm = { MESSAGE_MAGIC, 1, 1, 1, getpid() };
    req.tenantId = 1;
    req.affinity = 1;
    req.strategy = 1;
    CopyKey(req.key, "abcdslice", KEY_MAX_SIZE);
    req.length = NO_128;
    req.flowId = 0;
    req.mrKey = 1;
    req.sliceLen = 0;
    req.ioStrategy = 0;
    req.memFromServer = true;
    req.mrAddress = 0ULL;
    req.mrSize = 0;
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "WRITE_SLICE_NULL_FAIL", 0, 1, userParam);
    auto ret = mirror->MirrorServerPut(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
    BioHvsDeactiveTracePoint(0, "WRITE_SLICE_NULL_FAIL");
}

TEST_F(TestWCache, test_bio_olc_low_water_level)
{
    LOG_INFO("test_bio_olc_low_water_level");
    uint64_t frontWriteBw = NO_1;
    uint64_t evict2DiskBw = NO_2;
    uint32_t proc = 0;
    auto ret = CacheOverloadCtrl::Instance().LowWaterLevelQuota(frontWriteBw, evict2DiskBw, proc);
    EXPECT_EQ(proc, NO_1);

    frontWriteBw = NO_3;
    evict2DiskBw = NO_2;
    ret = CacheOverloadCtrl::Instance().LowWaterLevelQuota(frontWriteBw, evict2DiskBw, proc);
    EXPECT_EQ(proc, NO_2);

    frontWriteBw = NO_3;
    evict2DiskBw = NO_1;
    ret = CacheOverloadCtrl::Instance().LowWaterLevelQuota(frontWriteBw, evict2DiskBw, proc);
    EXPECT_EQ(proc, NO_2);

    frontWriteBw = NO_5;
    evict2DiskBw = NO_1;
    ret = CacheOverloadCtrl::Instance().LowWaterLevelQuota(frontWriteBw, evict2DiskBw, proc);
    EXPECT_EQ(proc, NO_3);
}

TEST_F(TestWCache, test_bio_olc_mid_water_level)
{
    LOG_INFO("test_bio_olc_mid_water_level");
    uint64_t frontWriteBw = NO_1;
    uint64_t evict2DiskBw = NO_2;
    uint32_t proc = 0;
    auto ret = CacheOverloadCtrl::Instance().MidWaterLevelQuota(frontWriteBw, evict2DiskBw, proc);
    EXPECT_EQ(proc, NO_1);

    frontWriteBw = NO_3;
    evict2DiskBw = NO_2;
    ret = CacheOverloadCtrl::Instance().MidWaterLevelQuota(frontWriteBw, evict2DiskBw, proc);
    EXPECT_EQ(proc, NO_2);

    frontWriteBw = NO_3;
    evict2DiskBw = NO_1;
    ret = CacheOverloadCtrl::Instance().MidWaterLevelQuota(frontWriteBw, evict2DiskBw, proc);
    EXPECT_EQ(proc, NO_2);

    frontWriteBw = NO_5;
    evict2DiskBw = NO_1;
    ret = CacheOverloadCtrl::Instance().MidWaterLevelQuota(frontWriteBw, evict2DiskBw, proc);
    EXPECT_EQ(proc, NO_3);
}

TEST_F(TestWCache, test_bio_olc_high_water_level)
{
    LOG_INFO("test_bio_olc_high_water_level");
    uint64_t frontWriteBw = NO_1;
    uint64_t evict2DiskBw = NO_2;
    uint32_t proc = 0;
    auto ret = CacheOverloadCtrl::Instance().HighWaterLevelQuota(frontWriteBw, evict2DiskBw, proc);
    EXPECT_EQ(proc, NO_1);

    frontWriteBw = NO_3;
    evict2DiskBw = NO_2;
    ret = CacheOverloadCtrl::Instance().HighWaterLevelQuota(frontWriteBw, evict2DiskBw, proc);
    EXPECT_EQ(proc, NO_2);

    frontWriteBw = NO_3;
    evict2DiskBw = NO_1;
    ret = CacheOverloadCtrl::Instance().HighWaterLevelQuota(frontWriteBw, evict2DiskBw, proc);
    EXPECT_EQ(proc, NO_2);

    frontWriteBw = NO_5;
    evict2DiskBw = NO_1;
    ret = CacheOverloadCtrl::Instance().HighWaterLevelQuota(frontWriteBw, evict2DiskBw, proc);
    EXPECT_EQ(proc, NO_2);
}

TEST_F(TestWCache, test_bio_olc_show)
{
    LOG_INFO("test_bio_olc_show");
    uint64_t vmVec = 0;
    uint64_t totalQuota = 0;
    uint64_t remainQuota = 0;
    std::unordered_map<QuotaHolder, uint64_t, QuotaHolderHash, QuotaHolderEqual> holders;
    CacheOverloadCtrl::Instance().Show(vmVec, totalQuota, remainQuota, holders);
}

TEST_F(TestWCache, test_bio_olc_recycle)
{
    LOG_INFO("test_bio_olc_recycle");
    QuotaHolder holder = { NO_1, NO_1024 };
    auto holdMap = CacheOverloadCtrl::Instance().GetHolders();
    auto iter = holdMap->find(holder);
    if (iter == holdMap->end()) {
        holdMap->emplace(holder, NO_4194304);
    }

    CacheOverloadCtrl::Instance().RecycleQuota(holder);
    iter = holdMap->find(holder);
    EXPECT_EQ(iter, holdMap->end());
}

TEST_F(TestWCache, test_wcache_tier_metaflow_error)
{
    LOG_INFO("test_wcache_tier_error");

    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "FLOW_SEAL_ERR", 0, 1, userParam);
    WCacheTier wCacheTier;
    auto ret = wCacheTier.Seal();
    EXPECT_EQ(ret, BIO_ERR);
    BioHvsDeactiveTracePoint(0, "FLOW_SEAL_ERR");
}

TEST_F(TestWCache, test_wcache_tier_dataflow_error)
{
    LOG_INFO("test_wcache_tier_error");

    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "FLOW_SEAL_ERR", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "FLOW_DATA_FLOW_ERR", 0, 1, userParam);
    WCacheTier wCacheTier;
    auto ret = wCacheTier.Seal();
    EXPECT_EQ(ret, BIO_ERR);
    BioHvsDeactiveTracePoint(0, "FLOW_SEAL_ERR");
    BioHvsDeactiveTracePoint(0, "FLOW_DATA_FLOW_ERR");
}
