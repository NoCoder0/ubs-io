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

#include <cerrno>
#include "bio_log.h"
#include "bio_config_instance.h"
#include "bio_client.h"
#include "bio_server.h"
#include "bio_trace.h"
#include "bio_functions.h"
#include "message_op.h"
#include "bio_crc_util.h"
#include "cache_overload_ctrl.h"
#include "bdm_core.h"
#include "bdm_disk.h"
#include "mirror_server.h"
#include "disk_resource_monitor.h"

using namespace ock::bio;
using namespace ock::hcom;

static uint16_t GetLocalVNodeId()
{
    auto bioServer = BioServer::Instance();
    if (bioServer->IsStandaloneMode()) {
        return bioServer->GetLocalNid().VNodeId();
    }
    return Cm::Instance()->GetCmLocalNodeId().VNodeId();
}

namespace {
int WaitSemaphore(sem_t &sem)
{
    int ret = 0;
    do {
        ret = sem_wait(&sem);
    } while (ret != 0 && errno == EINTR);
    return ret == 0 ? 0 : errno;
}

size_t GetBatchGetWireResponseLen(uint32_t count)
{
    return sizeof(BatchGetWireResponse) + static_cast<size_t>(count) * sizeof(BatchGetResultItem);
}
}

bool MirrorServer::CheckMagic(RequestComm &reqComm)
{
    if (UNLIKELY(reqComm.magic != MESSAGE_MAGIC)) {
        LOG_ERROR("Check message magic failed.");
        return false;
    }
    return true;
}

bool MirrorServer::CheckAll(RequestComm &reqComm)
{
    if (UNLIKELY(reqComm.magic != MESSAGE_MAGIC)) {
        LOG_ERROR("Check message magic failed.");
        return false;
    }
    uint64_t base = BioServer::Instance()->GetPtEntry(reqComm.ptId).version;
    if (UNLIKELY(reqComm.ptv != base)) {
        LOG_WARN("Check message pt version failed, base:" << base << ", ptv:" << reqComm.ptv << ".");
        return false;
    }
    return true;
}

void MirrorServer::RegisterOpcodeStep2(NetEnginePtr &netEngine)
{
    netEngine->RegisterNewRequestHandler(BIO_OP_SDK_CREATE_FLOW,
        std::bind(&MirrorServer::HandleCreateFlow, this, std::placeholders::_1));
    netEngine->RegisterNewRequestHandler(BIO_OP_SDK_DESTROY_FLOW,
        std::bind(&MirrorServer::HandleDestroyFlow, this, std::placeholders::_1));
    netEngine->RegisterNewRequestHandler(BIO_OP_SDK_CREATE_DATA_MSG_MEM_POOL,
        std::bind(&MirrorServer::HandleCreateDataMsgMemPool, this, std::placeholders::_1));
    netEngine->RegisterNewRequestHandler(BIO_OP_SDK_GET_SLICE,
        std::bind(&MirrorServer::HandleGetSlice, this, std::placeholders::_1));
    netEngine->RegisterNewRequestHandler(BIO_OP_SERVER_SYNC_DATA,
        std::bind(&MirrorServer::HandleSyncData, this, std::placeholders::_1));
    netEngine->RegisterNewRequestHandler(BIO_OP_SERVER_GET_EVICT_OFFSET,
        std::bind(&MirrorServer::HandleGetEvictOffset, this, std::placeholders::_1));
    netEngine->RegisterNewRequestHandler(BIO_OP_SDK_FREE_MEM,
        std::bind(&MirrorServer::HandleFreeMem, this, std::placeholders::_1));
    netEngine->RegisterNewRequestHandler(BIO_OP_SDK_NOTIFY_UPDATE,
        std::bind(&MirrorServer::HandleNotifyUpdate, this, std::placeholders::_1));
    netEngine->RegisterNewRequestHandler(BIO_OP_SDK_CHECK_UPDATE_READY,
        std::bind(&MirrorServer::HandleCheckUpdateReady, this, std::placeholders::_1));
    netEngine->RegisterNewRequestHandler(BIO_OP_SERVER_CHECK_REMOTE_UPDATE_READY,
        std::bind(&MirrorServer::HandleCheckRemoteUpdateReady, this, std::placeholders::_1));
    netEngine->RegisterNewRequestHandler(BIO_OP_SDK_GET_UFS_CONFIG,
        std::bind(&MirrorServer::HandleGetUnderFsConfig, this, std::placeholders::_1));
    netEngine->RegisterNewRequestHandler(BIO_OP_SERVER_PROCBROCKEN_SYNC_FLOW,
        std::bind(&MirrorServer::HandleProcBrokenSyncFlow, this, std::placeholders::_1));
    netEngine->RegisterNewRequestHandler(BIO_OP_SDK_GET_CACHE_HIT,
        std::bind(&MirrorServer::HandleGetCacheHit, this, std::placeholders::_1));
    netEngine->RegisterNewRequestHandler(BIO_OP_SDK_QUERY_CACHE_RESOURCE,
        std::bind(&MirrorServer::HandleQueryCacheResource, this, std::placeholders::_1));
    netEngine->RegisterNewRequestHandler(BIO_OP_SDK_GET_TRACE_POINTS,
        std::bind(&MirrorServer::HandleGetTracePoints, this, std::placeholders::_1));
}

void MirrorServer::RegisterOpcode()
{
    auto netEngine = BioServer::Instance()->GetNetEngine();
    netEngine->RegisterNewRequestHandler(BIO_OP_SDK_SHM_INIT,
        std::bind(&MirrorServer::HandleShmInit, this, std::placeholders::_1));
    netEngine->RegisterNewRequestHandler(BIO_OP_SDK_GET_NODE_INFO,
        std::bind(&MirrorServer::HandleQueryNodeInfo, this, std::placeholders::_1));
    netEngine->RegisterNewRequestHandler(BIO_OP_SDK_GET_NODE_INFO_BY_PT,
        std::bind(&MirrorServer::HandleQueryNodeInfoByPt, this, std::placeholders::_1));
    netEngine->RegisterNewRequestHandler(BIO_OP_SDK_GET_QUOTA_INFO,
        std::bind(&MirrorServer::HandleQueryQuota, this, std::placeholders::_1));
    netEngine->RegisterNewRequestHandler(BIO_OP_SDK_ALLOC_QUOTA,
        std::bind(&MirrorServer::HandleAllocQuota, this, std::placeholders::_1));
    netEngine->RegisterNewRequestHandler(BIO_OP_SDK_FREE_QUOTA,
        std::bind(&MirrorServer::HandleFreeQuota, this, std::placeholders::_1));
    netEngine->RegisterNewRequestHandler(BIO_OP_SDK_GET_NODE_VIEW,
        std::bind(&MirrorServer::HandleQueryNodeView, this, std::placeholders::_1));
    netEngine->RegisterNewRequestHandler(BIO_OP_SDK_QUERY_PT_VIEW,
        std::bind(&MirrorServer::HandleQueryPtView, this, std::placeholders::_1));
    netEngine->RegisterNewRequestHandler(BIO_OP_SDK_PUT,
        std::bind(&MirrorServer::HandlePut, this, std::placeholders::_1));
    netEngine->RegisterNewRequestHandler(BIO_OP_BATCH_PARSE_KEY_ADDR,
        std::bind(&MirrorServer::HandleBatchParseKeyAddr, this, std::placeholders::_1));
    netEngine->RegisterNewRequestHandler(BIO_OP_SDK_GET,
        std::bind(&MirrorServer::HandleGet, this, std::placeholders::_1));
    netEngine->RegisterNewRequestHandler(BIO_OP_SDK_BATCH_GET,
        std::bind(&MirrorServer::HandleBatchGet, this, std::placeholders::_1));
    netEngine->RegisterNewRequestHandler(BIO_OP_SDK_DELETE,
        std::bind(&MirrorServer::HandleDelete, this, std::placeholders::_1));
    netEngine->RegisterNewRequestHandler(BIO_OP_SDK_STAT,
        std::bind(&MirrorServer::HandleStat, this, std::placeholders::_1));
    netEngine->RegisterNewRequestHandler(BIO_OP_SDK_BATCH_EXIST,
        std::bind(&MirrorServer::HandleBatchExist, this, std::placeholders::_1));
    netEngine->RegisterNewRequestHandler(BIO_OP_SDK_LIST,
        std::bind(&MirrorServer::HandleList, this, std::placeholders::_1));
    netEngine->RegisterNewRequestHandler(BIO_OP_SDK_LOAD,
        std::bind(&MirrorServer::HandleLoad, this, std::placeholders::_1));
    netEngine->RegisterNewRequestHandler(BIO_OP_SDK_ADD_DISK,
        std::bind(&MirrorServer::HandleAddDisk, this, std::placeholders::_1));

    RegisterOpcodeStep2(netEngine);
    netEngine->RegisterNewRequestHandler(BIO_OP_SDK_READ_EVICT_LOG, [this](ServiceContext &ctx) -> int32_t {
        auto net = BioServer::Instance()->GetNetEngine();
        if (!Ready()) {
            net->Reply(ctx, BIO_NOT_READY, nullptr, 0);
            return BIO_OK;
        }
        if (ctx.MessageData() == nullptr || ctx.MessageDataLen() != sizeof(EvictLogRequest)) {
            net->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
            return BIO_OK;
        }
        EvictLogResponse response{};
        auto request = static_cast<const EvictLogRequest *>(ctx.MessageData());
        auto ret = WCacheManager::Instance()->ReadEvictLog(*request, response);
        net->Reply(ctx, ret, ret == BIO_OK ? &response : nullptr, ret == BIO_OK ? sizeof(response) : 0);
        return BIO_OK;
    });
}

void MirrorServer::ReplyListResultLocal(ServiceContext &ctx, std::unordered_map<std::string, ObjStat> &objs,
    ListRequest &req)
{
    uintptr_t address = 0;
    uint32_t index = 0;
    if (!objs.empty()) {
        uint64_t key;
        auto ret = BioServer::Instance()->GetNetEngine()->AllocLocalMrSingle(address, key);
        if (UNLIKELY(ret != BIO_OK)) {
            BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_ALLOC_FAIL, nullptr, 0);
            return;
        }

        uint32_t segmentSize = BioServer::Instance()->GetNetEngine()->GetDataPage();
        auto addrP = reinterpret_cast<ObjStat *>(address);
        uint32_t totalSize = 0;
        for (auto &obj : objs) {
            totalSize += sizeof(ObjStat);
            if (totalSize > segmentSize) { // 防止addrP缓存区溢出.
                break;
            }
            CopyKey(addrP[index].key, obj.second.key, KEY_MAX_SIZE);
            addrP[index].size = obj.second.size;
            addrP[index].time = obj.second.time;
            index++;
        }
    }

    ListResponse rsp;
    rsp.addr = address;
    rsp.addrOffset =
        (address != 0) ? BioServer::Instance()->GetNetEngine()->GetAddressOffset(static_cast<uint64_t>(address)) : 0;
    rsp.num = index;
    rsp.buffLen = 0;
    BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_OK, static_cast<void *>(&rsp), sizeof(ListResponse));
}

void MirrorServer::ReplyListResultRemote(ServiceContext &ctx, ListRequest *req,
    std::unordered_map<std::string, ObjStat> &objs)
{
    uintptr_t lAddress = 0;
    uint32_t index = 0;
    if (!objs.empty()) {
        uint64_t lKey;
        auto ret = BioServer::Instance()->GetNetEngine()->AllocLocalMrSingle(lAddress, lKey);
        if (UNLIKELY(ret != BIO_OK)) {
            BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_ALLOC_FAIL, nullptr, 0);
            return;
        }

        uint32_t segmentSize = BioServer::Instance()->GetNetEngine()->GetDataPage();
        auto addrP = reinterpret_cast<ObjStat *>(lAddress);
        if (addrP == nullptr) {
            LOG_ERROR("Invalid address!");
            return;
        }
        uint32_t totalSize = 0;
        for (auto &obj : objs) {
            totalSize += sizeof(ObjStat);
            if (totalSize > segmentSize) { // 防止addrP缓存区溢出.
                break;
            }
            CopyKey(addrP[index].key, obj.second.key, KEY_MAX_SIZE);
            addrP[index].size = obj.second.size;
            addrP[index].time = obj.second.time;
            index++;
        }
        if (UNLIKELY(req->size < (sizeof(ObjStat) * index))) {
            LOG_ERROR("request mr size failed, size:" << req->size << ".");
            BioServer::Instance()->GetNetEngine()->FreeLocalMrSingle(lAddress);
            BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INNER_ERR, nullptr, 0);
            return;
        }

        NetRequest writeReq = BioServer::Instance()->GetNetEngine()->InitNetRequest(lAddress,
            req->address, lKey, req->mrKey, sizeof(ObjStat) * index);
        ret = BioServer::Instance()->GetNetEngine()->SyncWrite(ctx.Channel(), writeReq);
        if (UNLIKELY(ret != BIO_OK)) {
            LOG_ERROR("Sync write failed, ret:" << ret << ".");
            BioServer::Instance()->GetNetEngine()->FreeLocalMrSingle(lAddress);
            BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INNER_ERR, nullptr, 0);
            return;
        }
    }

    ListResponse rsp;
    rsp.addr = 0;
    rsp.addrOffset = 0;
    rsp.num = index;
    rsp.buffLen = 0;
    BioServer::Instance()->GetNetEngine()->FreeLocalMrSingle(lAddress);
    BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_OK, static_cast<void *>(&rsp), sizeof(ListResponse));
}

BResult MirrorServer::CreateFlow(uint64_t procId, uint16_t ptId, uint64_t ptv, uint64_t flowId, bool isDegrade)
{
    BResult ret = BIO_OK;
    BIO_TP_START(MIRROR_FLOW_CREATE_WCACHE_FAIL, &ret, BIO_ERR);
    ret = Cache::Instance().CreateWCache(procId, ptId, ptv, flowId, isDegrade);
    BIO_TP_END;
    if (UNLIKELY(ret != BIO_OK)) {
        LOG_ERROR("Create write cache failed, ret:" << ret << ", procId:" << procId << ", ptId:" << ptId << ".");
        return ret;
    }

    ret = Cache::Instance().CreateRCache(ptId, ptv);
    if (UNLIKELY(ret != BIO_OK)) {
        LOG_ERROR("Create read cache failed, ret:" << ret << ", ptId:" << ptId << ".");
    } else {
        LOG_INFO("Create read cache success, procId:" << procId << ", ptId:" << ptId << ", flowId:" << flowId);
    }

    return ret;
}

BResult MirrorServer::CreateFlowMaster(uint64_t procId, uint16_t ptId, uint64_t ptv, CreateFlowResponse &flowInfo)
{
    uint64_t base = BioServer::Instance()->GetPtEntry(ptId).version;
    if (UNLIKELY(ptv != base)) {
        LOG_WARN("Check message pt version failed, base:" << base << ", ptv:" << ptv << ".");
        return BIO_CHECK_PT_FAIL;
    }

    BResult ret = BIO_ERR;
    uint64_t reuseFlowId;
    while ((ret = WCacheManager::Instance()->GetReuseFlowId(ptId, reuseFlowId)) != BIO_NOT_EXISTS) {
        auto wCache = WCacheManager::Instance()->GetWCache(reuseFlowId);
        if (wCache == nullptr) {
            LOG_ERROR("Get wCache failed, flowId:" << reuseFlowId << ".");
            return BIO_NOT_EXISTS;
        }
        if (wCache->GetPtv() != base) {
            LOG_DEBUG("Check pt version failed, destroy flow, flowId:" << reuseFlowId << ", ptv:" << wCache->GetPtv() <<
                ", base:" << base << ", isDegrade:" << wCache->GetDegradeState() << ", index:" << wCache->GetIndex() <<
                ", offset:" << wCache->GetOffset() << ".");
            auto ret = DestroyFlow(wCache->GetProcId(), ptId, wCache->GetPtv(), reuseFlowId);
            if (UNLIKELY(ret != BIO_OK)) {
                LOG_ERROR("Destroy flow failed, flowId:" << reuseFlowId << ", ret:" << ret << ".");
                continue;
            }
        } else {
            LOG_DEBUG("Find reuse flow success, flowId:" << reuseFlowId << ", isDegrade:" <<
                wCache->GetDegradeState() << ", index:" << wCache->GetIndex() << ", offset:" << wCache->GetOffset() <<
                ".");
            flowInfo = { reuseFlowId, wCache->GetDegradeState(), wCache->GetIndex(), wCache->GetOffset(), false };
            wCache->SetProcId(procId);
            return BIO_OK;
        }
    }

    ret = Cache::Instance().AllocateFlowId(procId, ptId, ptv, flowInfo.flowId);
    if (UNLIKELY(ret != BIO_OK)) {
        LOG_ERROR("Alloc flow id failed, ret:" << ret << ", procId:" << procId << ", ptId:" << ptId << ".");
        return ret;
    }

    flowInfo.isDegrade = BioServer::Instance()->GetServiceState(); // 升级过程中，创建降级Cache实例
    return CreateFlow(procId, ptId, ptv, flowInfo.flowId, flowInfo.isDegrade);
}

BResult MirrorServer::CreateFlowSlave(uint64_t procId, uint16_t ptId, uint64_t ptv, uint64_t flowId, bool isDegrade)
{
    uint64_t base = BioServer::Instance()->GetPtEntry(ptId).version;
    if (UNLIKELY(ptv != base)) {
        LOG_WARN("Check message pt version failed, base:" << base << ", ptv:" << ptv << ".");
        return BIO_CHECK_PT_FAIL;
    }
    auto ret = CreateFlow(procId, ptId, ptv, flowId, isDegrade);
    if (UNLIKELY(ret != BIO_OK)) {
        return ret;
    }
    return ret;
}

BResult MirrorServer::DestroyFlow(uint64_t procId, uint16_t ptId, uint64_t ptv, uint64_t flowId)
{
    auto ret = Cache::Instance().DestroyWCache(procId, ptId, ptv, flowId);
    if (UNLIKELY(ret != BIO_OK)) {
        LOG_ERROR("Destroy write cache failed, ret:" << ret << ", procId:" << procId << ", ptId:" << ptId << ".");
    }
    return ret;
}

BResult MirrorServer::GetSlice(uint64_t flowId, uint64_t flowOffset, uint64_t flowIndex, uint64_t length,
    WCacheSlicePtr &slice)
{
    SliceKey sliceKey(flowId, flowOffset, FLOW_MEMORY, length, flowIndex);
    BResult ret = BIO_OK;
    BIO_TP_START(MIRROR_SERVER_GET_SLICE_FAIL, &ret, BIO_INNER_RETRY);
    ret = Cache::Instance().GetWCacheSlice(sliceKey, slice);
    BIO_TP_END;
    return ret;
}

void MirrorServer::QueryCacheQuota(QueryQuotaRequest &req, QueryQuotaResponse &rsp)
{
    static uint64_t defaultPreloadSize = NO_128 * NO_1024 * NO_1024; // 默认预取大小为128M.
    uint64_t totalQuota = CacheOverloadCtrl::Instance().GetAvailableQuota();
    rsp.preloadSize = std::min<uint64_t>(defaultPreloadSize, ROUND_UP((totalQuota / NO_10), NO_4096));
    rsp.enable = mBioConfig->GetDaemonConfig().enableQos;
    LOG_INFO("Query quota info success, write cache quota:" << totalQuota << ", preload size:" <<
        (rsp.preloadSize / NO_1024 / NO_1024) << "M, enable:" << rsp.enable << ".");
}

BResult MirrorServer::AllocCacheQuota(AllocQuotaRequest &req, AllocQuotaResponse &rsp)
{
    if (UNLIKELY(!CheckAll(req.comm))) {
        return BIO_CHECK_PT_FAIL;
    }

    // 1. 检查cid和nid是否有效, 当前仅能检查跨节点SDK的quota申请请求.
    auto bioServer = BioServer::Instance();
    if (bioServer == nullptr) {
        LOG_ERROR("Bio server instance get fail");
        return BIO_INNER_RETRY;
    }
    auto rpcEngine = bioServer->GetNetEngine();
    if (rpcEngine == nullptr && !bioServer->IsStandaloneMode()) {
        LOG_ERROR("Net engine get fail");
        return BIO_INNER_RETRY;
    }
    if (!bioServer->IsStandaloneMode() &&
        req.nid != bioServer->GetLocalNid().VNodeId() && !rpcEngine->IsChannelExist(req.nid, req.cid)) {
        LOG_ERROR("Invalid nodeId " << req.nid << " or cid " << req.cid << ", need retry.");
        return BIO_INVALID_PARAM;
    }

    // 2. 限制最大单次申请quota大小不超过1GB.
    if (req.allocQuota > NO_1024 * IO_SIZE_1M) {
        LOG_ERROR("Alloc quota size failed, size:" << req.allocQuota << ", cid:" << req.cid << ", nid:" << req.nid);
        return BIO_INVALID_PARAM;
    }

    // 3. 申请quota资源.
    QuotaHolder holder = { req.nid, req.cid };
    BIO_TRACE_START(MIRROR_TRACE_QOS_ALLOC);
    BResult ret = CacheOverloadCtrl::Instance().AllocQuota(holder, req.allocQuota, rsp.exceptQuota);
    BIO_TRACE_END(MIRROR_TRACE_QOS_ALLOC, ret);
    if (ret != BIO_OK) {
        LOG_ERROR("Alloc quota failed, ret:" << ret << ", holder:" << req.nid << "-" << req.cid << ", size:" <<
            req.allocQuota << ".");
    }
    return ret;
}

BResult MirrorServer::FreeCacheQuota(FreeQuotaRequest &req)
{
    if (UNLIKELY(!CheckAll(req.comm))) {
        return BIO_CHECK_PT_FAIL;
    }

    QuotaHolder holder = { req.nid, req.cid };
    std::string innerKey = "RollbackCacheQuota";
    BIO_TRACE_START(MIRROR_TRACE_QOS_ROLLBACK);
    CacheOverloadCtrl::Instance().ReleaseQuota(innerKey.c_str(), holder, req.quota, 1);
    CacheOverloadCtrl::Instance().FreeQuota(req.quota, 1);
    BIO_TRACE_END(MIRROR_TRACE_QOS_ROLLBACK, BIO_OK);
    LOG_INFO("Rollback cache quota success, holder: " << req.nid << "-" << req.cid << ", size:" << req.quota << ".");
    return BIO_OK;
}

void MirrorServer::QueryNodeView(QueryNodeViewRequest &req, QueryNodeViewResponse &rsp)
{
    std::map<CmNodeId, CmNodeInfo, CmNodeIdCmp> nodeView = BioServer::Instance()->GetNodeView(&rsp.curNodeTimes);
    uint32_t index = 0;
    if (nodeView.size() > CLUSTER_NODE_SIZE) {
        return;
    }
    for (auto &nodeEntry : nodeView) {
        if (index == CLUSTER_NODE_SIZE) {
            break;
        }
        rsp.desc[index].groupId = nodeEntry.second.id.GroupId();
        rsp.desc[index].nodeId = nodeEntry.second.id.VNodeId();
        int32_t ret =
                strncpy_s(rsp.desc[index].ip, IP_MAX_SIZE, nodeEntry.second.ip.c_str(), nodeEntry.second.ip.size());
        if (ret != BIO_OK) {
            LOG_ERROR("strncpy_s faild, ret:"<< ret << ".");
            return;
        }
        rsp.desc[index].port = nodeEntry.second.port;
        rsp.desc[index].status = static_cast<uint16_t>(nodeEntry.second.status);
        rsp.desc[index].num = nodeEntry.second.disks.size();
        for (uint32_t j = 0; j < nodeEntry.second.disks.size(); j++) {
            rsp.desc[index].diskDesc[j].diskId = nodeEntry.second.disks[j].diskId;
            rsp.desc[index].diskDesc[j].diskStatus = static_cast<uint16_t>(nodeEntry.second.disks[j].diskStatus);
        }
        index++;
    }
    rsp.num = index;
    rsp.flag = (index == 0) ? 0 : 1;
}

void MirrorServer::QueryPtView(QueryPtViewRequest &req, QueryPtViewResponse &rsp)
{
    std::map<uint16_t, CmPtInfo> ptView = BioServer::Instance()->GetPtView(&rsp.curPtTimes);
    uint32_t index = 0;
    for (auto &ptEntry : ptView) {
        if (index == PT_SIZE) {
            break;
        }
        rsp.desc[index].version = ptEntry.second.version;
        rsp.desc[index].ptId = ptEntry.second.ptId;
        rsp.desc[index].state = static_cast<uint16_t>(ptEntry.second.state);
        rsp.desc[index].masterNodeId = ptEntry.second.masterNodeId;
        rsp.desc[index].masterDiskId = ptEntry.second.masterDiskId;
        for (uint32_t j = 0; j < ptEntry.second.copys.size(); j++) {
            rsp.desc[index].copys[j].nodeId = ptEntry.second.copys[j].nodeId;
            rsp.desc[index].copys[j].diskId = ptEntry.second.copys[j].diskId;
            rsp.desc[index].copys[j].state = static_cast<uint16_t>(ptEntry.second.copys[j].state);
        }
        rsp.copyNum = ptEntry.second.copys.size();
        index++;
    }
    rsp.num = index;
    rsp.flag = (index == 0) ? 0 : 1;
}

BResult MirrorServer::ReaderLocal(const SlicePtr &from, const SlicePtr &to, PutRequest &req)
{
    if (req.affinity == LOCAL_AFFINITY) {
        return BIO_OK;
    }
    return mSliceOp.Copy(from, to);
}

BResult MirrorServer::ReaderRemoteEquals(PutRequest &req, std::vector<NetMrInfo> &lMrVec,
    std::vector<NetMrInfo> &rMrVec, ServiceContext &netCtx)
{
    BResult ret = BIO_OK;
    for (uint32_t idx = 0; idx < lMrVec.size(); idx++) {
        NetRequest wReq = BioServer::Instance()->GetNetEngine()->InitNetRequest(lMrVec[idx].address,
            rMrVec[idx].address, lMrVec[idx].key, rMrVec[idx].key, lMrVec[idx].size);
        if (req.memFromServer) { // 性能考虑, 选择不同的channel进行单边读.
            ret = BioServer::Instance()->GetNetEngine()->SyncRead(req.comm.srcNid, wReq);
        } else {
            ret = BioServer::Instance()->GetNetEngine()->SyncRead(netCtx.Channel(), wReq);
        }
        if (UNLIKELY(ret != BIO_OK)) {
            LOG_ERROR("One side read failed, ret:" << ret << ", idx:" << idx << ", lKey:" << lMrVec[idx].key <<
                ", rKey:" << rMrVec[idx].key << ", size:" << lMrVec[idx].size << ".");
            break;
        }
    }
    return ret;
}

BResult MirrorServer::ReaderRemoteNotEquals(PutRequest &req, std::vector<NetMrInfo> &lMrVec,
    std::vector<NetMrInfo> &rMrVec, ServiceContext &netCtx)
{
    // 当前版本该场景的远端MrVec的size必须为1.
    ChkTrue(rMrVec.size() == 1, BIO_INNER_ERR, "Slice addr num not match, rAddrNum:" << rMrVec.size() << ".");
    uintptr_t rMrAddr = rMrVec[0].address;
    uint32_t rMrKey = rMrVec[0].key;
    uint64_t remoteSize = rMrVec[0].size;
    uint64_t totalSize = 0;

    BResult ret = BIO_OK;
    uint64_t off = 0;
    for (uint32_t idx = 0; idx < lMrVec.size(); idx++) {
        rMrAddr += off;
        ChkTrue((lMrVec[idx].size + off) <= remoteSize, BIO_INNER_ERR, "lSlice size exceeds rSlice size.");
        NetRequest wReq = BioServer::Instance()->GetNetEngine()->InitNetRequest(lMrVec[idx].address,
            rMrAddr, lMrVec[idx].key, rMrKey, lMrVec[idx].size);
        if (req.memFromServer) { // 性能考虑, 选择不同的channel进行单边读.
            ret = BioServer::Instance()->GetNetEngine()->SyncRead(req.comm.srcNid, wReq);
        } else {
            ret = BioServer::Instance()->GetNetEngine()->SyncRead(netCtx.Channel(), wReq);
        }
        if (UNLIKELY(ret != BIO_OK)) {
            LOG_ERROR("One side read failed, ret:" << ret << ", idx:" << idx << ", lKey:" << lMrVec[idx].key <<
                ", rKey:" << rMrKey << ", size:" << lMrVec[idx].size << ".");
            break;
        }
        off += lMrVec[idx].size;
    }
    return ret;
}

BResult MirrorServer::ReaderRemote(const SlicePtr &from, const SlicePtr &to, PutRequest &req, ServiceContext &netCtx)
{
    // 1. parse remote mr info
    BIO_TRACE_START(MIRROR_TRACE_PUT_READ_DATA);
    std::vector<FlowAddr> rFlowAddr = from->GetAddrs();
    std::vector<NetMrInfo> rMrVec;
    for (auto &addr : rFlowAddr) {
        MrInfo mr;
        addr.ToMrInfo(mr);
        rMrVec.emplace_back(NetMrInfo(mr.address, mr.size, req.mrKey));
    }

    // 2. parse local mr info
    std::vector<FlowAddr> lFlowAddr = to->GetAddrs();
    std::vector<NetMrInfo> lMrVec;
    for (auto &addr : lFlowAddr) {
        MrInfo mr;
        addr.ToMrInfo(mr);
        lMrVec.emplace_back(NetMrInfo(mr.address, mr.size, BioServer::Instance()->GetLocalMrKey()));
    }

    // 3. one side read
    BResult ret = BIO_OK;
    if (lMrVec.size() > rMrVec.size()) {
        ret = ReaderRemoteNotEquals(req, lMrVec, rMrVec, netCtx);
    } else if (lMrVec.size() == rMrVec.size()) {
        ret = ReaderRemoteEquals(req, lMrVec, rMrVec, netCtx);
    } else {
        LOG_ERROR("Slice addr num not match, lAddrNum:" << lMrVec.size() << ", rAddrNum:" << rMrVec.size() << ".");
        ret = BIO_INNER_ERR;
    }
    BIO_TRACE_END(MIRROR_TRACE_PUT_READ_DATA, ret);
    return ret;
}

BResult MirrorServer::Put(PutRequest &req, const WCacheSlicePtr &sliceP, ServiceContext &netCtx, uint32_t &ioStrategy)
{
    if (UNLIKELY(!CheckAll(req.comm))) {
        return BIO_CHECK_PT_FAIL;
    }

    LOG_DEBUG("Mirror server put, key:" << req.key << ", srcNid:" << req.comm.srcNid << ", flowId:" <<
        sliceP->GetFlowId() << ", offsetInFlow:" << sliceP->GetOffsetInFlow() << ", indexInFlow:" <<
        sliceP->GetIndexInFlow() << ", slice: " << sliceP->ToString() << ", rFlowSize:" << sliceP->GetAddrs().size() <<
        "." << " ptVersion:" << BioServer::Instance()->GetPtEntry(req.comm.ptId).version << ", ptId:" << req.comm.ptId);

    auto reader = [&req, &netCtx, this](const SlicePtr &from, const SlicePtr &to) -> BResult {
        if (req.comm.srcNid == BioServer::Instance()->GetLocalNid().VNodeId()) {
            return ReaderLocal(from, to, req);
        } else {
            return ReaderRemote(from, to, req, netCtx);
        }
    };

    CacheAttr attr(static_cast<RealIoStrategy>(req.ioStrategy), req.tenantId,
        static_cast<AffinityStrategy>(req.affinity), static_cast<WriteStrategy>(req.strategy));
    BIO_TRACE_START(MIRROR_TRACE_PUT);
    BResult ret = Cache::Instance().Put(req.key, sliceP, reader, attr);
    BIO_TRACE_END(MIRROR_TRACE_PUT, ret);
    if (UNLIKELY(ret != BIO_OK)) {
        LOG_ERROR("Put to write cache failed, ret:" << ret << ", key:" << req.key << ".");
        return ret;
    }
    if (mBioConfig->GetDaemonConfig().enableQos) {
        QuotaHolder holder = { static_cast<uint32_t>(req.quotaNid), req.quotaCid };
        BIO_TRACE_START(MIRROR_TRACE_QOS_RELEASE);
        CacheOverloadCtrl::Instance().ReleaseQuota(req.key, holder, req.length, 0);
        BIO_TRACE_END(MIRROR_TRACE_QOS_RELEASE, BIO_OK);
    }
    ioStrategy = static_cast<uint32_t>(attr.ioStrategy); // 矫正sdk端IO写策略
    return ret;
}

void MirrorServer::InitGetResponse(GetResponse &rsp)
{
    rsp.isAlloc = false;
    rsp.num = 0;
    for (uint32_t idx = 0; idx < SLICE_ADDR_SIZE; idx++) {
        rsp.address[idx] = 0;
        rsp.addrOffset[idx] = 0;
        rsp.addrLen[idx] = 0;
    }
    rsp.realLen = 0;
}

BResult MirrorServer::WriterLocalSameProcess(const SlicePtr &from, const SlicePtr &to, uint32_t rKey)
{
    std::vector<NetMrInfo> rMrVec;
    for (auto addr : to->GetAddrs()) {
        MrInfo mr{};
        addr.ToMrInfo(mr);
        rMrVec.emplace_back(NetMrInfo(mr.address, mr.size, rKey));
    }
    ChkTrue(rMrVec.size() == 1, BIO_INNER_ERR, "Remote addr size not equal to 1, size:" << rMrVec.size() << ".");

    BIO_TRACE_START(MIRROR_TRACE_GET_COPY_DATA);
    BResult ret = mSliceOp.Copy(from, reinterpret_cast<char *>(rMrVec[0].address), from->GetLength());
    BIO_TRACE_END(MIRROR_TRACE_GET_COPY_DATA, ret);
    if (UNLIKELY(ret != BIO_OK)) {
        LOG_ERROR("Slice copy failed, ret:" << ret << ".");
    }
    return ret;
}

BResult MirrorServer::WriterParseMrInfo(const SlicePtr &from, const SlicePtr &to, std::vector<NetMrInfo> &rMrVec,
    std::vector<NetMrInfo> &lMrVec, uint32_t rKey, bool &isAlloc)
{
    // 1. parse remote mr info
    uint64_t totalLen = 0;
    for (auto addr : to->GetAddrs()) {
        MrInfo mr;
        addr.ToMrInfo(mr);
        rMrVec.emplace_back(NetMrInfo(mr.address, mr.size, rKey));
        totalLen += mr.size;
    }

    // The destination MR describes capacity; only the source data should be sent.
    const uint64_t actualLen = from->GetLength();
    if (UNLIKELY(actualLen > totalLen || actualLen > to->GetLength())) {
        LOG_ERROR("Source length exceeds request or MR capacity, length:" << actualLen <<
            ", requested:" << to->GetLength() << ", capacity:" << totalLen);
        return BIO_INVALID_PARAM;
    }
    totalLen = actualLen;

    // 2. parse local mr info
    NetMrInfo bioMr;
    BResult ret = BioServer::Instance()->MemAlloc(totalLen, bioMr);
    if (UNLIKELY(ret != BIO_OK)) {
        LOG_ERROR("Alloc rdma memory failed, ret:" << ret << ", length:" << totalLen << ".");
        return ret;
    }
    ret = mSliceOp.Copy(from, reinterpret_cast<char *>(bioMr.address), totalLen);
    if (UNLIKELY(ret != BIO_OK)) {
        LOG_ERROR("Slice copy failed, ret:" << ret << ".");
        BioServer::Instance()->MemFree(bioMr.address);
        return ret;
    }
    isAlloc = true;
    lMrVec.emplace_back(bioMr);
    return BIO_OK;
}

BResult MirrorServer::WriterLocalDiffProcess(bool &isAlloc, std::vector<NetMrInfo> &lMrVec, GetResponse &rsp,
    GetRequest &req)
{
    auto lMrSize = lMrVec.size();
    BIO_TP_START(WCACHE_READ_LOCAL_RMRSIZE_ERR, &lMrSize, NO_5);
    BIO_TP_END;
    if (UNLIKELY(lMrSize > SLICE_ADDR_SIZE)) {
        if (isAlloc) {
            for (auto mr : lMrVec) {
                BIO_TP_START(NO_PROCESS_MEM_FREE, 0);
                BioServer::Instance()->MemFree(mr.address);
                BIO_TP_END;
            }
        }
        LOG_ERROR("Local mr size exceed 4, size:" << lMrVec.size() << ".");
        return BIO_INNER_ERR;
    }
    rsp.isAlloc = isAlloc;
    rsp.num = lMrVec.size();
    for (uint32_t idx = 0; idx < lMrVec.size(); idx++) {
        rsp.address[idx] = isAlloc ? lMrVec[idx].address : 0;
        rsp.addrOffset[idx] =
            BioServer::Instance()->GetNetEngine()->GetAddressOffset(static_cast<uint64_t>(lMrVec[idx].address));
        rsp.addrLen[idx] = lMrVec[idx].size;
    }

    return BIO_OK;
}

BResult MirrorServer::BatchSingleWriterRemote(bool isAlloc, std::vector<NetMrInfo> &lMrVec,
                                              std::vector<NetMrInfo> &rMrVec, BatchGetRequest *req)
{
    auto rMrSize = rMrVec.size();
    if (UNLIKELY(rMrSize != NO_1)) {
        if (isAlloc) {
            for (auto mr : lMrVec) {
                BioServer::Instance()->MemFree(mr.address);
            }
        }
        LOG_ERROR("Remote addr size not equal to 1, size:" << rMrVec.size() << ".");
        return BIO_INNER_ERR;
    }
    uint64_t off = 0;
    BResult ret = BIO_OK;
    BIO_TRACE_START(MIRROR_TRACE_GET_WRITE_DATA);
    for (uint32_t idx = 0; idx < lMrVec.size(); idx++) {
        if (UNLIKELY(lMrVec[idx].size + off) > rMrVec[0].size) {
            ret = BIO_INNER_ERR;
            break;
        }
        NetRequest rReq = BioServer::Instance()->GetNetEngine()->InitNetRequest(lMrVec[idx].address,
            rMrVec[0].address + off, lMrVec[idx].key, rMrVec[0].key, lMrVec[idx].size);
        uint32_t dstPid = req->isConvDeploy ? 0 : static_cast<uint32_t>(req->pid); // 融合部署场景目的端PID填充0
        ret = BioServer::Instance()->GetNetEngine()->SyncWrite(req->srcNid, dstPid, rReq);
        if (UNLIKELY(ret != BIO_OK)) {
            LOG_ERROR("Sync write failed, ret:" << ret << ", index:" << idx << ", lKey:" << lMrVec[idx].key <<
                                                ", rKey:" << rMrVec[0].key << ", size:" << lMrVec[idx].size << ".");
            break;
        }
        off += lMrVec[idx].size;
    }
    if (isAlloc) {
        for (auto mr : lMrVec) {
            BioServer::Instance()->MemFree(mr.address);
        }
    }
    BIO_TRACE_END(MIRROR_TRACE_GET_WRITE_DATA, ret);

    return ret;
}

BResult MirrorServer::WriterRemote(bool isAlloc, std::vector<NetMrInfo> &lMrVec, std::vector<NetMrInfo> &rMrVec,
    ServiceContext &netCtx, GetRequest &req)
{
    auto rMrSize = rMrVec.size();
    BIO_TP_START(WCACHE_READ_REMOTE_RMRSIZE_ERR, &rMrSize, NO_4);
    BIO_TP_END;
    if (UNLIKELY(rMrSize != NO_1)) {
        if (isAlloc) {
            for (auto mr : lMrVec) {
                BIO_TP_START(NO_PROCESS_MEM_FREE, 0);
                BioServer::Instance()->MemFree(mr.address);
                BIO_TP_END;
            }
        }
        LOG_ERROR("Remote addr size not equal to 1, size:" << rMrVec.size() << ".");
        return BIO_INNER_ERR;
    }
    uint64_t off = 0;
    BResult ret = BIO_OK;
    BIO_TRACE_START(MIRROR_TRACE_GET_WRITE_DATA);
    BIO_TP_START(WCACHE_READ_CALLBACK_FAIL, &ret, BIO_ERR);
    for (uint32_t idx = 0; idx < lMrVec.size(); idx++) {
        if (UNLIKELY(lMrVec[idx].size + off) > rMrVec[0].size) {
            ret = BIO_INNER_ERR;
            break;
        }
        NetRequest rReq = BioServer::Instance()->GetNetEngine()->InitNetRequest(lMrVec[idx].address,
            rMrVec[0].address + off, lMrVec[idx].key, rMrVec[0].key, lMrVec[idx].size);
        uint32_t dstPid = req.isConvDeploy ? 0 : static_cast<uint32_t>(req.comm.pid); // 融合部署场景目的端PID填充0
        ret = BioServer::Instance()->GetNetEngine()->SyncWrite(req.comm.srcNid, dstPid, rReq);
        if (UNLIKELY(ret != BIO_OK)) {
            LOG_ERROR("Sync write failed, ret:" << ret << ", index:" << idx << ", lKey:" << lMrVec[idx].key <<
                ", rKey:" << rMrVec[0].key << ", size:" << lMrVec[idx].size << ".");
            break;
        }
        off += lMrVec[idx].size;
    }
    if (isAlloc) {
        for (auto mr : lMrVec) {
            BioServer::Instance()->MemFree(mr.address);
        }
    }
    BIO_TP_END;
    BIO_TRACE_END(MIRROR_TRACE_GET_WRITE_DATA, ret);

    return ret;
}

BResult MirrorServer::GetConvergence(GetRequest &req, GetResponse &rsp)
{
    if (UNLIKELY(!CheckAll(req.comm))) {
        return BIO_CHECK_PT_FAIL;
    }

    InitGetResponse(rsp);
    // 根据req组装slice.
    MrInfo mrInfo = { req.address, static_cast<uint32_t>(req.size) };
    std::vector<FlowAddr> addrVec = { FlowAddr(mrInfo) };
    RCacheSlicePtr sliceP = MakeRef<RCacheSlice>(req.ptId, req.length, addrVec);
    if (UNLIKELY(sliceP == nullptr)) {
        LOG_ERROR("Make rcache slice failed.");
        return BIO_ALLOC_FAIL;
    }

    LOG_DEBUG("Mirror server get, key:" << req.key << ", srcNid:" << req.comm.srcNid << ", offset:" << req.offset <<
        ", length:" << req.length << ", mr size:" << req.size << ", mr key:" << req.mrKey << ", slice: " <<
        sliceP->ToString() << ", rFlowSize:" << sliceP->GetAddrs().size() << ".");

    auto writer = [&req, &rsp, this](const SlicePtr &from, const SlicePtr &to) -> BResult {
        // case 1: 同节点同进程的缓存客户端读请求处理
        return WriterLocalSameProcess(from, to, req.mrKey);
    };

    BIO_TRACE_START(MIRROR_TRACE_GET);
    BResult ret = Cache::Instance().Get(req.key, req.offset, sliceP, writer, rsp.realLen);
    BIO_TRACE_END(MIRROR_TRACE_GET, ret);
    if (UNLIKELY(ret != BIO_OK)) {
        LOG_ERROR("Get key from cache failed, ret:" << ret << ", key:" << req.key << ", offset:" << req.offset << ".");
    } else {
        if (mBioConfig->GetDaemonConfig().enableCrc) {
            rsp.dataCrc = sliceP->GetDataCrc();
        }
    }
    return ret;
}

BResult MirrorServer::ParseKeyAddr(const Key &key, uint16_t ptId, BatchKeyAddrInfo *info)
{
    bool isRetry = false;
    BResult ret = BIO_OK;
    do {
        isRetry = false;
        ret = Cache::Instance().ParseKeyAddr(key, ptId, info);
        if (UNLIKELY(ret != BIO_OK)) {
            LOG_ERROR("Parse key:" << key << " addrs fail, ret:" << ret);
        }
        if (UNLIKELY(ret == BIO_INNER_RETRY)) {
            isRetry = true;
            usleep(NO_100);
        }
    } while (isRetry);
    return ret;
}

BResult MirrorServer::Get(GetRequest &req, GetResponse &rsp, ServiceContext &netCtx)
{
    if (UNLIKELY(!CheckAll(req.comm))) {
        return BIO_CHECK_PT_FAIL;
    }

    InitGetResponse(rsp);
    // 根据req组装slice.
    MrInfo mrInfo = { req.address, static_cast<uint32_t>(req.size) };
    std::vector<FlowAddr> addrVec = { FlowAddr(mrInfo) };
    RCacheSlicePtr sliceP = MakeRef<RCacheSlice>(req.ptId, req.length, addrVec);
    if (UNLIKELY(sliceP == nullptr)) {
        LOG_ERROR("Make rcache slice failed.");
        return BIO_ALLOC_FAIL;
    }

    LOG_DEBUG("Mirror server get, key:" << req.key << ", srcNid:" << req.comm.srcNid << ", offset:" << req.offset <<
        ", length:" << req.length << ", mr size:" << req.size << ", mr key:" <<
        req.mrKey << ", slice: " << sliceP->ToString() << ", rFlowSize:" << sliceP->GetAddrs().size() << "."
        << " ptVersion:" << BioServer::Instance()->GetPtEntry(req.comm.ptId).version << ", ptId:" << req.comm.ptId);

    auto writer = [&req, &rsp, &netCtx, this](const SlicePtr &from, const SlicePtr &to) -> BResult {
        bool isAlloc = false;
        std::vector<NetMrInfo> rMrVec;
        std::vector<NetMrInfo> lMrVec;
        BResult ret = WriterParseMrInfo(from, to, rMrVec, lMrVec, req.mrKey, isAlloc);
        if (ret != BIO_OK) {
            return ret;
        }

        // case 1: 同节点跨进程的缓存客户端读请求处理.
        if ((req.comm.srcNid == BioServer::Instance()->GetLocalNid().VNodeId()) && (req.comm.pid != getpid())) {
            return WriterLocalDiffProcess(isAlloc, lMrVec, rsp, req);
        }

        // cse 2: 跨节点的缓存客户端读请求处理.
        return WriterRemote(isAlloc, lMrVec, rMrVec, netCtx, req);
    };

    BIO_TRACE_START(MIRROR_TRACE_GET);
    BResult ret = Cache::Instance().Get(req.key, req.offset, sliceP, writer, rsp.realLen);
    BIO_TRACE_END(MIRROR_TRACE_GET, ret);
    if (UNLIKELY(ret != BIO_OK)) {
        LOG_ERROR("Get key from cache failed, ret:" << ret << ", key:" << req.key << ", offset:" << req.offset << ".");
    } else {
        if (mBioConfig->GetDaemonConfig().enableCrc) {
            rsp.dataCrc = sliceP->GetDataCrc();
        }
    }
    return ret;
}

BResult MirrorServer::BatchSingleGet(GetKeyInfo &keyInfo, uint64_t &realLen, BatchGetRequest *req,
    BdmCopyBatchContext *bdmBatch, BResult *keyResult)
{
    MrInfo mrInfo;
    uint16_t localNid = GetLocalVNodeId();
    RCacheSlicePtr sliceP = nullptr;
    if (req->srcNid != localNid || BioServer::Instance()->IsStandaloneMode()) {
        mrInfo = { keyInfo.address, static_cast<uint32_t>(keyInfo.size) };
        std::vector<FlowAddr> addrVec = { FlowAddr(mrInfo) };
        sliceP = MakeRef<RCacheSlice>(keyInfo.ptId, keyInfo.length, addrVec);
    } else {
        auto localAddr = TransDataMsgMemAddr(req->pid, keyInfo.addressOffset, keyInfo.size);
        if (UNLIKELY(localAddr == 0)) {
            LOG_ERROR("Get data message memory address failed, pid:" << req->pid <<
                                                                     ", offset:" << keyInfo.addressOffset <<
                                                                     ", size:" << keyInfo.size << ".");
            return BIO_NOT_READY;
        }
        mrInfo = { localAddr, static_cast<uint32_t>(keyInfo.size) };
        std::vector<FlowAddr> addrVec = { FlowAddr(mrInfo) };
        sliceP = MakeRef<RCacheSlice>(keyInfo.ptId, keyInfo.length, addrVec);
    }

    if (UNLIKELY(sliceP == nullptr)) {
        LOG_ERROR("Make rcache slice failed.");
        return BIO_ALLOC_FAIL;
    }

    LOG_DEBUG("Mirror server get, key:" << keyInfo.key << ", offset:" << keyInfo.offset <<
                                        ", length:" << keyInfo.length << ", mr size:" << keyInfo.size << ", slice: " <<
                                        sliceP->ToString() << ", rFlowSize:" << sliceP->GetAddrs().size() << "."
                                        << " ptVersion:" << BioServer::Instance()->GetPtEntry(keyInfo.ptId).version <<
                                        ", ptId:" << keyInfo.ptId);

    auto writerLocal = [&keyInfo, this](const SlicePtr &from, const SlicePtr &to) -> BResult {
        from->IncreaseRef();

        if (!BioServer::Instance()->IsStandaloneMode() || from->GetFlowType() != FLOW_DISK) {
            BIO_TRACE_START(WCACHE_TRACE_GET_COPY_TO_TARGET);
            BResult ret = mSliceOp.Copy(from, to);
            BIO_TRACE_END(WCACHE_TRACE_GET_COPY_TO_TARGET, ret);
            from->DecreaseRef();
            return ret;
        }

        uint64_t totalLen = 0;
        for (auto addr : to->GetAddrs()) {
            MrInfo mr{};
            addr.ToMrInfo(mr);
            totalLen += mr.size;
        }
        char *addr = reinterpret_cast<char *>(malloc(sizeof(char) * totalLen));
        if (UNLIKELY(addr == nullptr)) {
            LOG_ERROR("Alloc memory failed, length:" << totalLen << ".");
            from->DecreaseRef();
            return BIO_ALLOC_FAIL;
        }
        BResult ret = mSliceOp.Copy(from, addr, totalLen);
        if (ret != BIO_OK) {
            LOG_ERROR("Copy data from server failed, ret:" << ret << ", length:" << totalLen <<
                ", key:" << keyInfo.key);
            free(addr);
            from->DecreaseRef();
            return ret;
        }
        ret = mSliceOp.Copy(addr, to);
        if (ret != BIO_OK) {
            LOG_ERROR("Copy data to dst failed, ret:" << ret << ", length:" << totalLen << ", key:" << keyInfo.key);
        }
        free(addr);
        from->DecreaseRef();
        return ret;
    };
    auto writerRemote = [&keyInfo, req, this](const SlicePtr &from, const SlicePtr &to) -> BResult {
        bool isAlloc = false;
        std::vector<NetMrInfo> rMrVec;
        std::vector<NetMrInfo> lMrVec;
        BResult ret = WriterParseMrInfo(from, to, rMrVec, lMrVec, keyInfo.mrKey, isAlloc);
        if (ret != BIO_OK) {
            return ret;
        }
        return BatchSingleWriterRemote(isAlloc, lMrVec, rMrVec, req);
    };
    auto writer = [req, localNid, &writerLocal, &writerRemote](const SlicePtr &from, const SlicePtr &to) -> BResult {
        return req->srcNid == localNid ? writerLocal(from, to) : writerRemote(from, to);
    };

    BIO_TRACE_START(MIRROR_TRACE_GET);
    BResult ret = BIO_OK;
    if (req->srcNid == localNid && bdmBatch != nullptr && Cache::Instance().CanBatchWCacheRead()) {
        WCacheBatchSliceWriter batchWriter =
            [bdmBatch, keyResult, &writer](const SlicePtr &from, const SlicePtr &to,
                WCacheSliceRefPtr &sliceRef) -> BResult {
            if (BioConfig::Instance()->GetDaemonConfig().bdmBatchReadStandaloneUseScratchPool &&
                from->GetFlowType() == FLOW_DISK) {
                return bdmBatch->EnqueueDiskToTempThenCopy(from, to, keyResult, sliceRef);
            }
            if (CacheSliceOperator::CanBatchDiskToMemory(from, to)) {
                return bdmBatch->EnqueueDiskToMemory(from, to, keyResult, sliceRef);
            }
            return writer(from, to);
        };
        ret = Cache::Instance().GetWCacheBatch(keyInfo.key, keyInfo.offset, sliceP, batchWriter, realLen);
    } else {
        ret = Cache::Instance().Get(keyInfo.key, keyInfo.offset, sliceP, writer, realLen);
    }
    BIO_TRACE_END(MIRROR_TRACE_GET, ret);
    if (UNLIKELY(ret != BIO_OK)) {
        LOG_ERROR("Get key from cache failed, ret:" << ret << ", key:" << keyInfo.key <<
                                                    ", offset:" << keyInfo.offset << ".");
    }
    return ret;
}

BResult MirrorServer::Delete(DeleteRequest &req)
{
    if (UNLIKELY(!CheckAll(req.comm))) {
        return BIO_CHECK_PT_FAIL;
    }
    BIO_TRACE_START(MIRROR_TRACE_DEL);
    BResult ret = Cache::Instance().Delete(req.comm.ptId, req.key);
    BIO_TRACE_END(MIRROR_TRACE_DEL, ret);
    if (UNLIKELY(ret != BIO_OK)) {
        LOG_ERROR("Delete key failed, ret:" << ret << ", key:" << req.key << ", ptId:" << req.comm.ptId << ".");
    } else {
        LOG_DEBUG("Mirror server delete success, key:" << req.key << ", ptId:" << req.comm.ptId << ".");
    }
    return ret;
}

BResult MirrorServer::AddDisk(AddDiskRequest &req)
{
    if (UNLIKELY(req.comm.magic != MESSAGE_MAGIC)) {
        LOG_ERROR("Check message magic failed.");
        return false;
    }

    BResult ret = AddDiskImpl(req);
    if (UNLIKELY(ret != BIO_OK)) {
        LOG_ERROR("Add disk failed, diskPath: " << req.diskPath << ".");
        return ret;
    }

    return BIO_OK;
}

BResult MirrorServer::AddDiskImpl(AddDiskRequest &req)
{
    if (BioServer::Instance()->IsStandaloneMode()) {
        LOG_ERROR("Standalone mode does not support adding disk dynamically.");
        return BIO_INVALID_PARAM;
    }

    if (!mBioConfig->GetDaemonConfig().hasDiskCache) {
        LOG_ERROR("Add disk is not supported when disk cache is disabled.");
        return BIO_INVALID_PARAM;
    }

    std::lock_guard<std::mutex> lock(mDiskViewMutex);
    if (BdmGetNormalDiskNum() >= DISK_DEV_NUM) {
        LOG_ERROR("The number of available disks must not exceed " << DISK_DEV_NUM << ".");
        return BIO_ERR;
    }
    uint32_t diskId = DISK_ID_INVALID;
    BResult ret = BIO_OK;
    req.diskPath[FILE_PATH_MAX_LEN - 1] = '\0';
    std::string diskPath = req.diskPath;
    BIO_TP_START(SERVER_NO_DISK_CHECK, 0);
    ChkTrue(FileUtil::CanonicalPath(diskPath), BIO_ERR, "The device does not exist.");
    BIO_TP_END
    bool isExist;
    BIO_TP_START(SERVER_OLD_DISK_EXIST, &isExist, true);
    BIO_TP_START(SERVER_SET_OLD_DISK_ID, &diskId, 0);
    isExist = mBioConfig->CheckDiskIsExist(diskPath, diskId);
    BIO_TP_END;
    BIO_TP_END;
    if (isExist) {
        ret = AddOldDiskImpl(diskPath, diskId);
        if (UNLIKELY(ret != BIO_OK)) {
            LOG_ERROR("Add old disk failed, diskPath: " << diskPath << ".");
            return BIO_INNER_ERR;
        }
        return BIO_OK;
    }

    BIO_TP_START(SERVER_ADD_NEW_DISK_FAIL, &ret, BIO_INNER_ERR);
    ret = AddNewDiskImpl(diskPath);
    BIO_TP_END;
    if (UNLIKELY(ret != BIO_OK)) {
        LOG_ERROR("Add new disk failed, diskPath: " << diskPath << ".");
        return ret;
    }

    return BIO_OK;
}

BResult MirrorServer::AddOldDiskImpl(const std::string &diskPath, uint16_t diskId)
{
    LOG_DEBUG("Start to add old disk, diskPath: " << diskPath << ".");
    // reset disk
    BResult res = BioServer::Instance()->BioDiskReset(diskId);
    if (UNLIKELY(res != BIO_OK)) {
        LOG_ERROR("Reset disk failed, diskId: " << diskId << " , res: " << res);
        return res;
    }

    // update diskInfo to cm
    int32_t ret = CmAddNewDisk(diskId, CM_DISK_NORMAL, false);
    if (ret != BIO_OK) {
        LOG_ERROR("Report disk normal failed, diskId: " << diskId << " , diskPath: " << diskPath);
        return ret;
    }

    LOG_DEBUG("Finish to add old disk, diskPath: " << diskPath << ".");
    return BIO_OK;
}

BResult MirrorServer::AddNewDiskImpl(std::string &diskPath)
{
    LOG_DEBUG("Start to add new disk, diskPath: " << diskPath << ".");
    // Determine whether the number of disks exceeds 8
    BResult ret = BIO_OK;
    uint32_t diskCount = BdmGetDiskCount();
    if (UNLIKELY(diskCount >= DISK_MAX_SIZE)) {
        LOG_ERROR("The number of total disks must not exceed " << DISK_MAX_SIZE << ".");
        return BIO_ERR;
    }

    // write diskPath to config
    ret = mBioConfig->CreateDiskConfBak(diskPath);
    if (UNLIKELY(ret != BIO_OK)) {
        LOG_ERROR("Update disk config failed, diskPath: " << diskPath << ", ret: " << ret);
        return ret;
    }

    // add disk to bdm
    BIO_TP_START(SERVER_BDM_UPDATE_SUCCESS, &ret, BIO_OK);
    ret = BioServer::Instance()->BioBdmUpdate(diskPath);
    BIO_TP_END;
    if (UNLIKELY(ret != BIO_OK)) {
        LOG_ERROR("Update new disk to bdm failed, diskPath: " << diskPath << ", ret: " << ret);
        return ret;
    }

    // update info to cm
    uint32_t diskId = diskCount;
    ret = CmAddNewDisk(diskId, CM_DISK_NORMAL, true);
    if (ret != BIO_OK) {
        LOG_ERROR("Update new disk status failed, ret: " << ret);
        return ret;
    }

    // replace config file
    ret = mBioConfig->ReplaceFile(CONFIG_PATH, CONFIG_PATH_BAK);
    if (UNLIKELY(ret != BIO_OK)) {
        LOG_ERROR("Update new disk to bdm failed, diskPath: " << diskPath << ", ret: " << ret);
        return ret;
    }

    mBioConfig->ResizeDaemonConfigDisks(diskPath);
    LOG_DEBUG("Finish to add new disk, diskPath: " << diskPath << ".");
    return BIO_OK;
}

BResult MirrorServer::List(ListRequest &req, std::unordered_map<std::string, ObjStat> &objs)
{
    std::unordered_map<std::string, CacheObjStat> cacheStatInfo;
    BResult ret = Cache::Instance().List(req.prefix, req.comm.ptId, req.isListUnderFs, cacheStatInfo);
    if (UNLIKELY(ret != BIO_OK)) {
        LOG_ERROR("List key failed, ret:" << ret << ", prefix:" << req.prefix << ".");
    } else {
        for (auto &info : cacheStatInfo) {
            if (objs.size() >= 1000U) {
                return BIO_OK;
            }
            ObjStat stat;
            CopyKey(stat.key, info.first.c_str(), MAX_KEY_SIZE);
            stat.size = info.second.size;
            stat.time = info.second.time;
            objs.insert({ info.first, stat });
        }
        LOG_DEBUG("Mirror server List success, prefix:" << req.prefix << ", ptId:" << req.comm.ptId << ", num:" <<
            objs.size() << ".");
    }
    return ret;
}

BResult MirrorServer::Stat(StatRequest &req, ObjStat &objInfo)
{
    if (UNLIKELY(!CheckAll(req.comm))) {
        return BIO_CHECK_PT_FAIL;
    }
    std::string key(req.key);
    CacheObjStat objStat;
    BIO_TRACE_START(MIRROR_TRACE_STAT);
    BResult ret = Cache::Instance().Stat(req.comm.ptId, const_cast<char *>(key.c_str()), objStat);
    BIO_TRACE_END(MIRROR_TRACE_STAT, ret);
    if (UNLIKELY(ret != BIO_OK)) {
        LOG_ERROR("Stat key failed, ret:" << ret << ", key:" << key << ".");
    } else {
        CopyKey(objInfo.key, req.key, KEY_MAX_SIZE);
        objInfo.size = objStat.size;
        objInfo.time = objStat.time;
    }
    return ret;
}

BResult MirrorServer::BatchExist(BatchExistRequest *req, BatchExistResponse &rsp)
{
    if (req->count == 0 || req->count > KEY_MAX_COUNT) {
        return BIO_INVALID_PARAM;
    }
    BIO_TRACE_START(MIRROR_TRACE_BATCH_EXIST);
    for (uint32_t idx = 0; idx < req->count; idx++) {
        std::string key(req->keys[idx].key);
        rsp.result[idx] = Cache::Instance().Exist(req->keys[idx].ptVec, const_cast<char *>(key.c_str()));
        rsp.index[idx] = req->keys[idx].index;
    }
    rsp.count = req->count;
    BIO_TRACE_END(MIRROR_TRACE_BATCH_EXIST, BIO_OK);
    return BIO_OK;
}

BResult MirrorServer::BatchExistConvergence(BatchExistRequest &req, BatchExistResponse &rsp)
{
    if (req.count == 0 || req.count > KEY_MAX_COUNT) {
        return BIO_INVALID_PARAM;
    }
    BIO_TRACE_START(MIRROR_TRACE_BATCH_EXIST);
    for (uint32_t idx = 0; idx < req.count; idx++) {
        std::string key(req.keys[idx].key);
        rsp.result[idx] = Cache::Instance().Exist(req.keys[idx].ptVec, const_cast<char *>(key.c_str()));
        rsp.index[idx] = req.keys[idx].index;
    }
    rsp.count = req.count;
    BIO_TRACE_END(MIRROR_TRACE_BATCH_EXIST, BIO_OK);
    return BIO_OK;
}

BResult MirrorServer::Load(LoadRequest &req)
{
    if (UNLIKELY(!CheckAll(req.comm))) {
        return BIO_CHECK_PT_FAIL;
    }
    BIO_TRACE_START(MIRROR_TRACE_LOAD);
    uint64_t realLen = 0;
    BResult ret = Cache::Instance().Load(req.comm.ptId, req.key, req.offset, req.length, realLen);
    BIO_TRACE_END(MIRROR_TRACE_LOAD, ret);
    return ret == BIO_LOAD_ALLOC_FAIL ? BIO_ALLOC_FAIL : ret;
}

BResult MirrorServer::NotifyUpdate(NotifyUpdateRequest &req)
{
    return BioServer::Instance()->ReportServiceState(req.flag);
}

BResult MirrorServer::CheckUpdateReady(CheckUpdateReadyRequest &req, CheckUpdateReadyResponse &rsp)
{
    if (BioServer::Instance()->IsStandaloneMode()) {
        auto chkRet = Cache::Instance().ServiceUngradeFlush();
        rsp.flag = (chkRet == BIO_OK);
        return BIO_OK;
    }

    auto rpcEngine = BioServer::Instance()->GetNetEngine();
    uint64_t curNodeTimes = 0;
    std::map<CmNodeId, CmNodeInfo, CmNodeIdCmp> nodeView = BioServer::Instance()->GetNodeView(&curNodeTimes);
    uint16_t localNid = BioServer::Instance()->GetCm()->GetCmLocalNodeId().VNodeId();
    rsp.flag = true;

    for (auto &nodeEntry : nodeView) {
        uint16_t dstNid = nodeEntry.second.id.VNodeId();
        if (localNid == dstNid) {
            auto chkRet = Cache::Instance().ServiceUngradeFlush();
            if (chkRet != BIO_OK) {
                rsp.flag = false;
                LOG_WARN("Check local node update not ready, localNid:" << localNid << ", chkRet: " << chkRet << ".");
            }
        } else {
            CheckRemoteUpdateReadyRequest ckRemoteReq = { { MESSAGE_MAGIC, req.comm.ptId, req.comm.ptv, localNid,
                                                            getpid() } };
            CheckRemoteUpdateReadyResponse *ckRemoteRsp = nullptr;
            uint64_t ckRemoteRspLen = 0;
            auto ret = rpcEngine->SyncCall<CheckRemoteUpdateReadyRequest, CheckRemoteUpdateReadyResponse>(dstNid,
                BIO_OP_SERVER_CHECK_REMOTE_UPDATE_READY, ckRemoteReq, &ckRemoteRsp, ckRemoteRspLen);
            if (UNLIKELY(ret != BIO_OK || ckRemoteRsp == nullptr)) {
                LOG_ERROR("Send check remote node update request failed, ret:" << ret << ", dstNid:" << dstNid << ".");
                rsp.flag = false;
                break;
            }
            rsp.flag = ckRemoteRsp->flag;
            if (!rsp.flag) {
                LOG_WARN("Check remote node update not ready, dstNid:" << dstNid << ".");
            }
            free(ckRemoteRsp);
        }
    }

    return BIO_OK;
}

BResult MirrorServer::SyncData(SyncDataRequest &req)
{
    LOG_DEBUG("Master sync data, ptId:" << req.comm.ptId << ", version:" << req.comm.ptv);
    BIO_TRACE_START(MIRROR_TRACE_SYNC_DATA);
    BResult ret = Cache::Instance().Flush(req.comm.ptId, req.comm.ptv);
    BIO_TRACE_END(MIRROR_TRACE_SYNC_DATA, ret);
    if (UNLIKELY(ret != BIO_OK)) {
        LOG_WARN("Sync data failed:" << ret << ", ptId:" << req.comm.ptId << ", version:" << req.comm.ptv);
    }
    return ret;
}

BResult MirrorServer::SendSyncData(uint16_t ptId, uint16_t masterNodeId, uint64_t version)
{
    NetEnginePtr rpcEngine = BioServer::Instance()->GetNetEngine();
    uint16_t localNodeId = Cm::Instance()->GetCmLocalNodeId().nodeId;
    BResult rspRet;
    SyncDataRequest req = { { MESSAGE_MAGIC, ptId, version, localNodeId, getpid() } };
    auto ret = rpcEngine->SyncCall<SyncDataRequest, BResult>(static_cast<BioNodeId>(masterNodeId),
        BIO_OP_SERVER_SYNC_DATA, req, rspRet);
    if (UNLIKELY(ret != BIO_OK)) {
        LOG_ERROR("Send sync sync data failed:" << ret << ", ptId:" << ptId << ", version:" << version);
        return ret;
    }
    return rspRet;
}

BResult MirrorServer::Initialize()
{
    std::lock_guard<std::mutex> lock(mStartLock);
    if (mStarted) {
        return BIO_OK;
    }
    if (BioServer::Instance()->GetNetEngine() != nullptr) {
        RegisterOpcode();
    }
    mBioConfig = BioConfig::Instance();
    if (mBioConfig == nullptr) {
        LOG_ERROR("Mirror server init bio config failed");
        return BIO_NOT_READY;
    }

    mBatchGetExecutor = ExecutorService::Create(mBioConfig->GetDaemonConfig().batchGetThreadNum,
                                                SERVER_BATCH_GET_QUEUE_SIZE);
    if (UNLIKELY(mBatchGetExecutor == nullptr)) {
        LOG_ERROR("Failed to create execution service for get kv, probably out of memory");
        return BIO_ALLOC_FAIL;
    }
    auto ret = mBatchGetExecutor->Start();
    if (!ret) {
        LOG_ERROR("Failed to start execution service for get kv, probably out of memory");
        return BIO_INNER_ERR;
    }
    mStarted = true;
    return BIO_OK;
}

int32_t MirrorServer::MirrorServerShmInit(ServiceContext &ctx, ShmInitRequest *req)
{
    if (UNLIKELY(req->comm.magic != MESSAGE_MAGIC)) {
        LOG_ERROR("Check message magic failed.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_CHECK_PT_FAIL, nullptr, 0);
        return BIO_OK;
    }

    ShmInitResponse rsp{};
    auto config = mBioConfig->GetDaemonConfig();
    rsp.netSegmentSize = mBioConfig->GetNetConfig().netSegmentSize;
    rsp.serverPid = getpid();
    rsp.scene = config.workScene;
    rsp.alignSize = config.workIoAlignSize;
    rsp.ioTimeOut = config.workIoTimeOut;
    rsp.netTimeOut = config.workNetTimeOut;
    rsp.logLevel = config.logLevel;
    rsp.enableHtrace = config.enableTrace;
    rsp.enableCrc = config.enableCrc;
    rsp.enableCli = config.enableCli;
    rsp.enablePrometheus = config.enablePrometheus;
    rsp.scrapeIntervalSec = config.scrapeIntervalSec;
    auto ret = strcpy_s(rsp.listenAddress, sizeof(rsp.listenAddress), config.listenAddress.c_str());
    if (ret != 0) {
        return BIO_ERR;
    }

    ret = BioServer::Instance()->GetNetEngine()->QueryShmInfo(rsp.memFd, rsp.offset, rsp.length, rsp.mKey);
    if (ret != BIO_OK) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INNER_ERR, nullptr, 0);
        return BIO_OK;
    }
    ret = BioServer::Instance()->GetNetEngine()->SendFds(ctx.Channel(), &rsp.memFd, NO_1);
    if (ret != BIO_OK) {
        LOG_ERROR("Send fds failed, ret:" << ret << ".");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INNER_ERR, nullptr, 0);
    } else {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_OK, &rsp, sizeof(ShmInitResponse));
    }
    return BIO_OK;
}

int32_t MirrorServer::HandleShmInit(ServiceContext &ctx)
{
    if (UNLIKELY(!Ready())) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_NOT_READY, nullptr, 0);
        return BIO_OK;
    }

    if (UNLIKELY(ctx.MessageDataLen() != sizeof(ShmInitRequest)) || UNLIKELY(ctx.MessageData() == nullptr)) {
        LOG_ERROR("Receive shm init message len:" << ctx.MessageDataLen() << " or message data invalid.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }

    auto req = static_cast<ShmInitRequest *>(ctx.MessageData());
    return MirrorServerShmInit(ctx, req);
}

int32_t MirrorServer::MirrorServerQueryNodeInfo(ServiceContext &ctx, GetLocalNidRequest *req)
{
    if (UNLIKELY(req->comm.magic != MESSAGE_MAGIC)) {
        LOG_ERROR("Check message magic failed.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_CHECK_PT_FAIL, nullptr, 0);
        return BIO_OK;
    }

    CmNodeId localNid = BioServer::Instance()->GetLocalNid();
    GetLocalNidResponse rsp = { localNid.GroupId(), localNid.VNodeId(), BioServer::Instance()->GetNetProtocol() };
    BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_OK, &rsp, sizeof(GetLocalNidResponse));
    return BIO_OK;
}

int32_t MirrorServer::HandleQueryNodeInfo(ServiceContext &ctx)
{
    if (UNLIKELY(!Ready())) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_NOT_READY, nullptr, 0);
        return BIO_OK;
    }

    if (UNLIKELY(ctx.MessageDataLen() != sizeof(GetLocalNidRequest)) || UNLIKELY(ctx.MessageData() == nullptr)) {
        LOG_ERROR("Receive query pt message len:" << ctx.MessageDataLen() << " or message data invalid.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }

    auto req = static_cast<GetLocalNidRequest *>(ctx.MessageData());
    return MirrorServerQueryNodeInfo(ctx, req);
}

int32_t MirrorServer::MirrorServerQueryNodeInfoByPt(ServiceContext &ctx, FileLocationQueryReq *req)
{
    // 1. 查询PT主节点信息.
    FileLocationQueryRsp rsp;
    CmNodeInfo nodeInfo;
    auto ret = BioServer::Instance()->GetNodeInfo({0, req->masterPtId}, nodeInfo);
    if (UNLIKELY(ret != BIO_OK)) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }
    ret = memcpy_s(rsp.hostMaster, NODE_DESC_SIZE, nodeInfo.ip.c_str(), nodeInfo.ip.length());
    if (UNLIKELY(ret != BIO_OK)) {
        LOG_ERROR("Memory copy failed.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INNER_ERR, nullptr, 0);
        return BIO_OK;
    }
    if (nodeInfo.ip.length() > NODE_DESC_SIZE) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INNER_ERR, nullptr, 0);
        return BIO_OK;
    }
    rsp.hostMaster[nodeInfo.ip.length()] = '\0';
    rsp.portMaster = nodeInfo.port;

    // 2. 查询PT备节点信息.
    ret = BioServer::Instance()->GetNodeInfo({0, req->slavePtId}, nodeInfo);
    if (UNLIKELY(ret != BIO_OK)) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }
    ret = memcpy_s(rsp.hostSlave, NODE_DESC_SIZE, nodeInfo.ip.c_str(), nodeInfo.ip.length());
    if (UNLIKELY(ret != BIO_OK)) {
        LOG_ERROR("Memory copy failed.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INNER_ERR, nullptr, 0);
        return BIO_OK;
    }
    rsp.hostSlave[nodeInfo.ip.length()] = '\0';
    rsp.portSlave = nodeInfo.port;

    BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_OK, &rsp, sizeof(FileLocationQueryRsp));
    return BIO_OK;
}

int32_t MirrorServer::HandleQueryNodeInfoByPt(ServiceContext &ctx)
{
    if (UNLIKELY(!Ready())) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_NOT_READY, nullptr, 0);
        return BIO_OK;
    }

    if (UNLIKELY(ctx.MessageDataLen() != sizeof(FileLocationQueryReq)) || UNLIKELY(ctx.MessageData() == nullptr)) {
        LOG_ERROR("Receive query pt message len:" << ctx.MessageDataLen() << " or message data invalid.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }

    auto req = static_cast<FileLocationQueryReq *>(ctx.MessageData());
    return MirrorServerQueryNodeInfoByPt(ctx, req);
}

int32_t MirrorServer::MirrorServerQueryQuota(ServiceContext &ctx, QueryQuotaRequest *req)
{
    if (UNLIKELY(req->comm.magic != MESSAGE_MAGIC)) {
        LOG_ERROR("Check message magic failed.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_CHECK_PT_FAIL, nullptr, 0);
        return BIO_OK;
    }

    QueryQuotaResponse rsp;
    QueryCacheQuota(*req, rsp);
    BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_OK, &rsp, sizeof(QueryQuotaResponse));
    return BIO_OK;
}

int32_t MirrorServer::MirrorServerAllocQuota(ServiceContext &ctx, AllocQuotaRequest *req)
{
    AllocQuotaResponse rsp;
    auto ret = AllocCacheQuota(*req, rsp);
    if (UNLIKELY(ret != BIO_OK)) {
        rsp.exceptQuota = 0;
    }
    BioServer::Instance()->GetNetEngine()->Reply(ctx, ret, &rsp, sizeof(AllocQuotaResponse));
    return BIO_OK;
}

int32_t MirrorServer::MirrorServerFreeQuota(ServiceContext &ctx, FreeQuotaRequest *req)
{
    // 限制单次最大释放quota的大小为1GB
    if (req->quota > NO_1024 * IO_SIZE_1M) {
        LOG_ERROR("Invalid free quota size:" << req->quota << ", cid:" << req->cid << ", nid:" << req->nid);
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_CHECK_PT_FAIL, nullptr, 0);
        return BIO_OK;
    }

    auto ret = FreeCacheQuota(*req);
    BioServer::Instance()->GetNetEngine()->Reply(ctx, ret, &ret, sizeof(BResult));
    return BIO_OK;
}

int32_t MirrorServer::MirrorServerQueryNodeView(ServiceContext &ctx, QueryNodeViewRequest *req)
{
    if (UNLIKELY(req->comm.magic != MESSAGE_MAGIC)) {
        LOG_ERROR("Check message magic failed.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_CHECK_PT_FAIL, nullptr, 0);
        return BIO_OK;
    }

    QueryNodeViewResponse rsp;
    QueryNodeView(*req, rsp);
    BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_OK, &rsp, sizeof(QueryNodeViewResponse));
    return BIO_OK;
}

int32_t MirrorServer::HandleQueryQuota(ServiceContext &ctx)
{
    if (UNLIKELY(!Ready())) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_NOT_READY, nullptr, 0);
        return BIO_OK;
    }

    if (UNLIKELY(ctx.MessageDataLen() != sizeof(QueryQuotaRequest)) || UNLIKELY(ctx.MessageData() == nullptr)) {
        LOG_ERROR("Receive query quota message len:" << ctx.MessageDataLen() << " or message data invalid.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }

    auto req = static_cast<QueryQuotaRequest *>(ctx.MessageData());
    return MirrorServerQueryQuota(ctx, req);
}

int32_t MirrorServer::HandleAllocQuota(ServiceContext &ctx)
{
    if (UNLIKELY(!Ready())) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_NOT_READY, nullptr, 0);
        return BIO_OK;
    }

    if (UNLIKELY(ctx.MessageDataLen() != sizeof(AllocQuotaRequest)) || UNLIKELY(ctx.MessageData() == nullptr)) {
        LOG_ERROR("Receive alloc quota message len:" << ctx.MessageDataLen() << " or message data invalid.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }

    auto req = static_cast<AllocQuotaRequest *>(ctx.MessageData());
    return MirrorServerAllocQuota(ctx, req);
}

int32_t MirrorServer::HandleFreeQuota(ServiceContext &ctx)
{
    if (UNLIKELY(!Ready())) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_NOT_READY, nullptr, 0);
        return BIO_OK;
    }

    if (UNLIKELY(ctx.MessageDataLen() != sizeof(FreeQuotaRequest)) || UNLIKELY(ctx.MessageData() == nullptr)) {
        LOG_ERROR("Receive free quota message len:" << ctx.MessageDataLen() << " or message data invalid.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }

    auto req = static_cast<FreeQuotaRequest *>(ctx.MessageData());
    return MirrorServerFreeQuota(ctx, req);
}

int32_t MirrorServer::HandleQueryNodeView(ServiceContext &ctx)
{
    if (UNLIKELY(!Ready())) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_NOT_READY, nullptr, 0);
        return BIO_OK;
    }

    if (UNLIKELY(ctx.MessageDataLen() != sizeof(QueryNodeViewRequest)) || UNLIKELY(ctx.MessageData() == nullptr)) {
        LOG_ERROR("Receive query pt message len:" << ctx.MessageDataLen() << " or message data invalid.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }

    auto req = static_cast<QueryNodeViewRequest *>(ctx.MessageData());
    return MirrorServerQueryNodeView(ctx, req);
}

int32_t MirrorServer::MirrorServerQueryPtView(ServiceContext &ctx, QueryPtViewRequest *req)
{
    if (UNLIKELY(req->comm.magic != MESSAGE_MAGIC)) {
        LOG_ERROR("Check message magic failed.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_CHECK_PT_FAIL, nullptr, 0);
        return BIO_OK;
    }

    QueryPtViewResponse rsp;
    QueryPtView(*req, rsp);
    BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_OK, &rsp, sizeof(QueryPtViewResponse));
    return BIO_OK;
}


int32_t MirrorServer::HandleQueryPtView(ServiceContext &ctx)
{
    if (UNLIKELY(!Ready())) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_NOT_READY, nullptr, 0);
        return BIO_OK;
    }

    if (UNLIKELY(ctx.MessageDataLen() != sizeof(QueryPtViewRequest)) || UNLIKELY(ctx.MessageData() == nullptr)) {
        LOG_ERROR("Receive query pt message len:" << ctx.MessageDataLen() << " or message data invalid.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }

    auto req = static_cast<QueryPtViewRequest *>(ctx.MessageData());
    return MirrorServerQueryPtView(ctx, req);
}

BResult MirrorServer::GetFlowGlobEvictOffset(uint16_t ptId, uint64_t flowId, uint64_t &flowOffset)
{
    return SendFlowGetEvictOffset(ptId, flowId, flowOffset);
}

BResult MirrorServer::GetEvictOffset(GetEvictRequest &req, uint64_t &flowOffset)
{
    BIO_TRACE_START(MIRROR_TRACE_GET_EVICT_OFFSET);
    BResult ret = Cache::Instance().GetEvictOffset(req.flowId, flowOffset);
    BIO_TRACE_END(MIRROR_TRACE_GET_EVICT_OFFSET, ret);
    if (UNLIKELY(ret != BIO_OK)) {
        LOG_WARN("Get evict offset failed:" << ret << ", ptId:" << req.comm.ptId << ", flowId:" << req.flowId);
    }
    return ret;
}

BResult MirrorServer::SendFlowGetEvictOffset(uint16_t ptId, uint64_t flowId, uint64_t &flowOffset)
{
    NetEnginePtr rpcEngine = BioServer::Instance()->GetNetEngine();
    CmPtInfo cache;
    auto ret = Cm::Instance()->GetPtInfo(ptId, cache);
    if (ret != BIO_OK) {
        LOG_ERROR("Get ptInfo fail:" << ret << ", ptId:" << ptId);
        return ret;
    }

    uint16_t localNodeId = Cm::Instance()->GetCmLocalNodeId().nodeId;
    GetEvictRequest req = { { MESSAGE_MAGIC, ptId, cache.version, localNodeId, getpid() }, flowId };
    ret = rpcEngine->SyncCall<GetEvictRequest, uint64_t>(static_cast<BioNodeId>(cache.masterNodeId),
        BIO_OP_SERVER_GET_EVICT_OFFSET, req, flowOffset);
    if (UNLIKELY(ret != BIO_OK)) {
        LOG_ERROR("Send get evict offset failed:" << ret << ", ptId:" << ptId << ", version:" << cache.version);
    }
    return ret;
}

bool MirrorServer::IsValidSliceAddress(WCacheSlicePtr &sliceP)
{
    auto addrVec = sliceP->GetAddrs();
    for (auto addr : addrVec) {
        auto beginAddr = addr.chunkId + addr.chunkOffset;
        auto endAddr = beginAddr + addr.chunkLen;
        if (!BioServer::Instance()->GetNetEngine()->IsValidAddress(beginAddr, endAddr)) {
            return false;
        }
    }
    return true;
}

uintptr_t MirrorServer::ParseRealAddress(PutRequest *req)
{
    uintptr_t retAddr = req->mrAddress;
    if (req->comm.srcNid == BioServer::Instance()->GetLocalNid().VNodeId() && req->affinity == GLOBAL_BALANCE) {
        retAddr = TransDataMsgMemAddr(req->comm.pid, req->mrOffset, req->mrSize);
    }
    return retAddr;
}

int32_t MirrorServer::MirrorServerPut(ServiceContext &ctx, PutRequest *req)
{
    WCacheSlicePtr sliceP = nullptr;
    if (req->sliceLen == 0) { // case 1：slice资源来自于SDK端, 使用req中的MR信息
        auto realAddr = ParseRealAddress(req);
        if (UNLIKELY(realAddr == 0)) {
            LOG_ERROR("Put data message memory address invalid, pid:" << req->comm.pid <<
                                                                      ", offset:" << req->mrOffset <<
                                                                      ", size:" << req->mrSize << ".");
            BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_NOT_READY, nullptr, 0);
            return BIO_OK;
        }
        MrInfo mrInfo = { realAddr, static_cast<uint32_t>(req->mrSize) };
        std::vector<FlowAddr> addrVec = { FlowAddr(mrInfo) };
        BIO_TP_START(PUT_SLICE_ZERO_ALLOC_FAIL, &sliceP, nullptr);
        sliceP = MakeRef<WCacheSlice>(req->flowId, req->flowOffset, req->flowIndex, req->length, addrVec);
        BIO_TP_END;
        if (UNLIKELY(sliceP == nullptr)) {
            LOG_ERROR("Make wcache slice failed.");
            BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_ALLOC_FAIL, nullptr, 0);
            return BIO_OK;
        }
        sliceP->SetDataCrc(req->dataCrc);
    } else { // case 2：slice资源来自于Server端, 使用req中的slice buffer信息
        if (UNLIKELY((sizeof(PutRequest) > UINT64_MAX - req->sliceLen) ||
                     (ctx.MessageDataLen() < sizeof(PutRequest) + req->sliceLen))) {
            BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
            return BIO_OK;
        }
        BIO_TP_START(PUT_SLICE_NORMAL_ALLOC_FAIL, &sliceP, nullptr);
        sliceP = MakeRef<WCacheSlice>();
        BIO_TP_END;
        if (UNLIKELY(sliceP == nullptr)) {
            LOG_ERROR("Make wcache slice failed.");
            BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_ALLOC_FAIL, nullptr, 0);
            return BIO_OK;
        }
        auto ret = sliceP->Deserialize(req->sliceBuf, req->sliceLen);
        if (UNLIKELY(ret != BIO_OK)) {
            LOG_ERROR("Deserialize slice failed, ret:" << ret << ".");
            BioServer::Instance()->GetNetEngine()->Reply(ctx, ret, nullptr, 0);
            return BIO_OK;
        }
        // 校验slice buffer反序列化后的slice中的地址有效性
        if (static_cast<uint16_t>(req->comm.srcNid) == BioServer::Instance()->GetLocalNid().VNodeId()) {
            if (!IsValidSliceAddress(sliceP)) {
                BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
                return BIO_OK;
            }
        }
        sliceP->SetDataCrc(req->dataCrc);
    }

    BIO_TRACE_START(MIRROR_TRACE_PUT_RECEIVE_REMOTE);
    BResult result = BIO_OK;
    uint32_t ioStrategy = 0;
    BIO_TP_START(MIRROR_SERVER_HDL_PUT_FAIL, &result, BIO_INNER_RETRY);
    result = Put(*req, sliceP, ctx, ioStrategy);
    BIO_TP_END;
    BIO_TRACE_END(MIRROR_TRACE_PUT_RECEIVE_REMOTE, result);

    PutResponse rsp;
    rsp.ioStrategy = ioStrategy;
    BioServer::Instance()->GetNetEngine()->Reply(ctx, result, &rsp, sizeof(PutResponse));
    return BIO_OK;
}

int32_t MirrorServer::HandlePut(ServiceContext &ctx)
{
    if (UNLIKELY(!Ready())) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_NOT_READY, nullptr, 0);
        return BIO_OK;
    }

    if (UNLIKELY(ctx.MessageDataLen() < sizeof(PutRequest)) || UNLIKELY(ctx.MessageData() == nullptr)) {
        LOG_ERROR("Receive put message len:" << ctx.MessageDataLen() << " or message data invalid.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_NET_RETRY, nullptr, 0);
        return BIO_OK;
    }

    auto req = static_cast<PutRequest *>(ctx.MessageData());
    return MirrorServerPut(ctx, req);
}

int32_t MirrorServer::MirrorServerGet(ServiceContext &ctx, GetRequest *req)
{
    GetResponse rsp;
    BResult result;
    BIO_TP_START(MIRROR_SERVER_HDL_GET_FAIL, &result, BIO_INNER_RETRY);
    result = Get(*req, rsp, ctx);
    BIO_TP_END;
    if (result != BIO_OK) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, result, nullptr, 0);
        return BIO_OK;
    }

    BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_OK, static_cast<void *>(&rsp), sizeof(GetResponse));
    return BIO_OK;
}

int32_t MirrorServer::MirrorServerBatchGet(ServiceContext &ctx, BatchGetRequest *req)
{
    if (UNLIKELY(req->count == 0 || req->count > NET_BATCH_GET_MAX_COUNT)) {
        LOG_ERROR("Invalid batch get count:" << req->count << ".");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }
    size_t reqLen = sizeof(BatchGetRequest) + req->count * sizeof(GetKeyInfo);
    if (UNLIKELY(ctx.MessageDataLen() < reqLen)) {
        LOG_ERROR("Invalid batch get message len:" << ctx.MessageDataLen() << ", expect:" << reqLen << ".");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }

    sem_t sem;
    if (sem_init(&sem, 0, 0) != 0) {
        LOG_ERROR("Init batch get semaphore failed, errno:" << errno << ".");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INNER_ERR, nullptr, 0);
        return BIO_OK;
    }
    std::vector<uint64_t> realLengths(req->count);
    std::vector<int32_t> results(req->count, BIO_OK);
    std::atomic<uint32_t> pending(req->count);
    uint32_t submittedNum = 0;
    BResult ret = BIO_OK;
    BdmCopyBatchContext bdmBatch;

    BIO_TRACE_START(MIRROR_TRACE_BATCH_GET);
    BIO_TRACE_START(MIRROR_TRACE_BATCH_GET_LOOKUP);
    for (uint32_t i = 0; i < req->count; i++) {
        uint32_t index = i;
        std::function<void()> func = [&, index]() {
            BIO_TRACE_START(MIRROR_TRACE_BATCH_SINGLE_GET);
            BResult keyRet =
                BatchSingleGet(req->keysInfo[index], realLengths[index], req, &bdmBatch, &results[index]);
            if (keyRet != BIO_OK) {
                results[index] = keyRet;
            }
            BIO_TRACE_END(MIRROR_TRACE_BATCH_SINGLE_GET, keyRet);
            if (pending.fetch_sub(1) == 1) {
                sem_post(&sem);
            }
        };

        if (!mBatchGetExecutor->Execute(func)) {
            LOG_ERROR("Execute batch get data from shm failed, batch num: " << req->count << " i:" << i);
            ret = BIO_INNER_RETRY;
            uint32_t unsubmittedNum = req->count - submittedNum;
            if (pending.fetch_sub(unsubmittedNum) == unsubmittedNum) {
                sem_post(&sem);
            }
            break;
        }
        submittedNum++;
    }
    int waitRet = WaitSemaphore(sem);
    if (UNLIKELY(waitRet != 0)) {
        LOG_ERROR("Wait batch get task failed, errno:" << waitRet << ".");
        ret = BIO_INNER_ERR;
    }
    BIO_TRACE_END(MIRROR_TRACE_BATCH_GET_LOOKUP, ret);
    sem_destroy(&sem);
    BIO_TRACE_START(MIRROR_TRACE_BATCH_GET_FINAL_FLUSH);
    BResult batchRet = bdmBatch.Submit();
    BIO_TRACE_END(MIRROR_TRACE_BATCH_GET_FINAL_FLUSH, batchRet);
    if (batchRet != BIO_OK) {
        LOG_ERROR("Submit batch get bdm reads failed, ret:" << batchRet << ".");
    }
    BIO_TRACE_END(MIRROR_TRACE_BATCH_GET, ret);
    if (UNLIKELY(ret != BIO_OK)) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, ret, nullptr, 0);
        return BIO_OK;
    }

    size_t rspLen = GetBatchGetWireResponseLen(req->count);
    auto rsp = reinterpret_cast<BatchGetWireResponse *>(calloc(1, rspLen));
    if (UNLIKELY(rsp == nullptr)) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_ALLOC_FAIL, nullptr, 0);
        return BIO_OK;
    }
    rsp->nodeId = Cm::Instance()->GetCmLocalNodeId().VNodeId();
    rsp->count = req->count;
    for (uint32_t i = 0; i < req->count; i++) {
        rsp->items[i].result = results[i];
        rsp->items[i].realLength = realLengths[i];
    }
    BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_OK, static_cast<void *>(rsp), rspLen);
    free(rsp);
    return BIO_OK;
}

BResult MirrorServer::BatchGetConvergence(BatchGetRequest &req, BatchGetResponse &rsp)
{
    rsp = {};
    if (UNLIKELY(req.count == 0 || req.count > STANDALONE_BATCH_GET_MAX_COUNT)) {
        LOG_ERROR("Invalid convergence batch get count:" << req.count << ".");
        return BIO_INVALID_PARAM;
    }

    sem_t sem;
    if (sem_init(&sem, 0, 0) != 0) {
        LOG_ERROR("Init convergence batch get semaphore failed, errno:" << errno << ".");
        return BIO_INNER_ERR;
    }
    std::vector<uint64_t> realLengths(req.count);
    std::vector<int32_t> results(req.count, BIO_OK);
    std::atomic<uint32_t> pending(req.count);
    uint32_t submittedNum = 0;
    BResult ret = BIO_OK;
    BdmCopyBatchContext bdmBatch;

    BIO_TRACE_START(MIRROR_TRACE_BATCH_GET_CONVERGENCE);
    BIO_TRACE_START(MIRROR_TRACE_BATCH_GET_LOOKUP);
    for (uint32_t i = 0; i < req.count; i++) {
        uint32_t index = i;
        std::function<void()> func = [&, index]() {
            BIO_TRACE_START(MIRROR_TRACE_BATCH_SINGLE_GET_CONVERGENCE);
            BResult keyRet =
                BatchSingleGet(req.keysInfo[index], realLengths[index], &req, &bdmBatch, &results[index]);
            if (keyRet != BIO_OK) {
                results[index] = keyRet;
            }
            BIO_TRACE_END(MIRROR_TRACE_BATCH_SINGLE_GET_CONVERGENCE, keyRet);
            if (pending.fetch_sub(1) == 1) {
                sem_post(&sem);
            }
        };

        if (!mBatchGetExecutor->Execute(func)) {
            LOG_ERROR("Execute batch get data from shm failed, batch num: " << req.count << " i:" << i);
            ret = BIO_INNER_RETRY;
            uint32_t unsubmittedNum = req.count - submittedNum;
            if (pending.fetch_sub(unsubmittedNum) == unsubmittedNum) {
                sem_post(&sem);
            }
            break;
        }
        submittedNum++;
    }
    int waitRet = WaitSemaphore(sem);
    if (UNLIKELY(waitRet != 0)) {
        LOG_ERROR("Wait convergence batch get task failed, errno:" << waitRet << ".");
        ret = BIO_INNER_ERR;
    }
    BIO_TRACE_END(MIRROR_TRACE_BATCH_GET_LOOKUP, ret);
    sem_destroy(&sem);
    BIO_TRACE_START(MIRROR_TRACE_BATCH_GET_FINAL_FLUSH);
    BResult batchRet = bdmBatch.Submit();
    BIO_TRACE_END(MIRROR_TRACE_BATCH_GET_FINAL_FLUSH, batchRet);
    if (batchRet != BIO_OK) {
        LOG_ERROR("Submit convergence batch get bdm reads failed, ret:" << batchRet << ".");
    }
    BIO_TRACE_END(MIRROR_TRACE_BATCH_GET_CONVERGENCE, ret);
    if (UNLIKELY(ret != BIO_OK)) {
        return ret;
    }

    rsp.nodeId = GetLocalVNodeId();
    rsp.count = req.count;
    for (uint32_t i = 0; i < req.count; i++) {
        rsp.results[i] = results[i];
        rsp.realLengths[i] = realLengths[i];
    }
    return BIO_OK;
}

int32_t MirrorServer::MirrorServerBatchParseKeyAddr(ServiceContext &ctx, BatchParseKeyAddrRequest *req)
{
    if (UNLIKELY(req->count > NO_1024)) {
        LOG_ERROR("Batch parse key fail, request count:" << req->count << " invalid.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }

    uint32_t rspLen = sizeof(BatchParseKeyAddrResp) + sizeof(BatchKeyAddrInfo) * req->count;
    BatchParseKeyAddrResp *rsp = reinterpret_cast<BatchParseKeyAddrResp*>(malloc(rspLen));
    if (UNLIKELY(rsp == nullptr)) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_ALLOC_FAIL, nullptr, 0);
        return BIO_OK;
    }
    for (uint32_t i = 0; i < req->count; i++) {
        BIO_TRACE_START(MIRROR_TRACE_PARSE_KEY_ADDR);
        rsp->infos[i].result = ParseKeyAddr(req->infos[i].key, req->infos[i].ptId, &(rsp->infos[i]));
        BIO_TRACE_END(MIRROR_TRACE_PARSE_KEY_ADDR, rsp->infos[i].result);
    }

    BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_OK, static_cast<void *>(rsp), rspLen);
    free(rsp);
    return BIO_OK;
}

int32_t MirrorServer::HandleBatchParseKeyAddr(ServiceContext &ctx)
{
    if (UNLIKELY(!Ready())) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_NOT_READY, nullptr, 0);
        return BIO_OK;
    }

    if (UNLIKELY(ctx.MessageDataLen() < sizeof(BatchParseKeyAddrRequest)) || UNLIKELY(ctx.MessageData() == nullptr)) {
        LOG_ERROR("Receive get message len:" << ctx.MessageDataLen() << " or message data invalid.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }

    auto req = static_cast<BatchParseKeyAddrRequest *>(ctx.MessageData());
    if (UNLIKELY(sizeof(BatchParseKeyAddrRequest) + sizeof(BatchKeyInfo) * req->count >  ctx.MessageDataLen())) {
        LOG_ERROR("Receive get message len:" << ctx.MessageDataLen() << " is invalid.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }
    return MirrorServerBatchParseKeyAddr(ctx, req);
}

int32_t MirrorServer::HandleGet(ServiceContext &ctx)
{
    if (UNLIKELY(!Ready())) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_NOT_READY, nullptr, 0);
        return BIO_OK;
    }

    if (UNLIKELY(ctx.MessageDataLen() != sizeof(GetRequest)) || UNLIKELY(ctx.MessageData() == nullptr)) {
        LOG_ERROR("Receive get message len:" << ctx.MessageDataLen() << " or message data invalid.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }

    auto req = static_cast<GetRequest *>(ctx.MessageData());
    return MirrorServerGet(ctx, req);
}

int32_t MirrorServer::HandleBatchGet(ServiceContext &ctx)
{
    if (UNLIKELY(!Ready())) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_NOT_READY, nullptr, 0);
        return BIO_OK;
    }

    if (UNLIKELY(ctx.MessageDataLen() < sizeof(BatchGetRequest)) || UNLIKELY(ctx.MessageData() == nullptr)) {
        LOG_ERROR("Receive get message len:" << ctx.MessageDataLen() << " or message data invalid.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }

    auto req = static_cast<BatchGetRequest *>(ctx.MessageData());
    return MirrorServerBatchGet(ctx, req);
}

int32_t MirrorServer::MirrorServerDelete(ServiceContext &ctx, DeleteRequest *req)
{
    BResult result;
    BIO_TP_START(MIRROR_SERVER_HDL_DELETE_FAIL, &result, BIO_INNER_RETRY);
    result = Delete(*req);
    BIO_TP_END;
    BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_OK, static_cast<void *>(&result), sizeof(BResult));
    return BIO_OK;
}

int32_t MirrorServer::HandleDelete(ServiceContext &ctx)
{
    if (UNLIKELY(!Ready())) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_NOT_READY, nullptr, 0);
        return BIO_OK;
    }

    if (UNLIKELY(ctx.MessageDataLen() != sizeof(DeleteRequest)) || UNLIKELY(ctx.MessageData() == nullptr)) {
        LOG_ERROR("Receive delete message len:" << ctx.MessageDataLen() << " or message data invalid.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }

    auto req = static_cast<DeleteRequest *>(ctx.MessageData());
    return MirrorServerDelete(ctx, req);
}

int32_t MirrorServer::MirrorServerAddDisk(ServiceContext &ctx, AddDiskRequest *req)
{
    BResult ret;
    BIO_TP_START(MIRROR_SERVER_ADD_DISK_FAIL, &ret, BIO_INNER_RETRY);
    ret = AddDisk(*req);
    BIO_TP_END;
    if (UNLIKELY(ret != BIO_OK)) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, ret, nullptr, 0);
        return BIO_OK;
    }

    AddDiskResponse rsp;
    rsp.result = BIO_OK;
    BioServer::Instance()->GetNetEngine()->Reply(ctx, ret, static_cast<void *>(&rsp), sizeof(rsp));
    return BIO_OK;
}

int32_t MirrorServer::HandleAddDisk(ServiceContext &ctx)
{
    if (UNLIKELY(!Ready())) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_NOT_READY, nullptr, 0);
        return BIO_OK;
    }

    if (UNLIKELY(ctx.MessageDataLen() != sizeof(AddDiskRequest)) || UNLIKELY(ctx.MessageData() == nullptr)) {
        LOG_ERROR("Receive add disk message len:" << ctx.MessageDataLen() << " or message data invalid.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }

    auto req = static_cast<AddDiskRequest *>(ctx.MessageData());
    return MirrorServerAddDisk(ctx, req);
}

int32_t MirrorServer::MirrorServerStat(ServiceContext &ctx, StatRequest *req)
{
    ObjStat objInfo;
    BResult ret = BIO_INNER_ERR;
    BIO_TP_START(MIRROR_SERVER_HDL_STAT_FAIL, &ret, BIO_INNER_RETRY);
    ret = Stat(*req, objInfo);
    BIO_TP_END;
    if (ret != BIO_OK) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, ret, nullptr, 0);
        return BIO_OK;
    }

    BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_OK, static_cast<void *>(&objInfo), sizeof(ObjStat));
    return BIO_OK;
}

int32_t MirrorServer::MirrorServerBtachExist(ServiceContext &ctx, BatchExistRequest *req)
{
    BatchExistResponse rsp;
    BResult ret = BatchExist(req, rsp);
    if (ret != BIO_OK) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, ret, nullptr, 0);
        return BIO_OK;
    }

    BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_OK, static_cast<void *>(&rsp), sizeof(BatchExistResponse));
    return BIO_OK;
}

int32_t MirrorServer::HandleStat(ServiceContext &ctx)
{
    if (UNLIKELY(!Ready())) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_NOT_READY, nullptr, 0);
        return BIO_OK;
    }

    if (UNLIKELY(ctx.MessageDataLen() != sizeof(StatRequest)) || UNLIKELY(ctx.MessageData() == nullptr)) {
        LOG_ERROR("Receive delete message len:" << ctx.MessageDataLen() << " or message data invalid.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }

    auto req = static_cast<StatRequest *>(ctx.MessageData());
    return MirrorServerStat(ctx, req);
}

int32_t MirrorServer::HandleBatchExist(ServiceContext &ctx)
{
    if (UNLIKELY(!Ready())) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_NOT_READY, nullptr, 0);
        return BIO_OK;
    }

    if (UNLIKELY(ctx.MessageDataLen() < sizeof(BatchExistRequest)) || UNLIKELY(ctx.MessageData() == nullptr)) {
        LOG_ERROR("Receive delete message len:" << ctx.MessageDataLen() << " or message data invalid.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }

    auto req = static_cast<BatchExistRequest *>(ctx.MessageData());
    if (UNLIKELY(req->count == 0 || req->count > KEY_MAX_COUNT)) {
        LOG_ERROR("Invalid batch exist count:" << req->count << ".");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }
    size_t reqLen = sizeof(BatchExistRequest) + req->count * sizeof(BatchExistKeyInfo);
    if (UNLIKELY(ctx.MessageDataLen() < reqLen)) {
        LOG_ERROR("Invalid batch exist message len:" << ctx.MessageDataLen() << ", expect:" << reqLen << ".");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }
    return MirrorServerBtachExist(ctx, req);
}

int32_t MirrorServer::MirrorServerList(ServiceContext &ctx, ListRequest *req)
{
    std::unordered_map<std::string, ObjStat> objs;
    BResult ret = BIO_INNER_ERR;
    BIO_TP_START(MIRROR_SERVER_HDL_LIST_FAIL, &ret, BIO_INNER_RETRY);
    ret = List(*req, objs);
    BIO_TP_END;
    if (ret != BIO_OK) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, ret, nullptr, 0);
        return BIO_OK;
    }

    if (static_cast<uint16_t>(req->comm.srcNid) == BioServer::Instance()->GetLocalNid().VNodeId()) {
        ReplyListResultLocal(ctx, objs, *req);
    } else {
        ReplyListResultRemote(ctx, req, objs);
    }
    return BIO_OK;
}

int32_t MirrorServer::HandleList(ServiceContext &ctx)
{
    if (UNLIKELY(!Ready())) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_NOT_READY, nullptr, 0);
        return BIO_OK;
    }

    if (UNLIKELY(ctx.MessageDataLen() != sizeof(ListRequest)) || UNLIKELY(ctx.MessageData() == nullptr)) {
        LOG_ERROR("Receive load message len:" << ctx.MessageDataLen() << " or message data invalid.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }

    auto req = static_cast<ListRequest *>(ctx.MessageData());
    return MirrorServerList(ctx, req);
}

int32_t MirrorServer::MirrorServerLoad(ServiceContext &ctx, LoadRequest *req)
{
    BResult ret = BIO_INNER_ERR;
    BIO_TP_START(MIRROR_SERVER_HDL_LOAD_FAIL, &ret, BIO_INNER_RETRY);
    ret = Load(*req);
    BIO_TP_END;
    BioServer::Instance()->GetNetEngine()->Reply(ctx, ret, nullptr, 0);
    return BIO_OK;
}

int32_t MirrorServer::HandleLoad(ServiceContext &ctx)
{
    if (UNLIKELY(!Ready())) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_NOT_READY, nullptr, 0);
        return BIO_OK;
    }

    if (UNLIKELY(ctx.MessageDataLen() != sizeof(LoadRequest) || ctx.MessageData() == nullptr)) {
        LOG_ERROR("Receive load message len:" << ctx.MessageDataLen() << " or message data invalid.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }

    auto req = static_cast<LoadRequest *>(ctx.MessageData());
    return MirrorServerLoad(ctx, req);
}

int32_t MirrorServer::MirrorServerCreateFlow(ServiceContext &ctx, CreateFlowRequest *req)
{
    static uint64_t defaultMaxFlowNum = NO_256 * NO_8192; // 默认Flow的规格为256*8K个.
    if (UNLIKELY(req->comm.magic != MESSAGE_MAGIC)) {
        LOG_ERROR("Check message magic failed.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_CHECK_PT_FAIL, nullptr, 0);
        return BIO_OK;
    }
    {
        ReadLocker<ReadWriteLock> lock(&flowNumLock);
        if (mflowNum > defaultMaxFlowNum) {
            LOG_ERROR("Invalid flowNum:" << mflowNum << ",exceed the specification limit of the flow, limit:"
                                         << defaultMaxFlowNum << ".");
            BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INNER_ERR, nullptr, 0);
            return BIO_OK;
        }
    }

    BResult result = BIO_OK;
    CreateFlowResponse flowInfo = { UINT64_MAX, false, 0, 0, true };
    BIO_TRACE_START(MIRROR_TRACE_CREATE_FLOW);
    if (req->opType == 0) {
        result = CreateFlowMaster(req->comm.pid, req->comm.ptId, req->comm.ptv, flowInfo);
        if (UNLIKELY(result != BIO_OK)) {
            LOG_ERROR("Master create flow failed, ret:" << result << ", ptId:" << req->comm.ptId << ".");
            flowInfo.flowId = UINT64_MAX;
        }
    } else if (req->opType == 1) {
        result = CreateFlowSlave(req->comm.pid, req->comm.ptId, req->comm.ptv, req->flowId, req->isDegrade);
        if (UNLIKELY(result != BIO_OK)) {
            LOG_ERROR("Slave create flow failed, ret:" << result << ", ptId:" << req->comm.ptId << ".");
            flowInfo.flowId = UINT64_MAX;
        } else {
            flowInfo.flowId = 0;
        }
    } else {
        LOG_ERROR("Invalid op type, opType:" << req->opType << ", ptId:" << req->comm.ptId << ".");
    }
    BIO_TRACE_END(MIRROR_TRACE_CREATE_FLOW, BIO_OK);

    {
        WriteLocker<ReadWriteLock> lock(&flowNumLock);
        mflowNum++;
    }

    BioServer::Instance()->GetNetEngine()->Reply(ctx, result, static_cast<void *>(&flowInfo),
        sizeof(CreateFlowResponse));
    return BIO_OK;
}

int32_t MirrorServer::HandleCreateFlow(ServiceContext &ctx)
{
    if (UNLIKELY(!Ready())) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_NOT_READY, nullptr, 0);
        return BIO_OK;
    }

    if (UNLIKELY(ctx.MessageDataLen() != sizeof(CreateFlowRequest)) || UNLIKELY(ctx.MessageData() == nullptr)) {
        LOG_ERROR("Receive create flow message len:" << ctx.MessageDataLen() << " or message data invalid.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }

    auto req = static_cast<CreateFlowRequest *>(ctx.MessageData());
    return MirrorServerCreateFlow(ctx, req);
}

int32_t MirrorServer::MirrorServerDestroyFlow(ServiceContext &ctx, DestroyFlowRequest *req)
{
    if (UNLIKELY(!CheckAll(req->comm))) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_CHECK_PT_FAIL, nullptr, 0);
        return BIO_OK;
    }

    BIO_TRACE_START(MIRROR_TRACE_DESTROY_FLOW);
    auto result = DestroyFlow(req->comm.pid, req->comm.ptId, req->comm.ptv, req->flowId);
    BIO_TRACE_END(MIRROR_TRACE_DESTROY_FLOW, result);
    if (UNLIKELY(result != BIO_OK)) {
        LOG_ERROR("Destroy flow failed, ret:" << result << ", ptId:" << req->comm.ptId << ".");
    } else {
        WriteLocker<ReadWriteLock> lock(&flowNumLock);
        if (mflowNum > 0) {
            mflowNum--;
        };
    }

    BioServer::Instance()->GetNetEngine()->Reply(ctx, result, static_cast<void *>(&req->flowId), sizeof(uint64_t));
    return BIO_OK;
}

int32_t MirrorServer::HandleDestroyFlow(ServiceContext &ctx)
{
    if (UNLIKELY(!Ready())) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_NOT_READY, nullptr, 0);
        return BIO_OK;
    }

    if (UNLIKELY(ctx.MessageDataLen() != sizeof(DestroyFlowRequest)) || UNLIKELY(ctx.MessageData() == nullptr)) {
        LOG_ERROR("Receive destroy flow message len:" << ctx.MessageDataLen() << " or message data invalid.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }
    auto req = static_cast<DestroyFlowRequest *>(ctx.MessageData());

    if (BioServer::Instance()->GetCrbProcessing()) {
        LOG_WARN("Crb processing, do not destroy flow! flowId:" << req->flowId << ", ptId:" << req->comm.ptId);
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_OK, nullptr, 0);
        return BIO_OK;
    }

    return MirrorServerDestroyFlow(ctx, req);
}

int32_t MirrorServer::MirrorServerCreateDataMsgMemPool(ServiceContext &ctx, CreateDataMsgMemPoolRequest *req)
{
    // 1. 创建共享内存.
    uint64_t sdkPoolSize = BioConfig::Instance()->GetDaemonConfig().sdkPoolSize;
    int32_t shmFd = 0;
    std::string shmName = "bio_data_msg_mem_pool" + std::to_string(req->comm.pid);
    auto ret = BioServer::Instance()->GetNetEngine()->CreateShmFdWithName(shmFd, sdkPoolSize, shmName);
    if (ret != BIO_OK) {
        LOG_ERROR("Failed to create shm fd, size:" << sdkPoolSize << ", name:" << shmName << ".");
        return ret;
    }

    off_t offset = 0;
    auto address = mmap(nullptr, sdkPoolSize, PROT_READ | PROT_WRITE, MAP_SHARED, shmFd, offset);
    if (address == MAP_FAILED) {
        LOG_ERROR("Mmap shm size " << sdkPoolSize << " offset " << offset << " failed, error:" << strerror(errno));
        close(shmFd);
        shmFd = -1;
        return BIO_ERR;
    }
    memset_s(address, sdkPoolSize, 0, sdkPoolSize);

    ret = BioServer::Instance()->GetNetEngine()->SendFds(ctx.Channel(), &shmFd, NO_1);
    if (ret != BIO_OK) {
        LOG_ERROR("Send fds failed, ret:" << ret << ", name:" << shmName << ".");
        if (munmap(address, sdkPoolSize) == -1) {
            NET_LOG_ERROR("munmap address failed.");
        }
        close(shmFd);
        shmFd = -1;
        return BIO_ERR;
    }

    // 2. 将内存池信息加入管理MAP中.
    std::lock_guard<std::mutex> lock(mDataMsgMemLock);
    auto iter = mDataMsgMemMgr.find(req->comm.pid);
    if (iter != mDataMsgMemMgr.end()) {
        LOG_WARN("Replace old data message memory pool, holder:" << req->comm.pid << ".");
        auto &item = iter->second;
        BioServer::Instance()->GetNetEngine()->DestroyShmFdWithPid(item.memFd, item.address, req->comm.pid,
                                                                   item.length);
        mDataMsgMemMgr.erase(iter);
    }
    mDataMsgMemMgr.emplace(req->comm.pid, DataMsgMemItem(shmFd, offset, sdkPoolSize, static_cast<uint8_t *>(address)));
    LOG_INFO("Succeed to create data message memory pool, size:" << sdkPoolSize << ", holder:" << req->comm.pid << ".");

    CreateDataMsgMemPoolResponse rsp;
    rsp.memFd = shmFd;
    rsp.offset = offset;
    rsp.poolSize = sdkPoolSize;
    rsp.blockSize = BioConfig::Instance()->GetDaemonConfig().segment;
    BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_OK, &rsp, sizeof(CreateDataMsgMemPoolResponse));
    return BIO_OK;
}

int32_t MirrorServer::MirrorServerGetSlice(ServiceContext &ctx, GetSliceRequest *req)
{
    if (UNLIKELY(!CheckAll(req->comm))) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_CHECK_PT_FAIL, nullptr, 0);
        return BIO_OK;
    }

    if (!CheckGetSliceReq(req)) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INNER_ERR, nullptr, 0);
        return BIO_OK;
    }

    WCacheSlicePtr sliceP = nullptr;
    BResult ret = GetSlice(req->flowId, req->flowOffset, req->flowIndex, req->length, sliceP);
    if (UNLIKELY(ret != BIO_OK)) {
        LOG_ERROR("Get slice failed:" << ret << ".");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, ret, nullptr, 0);
        return BIO_OK;
    }

    uint64_t sliceLen = sliceP->GetSerializeLen();
    uint8_t *tmp = nullptr;
    BIO_TP_START(GET_SLICE_ALLOC_FAIL, &tmp, nullptr);
    tmp = new (std::nothrow) uint8_t[sizeof(GetSliceResponse) + sliceLen];
    BIO_TP_END;
    if (UNLIKELY(tmp == nullptr)) {
        LOG_ERROR("Alloc memory failed, len:" << sizeof(GetSliceResponse) + sliceLen << ".");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_ALLOC_FAIL, nullptr, 0);
        return BIO_OK;
    }
    auto rsp = static_cast<GetSliceResponse *>(static_cast<void *>(tmp));

    std::vector<FlowAddr> addrVec = sliceP->GetAddrs();
    if (addrVec.size() > SLICE_ADDR_MAX_SIZE) { // 限制最大获取64M.
        LOG_ERROR("Get slice num " << addrVec.size() << " exceed " << SLICE_ADDR_MAX_SIZE << ".");
        delete[] tmp;
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INNER_ERR, nullptr, 0);
        return BIO_INNER_ERR;
    }
    rsp->addrNum = addrVec.size();
    for (uint32_t i = 0; i < addrVec.size(); i++) {
        rsp->addr[i].chunkId = addrVec[i].chunkId;
        rsp->addr[i].chunkOffset = addrVec[i].chunkOffset;
        rsp->addr[i].chunkLen = addrVec[i].chunkLen;
        rsp->addrOffset[i] =
            BioServer::Instance()->GetNetEngine()->GetAddressOffset(addrVec[i].chunkId + addrVec[i].chunkOffset);
    }
    rsp->sliceLen = sliceLen;
    uint64_t outSliceLen = 0;
    ret = sliceP->Serialize(rsp->sliceBuf, rsp->sliceLen, outSliceLen);
    if (ret != BIO_OK) {
        delete[] tmp;
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INNER_ERR, nullptr, 0);
        return BIO_OK;
    }
    if (UNLIKELY(outSliceLen != sliceLen)) {
        LOG_ERROR("Serialize slice failed, outSliceLen:" << outSliceLen << ", sliceLen:" << sliceLen << ".");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INNER_ERR, nullptr, 0);
    } else {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_OK, rsp, sizeof(GetSliceResponse) + sliceLen);
    }
    delete[] tmp;
    return BIO_OK;
}

bool MirrorServer::CheckGetSliceReq(GetSliceRequest *req)
{
    if (req->length > IO_SIZE_64M) {
        LOG_ERROR("get slice length too long, length:" << req->length << ".");
        return false;
    }
    return true;
}

void MirrorServer::RecycleDataMsgMem(uint32_t pid)
{
    DataMsgMemItem item;
    {
        std::lock_guard<std::mutex> lock(mDataMsgMemLock);
        auto iter = mDataMsgMemMgr.find(static_cast<pid_t>(pid));
        if (iter == mDataMsgMemMgr.end()) {
            return;
        }
        item = iter->second;
        mDataMsgMemMgr.erase(iter);
    }

    if (item.memFd != -1) {
        BioServer::Instance()->GetNetEngine()->DestroyShmFdWithPid(item.memFd, item.address, pid, item.length);
    }
    LOG_INFO("Succeed to recycle data message memory, holder:" << pid << ".");
}

int32_t MirrorServer::HandleCreateDataMsgMemPool(ServiceContext &ctx)
{
    if (UNLIKELY(!Ready())) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_NOT_READY, nullptr, 0);
        return BIO_OK;
    }

    if (UNLIKELY(ctx.MessageDataLen() != sizeof(CreateDataMsgMemPoolRequest)) ||
        UNLIKELY(ctx.MessageData() == nullptr)) {
        LOG_ERROR("Receive create flow message len:" << ctx.MessageDataLen() << " or message data invalid.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }

    auto req = static_cast<CreateDataMsgMemPoolRequest *>(ctx.MessageData());
    return MirrorServerCreateDataMsgMemPool(ctx, req);
}

int32_t MirrorServer::HandleGetSlice(ServiceContext &ctx)
{
    if (UNLIKELY(!Ready())) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_NOT_READY, nullptr, 0);
        return BIO_OK;
    }

    if (UNLIKELY(ctx.MessageDataLen() != sizeof(GetSliceRequest)) || UNLIKELY(ctx.MessageData() == nullptr)) {
        LOG_ERROR("Receive create flow message len:" << ctx.MessageDataLen() << " or message data invalid.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }

    auto req = static_cast<GetSliceRequest *>(ctx.MessageData());
    return MirrorServerGetSlice(ctx, req);
}

int32_t MirrorServer::MirrorServerSyncData(ServiceContext &ctx, SyncDataRequest *req)
{
    if (UNLIKELY(!CheckAll(req->comm))) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_CHECK_PT_FAIL, nullptr, 0);
        return BIO_OK;
    }

    BResult ret = SyncData(*req);
    BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_OK, static_cast<void *>(&ret), sizeof(BResult));
    return BIO_OK;
}

int32_t MirrorServer::HandleSyncData(ServiceContext &ctx)
{
    if (UNLIKELY(!Ready())) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_NOT_READY, nullptr, 0);
        return BIO_OK;
    }

    if (UNLIKELY(ctx.MessageDataLen() != sizeof(SyncDataRequest)) || UNLIKELY(ctx.MessageData() == nullptr)) {
        LOG_ERROR("Receive sync data message len:" << ctx.MessageDataLen() << " or message data invalid.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }

    auto req = static_cast<SyncDataRequest *>(ctx.MessageData());
    return MirrorServerSyncData(ctx, req);
}

int32_t MirrorServer::MirrorServerGetEvictOffset(ServiceContext &ctx, GetEvictRequest *req)
{
    if (UNLIKELY(!CheckAll(req->comm))) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_CHECK_PT_FAIL, nullptr, 0);
        return BIO_OK;
    }

    uint64_t flowOffset;
    BIO_TRACE_START(MIRROR_TRACE_GET_EVICT_OFFSET_HDL);
    BResult ret = GetEvictOffset(*req, flowOffset);
    BIO_TRACE_END(MIRROR_TRACE_GET_EVICT_OFFSET_HDL, ret);
    if (ret != BIO_OK) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, ret, nullptr, 0);
        return BIO_OK;
    }

    BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_OK, static_cast<void *>(&flowOffset), sizeof(uint64_t));
    return BIO_OK;
}

int32_t MirrorServer::HandleGetEvictOffset(ServiceContext &ctx)
{
    if (UNLIKELY(!Ready())) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_NOT_READY, nullptr, 0);
        return BIO_OK;
    }

    if (UNLIKELY(ctx.MessageDataLen() != sizeof(GetEvictRequest)) || UNLIKELY(ctx.MessageData() == nullptr)) {
        LOG_ERROR("Receive sync data message len:" << ctx.MessageDataLen() << " or message data invalid.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }

    auto req = static_cast<GetEvictRequest *>(ctx.MessageData());
    return MirrorServerGetEvictOffset(ctx, req);
}

int32_t MirrorServer::MirrorServerFreeMem(ServiceContext &ctx, FreeMemRequest *req)
{
    if (!CheckFreeMemReq(req)) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }

    for (uint32_t idx = 0; idx < req->num; idx++) {
        auto addr = BioServer::Instance()->GetNetEngine()->GetShmAddress(req->addr[idx], 0);
        BioServer::Instance()->GetNetEngine()->FreeLocalMrSingle(reinterpret_cast<uintptr_t>(addr));
    }

    BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_OK, nullptr, 0);
    return BIO_OK;
}

bool MirrorServer::CheckFreeMemReq(FreeMemRequest *req)
{
    BIO_TP_START(MIRRIR_SERVER_CHECK_FREE_MEM_REQ_PASS_CHECK, 0);
    bool ckRet = CheckAll(req->comm);
    if (!ckRet) {
        return ckRet;
    }
    BIO_TP_END;

    if (req->num > SLICE_ADDR_SIZE) {
        LOG_ERROR("Invalid param num: " << req->num << ".");
        return false;
    }
    return true;
}

int32_t MirrorServer::HandleFreeMem(ServiceContext &ctx)
{
#ifndef DEBUG_UT
    if (UNLIKELY(!Ready())) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_NOT_READY, nullptr, 0);
        return BIO_OK;
    }

    if (UNLIKELY(ctx.MessageDataLen() != sizeof(FreeMemRequest)) || UNLIKELY(ctx.MessageData() == nullptr)) {
        LOG_ERROR("Receive sync data message len:" << ctx.MessageDataLen() << " or message data invalid.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }

    auto req = static_cast<FreeMemRequest *>(ctx.MessageData());
#else
    FreeMemRequest *req = new FreeMemRequest();
    req->comm.magic = NO_1;
#endif

    return MirrorServerFreeMem(ctx, req);
}

int32_t MirrorServer::MirrorServerNotifyUpdate(ServiceContext &ctx, NotifyUpdateRequest *req)
{
    if (!CheckNotifyUpdateReq(req)) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INNER_ERR, nullptr, 0);
        return BIO_OK;
    }

    auto ret = NotifyUpdate(*req);
    BioServer::Instance()->GetNetEngine()->Reply(ctx, ret, nullptr, 0);
    return BIO_OK;
}

bool MirrorServer::CheckNotifyUpdateReq(NotifyUpdateRequest *req)
{
    return CheckMagic(req->comm);
}

int32_t MirrorServer::HandleNotifyUpdate(ServiceContext &ctx)
{
    if (UNLIKELY(!Ready())) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_NOT_READY, nullptr, 0);
        return BIO_OK;
    }

    if (UNLIKELY(ctx.MessageDataLen() != sizeof(NotifyUpdateRequest)) || UNLIKELY(ctx.MessageData() == nullptr)) {
        LOG_ERROR("Receive sync data message len:" << ctx.MessageDataLen() << " or message data invalid.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }

    auto req = static_cast<NotifyUpdateRequest *>(ctx.MessageData());
    return MirrorServerNotifyUpdate(ctx, req);
}

int32_t MirrorServer::MirrorServerCheckUpdateReady(ServiceContext &ctx, CheckUpdateReadyRequest *req)
{
    if (!CheckUpdateReadyReq(req)) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }

    CheckUpdateReadyResponse rsp;
    auto ret = CheckUpdateReady(*req, rsp);
    BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_OK, &rsp, sizeof(CheckUpdateReadyResponse));
    return BIO_OK;
}

bool MirrorServer::CheckUpdateReadyReq(CheckUpdateReadyRequest *req)
{
    return CheckMagic(req->comm);
}

int32_t MirrorServer::HandleCheckUpdateReady(ServiceContext &ctx)
{
    if (UNLIKELY(!Ready())) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_NOT_READY, nullptr, 0);
        return BIO_OK;
    }

    if (UNLIKELY(ctx.MessageDataLen() != sizeof(CheckUpdateReadyRequest)) || UNLIKELY(ctx.MessageData() == nullptr)) {
        LOG_ERROR("Receive sync data message len:" << ctx.MessageDataLen() << " or message data invalid.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }

    auto req = static_cast<CheckUpdateReadyRequest *>(ctx.MessageData());
    return MirrorServerCheckUpdateReady(ctx, req);
}

int32_t MirrorServer::MirrorServerCheckRemoteUpdateReady(ServiceContext &ctx, CheckRemoteUpdateReadyRequest *req)
{
    CheckRemoteUpdateReadyResponse rsp;
    auto chkRet = Cache::Instance().ServiceUngradeFlush();
    if (chkRet != BIO_OK) {
        rsp.flag = false;
        LOG_WARN("Check remote update ready failed, chkRet: " << chkRet << ".");
    } else {
        rsp.flag = true;
    }
    BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_OK, &rsp, sizeof(CheckRemoteUpdateReadyResponse));
    return BIO_OK;
}

int32_t MirrorServer::HandleCheckRemoteUpdateReady(ServiceContext &ctx)
{
    if (UNLIKELY(!Ready())) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_NOT_READY, nullptr, 0);
        return BIO_OK;
    }

    if (UNLIKELY(ctx.MessageDataLen() != sizeof(CheckRemoteUpdateReadyRequest)) ||
        UNLIKELY(ctx.MessageData() == nullptr)) {
        LOG_ERROR("Receive sync data message len:" << ctx.MessageDataLen() << " or message data invalid.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }

    auto req = static_cast<CheckRemoteUpdateReadyRequest *>(ctx.MessageData());
    return MirrorServerCheckRemoteUpdateReady(ctx, req);
}

int32_t MirrorServer::MirrorServerGetUnderFsConfig(ServiceContext &ctx, GetUnderFsConfigRequest *req)
{
    if (UNLIKELY(req->comm.magic != MESSAGE_MAGIC)) {
        LOG_ERROR("Check message magic failed.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }

    GetUnderFsConfigResponse rsp;
    BioConfig::UnderFsConfig config = UnderFsConfig::Instance()->GetUnderFsConfig();

    int32_t ret = BIO_INNER_ERR;
    do {
        // 把 destsz 设为 KEY_MAX_SIZE-1，让超长时直接报错，保证在数组的最后一个元素之后的位置写入空字符，不会导致数组越界
        ret = memcpy_s(rsp.underFsType, KEY_MAX_SIZE - 1, config.underFsType.c_str(), config.underFsType.size());
        if (UNLIKELY(ret != BIO_OK)) {
            break;
        }
        rsp.underFsType[config.underFsType.size()] = '\0';

        ret = memcpy_s(rsp.hdfsConfig.nameNode, KEY_MAX_SIZE - 1, config.hdfsConfig.nameNode.c_str(),
            config.hdfsConfig.nameNode.size());
        if (UNLIKELY(ret != BIO_OK)) {
            break;
        }
        rsp.hdfsConfig.nameNode[config.hdfsConfig.nameNode.size()] = '\0';

        ret = memcpy_s(rsp.hdfsConfig.workingPath, KEY_MAX_SIZE - 1, config.hdfsConfig.workingPath.c_str(),
                       config.hdfsConfig.workingPath.size());
        if (UNLIKELY(ret != BIO_OK)) {
            break;
        }
        rsp.hdfsConfig.workingPath[config.hdfsConfig.workingPath.size()] = '\0';

        ret = memcpy_s(rsp.cephConfig.user, KEY_MAX_SIZE - 1, config.cephConfig.user.c_str(),
                       config.cephConfig.user.size());
        if (UNLIKELY(ret != BIO_OK)) {
            break;
        }
        rsp.cephConfig.user[config.cephConfig.user.size()] = '\0';

        ret = memcpy_s(rsp.cephConfig.cfgPath, KEY_MAX_SIZE - 1, config.cephConfig.cfgPath.c_str(),
                       config.cephConfig.cfgPath.size());
        if (UNLIKELY(ret != BIO_OK)) {
            break;
        }
        rsp.cephConfig.cfgPath[config.cephConfig.cfgPath.size()] = '\0';

        ret = memcpy_s(rsp.cephConfig.cluster, KEY_MAX_SIZE - 1, config.cephConfig.cluster.c_str(),
                       config.cephConfig.cluster.size());
        if (UNLIKELY(ret != BIO_OK)) {
            break;
        }
        rsp.cephConfig.cluster[config.cephConfig.cluster.size()] = '\0';

        ret = memcpy_s(rsp.cephConfig.pool, KEY_MAX_SIZE - 1, config.cephConfig.pools.at(0).c_str(),
                       config.cephConfig.pools.at(0).size());
        if (UNLIKELY(ret != BIO_OK)) {
            break;
        }
        rsp.cephConfig.pool[config.cephConfig.pools.at(0).size()] = '\0';
    } while (false);
    if (ret != BIO_OK) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, ret, nullptr, 0);
    } else {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_OK, &rsp, sizeof(GetUnderFsConfigResponse));
    }
    return BIO_OK;
}

int32_t MirrorServer::HandleGetUnderFsConfig(ServiceContext &ctx)
{
    if (UNLIKELY(!Ready())) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_NOT_READY, nullptr, 0);
        return BIO_OK;
    }

    if (UNLIKELY(ctx.MessageDataLen() != sizeof(GetUnderFsConfigRequest)) || UNLIKELY(ctx.MessageData() == nullptr)) {
        LOG_ERROR("Receive sync data message len:" << ctx.MessageDataLen() << " or message data invalid.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }

    auto req = static_cast<GetUnderFsConfigRequest *>(ctx.MessageData());
    return MirrorServerGetUnderFsConfig(ctx, req);
}

int32_t MirrorServer::HandleProcBrokenSyncFlow(ServiceContext &ctx)
{
    if (UNLIKELY(!Ready())) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_NOT_READY, nullptr, 0);
        return BIO_OK;
    }

    if (UNLIKELY(ctx.MessageDataLen() != sizeof(ProcFlowSyncRequest)) || UNLIKELY(ctx.MessageData() == nullptr)) {
        LOG_ERROR("Receive Proc Flow Sync message len:" << ctx.MessageDataLen() << " or message data invalid.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }

    auto req = static_cast<ProcFlowSyncRequest *>(ctx.MessageData());
    return MirrorServerProcBrokenSyncFlow(ctx, req);
}

int32_t MirrorServer::MirrorServerProcBrokenSyncFlow(ServiceContext &ctx, ProcFlowSyncRequest *req)
{
    ProcFlowSyncResponse rsp;
    rsp.needDestroy = true;
    bool needDestroy;
    auto ret = Cache::Instance().ProcBrokenSyncFlow(req->flowId, req->index, req->offset, needDestroy);
    if (ret == BIO_OK) {
        rsp.needDestroy = needDestroy;
        rsp.nodeId = Cm::Instance()->GetCmLocalNodeId().VNodeId();
    }
    BioServer::Instance()->GetNetEngine()->Reply(ctx, ret, &rsp, sizeof(ProcFlowSyncResponse));
    return BIO_OK;
}

BResult MirrorServer::CalcCacheResourceLocal(CacheResourceRequest *req, CacheResourceResponse *rsp)
{
    if (req == nullptr || rsp == nullptr) {
        return BIO_INVALID_PARAM;
    }
    *rsp = {};
    rsp->nodeId = BioServer::Instance()->GetLocalNid().VNodeId();
    CacheResDescription desc{};
    Cache::Instance().GetCacheResources(desc, WRITE_CACHE);
    rsp->wCacheMemCapacity = desc.memCapacity;
    rsp->wCacheDiskCapacity = desc.diskCapacity;
    rsp->wCacheMemUsedSize = desc.memUsedSize;
    rsp->wCacheDiskUsedSize = desc.diskUsedSize;
    Cache::Instance().GetCacheResources(desc, READ_CACHE);
    rsp->rCacheMemCapacity = desc.memCapacity;
    rsp->rCacheDiskCapacity = desc.diskCapacity;
    rsp->rCacheMemUsedSize = desc.memUsedSize;
    rsp->rCacheDiskUsedSize = desc.diskUsedSize;
    if (req->includeDiskInfo) {
        DiskResourceMonitor::Query(rsp->disks, rsp->diskNum);
    }
    return BIO_OK;
}

BResult MirrorServer::GetCacheHitLocal(CacheHitResponse *rsp)
{
    rsp->nodeId = BioServer::Instance()->GetLocalNid().VNodeId();
    rsp->rCacheHitMemCount = RCacheStatistic::Instance().GetHitMemCount();
    rsp->rCacheHitDiskCount = RCacheStatistic::Instance().GetHitDiskCount();
    rsp->rCacheHitCount = RCacheStatistic::Instance().GetHitCount();
    rsp->rCacheTotalCount = RCacheStatistic::Instance().GetTotalCount();
    rsp->wCacheHitMemCount = WCacheStatistic::Instance().GetHitMemCount();
    rsp->wCacheHitDiskCount = WCacheStatistic::Instance().GetHitDiskCount();
    rsp->wCacheHitCount = WCacheStatistic::Instance().GetHitCount();
    rsp->wCacheTotalCount = WCacheStatistic::Instance().GetTotalCount();
    rsp->backendHitCount = DiskStatistic::Instance().GetHitCount();
    return BIO_OK;
}

int32_t MirrorServer::HandleQueryCacheResource(ServiceContext &ctx)
{
    if (UNLIKELY(!Ready())) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_NOT_READY, nullptr, 0);
        return BIO_OK;
    }

    BIO_TP_START(CALC_CACHE_RESOURCE, 0);
    if (UNLIKELY(ctx.MessageDataLen() != sizeof(CacheResourceRequest)) || UNLIKELY(ctx.MessageData() == nullptr)) {
        LOG_ERROR("Receive sync data message len:" << ctx.MessageDataLen() << " or message data invalid.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }
    BIO_TP_END;

    return MirrorServerQueryCacheResource(ctx);
}

BResult MirrorServer::GetTracePointsLocal(GetTracePointsResponse *rsp)
{
    rsp->traceDatabase = GetTraceData();
    if (rsp->traceDatabase.count == 0) {
        LOG_ERROR("Get trace data failed!");
        return BIO_ERR;
    }
    return BIO_OK;
}

int32_t MirrorServer::HandleGetTracePoints(ServiceContext &ctx)
{
    if (UNLIKELY(!Ready())) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_NOT_READY, nullptr, 0);
        return BIO_OK;
    }

    if (UNLIKELY(ctx.MessageDataLen() != sizeof(GetTracePointsRequest)) || UNLIKELY(ctx.MessageData() == nullptr)) {
        LOG_ERROR("Receive sync data message len:" << ctx.MessageDataLen() << " or message data invalid.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }

    return MirrorServerGetTracePoints(ctx);
}

TraceDatabase MirrorServer::GetTraceData()
{
    TraceDatabase traceDatabase = {};
    auto tracePoints = ock::htracer::HtracerManager::GetTracePoints();
    BIO_TP_START(SERVER_GET_TRACEDATA_ERR, &tracePoints, nullptr);
    BIO_TP_END;
    if (tracePoints == nullptr) {
        traceDatabase.count = 0;
        return traceDatabase;
    }
    int traceCount = 0;
    for (int i = 0; i < ock::htracer::MAX_SERVICE_NUM; ++i) {
        for (int j = 0; j < ock::htracer::MAX_INNER_ID_NUM; ++j) {
            auto &traceInfo = tracePoints[i][j];
            if (!traceInfo.NameValid()) {
                continue;
            }
            BIO_TP_START(MIRROR_SERVER_TRACE_EXCEED_ARRAY_SIZE, &traceCount, TRACE_MAX_NUM);
            BIO_TP_END;
            if (traceCount >= TRACE_MAX_NUM) {
                LOG_ERROR("Trace num exceeds the array size," << "max trace num: " << TRACE_MAX_NUM);
                traceDatabase.count = traceCount;
                return traceDatabase;
            }

            TraceData traceData;
            auto str = traceInfo.GetName();
            auto ret = strcpy_s(traceData.traceName, sizeof(traceData.traceName), str.c_str());
            if (ret != 0) {
                LOG_ERROR("strcpy_s failed with error code: " << ret << ".");
                return TraceDatabase();
            }

            traceData.metrics.beginData = traceInfo.GetBegin();
            traceData.metrics.goodEnd = traceInfo.GetGoodEnd();
            traceData.metrics.badEnd = traceInfo.GetBadEnd();
            traceData.metrics.min = (traceInfo.GetMin() == UINT64_MAX ?
                0 : static_cast<uint64_t>(static_cast<double>(traceInfo.GetMin()) / ock::htracer::unitStep));
            traceData.metrics.max = static_cast<uint64_t>(static_cast<double>(traceInfo.GetMax()) /
                ock::htracer::unitStep);
            traceData.metrics.total = static_cast<uint64_t>(static_cast<double>(traceInfo.GetTotal()) /
                ock::htracer::unitStep);

            traceDatabase.traces[traceCount] = traceData;
            traceCount++;
        }
    }
    traceDatabase.count = traceCount;

    return traceDatabase;
}

int32_t MirrorServer::MirrorServerGetTracePoints(ServiceContext &ctx)
{
    GetTracePointsResponse rsp;
    rsp.traceDatabase = GetTraceData();
    if (rsp.traceDatabase.count == 0) {
        LOG_ERROR("Get trace data failed!");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_ERR, nullptr, 0);
        return BIO_OK;
    }

    BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_OK, static_cast<void *>(&rsp),
                                                 sizeof(GetTracePointsResponse));
    return BIO_OK;
}

int32_t MirrorServer::HandleGetCacheHit(ServiceContext &ctx)
{
    if (UNLIKELY(!Ready())) {
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_NOT_READY, nullptr, 0);
        return BIO_OK;
    }

    BIO_TP_START(GET_CACHE_HIT, 0);
    if (UNLIKELY(ctx.MessageDataLen() != sizeof(CacheHitRequest)) || UNLIKELY(ctx.MessageData() == nullptr)) {
        LOG_ERROR("Receive sync data message len:" << ctx.MessageDataLen() << " or message data invalid.");
        BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_INVALID_PARAM, nullptr, 0);
        return BIO_OK;
    }
    BIO_TP_END;

    return MirrorServerGetCacheHit(ctx);
}

int32_t MirrorServer::MirrorServerQueryCacheResource(ServiceContext &ctx)
{
    CacheResourceResponse rsp{};
    auto req = static_cast<CacheResourceRequest *>(ctx.MessageData());
    rsp.nodeId = BioServer::Instance()->GetLocalNid().VNodeId();
    CacheResDescription desc{};
    Cache::Instance().GetCacheResources(desc, WRITE_CACHE);
    rsp.wCacheMemCapacity = desc.memCapacity;
    rsp.wCacheDiskCapacity = desc.diskCapacity;
    rsp.wCacheMemUsedSize = desc.memUsedSize;
    rsp.wCacheDiskUsedSize = desc.diskUsedSize;
    Cache::Instance().GetCacheResources(desc, READ_CACHE);
    rsp.rCacheMemCapacity = desc.memCapacity;
    rsp.rCacheDiskCapacity = desc.diskCapacity;
    rsp.rCacheMemUsedSize = desc.memUsedSize;
    rsp.rCacheDiskUsedSize = desc.diskUsedSize;
    if (req != nullptr && req->includeDiskInfo) {
        DiskResourceMonitor::Query(rsp.disks, rsp.diskNum);
    }
    BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_OK, static_cast<void *>(&rsp), sizeof(CacheResourceResponse));
    return BIO_OK;
}

int32_t MirrorServer::MirrorServerGetCacheHit(ServiceContext &ctx)
{
    CacheHitResponse rsp;
    rsp.nodeId = BioServer::Instance()->GetLocalNid().VNodeId();
    rsp.rCacheHitMemCount = RCacheStatistic::Instance().GetHitMemCount();
    rsp.rCacheHitDiskCount = RCacheStatistic::Instance().GetHitDiskCount();
    rsp.rCacheHitCount = RCacheStatistic::Instance().GetHitCount();
    rsp.rCacheTotalCount = RCacheStatistic::Instance().GetTotalCount();
    rsp.wCacheHitMemCount = WCacheStatistic::Instance().GetHitMemCount();
    rsp.wCacheHitDiskCount = WCacheStatistic::Instance().GetHitDiskCount();
    rsp.wCacheHitCount = WCacheStatistic::Instance().GetHitCount();
    rsp.wCacheTotalCount = WCacheStatistic::Instance().GetTotalCount();
    rsp.backendHitCount = DiskStatistic::Instance().GetHitCount();
    BioServer::Instance()->GetNetEngine()->Reply(ctx, BIO_OK, static_cast<void *>(&rsp), sizeof(CacheHitResponse));
    return BIO_OK;
}
