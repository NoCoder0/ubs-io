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

#include <utility>
#include <cerrno>
#include <thread>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <linux/version.h>
#include "message.h"
#include "message_op.h"
#include "bio_client_log.h"
#include "bio_client_agent.h"
#include "bio_client_net.h"

using namespace ock::bio;
using namespace ock::bio::agent;
using namespace ock::bio::net;

static void Log(int level, const char *msg)
{
    if (level == 0) {
        CLIENT_LOG_TRACE(msg);
    } else if (level == 1U) {
        CLIENT_LOG_DEBUG(msg);
    } else if (level == 2U) {
        CLIENT_LOG_INFO(msg);
    } else if (level == 3U) {
        CLIENT_LOG_WARN(msg);
    } else if (level == 4U) {
        CLIENT_LOG_ERROR(msg);
    }
}

void BioClientNet::ApplyStandaloneRuntimeConfig(const StandaloneRuntimeConfigResponse &rsp)
{
    mServerPid = rsp.serverPid;
    mShmFd = -1;
    mShmOffset = 0;
    mShmLength = 0;
    mShmKey = 0;
    mNetSegmentSize = rsp.netSegmentSize;
    mWorkScene = rsp.scene;
    mWorkIoAlignSize = rsp.alignSize;
    mWorkIoTimeOut = rsp.ioTimeOut;
    mWorkNetTimeOut = rsp.netTimeOut;
    mLogLevel = rsp.logLevel;
    mEnableHtrace = rsp.enableHtrace;
    mEnableCrc = rsp.enableCrc;
    mEnableCli = rsp.enableCli;
    mEnablePrometheus = rsp.enablePrometheus;
    mPrometheusListenAddress = rsp.listenAddress;
    mPrometheusScrapeIntervalSec = rsp.scrapeIntervalSec;
}

BResult BioClientNet::StartPre(WorkerMode mode, NetOptions &netConf)
{
    mMode = mode;
    BResult ret = BIO_OK;
    if (mode == STANDALONE) {
        StandaloneRuntimeConfigResponse rsp{};
        ret = BioClientAgent::Instance()->GetRuntimeConfig(rsp);
        if (ret != BIO_OK) {
            CLIENT_LOG_ERROR("Get standalone runtime config failed, ret:" << ret << ".");
            return ret;
        }
        if (!CheckRuntimeConfigResp(rsp)) {
            CLIENT_LOG_ERROR("Invalid standalone runtime config, alignSize:" << rsp.alignSize << ", ioTimeOut:" <<
                rsp.ioTimeOut << ", netTimeOut:" << rsp.netTimeOut << ", logLevel:" << rsp.logLevel << ", scene:" <<
                rsp.scene << ".");
            return BIO_INNER_ERR;
        }
        rsp.listenAddress[MAX_LISTEN_ADDRESS_LENGTH - 1] = '\0';
        ApplyStandaloneRuntimeConfig(rsp);
        return BIO_OK;
    }
    if (mode == CONVERGENCE) { // 融合部署场景获取server端的net引擎实例, client和server共用一个net引擎.
        mNetEngine = BioClientAgent::Instance()->GetNetService();
        if (mNetEngine == nullptr) {
            ret = BIO_INNER_ERR;
        }
    } else { // 分离部署场景创建IPC服务.
        ret = StartIpcService(netConf);
        if (ret == BIO_OK) {
            netConf.netSegmentSize = mNetSegmentSize;
        }
    }
    return ret;
}

BResult BioClientNet::StartPost(uint16_t localNid, std::map<CmNodeId, CmNodeInfo, CmNodeIdCmp> nodeView,
    uint16_t protocol, const NetOptions netConf)
{
    if (mMode == CONVERGENCE || mMode == STANDALONE) {
        return BIO_OK;
    }
    mLocalNid = localNid;
    mNetEngine->SetLocalNodeId(localNid);

    std::string ipMask;
    uint16_t port = UINT16_MAX;
    for (auto &node : nodeView) {
        if (node.second.id.VNodeId() == localNid) {
            ipMask = node.second.ip;
            port = node.second.port;
            break;
        }
    }
    if (port == UINT16_MAX) {
        CLIENT_LOG_ERROR("Not found local node info.");
        return BIO_OK;
    }

    auto ret = StartRpcService((ipMask + "/24"), port, static_cast<ServiceProtocol>(protocol), NO_4, netConf);
    if (ret != BIO_OK) {
        CLIENT_LOG_ERROR("Bio client start rpc service failed, result:" << ret << ".");
        return ret;
    }

    for (auto &node : nodeView) {
        if (node.second.id.VNodeId() == localNid) {
            continue;
        }
        if (node.second.status != CM_NODE_NORMAL) {
            continue;
        }
        ConnectInfo info(localNid, static_cast<uint32_t>(getpid()), node.second.id.VNodeId(), node.second.ip,
            node.second.port, NO_1);
        CLIENT_LOG_INFO("Connect to remote node:" << info.peerId.nid << ", ip:" << info.ip << ", port:" << info.port);
        BIO_TP_START(SDK_BIO_NET_START_CONNECT_FAIL, &ret, BIO_INNER_ERR);
        ret = mNetEngine->SyncConnect(info);
        BIO_TP_END;
        if (ret != BIO_OK) {
            CLIENT_LOG_ERROR("Connect to bio server failed, result:" << ret << ".");
            return ret;
        }
    }
    return BIO_OK;
}

void BioClientNet::Exit()
{
    if (mMode == SEPARATES) {
        StopInner();
    }
}

BResult BioClientNet::CorrectFd()
{
    if (mShmFd == -1) {
        return BIO_OK;
    }
    int32_t realFd = -1;
    auto result = mNetEngine->ReceiveFds(INVALID_NID, &realFd, 1U);
    if (result != BIO_OK) {
        CLIENT_LOG_ERROR("receive file mem fd failed, ret:" << result << ".");
        return BIO_ERR;
    }
    mShmFd = realFd;
    return BIO_OK;
}

BResult BioClientNet::CheckShmFd()
{
    struct stat buffer {};
    auto ret = fstat(mShmFd, &buffer);
    if (ret < 0) {
        CLIENT_LOG_ERROR("Read file failed, ret:" << strerror(errno) << ".");
        return BIO_ERR;
    }
    if (mShmOffset + mShmLength > static_cast<uint64_t>(buffer.st_size)) {
        CLIENT_LOG_ERROR("Check file size failed.");
        return BIO_ERR;
    }
    return BIO_OK;
}

BResult BioClientNet::ShmInitInner()
{
    // mShmFd = -1，分离部署server端不需要初始化shm，直接返回
    if (mShmFd == -1) {
        CLIENT_LOG_INFO("mShmFd is -1,not need ShmInitInner.");
        return BIO_OK;
    }
    if (CheckShmFd() != BIO_OK) {
        mShmFd = -1;
        return BIO_ERR;
    }

    auto offset = static_cast<off_t>(mShmOffset);
    auto address = mmap(nullptr, mShmLength, PROT_READ | PROT_WRITE, MAP_SHARED, mShmFd, offset);
    if (address == MAP_FAILED) {
        NET_LOG_ERROR("Mmap bio_shm size " << mShmLength << " offset " << offset << " failed, error:" <<
            strerror(errno));
        close(mShmFd);
        mShmFd = -1;
        return BIO_ERR;
    }
    mShmAddr = static_cast<uint8_t *>(address);
    return BIO_OK;
}

BResult BioClientNet::ShmInit()
{
    uint64_t defaultMaxShmSize = (300UL * 1024UL * 1024UL * 1024UL); // 默认最大可配置共享内存大小为300G
    ShmInitRequest req = { { MESSAGE_MAGIC, 0, 0, INVALID_NID, getpid() } };
    ShmInitResponse rsp;
    BResult ret = mNetEngine->SyncCall<ShmInitRequest, ShmInitResponse>(INVALID_NID, BIO_OP_SDK_SHM_INIT, req, rsp);
    if (ret != BIO_OK) {
        CLIENT_LOG_ERROR("Send shm init request failed, ret:" << ret << ".");
        return ret;
    }
    if (!CheckShmInitResp(rsp)) {
        CLIENT_LOG_ERROR("Invalid responses param, alignSize:" << rsp.alignSize << ", ioTimeOut:"
            << rsp.ioTimeOut << ", netTimeOut: " << rsp.ioTimeOut << ", netTimeOut:" << rsp.netTimeOut << ", logLevel:"
            << rsp.logLevel << ", scene:" << rsp.scene << ".");
        return BIO_INNER_ERR;
    }

    mNetSegmentSize = rsp.netSegmentSize;
    mEnableHtrace = rsp.enableHtrace;
    mShmFd = rsp.memFd;
    mServerPid = rsp.serverPid;
    mShmOffset = rsp.offset;
    mShmLength = rsp.length;
    mShmKey = rsp.mKey;
    mWorkScene = rsp.scene;
    mWorkIoAlignSize = rsp.alignSize;
    mWorkIoTimeOut = rsp.ioTimeOut;
    mWorkNetTimeOut = rsp.netTimeOut;
    mLogLevel = rsp.logLevel;
    mEnableCrc = rsp.enableCrc;
    mEnableCli = rsp.enableCli;
    mEnablePrometheus = rsp.enablePrometheus;
    rsp.listenAddress[MAX_LISTEN_ADDRESS_LENGTH - 1] = '\0';
    mPrometheusListenAddress = rsp.listenAddress;
    mPrometheusScrapeIntervalSec = rsp.scrapeIntervalSec;
    CLIENT_LOG_INFO("Bio client configuration, scene:" << mWorkScene << ", io alignSize:" << mWorkIoAlignSize <<
        ", io timeout:" << mWorkIoTimeOut << ", net timeout:" << mWorkNetTimeOut << ", loglevel:" << mLogLevel << ".");

    mNetEngine->UpdateTimeOut(static_cast<int16_t>(mWorkNetTimeOut)); // 更新消息请求发送超时参数.
    ret = mNetEngine->UpdateChannelTimeOut(INVALID_NID); // 更新链路超时参数.
    if (ret != BIO_OK) {
        CLIENT_LOG_ERROR("Update channel timeout failed, ret:" << ret << ".");
        return ret;
    }
    // return ok 且 mShmFd=-1, 属于分离部署server端不需要初始化shm
    if (mShmFd != -1 && (mShmOffset != 0 || mShmLength > defaultMaxShmSize)) {
        CLIENT_LOG_ERROR("Get share memory param failed, offset:" << mShmOffset << ", length:" << mShmLength << ".");
        return BIO_ERR;
    }

    if (CorrectFd() != BIO_OK) {
        return BIO_ERR;
    }

    if ((ret = ShmInitInner()) != BIO_OK) {
        return ret;
    }

    mNetEngine->SetShmInfo(mShmFd, mShmAddr, mShmOffset, mShmLength);
    CLIENT_LOG_INFO("Bio client shm init success. offset:" << mShmOffset << ", length:" << mShmLength << ".");
    return BIO_OK;
}

BResult BioClientNet::StartIpcService(const NetOptions netConf)
{
    mNetEngine = MakeRef<NetEngine>();
    if (mNetEngine == nullptr) {
        CLIENT_LOG_ERROR("Make net engine failed.");
        return BIO_ALLOC_FAIL;
    }

    // 1. Initialize net engine
    int16_t timeoutSec = NO_32;
    auto ret = mNetEngine->Initialize(timeoutSec, 0, NO_1024, Log);
    if (ret != BIO_OK) {
        CLIENT_LOG_ERROR("Net engine initialize failed, result:" << ret << ".");
        return ret;
    }

    // 2. start ipc service, 固定配置4个worker，4条EP链接.
    NetOptions netOptions;
    netOptions.netSegmentSize = NO_128 * NO_1024;
    netOptions.FillNetBaseConfigs(NO_4, NO_4, Role::NET_CLIENT, ServiceProtocol::SHM);
    netOptions.FillNetTlsConfigs(netConf.enableTls, netConf.certificationPath, netConf.caCerPath, netConf.caCrlPath,
        netConf.privateKeyPath, netConf.privateKeyPassword, netConf.decrypterLibPath);
    ret = mNetEngine->Start(netOptions);
    if (ret != BIO_OK) {
        CLIENT_LOG_ERROR("Start ipc service failed, result:" << ret << ".");
        return ret;
    }

    // 3. set channel broken handler
    ret = SetChannelBrokenHandler();
    if (ret != BIO_OK) {
        CLIENT_LOG_ERROR("Set net channel broken handler failed, result:" << ret << ".");
        return ret;
    }

    // 4. recover ipc service
    ret = RecoverIpcService();
    if (ret != BIO_OK) {
        CLIENT_LOG_ERROR("Recover ipc failed, result:" << ret << ".");
    }
    return ret;
}

BResult BioClientNet::StartRpcService(std::string ipMask, uint16_t port, ServiceProtocol protocol, uint16_t workerNum,
    const NetOptions netConf)
{
    const uint64_t defaultMemorySize = (128UL * 1024UL * 1024UL); // 128M
    NetOptions netOptions;
    netOptions.ipMask = std::move(ipMask);
    netOptions.port = port;
    netOptions.role = Role::NET_CLIENT;
    netOptions.isBusyLoop = false;
    netOptions.isCreateMemPool = false;
    netOptions.memorySize = defaultMemorySize;
    netOptions.protocol = protocol;
    netOptions.connCount = workerNum;
    netOptions.handlerCount = workerNum;
    netOptions.netSegmentSize = netConf.netSegmentSize;
    netOptions.enableTls = netConf.enableTls;
    netOptions.certificationPath = netConf.certificationPath;    /* certification path */
    netOptions.caCerPath = netConf.caCerPath;                    /* caCer path */
    netOptions.caCrlPath = netConf.caCrlPath;                    /* caCrl path */
    netOptions.privateKeyPath = netConf.privateKeyPath;          /* private key path */
    netOptions.privateKeyPassword = netConf.privateKeyPassword;  /* private key password */
    netOptions.decrypterLibPath = netConf.decrypterLibPath;      /* decrypter lib path */
    return mNetEngine->Start(netOptions);
}

BResult BioClientNet::SetChannelBrokenHandler()
{
    auto channelBroken = [this](uint32_t nodeId, uint32_t pid) -> void {
        std::thread t([this, nodeId]() {
            if (nodeId == INVALID_NID) { // 本地server进程退出, 则一直尝试重连IPC.
                RecoverIpc();
            } else { // 远端server进程退出, 则一直尝试重连RPC.
                RecoverRpc(nodeId);
            }
        });
        t.detach();
    };
    BResult ret = mNetEngine->RegisterChannelBrokenHandler(channelBroken);
    if (ret != BIO_OK) {
        LOG_ERROR("Client regist channel broken handler failed, ret " << ret);
    }
    return ret;
}

void BioClientNet::RecoverIpc()
{
    constexpr uint16_t recoverInterval = 2;
    uint32_t retryCnt = 0;
    BResult ret;
    do {
        ret = RecoverIpcService();
        if (ret != BIO_OK) {
            CLIENT_LOG_WARN("Delay retry connect, retry cnt:" << retryCnt++);
            sleep(recoverInterval);
        }
    } while (ret != BIO_OK);
}

BResult BioClientNet::RecoverIpcService()
{
    // 1. sync connection to local bio server.
    ConnectInfo info(INVALID_NID, static_cast<uint32_t>(getpid()), INVALID_NID);
    auto ret = mNetEngine->SyncConnect(info);
    if (ret != BIO_OK) {
        CLIENT_LOG_ERROR("Connect to local bio server failed, result:" << ret << ".");
        return ret;
    }

    // 2. init shm pool.
    ret = ShmInit();
    if (ret != BIO_OK) {
        CLIENT_LOG_ERROR("Get shm info fail, result:" << ret << ".");
        return ret;
    }
    if (mIpcRecoveredHandler != nullptr) {
        ret = mIpcRecoveredHandler();
        if (ret != BIO_OK) {
            CLIENT_LOG_ERROR("Recover data message memory pool failed, result:" << ret << ".");
        }
    }
    return ret;
}

void BioClientNet::StopInner()
{
    mNetEngine->Stop();
}

BResult BioClientNet::Rebuild(uint16_t localNid, std::map<CmNodeId, CmNodeInfo, CmNodeIdCmp> nodeView)
{
    if (mMode == CONVERGENCE || mMode == STANDALONE) {
        return BIO_OK;
    }
    mLocalNid = localNid;
    for (auto &node : nodeView) {
        if (node.second.id.VNodeId() == localNid) {
            continue;
        }
        if (node.second.status != CM_NODE_NORMAL) {
            continue;
        }
        ConnectInfo info(localNid, static_cast<uint32_t>(getpid()), node.second.id.VNodeId(), node.second.ip,
            node.second.port, NO_1);
        CLIENT_LOG_INFO("Connect to remote node:" << info.peerId.nid << ", ip:" << info.ip << ", port:" << info.port <<
            ".");
        auto handler = [this](uintptr_t userCtx, int32_t ret, ConnectInfo &info) -> void {
            if (ret != BIO_OK) {
                RecoverRpc(info.peerId.nid);
            }
        };
        auto ret = mNetEngine->AsyncConnect(info, handler, 0);
        if (ret != BIO_OK) {
            CLIENT_LOG_ERROR("Connect to local bio server failed, result:" << ret << ".");
            return ret;
        }
    }
    return BIO_OK;
}

void BioClientNet::RecoverRpc(uint32_t peerId)
{
    if (mCheckOnLine == nullptr) {
        CLIENT_LOG_WARN("Unable check node isOnline, peer id:" << peerId << ".");
        return;
    }
    std::string ip;
    uint16_t port;
    if (!mCheckOnLine(peerId, ip, port)) {
        CLIENT_LOG_WARN("Target peer id:" << peerId << " is offline.");
        return;
    }
    CLIENT_LOG_INFO("Reconnect to remote node:" << peerId << ", ip:" << ip << ", port:" << port << ".");
    sleep(NO_2);

    ConnectInfo info(mLocalNid, static_cast<uint32_t>(getpid()), peerId, ip, port, NO_1);
    auto handler = [this](uintptr_t userCtx, int32_t ret, ConnectInfo &info) -> void {
        if (ret != BIO_OK) {
            RecoverRpc(info.peerId.nid);
        }
    };
    BResult result = mNetEngine->AsyncConnect(info, handler, 0);
    if (result != BIO_OK) {
        CLIENT_LOG_ERROR("Connect to " << info.peerId.nid << " failed, ret: " << result << ".");
    }
    return;
}


bool BioClientNet::CheckGetUnderFsConfigResp(GetUnderFsConfigResponse &rsp)
{
    size_t underFsTypeLen = strnlen(rsp.underFsType, KEY_MAX_SIZE);
    size_t nameNodeLen = strnlen(rsp.hdfsConfig.nameNode, KEY_MAX_SIZE);
    size_t workingPathLen = strnlen(rsp.hdfsConfig.workingPath, KEY_MAX_SIZE);
    size_t userLen = strnlen(rsp.cephConfig.user, KEY_MAX_SIZE);
    size_t clusterLen = strnlen(rsp.cephConfig.cluster, KEY_MAX_SIZE);
    size_t cfgPathLen = strnlen(rsp.cephConfig.cfgPath, KEY_MAX_SIZE);
    size_t poolLen = strnlen(rsp.cephConfig.pool, KEY_MAX_SIZE);

    return ((underFsTypeLen != 0 && underFsTypeLen < KEY_MAX_SIZE) && (nameNodeLen != 0 && nameNodeLen < KEY_MAX_SIZE)
            && (workingPathLen != 0 && workingPathLen < KEY_MAX_SIZE) && (userLen != 0 && userLen < KEY_MAX_SIZE) &&
            (clusterLen != 0 && clusterLen < KEY_MAX_SIZE) && (cfgPathLen != 0 && cfgPathLen < KEY_MAX_SIZE) &&
            (poolLen != 0 && poolLen < KEY_MAX_SIZE) &&
            ((strcmp(rsp.underFsType, "hdfs") == 0) || (strcmp(rsp.underFsType, "ceph") == 0)));
}

BResult BioClientNet::GetUnderFsConfig(BioConfig::UnderFsConfig &config)
{
    GetUnderFsConfigRequest req = { { MESSAGE_MAGIC, 0, 0, 0, getpid() } };
    GetUnderFsConfigResponse rsp;

    BIO_TP_START(SDK_CLIENT_GET_UNDERFS_CONFIG_PASS_SYNC_CALL, 0);
    BResult ret = mNetEngine->SyncCall<GetUnderFsConfigRequest, GetUnderFsConfigResponse>(INVALID_NID,
        BIO_OP_SDK_GET_UFS_CONFIG, req, rsp);
    if (ret != BIO_OK) {
        CLIENT_LOG_ERROR("Send get underfs configs request failed, ret:" << ret << ".");
        return ret;
    }
    BIO_TP_END;

    if (!CheckGetUnderFsConfigResp(rsp)) {
        CLIENT_LOG_ERROR("Check underfs configs failed.");
        return BIO_INNER_ERR;
    }

    config.underFsType = rsp.underFsType;
    config.hdfsConfig.nameNode = rsp.hdfsConfig.nameNode;
    config.hdfsConfig.workingPath = rsp.hdfsConfig.workingPath;
    config.cephConfig.user = rsp.cephConfig.user;
    config.cephConfig.cluster = rsp.cephConfig.cluster;
    config.cephConfig.cfgPath = rsp.cephConfig.cfgPath;
    config.cephConfig.pools.insert({ 0, rsp.cephConfig.pool });
    return BIO_OK;
}

bool BioClientNet::CheckShmInitResp(ShmInitResponse rsp)
{
    bool validAlignSize = rsp.alignSize <= NO_4194304 && rsp.alignSize >= NO_1;
    bool validIoTimeOut = rsp.ioTimeOut >= NO_60 && rsp.ioTimeOut <= NO_300;
    bool validNetTimeOut = rsp.netTimeOut >= NO_16 && rsp.netTimeOut <= NO_300;
    bool validLogLevel = rsp.logLevel <= NO_4;
    bool validScene = rsp.scene <= NO_1;
    return validAlignSize && validIoTimeOut && validNetTimeOut && validLogLevel && validScene;
}

bool BioClientNet::CheckRuntimeConfigResp(StandaloneRuntimeConfigResponse rsp)
{
    bool validAlignSize = rsp.alignSize <= NO_4194304 && rsp.alignSize >= NO_1;
    bool validIoTimeOut = rsp.ioTimeOut >= NO_60 && rsp.ioTimeOut <= NO_300;
    bool validNetTimeOut = rsp.netTimeOut >= NO_16 && rsp.netTimeOut <= NO_300;
    bool validLogLevel = rsp.logLevel <= NO_4;
    bool validScene = rsp.scene <= NO_1;
    return validAlignSize && validIoTimeOut && validNetTimeOut && validLogLevel && validScene;
}
