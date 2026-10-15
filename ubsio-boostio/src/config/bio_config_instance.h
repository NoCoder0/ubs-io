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

#ifndef BOOSTIO_BIO_CONFIG_INSTANCE_H
#define BOOSTIO_BIO_CONFIG_INSTANCE_H

#include "bio_config.h"
#include "bio_err.h"
#include "bio_kvc_config_values.h"

namespace ock {
namespace bio {
const auto LOG_LEVEL = std::make_pair("ubsio.log.level", "info");

const auto NET_DATA_PROTOCOL = std::make_pair("ubsio.net.data.protocol", "tcp");
const auto NET_RPC_DATA_BUSY_POLL_MODE = std::make_pair("ubsio.net.rpc.data.busy_polling_mode", "false");
const auto NET_RPC_DATA_WORKERS_COUNT = std::make_pair("ubsio.net.rpc.data.workers_count", 4);
const auto NET_IPC_DATA_BUSY_POLL_MODE = std::make_pair("ubsio.net.ipc.data.busy_polling_mode", "false");
const auto NET_IPC_DATA_WORKERS_COUNT = std::make_pair("ubsio.net.ipc.data.workers_count", 4);
const auto NET_DATA_IP_MASK = std::make_pair("ubsio.net.data.ip_mask", "127.0.0.1/24");
const auto NET_DATA_PORT = std::make_pair("ubsio.net.data.listen_port", 7201);
const auto NET_SEGMENT_SIZE = std::make_pair("ubsio.net.data.segment_size", 256);
const auto NET_RECV_REQUEST_HANDLE_THREAD_NUM = std::make_pair("ubsio.net.request.executor.thread.num", 8);
const auto NET_RECV_REQUEST_HANDLE_QUEUE_SIZE = std::make_pair("ubsio.net.request.executor.queue.size", 1024);
const auto NET_TLS_ENABLE_SWITCH = std::make_pair("ubsio.net.tls.enable.switch", "false");
const auto NET_TLS_CA_CERT_PATH = std::make_pair("ubsio.net.tls.ca.cert.path", "/path/CA/cacert.pem");
const auto NET_TLS_CA_CRL_PATH = std::make_pair("ubsio.net.tls.ca.crl.path", "");
const auto NET_TLS_SERVER_CERT_PATH = std::make_pair("ubsio.net.tls.server.cert.path", "/path/server/servercert.pem");
const auto NET_TLS_SERVER_KEY_PATH = std::make_pair("ubsio.net.tls.server.key.path", "/path/server/serverkey.pem");
const auto NET_TLS_SERVER_KEY_PASS_PATH = std::make_pair("ubsio.net.tls.server.key.pass.path", "");
const auto NET_TLS_SERVER_DECRYPTER_PATH = std::make_pair("ubsio.net.tls.server.decrypter.lib.path", "");
const auto NET_TLS_SERVER_SSL_LIB_DIR = std::make_pair("ubsio.net.tls.server.ssl.lib.dir", "");

const auto CM_INITIAL_NODE_NUM = std::make_pair("ubsio.cm.initial.nodes_count", 2);
const auto CM_COPY_NUM = std::make_pair("ubsio.cm.copy_num", 2);
const auto CM_PT_NUM = std::make_pair("ubsio.cm.pts_count", 16);
const auto CM_ZK_HOST = std::make_pair("ubsio.cm.zk_host", "127.0.0.1:2181");
const auto CM_NODE_REGISTER_TIMEOUT = std::make_pair("ubsio.cm.register_timeout_sec", 30);
const auto CM_NODE_REGISTER_PERM_TIMEOUT = std::make_pair("ubsio.cm.register_perm_timeout_sec", 60);

const auto BIO_TRACE_ENABLE = std::make_pair("ubsio.trace.enable", "true");

const auto DATA_CRC_ENABLE = std::make_pair("ubsio.data.crc.enable", "false");

const auto BIO_CACHE_QOS_ENABLE = std::make_pair("ubsio.cache.qos.enable", "false");

const auto BIO_WCACHE_NEGOTIATE_DELAY = std::make_pair("ubsio.wcache.negotiate.delay", 100);

const auto SEGMENT_SIZE_MB = std::make_pair("ubsio.segment.size_in_mb", 4);

const auto MEM_CAPACITY_SIZE_GB = std::make_pair("ubsio.mem.size_in_gb", 50);

const auto DISK_CONF_PATH = std::make_pair("ubsio.disk.path", "xxx:xxx:xxx");
const auto BDM_IO_ENGINE = std::make_pair("ubsio.bdm.io_engine", "sync");
const auto STANDALONE_DEVICE_COUNT = std::make_pair("ubsio.standalone.device_count", 1);
const auto STANDALONE_FORCE_NEW_DISK = std::make_pair("ubsio.standalone.force_new_disk", "false");
const auto SDK_MEM_CAPACITY_SIZE_MB = std::make_pair("ubsio.sdkmem.size_in_mb", 0);

const auto WCACHE_EVICT_WATER_LEVEL = std::make_pair("ubsio.wcache.evict_water_level", 0);

const auto WCACHE_DISK_EVICT_WATER_LEVEL = std::make_pair("ubsio.wcache.disk_evict_water_level", 90);

const auto RCACHE_EVICT_WATER_LEVEL = std::make_pair("ubsio.rcache.evict_water_level", 90);

const auto MEM_READ_WRITE_RATIO = std::make_pair("ubsio.cache.mem_read_write_ratio", "0:10");

const auto DISK_READ_WRITE_RATIO = std::make_pair("ubsio.cache.disk_read_write_ratio", "0:10");

const auto BIO_CLI_TOOLS_ENABLE = std::make_pair("ubsio.cli_tools.enable", "false");

const auto WORK_SCENE = std::make_pair("ubsio.work.scene", "none");
const auto WORK_IO_ALIGNSIZE = std::make_pair("ubsio.work.io.alignsize", 1);
const auto WORK_IO_TIMEOUT = std::make_pair("ubsio.work.io.timeout", 60);
const auto WORK_NET_TIMEOUT = std::make_pair("ubsio.work.net.timeout", 20);
const auto BATCH_GET_THREAD_NUM = std::make_pair("ubsio.batchget.thread.num", 32);
const auto BDM_BATCH_READ_WINDOW_KEYS = std::make_pair("ubsio.bdm.batch_read.window_keys", 128);
const auto BDM_BATCH_READ_WINDOW_BYTES_MB = std::make_pair("ubsio.bdm.batch_read.window_bytes_mb", 64);
const auto BDM_BATCH_READ_PIPELINE_DEPTH = std::make_pair("ubsio.bdm.batch_read.pipeline_depth", 4);
const auto BDM_BATCH_READ_TEMP_POOL_MB = std::make_pair("ubsio.bdm.batch_read.temp_pool_mb", 0);
const auto BDM_BATCH_READ_STANDALONE_USE_SCRATCH_POOL =
    std::make_pair("ubsio.bdm.batch_read.standalone.use_scratch_pool", "true");

const auto WCACHE_PARTITION_COUNT = std::make_pair("ubsio.wcache.partition_count", 1);
const auto WCACHE_COMPACTION_THRESHOLD = std::make_pair("ubsio.wcache.compaction_threshold", 30);

const auto UNDERFS_FILE_SYSTEM_TYPE = std::make_pair("ubsio.underfs.file_system_type", "none");
const auto UNDERFS_CEPH_CFG_PATH = std::make_pair("ubsio.underfs.ceph.cfg.path", "/etc/ceph/ceph.conf");
const auto UNDERFS_CEPH_CLUSTER = std::make_pair("ubsio.underfs.ceph.cluster", "ceph");
const auto UNDERFS_CEPH_USER = std::make_pair("ubsio.underfs.ceph.user", "client.admin");
const auto UNDERFS_CEPH_POOL = std::make_pair("ubsio.underfs.ceph.pool", "0:jfspool1,1:jfspool2");

const auto UNDERFS_HDFS_NAMENODE = std::make_pair("ubsio.underfs.hdfs.name_node", "default:0");
const auto UNDERFS_HDFS_WORKING_PATH = std::make_pair("ubsio.underfs.hdfs.working_path", "/hdfs");

const auto PROMETHEUS_ENABLE = std::make_pair("ubsio.prometheus.enable", "false");
const auto PROMETHEUS_LISTEN_ADDRESS = std::make_pair("ubsio.prometheus.exposer", "127.0.0.1:7204");
const auto PROMETHEUS_SCRAPE_INTERVAL_SEC = std::make_pair("ubsio.prometheus.scrape_interval_sec", 15);

const std::string CONF_INIT_BAK_SUFFIX = "ubsio.conf.bak.init";
const std::string CONF_BAK_SUFFIX = "ubsio.conf.bak";
const std::string CONF_SUFFIX = "ubsio.conf";

class BioConfig;
using BioConfigPtr = Ref<BioConfig>;

class BioConfig : public Configuration {
public:
    struct NetConfig {
        std::string dataIpMask = "127.0.0.1/24";
        std::string dataIp = "127.0.0.1";
        uint32_t netSegmentSize = 64;
        uint16_t dataPort = 7300;
        uint16_t protocol = 1;
        bool isRpcBusyLoop = false;
        uint16_t rpcDataWorkersCnt = 4;
        bool isIpcBusyLoop = false;
        uint16_t ipcDataWorkersCnt = 4;
        uint16_t handleRequestThreadNum = 8;
        uint16_t handleRequestQueueSize = 1024;
        bool enableTls = true;
        std::string tlsCaCertPath = "/path/CA/cacert.pem";                  /* CA根证书 */
        std::string tlsCaCrlPath = "";                                      /* 吊销列表文件，可选，如果无吊销证书可以不设置 */
        std::string tlsServerCertPath = "/path/server/servercert.pem";      /* server工作证书 */
        std::string tlsServerKeyPath = "/path/server/serverkey.pem";        /* server公钥 */
        std::string tlsServerKeyPassPath = "";                              /* server端私钥密文文件 */
        std::string decrypterLibPath = "";                                  /* server端解密函数文件 */
        std::string opensslLibDir = "";                                     /* openssl lib 目录 */
    };

    struct CmConfig {
        int32_t initialNodeNum = 2;
        int32_t copyNum = NO_2;
        int32_t nodeNum = 2;
        int32_t ptNum = 2;
        int32_t registeredTimeoutSec = 30;
        int32_t registeredPermTimeoutSec = 60;
        int32_t groupId = 0;
        std::string zkHost = "127.0.0.1:2181";
    };

    struct DaemonConfig {
        int32_t logLevel = 0;
        uint32_t negotiateDelay = 100;
        uint32_t segment = 4194304;    // 4MB
        uint64_t memCap = 53687091200; // 50GB
        uint64_t sdkPoolSize = 1024 * 1024 * 1;
        uint64_t wcacheMemEvictLevel = 0;
        uint64_t wcacheDiskEvictLevel = 100;
        uint64_t rcacheMemEvictLevel = 90;
        uint64_t rcacheDiskEvictLevel = 90;
        std::string memReadWriteRatio = "5:5";
        long memReadRatio = 5;
        long memWriteRatio = 5;
        std::string diskReadWriteRatio = "5:5";
        long diskReadRatio = 5;
        long diskWriteRatio = 5;
        std::vector<std::string> diskList;
        std::vector<int64_t> diskCaps;
        std::string bdmIoEngine = "sync";
        uint32_t standaloneDeviceCount = 1;
        bool standaloneForceNewDisk = false;
        uint32_t workScene = 0;
        uint32_t workIoAlignSize = 1;
        uint32_t workIoTimeOut = 60;
        uint32_t workNetTimeOut = 20;
        uint32_t batchGetThreadNum = 32;
        uint32_t bdmBatchReadWindowKeys = 128;
        uint32_t bdmBatchReadWindowBytesMb = 64;
        uint32_t bdmBatchReadPipelineDepth = 4;
        uint32_t bdmBatchReadTempPoolMb = 0;
        bool bdmBatchReadStandaloneUseScratchPool = true;
        bool enableCrc = false;
        bool enableTrace = true;
        bool enableQos = true;
        bool hasDiskCache = true;
        bool enableRCache = true;
        bool enablePrometheus = false;
        bool enableCli = false;
        std::string listenAddress = "127.0.0.1:7204";
        uint32_t scrapeIntervalSec = 15;
        uint32_t wcachePartitionCount = 1;
        uint32_t wcacheCompactionThreshold = 30;
    };

    struct ClientConfig {
        uint64_t localMrSize = 0;
    };

    struct CephConfig {
        std::string cfgPath;
        std::string cluster;
        std::string user;
        std::unordered_map<uint64_t, std::string> pools;
    };

    struct HdfsConfig {
        std::string nameNode;
        std::string workingPath;
    };

    struct UnderFsConfig {
        std::string underFsType;
        CephConfig cephConfig;
        HdfsConfig hdfsConfig;
    };

public:
    static const BioConfigPtr &Instance()
    {
        static auto instance = MakeRef<BioConfig>();
        return instance;
    }

    void BakFileProcess(const std::string &configPath);

    BResult Initialize(const std::string &configPath);

    void SetStandaloneMode(bool standaloneMode) noexcept
    {
        mStandaloneMode = standaloneMode;
    }

    void LoadDefaultConf() override;

    const NetConfig &GetNetConfig() const noexcept
    {
        return mNetConfig;
    }

    const CmConfig &GetCmConfig() const noexcept
    {
        return mCmConfig;
    }

    const DaemonConfig &GetDaemonConfig() const noexcept
    {
        return mDaemonConfig;
    }

    const ClientConfig &GetClientConfig() const noexcept
    {
        return mClientConfig;
    }

    const UnderFsConfig &GetUnderFsConfig() const noexcept
    {
        return mUnderFsConfig;
    }

    // Internal virtual device index assigned by the standalone slot lease.
    void SetStandaloneDeviceInfo(uint32_t deviceId);
    BResult SelectStandaloneDiskByDeviceInfo();
    uint32_t GetStandaloneDeviceId() const noexcept
    {
        return mStandaloneDeviceInfo.deviceId;
    }

    BResult UpdateStandaloneDiskCapacity(uint32_t diskId, int64_t capacity);

    uint64_t ModifyConfigEvictWaterLevel(uint8_t tier, uint64_t level);

    std::string ModifyConfigMemReadWriteRatio(const std::string &ratios);

    std::string ModifyConfigDiskReadWriteRatio(const std::string &ratios);

    BResult CreateDiskConfBak(const std::string &diskPath);

    BResult AddDiskPath(const std::string &diskPath, const std::string &configPath);

    BResult ReplaceFile(const std::string &oldFile, const std::string &newFile);

    bool CheckDiskIsExist(std::string &newDiskPath, uint32_t &diskId);

    void ResizeDaemonConfigDisks(std::string &newDiskPath);

private:
    void DumpToLog();

    BResult AutoConfAfterLoadFromFile(const ConfigurationPtr &conf);

    BResult AutoConfigNet(const ConfigurationPtr &conf);

    BResult AutoConfigCm(const ConfigurationPtr &conf);

    BResult AutoConfigDaemon(const ConfigurationPtr &conf);

    BResult AutoConfigDaemonLogAndOther(const ConfigurationPtr &conf);

    BResult AutoConfigDaemonCache(const ConfigurationPtr &conf);

    BResult AutoConfigDaemonDisk(const ConfigurationPtr &conf);

    BResult AutoConfigClient(const ConfigurationPtr &conf);

    BResult AutoConfigUnderFs(const ConfigurationPtr &conf);

    BResult SelectStandaloneVirtualDisks(uint16_t diskNum);
private:
    struct StandaloneDeviceInfo {
        bool configured{ false };
        uint32_t deviceId{ 0 };
    };

    NetConfig mNetConfig;
    CmConfig mCmConfig;
    DaemonConfig mDaemonConfig;
    ClientConfig mClientConfig;
    UnderFsConfig mUnderFsConfig;
    bool mStandaloneMode{ false };
    bool mInited{ false };
    StandaloneDeviceInfo mStandaloneDeviceInfo;
};
}
}
#endif // BOOSTIO_BIO_CONFIG_INSTANCE_H
