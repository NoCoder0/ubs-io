/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

 * ubs-io is licensed under the Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *      http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, EITHER EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */
#include <cstddef>
#include <cstring>
#include <cstdlib>
#include <fstream>
#include <functional>
#include <sys/wait.h>
#include <unistd.h>
#include <gtest/gtest.h>
#include "bio_c.h"

static_assert(sizeof(BioKvcConfigV1) == 24, "KVC V1 ABI size");
static_assert(offsetof(BioKvcConfigV1, tenantId) == 8, "KVC V1 tenant offset");
static_assert(offsetof(BioKvcConfigV1, manifestOneGet) == 20, "KVC V1 manifest offset");
static_assert(BIO_KVC_MODE_STANDALONE == 0 && BIO_KVC_MODE_REMOTE_DRAM_TCP == 1, "KVC V1 mode ABI");

namespace {
void InFreshProcess(const std::function<void()> &body)
{
    const pid_t pid = fork();
    ASSERT_GE(pid, 0);
    if (pid == 0) {
        body();
        _exit(::testing::Test::HasFailure() ? 1 : 0);
    }
    int status = 0;
    ASSERT_EQ(waitpid(pid, &status, 0), pid);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);
}

class ConfigFile {
public:
    explicit ConfigFile(const std::string &contents)
    {
        char name[] = "/tmp/ubsio-kvc-config-XXXXXX";
        const int fd = mkstemp(name);
        path = name;
        if (fd >= 0) close(fd);
        std::ofstream stream(path);
        stream << contents;
    }
    ~ConfigFile() { unlink(path.c_str()); }
    std::string path;
};

const std::string REMOTE = "ubsio.kvc.mode=remote_dram_tcp\nubsio.kvc.tenant_id=71\n"
    "ubsio.kvc.remote_ips=10.1.1.1\nubsio.net.tls.enable.switch=false\n";

CResult Load(const std::string &path, BioKvcConfigV1 &out, ClientOptionsConfig &options)
{
    out.version = BIO_KVC_CONFIG_VERSION_1;
    return BioLoadKvcConfig(path.c_str(), sizeof(out), &out, &options);
}
}

TEST(BioKvcConfig, DefaultsAndImmutableSnapshot)
{
    InFreshProcess([] {
        uint32_t readBlockLimit = 0;
        EXPECT_EQ(BioGetKvcReadBlockLimit(nullptr), RET_CACHE_EPERM);
        EXPECT_EQ(BioGetKvcReadBlockLimit(&readBlockLimit), RET_CACHE_NOT_READY);
        setenv("UBSIO_KVC_MODE", "invalid-legacy-mode", 1);
        setenv("UBSIO_KVC_TENANT_ID", "invalid-legacy-tenant", 1);
        setenv("UBSIO_KVC_REMOTE_IPS", "invalid-legacy-ip", 1);
        setenv("UBSIO_KVC_TLS_KEY", "/missing/legacy-key", 1);
        setenv("UBSIO_KVC_TUNING_FILE", "/missing/legacy-tuning", 1);
        ConfigFile file("");
        BioKvcConfigV1 out{};
        ClientOptionsConfig options{};
        options.logType = STDERR_TYPE;
        std::strcpy(options.logFilePath, "/tmp/caller-log");
        ASSERT_EQ(Load(file.path, out, options), RET_CACHE_OK);
        EXPECT_EQ(out.mode, BIO_KVC_MODE_STANDALONE);
        EXPECT_EQ(out.tenantId, 1U);
        EXPECT_EQ(out.readWorkers, 2U);
        ASSERT_EQ(BioGetKvcReadBlockLimit(&readBlockLimit), RET_CACHE_OK);
        EXPECT_EQ(readBlockLimit, 64U);
        EXPECT_EQ(out.manifestOneGet, 0U);
        EXPECT_EQ(options.logType, STDERR_TYPE);
        EXPECT_STREQ(options.logFilePath, "/tmp/caller-log");
        { std::ofstream stream(file.path); stream << "ubsio.kvc.mode=invalid\nubsio.kvc.read_block_limit=32\n"; }
        ASSERT_EQ(Load(file.path, out, options), RET_CACHE_OK);
        unlink(file.path.c_str());
        EXPECT_EQ(Load(file.path, out, options), RET_CACHE_OK);
        ASSERT_EQ(BioGetKvcReadBlockLimit(&readBlockLimit), RET_CACHE_OK);
        EXPECT_EQ(readBlockLimit, 64U);
        ConfigFile other("");
        EXPECT_EQ(Load(other.path, out, options), RET_CACHE_CONFLICT);
    });
}

TEST(BioKvcConfig, RemoteNodeCountsAndUint64)
{
    for (const auto &ips : {"10.1.1.1", "10.1.1.1, 10.1.1.2", "10.1.1.1,10.1.1.2,10.1.1.3,10.1.1.4"}) {
        InFreshProcess([&] {
            ConfigFile file(REMOTE + "ubsio.kvc.remote_ips=" + ips + "\nubsio.kvc.read_workers=8\n"
                "ubsio.kvc.read_block_limit=96\nubsio.kvc.manifest_one_get=false\n"
                "ubsio.kvc.tenant_id=18446744073709551615\n");
            BioKvcConfigV1 out{};
            ClientOptionsConfig options{};
            ASSERT_EQ(Load(file.path, out, options), RET_CACHE_OK);
            EXPECT_EQ(out.mode, BIO_KVC_MODE_REMOTE_DRAM_TCP);
            EXPECT_EQ(out.tenantId, UINT64_MAX);
            EXPECT_EQ(out.readWorkers, 8U);
            uint32_t readBlockLimit = 0;
            ASSERT_EQ(BioGetKvcReadBlockLimit(&readBlockLimit), RET_CACHE_OK);
            EXPECT_EQ(readBlockLimit, 96U);
        });
    }
}

TEST(BioKvcConfig, ConfigurableReadTuning)
{
    for (uint32_t workers : {1U, 3U, 4U, 16U}) {
        InFreshProcess([&] {
            ConfigFile file(REMOTE + "ubsio.kvc.read_workers=" + std::to_string(workers) +
                "\nubsio.kvc.read_block_limit=128\n");
            BioKvcConfigV1 out{};
            ClientOptionsConfig options{};
            ASSERT_EQ(Load(file.path, out, options), RET_CACHE_OK);
            EXPECT_EQ(out.readWorkers, workers);
            uint32_t readBlockLimit = 0;
            ASSERT_EQ(BioGetKvcReadBlockLimit(&readBlockLimit), RET_CACHE_OK);
            EXPECT_EQ(readBlockLimit, 128U);
        });
    }
}

TEST(BioKvcConfig, LocatorAndMissingPaths)
{
    InFreshProcess([] {
        BioKvcConfigV1 out{};
        ClientOptionsConfig options{};
        EXPECT_NE(Load("/missing/ubsio-startup-config.conf", out, options), RET_CACHE_OK);
        ConfigFile file("ubsio.kvc.manifest_one_get=true\n");
        setenv("UBSIO_CONFIG_PATH", file.path.c_str(), 1);
        out.version = BIO_KVC_CONFIG_VERSION_1;
        ASSERT_EQ(BioLoadKvcConfig(nullptr, sizeof(out), &out, &options), RET_CACHE_OK);
        EXPECT_EQ(out.manifestOneGet, 1U);
        EXPECT_NE(BioLoadKvcConfig(file.path.c_str(), sizeof(out), nullptr, &options), RET_CACHE_OK);
        EXPECT_NE(BioLoadKvcConfig(file.path.c_str(), sizeof(out), &out, nullptr), RET_CACHE_OK);
    });
}

TEST(BioKvcConfig, InvalidConfigurationDoesNotPublish)
{
    for (const auto &overrideValue : {"ubsio.kvc.mode=REMOTE_DRAM_TCP", "ubsio.kvc.tenant_id=1",
        "ubsio.kvc.tenant_id=0", "ubsio.kvc.tenant_id=18446744073709551616",
        "ubsio.kvc.tenant_id=+71", "ubsio.kvc.tenant_id=-1",
        "ubsio.kvc.remote_ips=", "ubsio.kvc.remote_ips=10.1.1.1,", "ubsio.kvc.remote_ips=bad",
        "ubsio.kvc.remote_ips=10.1.1.1,10.1.1.1", "ubsio.kvc.read_workers=0", "ubsio.kvc.read_workers=17",
        "ubsio.kvc.read_workers=8x", "ubsio.kvc.read_block_limit=15", "ubsio.kvc.read_block_limit=-1",
        "ubsio.kvc.read_block_limit=96x", "ubsio.kvc.read_block_limit=4294967296",
        "ubsio.kvc.manifest_one_get=1", "ubsio.net.tls.enable.switch=yes", "ubsio.net.tls.enable.switch=true",
        "ubsio.net.data.protocol=rdma", "ubsio.work.net.timeout=301"}) {
        InFreshProcess([&] {
            ConfigFile bad(REMOTE + overrideValue + "\n");
            BioKvcConfigV1 out{};
            ClientOptionsConfig options{};
            EXPECT_NE(Load(bad.path, out, options), RET_CACHE_OK);
            ConfigFile good(REMOTE);
            EXPECT_EQ(Load(good.path, out, options), RET_CACHE_OK);
        });
    }
}

TEST(BioKvcConfig, ExplicitTlsPathsAndAbiFailures)
{
    InFreshProcess([] {
        ConfigFile ca("fixture"), cert("fixture"), key("fixture");
        const std::string tls = REMOTE + "ubsio.kvc.read_workers=4\nubsio.work.net.timeout=300\n"
            "ubsio.kvc.remote_ips=10.1.101.177,10.1.101.178,10.1.101.27,10.1.101.32\n"
            "ubsio.net.tls.enable.switch=true\nubsio.net.tls.ca.cert.path=" + ca.path +
            "\nubsio.net.tls.server.cert.path=" + cert.path + "\nubsio.net.tls.server.key.path=" + key.path + "\n";
        BioKvcConfigV1 out{};
        ClientOptionsConfig options{};
        for (const auto &missing : {"ubsio.net.tls.ca.cert.path=", "ubsio.net.tls.server.cert.path=",
            "ubsio.net.tls.server.key.path="}) {
            ConfigFile bad(tls + missing + "\n");
            EXPECT_NE(Load(bad.path, out, options), RET_CACHE_OK);
        }
        ConfigFile file(tls);
        out.version = 0;
        EXPECT_NE(BioLoadKvcConfig(file.path.c_str(), sizeof(out), &out, &options), RET_CACHE_OK);
        out.version = BIO_KVC_CONFIG_VERSION_1;
        EXPECT_NE(BioLoadKvcConfig(file.path.c_str(), sizeof(out) - 1, &out, &options), RET_CACHE_OK);
        EXPECT_NE(BioLoadKvcConfig("relative.conf", sizeof(out), &out, &options), RET_CACHE_OK);
        EXPECT_NE(BioLoadKvcConfig("/tmp", sizeof(out), &out, &options), RET_CACHE_OK);
        EXPECT_EQ(Load(file.path, out, options), RET_CACHE_OK);
        EXPECT_EQ(options.enable, 1U);
        EXPECT_EQ(out.readWorkers, 4U);
        EXPECT_STREQ(options.privateKeyPath, key.path.c_str());
    });
}

// Deliberately independent of bio_test/main.cpp, which starts networking before tests.
int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
