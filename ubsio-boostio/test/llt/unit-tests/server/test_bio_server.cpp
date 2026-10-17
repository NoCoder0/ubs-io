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

#include <mockcpp/mockcpp.hpp>
#include <algorithm>
#include <cstring>
#include "gtest/gtest.h"
#include "message.h"
#include "bio_server.h"
#include "tracepoint.h"
#include "bio_server_c.h"
#include "bdm_obj.h"
#include "test_bio_server.h"
#include "expire_checker.h"
#include "interceptor_server.h"
#include "cache_overload_ctrl.h"

using namespace ock::bio;

namespace {
class TestableBioServer : public BioServer {
public:
    using BioServer::BuildClusterModules;
    using BioServer::BuildStandaloneModules;
};

std::vector<std::string> GetModuleNames(const std::vector<ModuleDesc> &modules)
{
    std::vector<std::string> names;
    for (const auto &module : modules) {
        names.emplace_back(module.name);
    }
    return names;
}
}

bool TestBioServer::gSetup = false;

void TestBioServer::SetUp()
{
    if (gSetup) {
        return;
    }
    gSetup = true;
    return;
}

void TestBioServer::TearDown()
{
    return;
}

TEST_F(TestBioServer, test_build_cluster_modules)
{
    TestableBioServer server;
    auto names = GetModuleNames(server.BuildClusterModules());
    std::vector<std::string> expected = {
#ifdef USE_DEBUG_TP_TOOLS
        "Tracepoint",
#endif
        "Diagnose", "Tracer", "UnderFs", "Bdm", "Net", "Flow", "Cache", "MirrorServer", "CM"
    };
    EXPECT_EQ(names, expected);
}

TEST_F(TestBioServer, test_build_standalone_modules)
{
    TestableBioServer server;
    auto names = GetModuleNames(server.BuildStandaloneModules());
    std::vector<std::string> expected = {
#ifdef USE_DEBUG_TP_TOOLS
        "Tracepoint",
#endif
        "Diagnose", "Tracer", "UnderFs", "StandaloneSlotLease", "Bdm", "StandaloneMem", "Flow",
        "StandaloneView", "Cache", "MirrorServer"
    };
    EXPECT_EQ(names, expected);
    EXPECT_EQ(std::find(names.begin(), names.end(), "Net"), names.end());
    EXPECT_EQ(std::find(names.begin(), names.end(), "CM"), names.end());
}

TEST_F(TestBioServer, test_bio_server_check_all)
{
    LOG_INFO("test_bio_server_check_all");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    RequestComm reqComm = { MESSAGE_MAGIC, 1, 1, 1, getpid() };
    auto ret = mirror->CheckAll(reqComm);
    EXPECT_EQ(ret, true);

    reqComm = { 0, 1, 1, 1, getpid() };
    ret = mirror->CheckAll(reqComm);
    EXPECT_EQ(ret, false);

    reqComm = { MESSAGE_MAGIC, 1, NO_2, 1, getpid() };
    ret = mirror->CheckAll(reqComm);
    EXPECT_EQ(ret, false);
}

TEST_F(TestBioServer, test_bio_server_qry_node_info)
{
    LOG_INFO("test_bio_server_qry_node_info");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    GetLocalNidRequest req = { { MESSAGE_MAGIC, 0, 0, 0, getpid() } };
    auto ret = mirror->MirrorServerQueryNodeInfo(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_bio_server_qry_node_info_by_pt)
{
    LOG_INFO("test_bio_server_qry_node_info_by_pt");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    FileLocationQueryReq req = { 1, 2 };
    auto ret = mirror->MirrorServerQueryNodeInfoByPt(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_bio_server_qry_node_view)
{
    LOG_INFO("test_bio_server_qry_node_view");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    QueryNodeViewRequest req = { { MESSAGE_MAGIC, 0, 0, 0, getpid() }, 0 };
    auto ret = mirror->MirrorServerQueryNodeView(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_bio_server_qry_pt_view)
{
    LOG_INFO("test_bio_server_qry_pt_view");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    QueryPtViewRequest req = { { MESSAGE_MAGIC, 0, 0, 0, getpid() }, 0 };
    auto ret = mirror->MirrorServerQueryPtView(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_bio_server_put_check_ok)
{
    LOG_INFO("test_bio_server_put_check_ok");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    PutRequest req;
    req.comm = { MESSAGE_MAGIC, 1, 1, 1, getpid() };
    req.tenantId = 1;
    req.affinity = 1;
    req.strategy = 1;
    CopyKey(req.key, "abcd", KEY_MAX_SIZE);
    req.length = NO_128;
    req.mrKey = 1;
    req.sliceLen = 0;
    req.ioStrategy = 0;
    req.memFromServer = false;
    req.mrAddress = 3UL;
    req.mrSize = 88UL;
    auto ret = mirror->MirrorServerPut(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_bio_server_put_check_affinity)
{
    LOG_INFO("test_bio_server_put_check_affinity");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    PutRequest req;
    req.comm = { MESSAGE_MAGIC, 1, 1, 1, getpid() };
    req.tenantId = 1;
    req.affinity = AFFINITY_BUTT + 1;
    req.strategy = 1;
    CopyKey(req.key, "abcd", KEY_MAX_SIZE);
    req.length = NO_128;
    req.mrKey = 1;
    req.sliceLen = 0;
    req.ioStrategy = 0;
    req.memFromServer = false;
    req.mrAddress = 3UL;
    req.mrSize = 88UL;
    auto ret = mirror->MirrorServerPut(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_bio_server_put_check_strategy)
{
    LOG_INFO("test_bio_server_put_check_strategy");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    PutRequest req;
    req.comm = { MESSAGE_MAGIC, 1, 1, 1, getpid() };
    req.tenantId = 1;
    req.affinity = 1;
    req.strategy = STRATEGY_BUTT + 1;
    CopyKey(req.key, "abcd", KEY_MAX_SIZE);
    req.length = NO_128;
    req.mrKey = 1;
    req.sliceLen = 0;
    req.ioStrategy = 0;
    req.memFromServer = false;
    req.mrAddress = 3UL;
    req.mrSize = 88UL;
    auto ret = mirror->MirrorServerPut(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_bio_server_put_check_key)
{
    LOG_INFO("test_bio_server_put_check_key");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    PutRequest req;
    req.comm = { MESSAGE_MAGIC, 1, 1, 1, getpid() };
    req.tenantId = 1;
    req.affinity = 1;
    req.strategy = 1;
    CopyKey(req.key, "..abcd", KEY_MAX_SIZE);
    req.length = NO_128;
    req.mrKey = 1;
    req.sliceLen = 0;
    req.ioStrategy = 0;
    req.memFromServer = false;
    req.mrAddress = 3UL;
    req.mrSize = 88UL;
    auto ret = mirror->MirrorServerPut(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_bio_server_put_check_ioStrategy)
{
    LOG_INFO("test_bio_server_put_check_ioStrategy");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    PutRequest req;
    req.comm = { MESSAGE_MAGIC, 1, 1, 1, getpid() };
    req.tenantId = 1;
    req.affinity = 1;
    req.strategy = 1;
    CopyKey(req.key, "abcd", KEY_MAX_SIZE);
    req.length = NO_128;
    req.mrKey = 1;
    req.sliceLen = 0;
    req.ioStrategy = WRITE_UNDERFS_BACK + 1;
    req.memFromServer = false;
    req.mrAddress = 3UL;
    req.mrSize = 88UL;
    auto ret = mirror->MirrorServerPut(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_bio_server_put_check_length)
{
    LOG_INFO("test_bio_server_put_check_length");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    PutRequest req;
    req.comm = { MESSAGE_MAGIC, 1, 1, 1, getpid() };
    req.tenantId = 1;
    req.affinity = 1;
    req.strategy = 1;
    CopyKey(req.key, "abcd", KEY_MAX_SIZE);
    req.length = 0;
    req.mrKey = 1;
    req.sliceLen = 0;
    req.ioStrategy = 0;
    req.memFromServer = false;
    req.mrAddress = 3UL;
    req.mrSize = 88UL;
    auto ret = mirror->MirrorServerPut(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_bio_server_put_check_slen)
{
    LOG_INFO("test_bio_server_put_check_slen");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    PutRequest req;
    req.comm = { MESSAGE_MAGIC, 1, 1, 1, getpid() };
    req.tenantId = 1;
    req.affinity = 1;
    req.strategy = 1;
    CopyKey(req.key, "abcd", KEY_MAX_SIZE);
    req.length = NO_128;
    req.mrKey = 1;
    req.sliceLen = 100;
    req.ioStrategy = 0;
    req.memFromServer = false;
    req.mrAddress = 3UL;
    req.mrSize = 88UL;
    auto ret = mirror->MirrorServerPut(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_bio_server_put_check_mr)
{
    LOG_INFO("test_bio_server_put_check_mr");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    PutRequest req;
    req.comm = { MESSAGE_MAGIC, 1, 1, 1, getpid() };
    req.tenantId = 1;
    req.affinity = 1;
    req.strategy = 1;
    CopyKey(req.key, "abcd", KEY_MAX_SIZE);
    req.length = NO_128;
    req.mrKey = 1;
    req.sliceLen = 0;
    req.ioStrategy = 0;
    req.memFromServer = false;
    req.mrAddress = 3UL;
    req.mrSize = BIO_IO_MAX_LEN + 1;
    auto ret = mirror->MirrorServerPut(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_bio_server_put_check_addr)
{
    LOG_INFO("test_bio_server_put_check_addr");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    PutRequest req;
    req.comm = { MESSAGE_MAGIC, 1, 1, 1, getpid() };
    req.tenantId = 1;
    req.affinity = 1;
    req.strategy = 1;
    CopyKey(req.key, "abcd", KEY_MAX_SIZE);
    req.length = NO_128;
    req.mrKey = 1;
    req.sliceLen = 0;
    req.ioStrategy = 0;
    req.memFromServer = false;
    req.mrAddress = 0UL;
    req.mrSize = 88UL;
    auto ret = mirror->MirrorServerPut(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_bio_server_put)
{
    LOG_INFO("test_bio_server_put");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    PutRequest req;
    req.comm = { MESSAGE_MAGIC, 1, 1, 1, getpid() };
    req.tenantId = 1;
    req.affinity = 1;
    req.strategy = 1;
    CopyKey(req.key, "abcd", KEY_MAX_SIZE);
    req.length = NO_128;
    req.mrKey = 1;
    req.sliceLen = NO_128;
    req.ioStrategy = 0;
    req.memFromServer = true;
    req.mrAddress = 0UL;
    req.mrSize = 0;
    auto ret = mirror->MirrorServerPut(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_bio_server_put_slice_alloc_fail_reply_ok)
{
    LOG_INFO("test_bio_server_put_slice_alloc_fail_reply_ok");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    PutRequest req;
    req.comm = { MESSAGE_MAGIC, 1, 1, 1, getpid() };
    req.tenantId = 1;
    req.affinity = 1;
    req.strategy = 1;
    CopyKey(req.key, "abcdslice", KEY_MAX_SIZE);
    req.length = NO_128;
    req.mrKey = 1;
    req.sliceLen = 0;
    req.ioStrategy = 0;
    req.memFromServer = true;
    req.mrAddress = 0UL;
    req.mrSize = 0;
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "PUT_SLICE_ZERO_ALLOC_FAIL", 0, 1, userParam);
    auto ret = mirror->MirrorServerPut(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
    BioHvsDeactiveTracePoint(0, "PUT_SLICE_ZERO_ALLOC_FAIL");
}

TEST_F(TestBioServer, test_bio_server_put_slice_normal_alloc_fail_reply_ok)
{
    LOG_INFO("test_bio_server_put_slice_normal_alloc_fail_reply_ok");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    PutRequest req;
    req.comm = { MESSAGE_MAGIC, 1, 1, 1, getpid() };
    req.tenantId = 1;
    req.affinity = 1;
    req.strategy = 1;
    CopyKey(req.key, "abcdslice", KEY_MAX_SIZE);
    req.length = NO_128;
    req.mrKey = 1;
    req.sliceLen = NO_128;
    req.ioStrategy = 0;
    req.memFromServer = true;
    req.mrAddress = 0ULL;
    req.mrSize = 0;
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "PUT_SLICE_NORMAL_ALLOC_FAIL", 0, 1, userParam);
    auto ret = mirror->MirrorServerPut(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
    BioHvsDeactiveTracePoint(0, "PUT_SLICE_NORMAL_ALLOC_FAIL");
}

TEST_F(TestBioServer, test_bio_server_get)
{
    LOG_INFO("test_bio_server_get");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    GetRequest req;
    req.comm = { MESSAGE_MAGIC, 1, 1, 1, getpid() };
    CopyKey(req.key, "abcd", KEY_MAX_SIZE);
    req.ptId = 1;
    req.offset = 0;
    req.length = NO_128;
    auto ret = mirror->MirrorServerGet(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_bio_server_get_offset_length)
{
    LOG_INFO("test_bio_server_get_offset_length");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    GetRequest req;
    req.comm = { MESSAGE_MAGIC, 1, 1, 1, getpid() };
    CopyKey(req.key, "abcd", KEY_MAX_SIZE);
    req.ptId = 1;
    req.offset = BIO_IO_MAX_LEN / NO_2;
    req.length = BIO_IO_MAX_LEN / NO_2 + 1;
    auto ret = mirror->MirrorServerGet(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_bio_server_get_check_offset)
{
    LOG_INFO("test_bio_server_get_check_offset");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    GetRequest req;
    req.comm = { MESSAGE_MAGIC, 1, 1, 1, getpid() };
    CopyKey(req.key, "abcd", KEY_MAX_SIZE);
    req.ptId = 1;
    req.offset = BIO_IO_MAX_LEN + 1;
    req.length = NO_128;
    auto ret = mirror->MirrorServerGet(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_bio_server_get_check_key)
{
    LOG_INFO("test_bio_server_get_check_key");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    GetRequest req;
    req.comm = { MESSAGE_MAGIC, 1, 1, 1, getpid() };
    CopyKey(req.key, "..abcd", KEY_MAX_SIZE);
    req.ptId = 1;
    req.offset = 0;
    req.length = NO_128;
    auto ret = mirror->MirrorServerGet(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_bio_server_get_read_local_lmrSize_err)
{
    LOG_INFO("test_bio_server_get_read_local_lmrSize_err");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    bool isAlloc = true;
    NetMrInfo mrinfo { 0, 0, 0 };
    std::vector<NetMrInfo> lMrVec { mrinfo };
    GetResponse rsp;
    GetRequest req;
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "WCACHE_READ_LOCAL_RMRSIZE_ERR", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_MEM_FREE", 0, 1, userParam);
    auto ret = mirror->WriterLocalDiffProcess(isAlloc, lMrVec, rsp, req);
    BioHvsDeactiveTracePoint(0, "WCACHE_READ_LOCAL_RMRSIZE_ERR");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_MEM_FREE");
    EXPECT_EQ(ret, BIO_INNER_ERR);
}

TEST_F(TestBioServer, test_bio_server_get_read_remote_lmrSize_err)
{
    LOG_INFO("test_bio_server_get_read_remote_lmrSize_err");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    bool isAlloc = true;
    NetMrInfo mrinfo { 0, 0, 0 };
    std::vector<NetMrInfo> lMrVec { mrinfo };
    std::vector<NetMrInfo> rMrVec { mrinfo };
    ServiceContext ctx;
    GetRequest req;
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "WCACHE_READ_REMOTE_RMRSIZE_ERR", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_MEM_FREE", 0, 1, userParam);
    auto ret = mirror->WriterRemote(isAlloc, lMrVec, rMrVec, ctx, req);
    BioHvsDeactiveTracePoint(0, "WCACHE_READ_REMOTE_RMRSIZE_ERR");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_MEM_FREE");
    EXPECT_EQ(ret, BIO_INNER_ERR);
}

TEST_F(TestBioServer, test_bio_server_get_read_err_reply_return_ok)
{
    LOG_INFO("test_bio_server_get_read_err_reply_return_ok");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    GetRequest req;
    req.comm = { MESSAGE_MAGIC, 1, 1, 0, getpid() };
    CopyKey(req.key, "abcd", KEY_MAX_SIZE);
    req.ptId = 1;
    req.offset = 0;
    req.length = NO_128;
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "WCACHE_READ_CALLBACK_FAIL", 0, 1, userParam);
    auto ret = mirror->MirrorServerGet(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
    BioHvsDeactiveTracePoint(0, "WCACHE_READ_CALLBACK_FAIL");
}

TEST_F(TestBioServer, test_list_list_err_return_fail)
{
    LOG_INFO("test_list_list_err_return_fail");
    ListRequest req;
    req.comm = { MESSAGE_MAGIC, 0, 0, 1, getpid() };
    CopyKey(req.prefix, "a", KEY_MAX_SIZE);
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "LIST_LIST_FAIL", 0, 1, userParam);
    auto ret = List(&req, nullptr);
    EXPECT_EQ(ret, BIO_ERR);
    BioHvsDeactiveTracePoint(0, "LIST_LIST_FAIL");
}

TEST_F(TestBioServer, test_list_malloc_rsp_err_return_fail)
{
    LOG_INFO("test_list_malloc_rsp_err_return_fail");
    ListRequest req;
    req.comm = { MESSAGE_MAGIC, 0, 0, 1, getpid() };
    CopyKey(req.prefix, "a", KEY_MAX_SIZE);
    ListResponse *listRsp = nullptr;
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "LIST_MALLOC_RSP_FAIL", 0, 1, userParam);
    auto ret = List(&req, &listRsp);
    EXPECT_EQ(ret, BIO_ALLOC_FAIL);
    BioHvsDeactiveTracePoint(0, "LIST_MALLOC_RSP_FAIL");
    delete[] listRsp;
}

TEST_F(TestBioServer, test_put_slice_length_eq_zero_return_fail)
{
    LOG_INFO("test_put_slice_length_eq_zero_return_fail");
    PutRequest req;
    req.comm = { MESSAGE_MAGIC, 1, 1, 1, getpid() };
    req.tenantId = 1;
    req.affinity = 1;
    req.strategy = 1;
    CopyKey(req.key, "abcd1", KEY_MAX_SIZE);
    req.length = NO_128;
    req.mrKey = 1;
    req.sliceLen = 0;
    req.ioStrategy = 0;
    req.memFromServer = true;
    req.mrAddress = 0UL;
    req.mrSize = 0;
    PutResponse response;
    auto ret = Put(&req, &response);
    EXPECT_EQ(ret, BIO_INNER_RETRY);
}

TEST_F(TestBioServer, test_put_slice_length_eq_zero_alloc_slice_err_return_fail)
{
    LOG_INFO("test_put_slice_length_eq_zero_alloc_slice_err_return_fail");
    PutRequest req;
    req.comm = { MESSAGE_MAGIC, 1, 1, 1, getpid() };
    req.tenantId = 1;
    req.affinity = 1;
    req.strategy = 1;
    CopyKey(req.key, "abcd1", KEY_MAX_SIZE);
    req.length = NO_128;
    req.mrKey = 1;
    req.sliceLen = 0;
    req.ioStrategy = 0;
    req.memFromServer = true;
    req.mrAddress = 0ULL;
    req.mrSize = 0;
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "PUT_SLICELEN_ZERO_ALLOC_SLICE_FAIL", 0, 1, userParam);
    PutResponse response;
    auto ret = Put(&req, &response);
    EXPECT_EQ(ret, BIO_ALLOC_FAIL);
    BioHvsDeactiveTracePoint(0, "PUT_SLICELEN_ZERO_ALLOC_SLICE_FAIL");
}

TEST_F(TestBioServer, test_put_slice_alloc_slice_err_return_fail)
{
    LOG_INFO("test_put_slice_alloc_slice_err_return_fail");
    PutRequest req;
    req.comm = { MESSAGE_MAGIC, 1, 1, 1, getpid() };
    req.tenantId = 1;
    req.affinity = 1;
    req.strategy = 1;
    CopyKey(req.key, "abcd1", KEY_MAX_SIZE);
    req.length = NO_128;
    req.mrKey = 1;
    req.sliceLen = NO_128;
    req.ioStrategy = 0;
    req.memFromServer = true;
    req.mrAddress = 0ULL;
    req.mrSize = 0;
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "PUT_ALLOC_SLICE_FAIL", 0, 1, userParam);
    PutResponse response;
    auto ret = Put(&req, &response);
    EXPECT_EQ(ret, BIO_ALLOC_FAIL);
    BioHvsDeactiveTracePoint(0, "PUT_ALLOC_SLICE_FAIL");
}

TEST_F(TestBioServer, test_get_slice_flowid_unexists_return_fail)
{
    LOG_INFO("test_get_slice_flowid_unexists_return_fail");
    GetSliceRequest req = { { MESSAGE_MAGIC, 1, 1, 1, getpid() }, NO_4194304, 0, 1, NO_128 };
    auto ret = GetSlice(&req, nullptr);
    EXPECT_EQ(ret, BIO_INNER_RETRY);
}

TEST_F(TestBioServer, test_bio_server_stat)
{
    LOG_INFO("test_bio_server_stat");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    StatRequest req;
    req.comm = { MESSAGE_MAGIC, 1, 1, 1, getpid() };
    CopyKey(req.key, "abcd", KEY_MAX_SIZE);
    auto ret = mirror->MirrorServerStat(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_bio_server_stat_check_key)
{
    LOG_INFO("test_bio_server_stat_check_key");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    StatRequest req;
    req.comm = { MESSAGE_MAGIC, 1, 1, 1, getpid() };
    CopyKey(req.key, "..abcd", KEY_MAX_SIZE);
    auto ret = mirror->MirrorServerStat(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_bio_server_list)
{
    LOG_INFO("test_bio_server_list");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    ListRequest req;
    req.comm = { MESSAGE_MAGIC, 0, 0, 1, getpid() };
    CopyKey(req.prefix, "a", KEY_MAX_SIZE);
    auto ret = mirror->MirrorServerList(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_bio_server_list_prefix_null_reply_ok)
{
    LOG_INFO("test_bio_server_list_prefix_null_reply_ok");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    ListRequest req;
    req.comm = { MESSAGE_MAGIC, 0, 0, 1, getpid() };
    req.isListUnderFs = false;
    CopyKey(req.prefix, "a", KEY_MAX_SIZE);
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "WCACHE_INDEX_TABLE_FAIL", 0, 1, userParam);
    auto ret = mirror->MirrorServerList(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
    BioHvsDeactiveTracePoint(0, "WCACHE_INDEX_TABLE_FAIL");
}

TEST_F(TestBioServer, test_bio_server_list_inner)
{
    LOG_INFO("test_bio_server_list_inner");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    std::unordered_map<std::string, ObjStat> objs;
    ListRequest *reqs;
    mirror->ReplyListResultLocal(ctx, objs, *reqs);

    ListRequest req;
    req.comm = { MESSAGE_MAGIC, 0, 0, 1, getpid() };
    CopyKey(req.prefix, "a", KEY_MAX_SIZE);
    mirror->ReplyListResultRemote(ctx, &req, objs);
}

TEST_F(TestBioServer, test_bio_server_list_inner_objs_notempty)
{
    LOG_INFO("test_bio_server_list_inner_objs_notempty");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    ObjStat stat;
    std::unordered_map<std::string, ObjStat> objs;
    objs.emplace("obj1", stat);
    ListRequest *reqs;
    mirror->ReplyListResultLocal(ctx, objs, *reqs);

    ListRequest req;
    req.comm = { MESSAGE_MAGIC, 0, 0, 1, getpid() };
    CopyKey(req.prefix, "a", KEY_MAX_SIZE);
    mirror->ReplyListResultRemote(ctx, &req, objs);
}

TEST_F(TestBioServer, test_mirror_master_create_flow_wcache_err)
{
    LOG_INFO("test_mirror_master_create_flow_wcache_err");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    CreateFlowRequest req = { { MESSAGE_MAGIC, 0, 1, 1, getpid() }, 0, 1 };
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "MIRROR_FLOW_CREATE_WCACHE_FAIL", 0, 1, userParam);
    mirror->MirrorServerCreateFlow(ctx, &req);
    BioHvsDeactiveTracePoint(0, "MIRROR_FLOW_CREATE_WCACHE_FAIL");
}

TEST_F(TestBioServer, test_slave_create_flow_ok)
{
    LOG_INFO("test_slave_create_flow_ok");
    CreateFlowRequest req = { { MESSAGE_MAGIC, 0, NO_128, NO_10, getpid() }, 1, 1 };
    auto ret = CreateFlowSlave(&req);
    EXPECT_EQ(ret, BIO_CHECK_PT_FAIL);
}

TEST_F(TestBioServer, destroy_flow_OK)
{
    LOG_INFO("destroy_flow_OK");
    DestroyFlowRequest req = { { MESSAGE_MAGIC, 0, NO_128, NO_10, getpid() }, 1 };
    auto ret = DestroyFlow(&req);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_mirror_master_create_flow_rcache_alloc_obj_err)
{
    LOG_INFO("test_mirror_master_create_flow_rcache_alloc_obj_err");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    CreateFlowRequest req = { { MESSAGE_MAGIC, 0, 1, 1, getpid() }, 0, 1 };
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "NO_PROCESS_RCACHE_FIND", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "RCACHE_ALLOC_OBJ_FAIL", 0, 1, userParam);
    mirror->MirrorServerCreateFlow(ctx, &req);
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_RCACHE_FIND");
    BioHvsDeactiveTracePoint(0, "RCACHE_ALLOC_OBJ_FAIL");
}

TEST_F(TestBioServer, test_mirror_server_destroy_flow_rcache_alloc_obj_err)
{
    LOG_INFO("test_mirror_server_destroy_flow_rcache_alloc_obj_err");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    DestroyFlowRequest req = { { MESSAGE_MAGIC, 0, 1, 1, getpid() }, 0 };
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "DESTROY_WCACHE_FAIL", 0, 1, userParam);
    auto ret = mirror->MirrorServerDestroyFlow(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
    BioHvsDeactiveTracePoint(0, "DESTROY_WCACHE_FAIL");
}

TEST_F(TestBioServer, test_mirror_server_get_offset_synccall_err)
{
    LOG_INFO("test_mirror_server_get_offset_synccall_err");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    uint64_t offset = 0;
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "SYNCCALL_FAIL", 0, 1, userParam);
    auto ret = mirror->GetFlowGlobEvictOffset(0, 0, offset);
    EXPECT_EQ(ret, BIO_NET_RETRY);
    BioHvsDeactiveTracePoint(0, "SYNCCALL_FAIL");
}

TEST_F(TestBioServer, test_mirror_server_get_offset_synccall_channel_err)
{
    LOG_INFO("test_mirror_server_get_offset_synccall_channel_err");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    uint64_t offset = 0;
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "SYNCCALL_CHANNEL_FAIL", 0, 1, userParam);
    auto ret = mirror->GetFlowGlobEvictOffset(0, 0, offset);
    EXPECT_EQ(ret, BIO_NET_RETRY);
    BioHvsDeactiveTracePoint(0, "SYNCCALL_CHANNEL_FAIL");
}

TEST_F(TestBioServer, test_mirror_server_get_offset_synccall_opcode_err)
{
    LOG_INFO("test_mirror_server_get_offset_synccall_opcode_err");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    uint64_t offset = 0;
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "SYNCCALL_OPCODE_FAIL", 0, 1, userParam);
    auto ret = mirror->GetFlowGlobEvictOffset(0, 0, offset);
    EXPECT_EQ(ret, BIO_INVALID_PARAM);
    BioHvsDeactiveTracePoint(0, "SYNCCALL_OPCODE_FAIL");
}

TEST_F(TestBioServer, test_mirror_other_create_flow_err)
{
    LOG_INFO("test_mirror_other_create_flow_err");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    CreateFlowRequest req = { { MESSAGE_MAGIC, 0, 0, 1, getpid() }, 3, 1 };
    mirror->MirrorServerCreateFlow(ctx, &req);
}

TEST_F(TestBioServer, test_mirror_master_create_flow_rcache_init_obj_err)
{
    LOG_INFO("test_mirror_master_create_flow_rcache_init_obj_err");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    CreateFlowRequest req = { { MESSAGE_MAGIC, 0, 0, 1, getpid() }, 0, 1 };
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "NO_PROCESS_RCACHE_FIND", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "RCACHE_INIT_OBJ_FAIL", 0, 1, userParam);
    mirror->MirrorServerCreateFlow(ctx, &req);
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_RCACHE_FIND");
    BioHvsDeactiveTracePoint(0, "RCACHE_INIT_OBJ_FAIL");
}

TEST_F(TestBioServer, test_bio_server_load_check_success)
{
    LOG_INFO("test_bio_server_load_check_success");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    LoadRequest req;
    req.offset = 0;
    req.comm = { MESSAGE_MAGIC, 1, 1, 1, getpid() };
    CopyKey(req.key, "abcd", KEY_MAX_SIZE);
    auto ret = mirror->MirrorServerLoad(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_bio_server_load_check_length)
{
    LOG_INFO("test_bio_server_load_check_length");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    LoadRequest req;
    req.offset = 0;
    req.length = BIO_IO_MAX_LEN + 1;
    req.comm = { MESSAGE_MAGIC, 1, 1, 1, getpid() };
    CopyKey(req.key, "abcd", KEY_MAX_SIZE);
    auto ret = mirror->MirrorServerLoad(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_bio_server_load)
{
    LOG_INFO("test_bio_server_load");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    LoadRequest req;
    req.comm = { MESSAGE_MAGIC, 1, 1, 1, getpid() };
    CopyKey(req.key, "abcd", KEY_MAX_SIZE);
    auto ret = mirror->MirrorServerLoad(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_bio_server_delete_disk_err)
{
    LOG_INFO("test_bio_server_delete_disk_err");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    DeleteRequest req;
    req.comm = { MESSAGE_MAGIC, 1, 1, 1, getpid() };
    CopyKey(req.key, "abcd", KEY_MAX_SIZE);
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "WCACHE_FLOW_DISK_FAIL", 0, 1, userParam);
    auto ret = mirror->Delete(req);
    EXPECT_EQ(ret, BIO_OK);
    BioHvsDeactiveTracePoint(0, "WCACHE_FLOW_DISK_FAIL");
}

TEST_F(TestBioServer, test_bio_server_delete_check)
{
    LOG_INFO("test_bio_server_delete_check");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    DeleteRequest req;
    req.comm = { MESSAGE_MAGIC, 1, 1, 1, getpid() };
    CopyKey(req.key, "..abcd", KEY_MAX_SIZE);
    auto ret = mirror->MirrorServerDelete(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_bio_server_delete)
{
    LOG_INFO("test_bio_server_delete");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    DeleteRequest req;
    req.comm = { MESSAGE_MAGIC, 1, 1, 1, getpid() };
    CopyKey(req.key, "abcd", KEY_MAX_SIZE);
    auto ret = mirror->MirrorServerDelete(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_bio_server_get_slice)
{
    LOG_INFO("test_bio_server_get_slice");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    GetSliceRequest req = { { MESSAGE_MAGIC, 1, 1, 1, getpid() }, 1, 0, 1, 128 };
    auto ret = mirror->MirrorServerGetSlice(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_bio_server_sync_data)
{
    LOG_INFO("test_bio_server_sync_data");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    SyncDataRequest req = { { MESSAGE_MAGIC, 1, 1, 1, getpid() } };
    auto ret = mirror->MirrorServerSyncData(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_bio_server_get_evict_off)
{
    LOG_INFO("test_bio_server_get_evict_off");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    GetEvictRequest req = { { MESSAGE_MAGIC, 1, 1, 1, getpid() }, 1 };
    auto ret = mirror->MirrorServerGetEvictOffset(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_bio_server_get_evict_off_check_fail)
{
    LOG_INFO("test_bio_server_get_evict_off_check_fail");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    GetEvictRequest req = { { MESSAGE_MAGIC, 1, 2, 1, getpid() }, 1 };
    auto ret = mirror->MirrorServerGetEvictOffset(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_bio_server_get_evict_off_flowid_err)
{
    LOG_INFO("test_bio_server_get_evict_off_flowid_err");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    GetEvictRequest req = { { MESSAGE_MAGIC, 1, 1, 1, getpid() }, NO_128 };
    auto ret = mirror->MirrorServerGetEvictOffset(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_bio_server_reader)
{
    LOG_INFO("test_bio_server_reader");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    std::vector<FlowAddr> addr;
    addr.emplace_back(FlowAddr(1, 0, NO_128));
    WCacheSlicePtr from = MakeRef<WCacheSlice>(1, 1, 1, NO_128, addr);
    WCacheSlicePtr to = MakeRef<WCacheSlice>(1, 1, 1, NO_128, addr);
    PutRequest req;
    req.memFromServer = false;
    ServiceContext netCtx;
    auto ret = mirror->ReaderRemote(from.Get(), to.Get(), req, netCtx);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_bio_server_reader_not_equal_server_false)
{
    LOG_INFO("test_bio_server_reader_not_equal_server_false");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    std::vector<FlowAddr> addr;
    addr.emplace_back(FlowAddr(1, 0, NO_128));
    std::vector<FlowAddr> addrTo;
    addrTo.emplace_back(FlowAddr(1, 0, NO_256));
    addrTo.emplace_back(FlowAddr(1, 0, NO_256));
    WCacheSlicePtr from = MakeRef<WCacheSlice>(1, 1, 1, NO_128, addr);
    WCacheSlicePtr to = MakeRef<WCacheSlice>(1, 1, 1, NO_256, addrTo);
    PutRequest req;
    req.memFromServer = true;
    req.comm.srcNid = NO_10;
    ServiceContext netCtx;
    auto ret = mirror->ReaderRemote(from.Get(), to.Get(), req, netCtx);
    EXPECT_EQ(ret, BIO_INNER_ERR);
}

TEST_F(TestBioServer, test_bio_server_reader_not_equal_to_err)
{
    LOG_INFO("test_bio_server_reader_not_equal_to_err");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    std::vector<FlowAddr> addr;
    addr.emplace_back(FlowAddr(1, 0, NO_128));
    addr.emplace_back(FlowAddr(1, 0, NO_128));
    std::vector<FlowAddr> addrTo;
    addrTo.emplace_back(FlowAddr(1, 0, NO_100));
    WCacheSlicePtr from = MakeRef<WCacheSlice>(1, 1, 1, NO_128, addr);
    WCacheSlicePtr to = MakeRef<WCacheSlice>(1, 1, 1, NO_100, addrTo);
    PutRequest req;
    req.memFromServer = false;
    req.comm.srcNid = NO_10;
    ServiceContext netCtx;
    auto ret = mirror->ReaderRemote(from.Get(), to.Get(), req, netCtx);
    EXPECT_EQ(ret, BIO_INNER_ERR);
}

TEST_F(TestBioServer, test_bio_server_writer)
{
    LOG_INFO("test_bio_server_writer");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    size_t size = NO_128;
    void *src = malloc(size);
    memset(src, 0, size);
    uintptr_t ptr_as_uintptr = (uintptr_t)src;
    uint64_t ptr_as_uint64 = (uint64_t)ptr_as_uintptr;
    std::vector<FlowAddr> addr;
    addr.emplace_back(FlowAddr(ptr_as_uint64, 0, NO_128));
    WCacheSlicePtr from = MakeRef<WCacheSlice>(1, 1, 1, NO_128, addr);
    WCacheSlicePtr to = MakeRef<WCacheSlice>(1, 1, 1, NO_128, addr);
    std::vector<NetMrInfo> rMrVec;
    rMrVec.emplace_back(NetMrInfo(123U, 128U, 1));
    std::vector<NetMrInfo> lMrVec;
    lMrVec.emplace_back(NetMrInfo(123U, 128U, 1));
    uint32_t rKey = 1;
    bool isAlloc = true;
    auto ret = mirror->WriterParseMrInfo(from.Get(), to.Get(), rMrVec, lMrVec, rKey, isAlloc);
    free(src);
    EXPECT_EQ(ret, BIO_OK);

    GetResponse rsp;
    GetRequest req;
    req.comm.srcNid = NO_1;
    ret = mirror->WriterLocalDiffProcess(isAlloc, lMrVec, rsp, req);

    ServiceContext netCtx;
    std::vector<NetMrInfo> rMrVec1;
    rMrVec1.emplace_back(NetMrInfo(123U, 128U, 1));
    isAlloc = false;
    ret = mirror->WriterRemote(isAlloc, lMrVec, rMrVec1, netCtx, req);
    EXPECT_EQ(ret, BIO_NET_RETRY);
}

TEST_F(TestBioServer, test_bio_server_writer_actual_length)
{
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    constexpr uint32_t chunkSize = 1024 * 1024;
    constexpr uint32_t mrCapacity = 4 * chunkSize;
    struct ReadCase {
        uint32_t actual;
        uint32_t requested;
    };
    for (const auto &test : {ReadCase{48, 48}, ReadCase{48, 49}, ReadCase{49, 49},
        ReadCase{chunkSize, chunkSize}, ReadCase{chunkSize + 37, chunkSize + 37},
        ReadCase{mrCapacity, mrCapacity}}) {
        std::vector<char> data(test.actual);
        for (size_t index = 0; index < data.size(); ++index) data[index] = static_cast<char>(index % 251);
        const auto address = reinterpret_cast<uintptr_t>(data.data());
        const uint32_t firstLength = test.actual / 2;
        std::vector<FlowAddr> fromAddrs{FlowAddr(address, 0, firstLength),
            FlowAddr(address, firstLength, test.actual - firstLength)};
        MrInfo target{1, mrCapacity};
        std::vector<FlowAddr> toAddrs{FlowAddr(target)};
        WCacheSlicePtr from = MakeRef<WCacheSlice>(1, 0, 0, test.actual, fromAddrs);
        RCacheSlicePtr to = MakeRef<RCacheSlice>(1, test.requested, toAddrs);
        std::vector<NetMrInfo> remoteMrs;
        std::vector<NetMrInfo> localMrs;
        bool isAlloc = false;
        ASSERT_EQ(mirror->WriterParseMrInfo(from.Get(), to.Get(), remoteMrs, localMrs, 1, isAlloc), BIO_OK);
        EXPECT_TRUE(isAlloc);
        EXPECT_EQ(localMrs.size(), 1U);
        if (!localMrs.empty()) {
            EXPECT_EQ(localMrs[0].size, test.actual);
            EXPECT_EQ(std::memcmp(reinterpret_cast<const void *>(localMrs[0].address),
                data.data(), data.size()), 0);
        }
        for (const auto &mr : localMrs) BioServer::Instance()->MemFree(mr.address);
    }
}

TEST_F(TestBioServer, test_bio_server_writer_rejects_oversized_source)
{
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    constexpr uint32_t actualLength = 48;
    constexpr uint32_t mrCapacity = 4 * 1024 * 1024;
    std::vector<char> data(actualLength);
    std::vector<FlowAddr> fromAddrs{FlowAddr(reinterpret_cast<uintptr_t>(data.data()), 0, actualLength)};
    WCacheSlicePtr from = MakeRef<WCacheSlice>(1, 0, 0, actualLength, fromAddrs);
    for (const auto &limits : {std::make_pair(actualLength - 1, mrCapacity),
        std::make_pair(actualLength, actualLength - 1), std::make_pair(actualLength, 0U)}) {
        MrInfo target{1, limits.second};
        std::vector<FlowAddr> toAddrs{FlowAddr(target)};
        RCacheSlicePtr to = MakeRef<RCacheSlice>(1, limits.first, toAddrs);
        std::vector<NetMrInfo> remoteMrs;
        std::vector<NetMrInfo> localMrs;
        bool isAlloc = false;
        EXPECT_EQ(mirror->WriterParseMrInfo(from.Get(), to.Get(), remoteMrs, localMrs, 1, isAlloc),
            BIO_INVALID_PARAM);
        EXPECT_FALSE(isAlloc);
        EXPECT_TRUE(localMrs.empty());
    }
}

TEST_F(TestBioServer, test_bio_server_writer_copy_slice_fail)
{
    LOG_INFO("test_bio_server_writer_copy_slice_fail");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    std::vector<FlowAddr> addr;
    addr.emplace_back(FlowAddr(1, 0, NO_128));
    WCacheSlicePtr from = MakeRef<WCacheSlice>(1, 1, 1, NO_128, addr, FLOW_DISK);
    WCacheSlicePtr to = MakeRef<WCacheSlice>(1, 1, 1, NO_128, addr);
    std::vector<NetMrInfo> rMrVec;
    rMrVec.emplace_back(NetMrInfo(123U, 128U, 1));
    std::vector<NetMrInfo> lMrVec;
    lMrVec.emplace_back(NetMrInfo(123U, 128U, 1));
    uint32_t rKey = 1;
    bool isAlloc = true;
    auto ret = mirror->WriterParseMrInfo(from.Get(), to.Get(), rMrVec, lMrVec, rKey, isAlloc);
    EXPECT_EQ(ret, BIO_DISK_IOERR);
}

TEST_F(TestBioServer, test_bio_server_handle)
{
    LOG_INFO("test_bio_server_handle");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    auto ret = mirror->HandleQueryNodeInfoByPt(ctx);
    EXPECT_EQ(ret, BIO_OK);

    ret = mirror->HandleQueryNodeInfo(ctx);
    EXPECT_EQ(ret, BIO_OK);

    ret = mirror->HandleQueryNodeView(ctx);
    EXPECT_EQ(ret, BIO_OK);

    ret = mirror->HandleQueryPtView(ctx);
    EXPECT_EQ(ret, BIO_OK);

    ret = mirror->HandleGetEvictOffset(ctx);
    EXPECT_EQ(ret, BIO_OK);

    ret = mirror->HandlePut(ctx);
    EXPECT_EQ(ret, BIO_OK);

    ret = mirror->HandleGet(ctx);
    EXPECT_EQ(ret, BIO_OK);

    ret = mirror->HandleDelete(ctx);
    EXPECT_EQ(ret, BIO_OK);

    ret = mirror->HandleStat(ctx);
    EXPECT_EQ(ret, BIO_OK);

    ret = mirror->HandleList(ctx);
    EXPECT_EQ(ret, BIO_OK);

    ret = mirror->HandleLoad(ctx);
    EXPECT_EQ(ret, BIO_OK);

    ret = mirror->HandleCreateFlow(ctx);
    EXPECT_EQ(ret, BIO_OK);

    ret = mirror->HandleDestroyFlow(ctx);
    EXPECT_EQ(ret, BIO_OK);

    ret = mirror->HandleGetSlice(ctx);
    EXPECT_EQ(ret, BIO_OK);

    ret = mirror->HandleSyncData(ctx);
    EXPECT_EQ(ret, BIO_OK);

    ret = mirror->HandleGetEvictOffset(ctx);
    EXPECT_EQ(ret, BIO_OK);

    ret = mirror->HandleFreeMem(ctx);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_bio_server_add_finish_list)
{
    LOG_INFO("test_bio_server_add_finish_list");
    MirrorServerCrbPtr crb = BioServer::Instance()->GetMirrorCrb();
    CmPtTaskPtr ptTask = MakeRef<CmPtTask>();
    CmPtInfo ptInfo;
    crb->JobAddFinishList(ptTask, ptInfo);
}

TEST_F(TestBioServer, test_bio_server_add_retry_list)
{
    LOG_INFO("test_bio_server_add_retry_list");
    MirrorServerCrbPtr crb = BioServer::Instance()->GetMirrorCrb();
    CmPtTaskPtr ptTask = MakeRef<CmPtTask>();
    CmPtInfo ptInfo;
    crb->JobAddRetryList(ptTask, ptInfo);
}

TEST_F(TestBioServer, test_bio_server_expire_clear)
{
    LOG_INFO("test_bio_server_expire_clear");
    MirrorServerCrbPtr crb = BioServer::Instance()->GetMirrorCrb();
    CmPtInfo ptInfo;
    ptInfo.ptId = 1;
    ptInfo.version = 1;
    crb->JobExpiredClear(ptInfo);
}

TEST_F(TestBioServer, test_bio_server_add_sync_data)
{
    LOG_INFO("test_bio_server_add_sync_data");
    MirrorServerCrbPtr crb = BioServer::Instance()->GetMirrorCrb();
    CmPtInfo ptInfo;
    ptInfo.ptId = 1;
    ptInfo.version = 1;
    crb->JobSyncData(ptInfo);
}

TEST_F(TestBioServer, test_start_server_log_init_err_return_fail)
{
    LOG_INFO("test_start_server_log_init_err_return_fail");
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "LOG_INIT_FAIL", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_SERVER_START", 0, 1, userParam);
    auto ret = BioServer::Instance()->Start();
    EXPECT_EQ(ret, BIO_INNER_ERR);
    BioHvsDeactiveTracePoint(0, "LOG_INIT_FAIL");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_SERVER_START");
}

TEST_F(TestBioServer, test_start_server_service_tracer_init_open_file_err_return_fail)
{
    LOG_INFO("test_start_server_service_tracer_init_open_file_err_return_fail");
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "TRACE_FILE_OPPEN_FAIL", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_SERVER_START", 0, 1, userParam);
    auto ret = BioServer::Instance()->Start();
    EXPECT_EQ(ret, BIO_ERR);
    BioHvsDeactiveTracePoint(0, "TRACE_FILE_OPPEN_FAIL");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_SERVER_START");
}

TEST_F(TestBioServer, test_start_server_service_tracer_init_create_dir_err_return_fail)
{
    LOG_INFO("test_start_server_service_tracer_init_create_dir_err_return_fail");
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "TRACE_CREATE_DIR_FAIL", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_SERVER_START", 0, 1, userParam);
    auto ret = BioServer::Instance()->Start();
    EXPECT_EQ(ret, BIO_ERR);
    BioHvsDeactiveTracePoint(0, "TRACE_CREATE_DIR_FAIL");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_SERVER_START");
}

TEST_F(TestBioServer, test_start_server_service_tracer_init_path_real_err_return_fail)
{
    LOG_INFO("test_start_server_service_tracer_init_path_real_err_return_fail");
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "TRACE_PATH_REAL_FAIL", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_SERVER_START", 0, 1, userParam);
    auto ret = BioServer::Instance()->Start();
    EXPECT_EQ(ret, BIO_ERR);
    BioHvsDeactiveTracePoint(0, "TRACE_PATH_REAL_FAIL");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_SERVER_START");
}

TEST_F(TestBioServer, test_start_server_underfs_init_dir_exist_return_ok)
{
    LOG_INFO("test_start_server_underfs_init_dir_exist_return_ok");
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "NO_PROCESS_UNDERFS_INIT", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_SERVER_START", 0, 1, userParam);
    auto ret = BioServer::Instance()->Start();
    EXPECT_EQ(ret, BIO_OK);
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_UNDERFS_INIT");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_SERVER_START");
}

TEST_F(TestBioServer, test_start_server_underfs_init_make_dir_err_return_fail)
{
    LOG_INFO("test_start_server_underfs_init_make_dir_err_return_fail");
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "NO_PROCESS_UNDERFS_INIT", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_SERVER_START", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "UNDERFS_MKDIR_FAIL", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "UNDERFS_OPEN_DIR_FAIL", 0, 1, userParam);
    auto ret = BioServer::Instance()->Start();
    EXPECT_EQ(ret, BIO_ERR);
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_UNDERFS_INIT");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_SERVER_START");
    BioHvsDeactiveTracePoint(0, "UNDERFS_MKDIR_FAIL");
    BioHvsDeactiveTracePoint(0, "UNDERFS_OPEN_DIR_FAIL");
}

TEST_F(TestBioServer, test_start_server_mirrorserver_init_executor_queue_init_err_return_fail)
{
    LOG_INFO("test_start_server_mirrorserver_init_executor_queue_init_err_return_fail");
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "NO_PROCESS_MIRROR_SERVER_CRB_INIT", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_MIRROR_SERVER_INIT", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_SERVER_START", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_EXECUTOR", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "QUEUE_INIT_FAIL", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_ROLLBACK_SERVICE_INIT", 0, 1, userParam);
    auto ret = BioServer::Instance()->Start();
    EXPECT_EQ(ret, BIO_ERR);
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_MIRROR_SERVER_CRB_INIT");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_MIRROR_SERVER_INIT");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_SERVER_START");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_EXECUTOR");
    BioHvsDeactiveTracePoint(0, "QUEUE_INIT_FAIL");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_ROLLBACK_SERVICE_INIT");
}

TEST_F(TestBioServer, test_start_server_mirrorserver_init_executor_thread_init_err_return_fail)
{
    LOG_INFO("test_start_server_mirrorserver_init_executor_thread_init_err_return_fail");
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "NO_PROCESS_MIRROR_SERVER_CRB_INIT", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_MIRROR_SERVER_INIT", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_SERVER_START", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_EXECUTOR", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "EXECUTOR_THREAD_FAIL", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_ROLLBACK_SERVICE_INIT", 0, 1, userParam);
    auto ret = BioServer::Instance()->Start();
    EXPECT_EQ(ret, BIO_ERR);
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_MIRROR_SERVER_CRB_INIT");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_MIRROR_SERVER_INIT");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_SERVER_START");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_EXECUTOR");
    BioHvsDeactiveTracePoint(0, "EXECUTOR_THREAD_FAIL");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_ROLLBACK_SERVICE_INIT");
}

TEST_F(TestBioServer, test_start_server_cache_init_rcache_evict_param_err_return_fail)
{
    LOG_INFO("test_start_server_cache_init_rcache_evict_param_err_return_fail");
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "NO_PROCESS_SERVER_START", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_ROLLBACK_SERVICE_INIT", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "RCACHE_EVICT_PARAM_FAIL", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_RCACHE_EVICT", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_CACHE_INIT", 0, 1, userParam);
    auto ret = BioServer::Instance()->Start();
    EXPECT_EQ(ret, BIO_ERR);
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_SERVER_START");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_ROLLBACK_SERVICE_INIT");
    BioHvsDeactiveTracePoint(0, "RCACHE_EVICT_PARAM_FAIL");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_RCACHE_EVICT");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_CACHE_INIT");
}

TEST_F(TestBioServer, test_start_server_cache_init_rcache_evict_thread_err_return_fail)
{
    LOG_INFO("test_start_server_cache_init_rcache_evict_thread_err_return_fail");
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "NO_PROCESS_SERVER_START", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_ROLLBACK_SERVICE_INIT", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "RCACHE_EVICT_THREAD_FAIL", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_RCACHE_EVICT", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_CACHE_INIT", 0, 1, userParam);
    auto ret = BioServer::Instance()->Start();
    EXPECT_EQ(ret, BIO_ERR);
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_SERVER_START");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_ROLLBACK_SERVICE_INIT");
    BioHvsDeactiveTracePoint(0, "RCACHE_EVICT_THREAD_FAIL");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_RCACHE_EVICT");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_CACHE_INIT");
}

TEST_F(TestBioServer, test_start_server_cache_init_rcache_gc_param_err_return_fail)
{
    LOG_INFO("test_start_server_cache_init_rcache_gc_param_err_return_fail");
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "NO_PROCESS_SERVER_START", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_ROLLBACK_SERVICE_INIT", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_RCACHE_EVICT", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_RCACHE_GC", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_CACHE_INIT", 0, 1, userParam);
    auto ret = BioServer::Instance()->Start();
    EXPECT_EQ(ret, BIO_ERR);
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_SERVER_START");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_ROLLBACK_SERVICE_INIT");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_RCACHE_EVICT");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_RCACHE_GC");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_CACHE_INIT");
}

TEST_F(TestBioServer, test_start_server_cache_init_rcache_gc_thread_err_return_fail)
{
    LOG_INFO("test_start_server_cache_init_rcache_gc_thread_err_return_fail");
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "NO_PROCESS_SERVER_START", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_ROLLBACK_SERVICE_INIT", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_RCACHE_EVICT", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_RCACHE_GC", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_CACHE_INIT", 0, 1, userParam);
    auto ret = BioServer::Instance()->Start();
    EXPECT_EQ(ret, BIO_ERR);
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_SERVER_START");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_ROLLBACK_SERVICE_INIT");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_RCACHE_EVICT");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_RCACHE_GC");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_CACHE_INIT");
}

TEST_F(TestBioServer, test_start_server_cache_init_cache_recover_err_return_fail)
{
    LOG_INFO("test_start_server_cache_init_cache_recover_err_return_fail");
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "NO_PROCESS_SERVER_START", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_ROLLBACK_SERVICE_INIT", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_CACHE_PROCESS", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_CACHE_INIT", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "CACHE_RECOVER_FM_GET_ALL_OBJECT_FAIL", 0, 1, userParam);
    auto ret = BioServer::Instance()->Start();
    EXPECT_EQ(ret, BIO_ERR);
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_SERVER_START");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_ROLLBACK_SERVICE_INIT");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_CACHE_PROCESS");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_CACHE_INIT");
    BioHvsDeactiveTracePoint(0, "CACHE_RECOVER_FM_GET_ALL_OBJECT_FAIL");
}

TEST_F(TestBioServer, test_start_server_service_start_err_return_fail)
{
    LOG_INFO("test_start_server_service_start_err_return_fail");
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "SERVICE_START_FAIL", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_SERVER_START", 0, 1, userParam);
    auto ret = BioServer::Instance()->Start();
    EXPECT_EQ(ret, BIO_ERR);
    BioHvsDeactiveTracePoint(0, "SERVICE_START_FAIL");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_SERVER_START");
}

TEST_F(TestBioServer, test_start_server_config_init_err_return_fail)
{
    LOG_INFO("test_start_server_config_init_err_return_fail");
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "CONFIG_INIT_FAIL", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_SERVER_START", 0, 1, userParam);
    BioHvsActiveTracePoint(0, "NO_PROCESS_CONFIG", 0, 1, userParam);
    auto ret = BioServer::Instance()->Start();
    EXPECT_EQ(ret, BIO_INNER_ERR);
    BioHvsDeactiveTracePoint(0, "CONFIG_INIT_FAIL");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_SERVER_START");
    BioHvsDeactiveTracePoint(0, "NO_PROCESS_CONFIG");
}

TEST_F(TestBioServer, test_start_server_quota_handle)
{
    ServiceContext ctx;
    AllocQuotaRequest allocReq = { { MESSAGE_MAGIC, NO_1, NO_1, NO_1, getpid() }, NO_1, NO_1024, NO_4194304 };
    auto ret = BioServer::Instance()->GetMirrorServer()->MirrorServerAllocQuota(ctx, &allocReq);
    EXPECT_EQ(ret, BIO_OK);

    FreeQuotaRequest freeReq = { { MESSAGE_MAGIC, NO_1, NO_1, NO_1, getpid() }, NO_1, NO_1024, NO_4194304 };
    ret = BioServer::Instance()->GetMirrorServer()->MirrorServerFreeQuota(ctx, &freeReq);
    EXPECT_EQ(ret, BIO_OK);

    QueryQuotaRequest queryReq = { { MESSAGE_MAGIC, NO_1, NO_1, NO_1, getpid() }};
    ret = BioServer::Instance()->GetMirrorServer()->MirrorServerQueryQuota(ctx, &queryReq);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_get_underfs_config_return_fial)
{
    LOG_INFO("test_get_underfs_config_return_fial");
    GetUnderFsConfigRequest req{ { MESSAGE_MAGIC, 1, 1, 1, 1 } };
    ServiceContext ctx;
    auto ret = MirrorServer::Instance()->MirrorServerGetUnderFsConfig(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_bioserver_Channel)
{
    LOG_INFO("test_bioserver_Channel");
    NetEnginePtr engine = BioServer::Instance()->GetNetEngine();
    std::string ipPort{"ipPort"};
    std::string payload{"payload"};
    auto ret = engine->NewChannel(ipPort, nullptr, payload);
    EXPECT_EQ(ret, BIO_ERR);
    engine->ChannelBroken(nullptr);
}

TEST_F(TestBioServer, test_handle_interceptor_alloc_page)
{
    LOG_INFO("test_handle_interceptor_alloc_page");
    ServiceContext ctx;
    auto ret = InterceptorServer::GetInstance().HandleInterceptorAllocPage(ctx);
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_check_interceptor_alloc_page_req)
{
    LOG_INFO("test_check_interceptor_alloc_page");
    InterceptorAllocPageReq *req = new InterceptorAllocPageReq();
    req->length =IO_SIZE_4M;
    auto ret = InterceptorServer::GetInstance().CheckInterceptorAllocPageReq(req);
    EXPECT_EQ(ret, true);
    free(req);
}

TEST_F(TestBioServer, test_check_interceptor_large_write_req)
{
    LOG_INFO("test_check_interceptor_large_write_req");
    InterceptorLargePwriteIn *req = new InterceptorLargePwriteIn();
    req->offset = 0;
    req->nbytes = 1;
    auto ret = InterceptorServer::GetInstance().CheckInterceptorLargeWriteReq(req);
    EXPECT_EQ(ret, false);
    req->nbytes = IO_SIZE_4M;
    ret = InterceptorServer::GetInstance().CheckInterceptorLargeWriteReq(req);
    EXPECT_EQ(ret, true);
    free(req);
}

TEST_F(TestBioServer, test_check_interceptor_write_req)
{
    LOG_INFO("test_check_interceptor_write_req");
    InterceptorPwriteIn *req = (InterceptorPwriteIn *) new char[sizeof(InterceptorPwriteIn) + 128];
    req->nbytes = IO_SIZE_8K + 1;
    req->offset = IO_SIZE_4M + 1;
    auto ret = InterceptorServer::GetInstance().CheckInterceptorWriteReq(req);
    EXPECT_EQ(ret, false);
    req->offset = 0;
    ret = InterceptorServer::GetInstance().CheckInterceptorWriteReq(req);
    EXPECT_EQ(ret, false);
    free(req);
}

TEST_F(TestBioServer, test_check_interceptor_read_req)
{
    LOG_INFO("test_check_interceptor_read_req");
    InterceptorPreadIn *req = new InterceptorPreadIn();
    req->nbytes = 0;
    req->offset = IO_SIZE_4M;
    auto ret = InterceptorServer::GetInstance().CheckInterceptorReadReq(req);
    EXPECT_EQ(ret, false);
    req->nbytes = 1;
    ret = InterceptorServer::GetInstance().CheckInterceptorReadReq(req);
    EXPECT_EQ(ret, true);
    free(req);
}

TEST_F(TestBioServer, test_mirror_server_check_remote_update_ready)
{
    LOG_INFO("test_mirror_server_check_remote_update_ready");
    ServiceContext ctx;
    CheckRemoteUpdateReadyRequest req;
    RequestComm comm = { 0, 0, 0, 0, 0 };
    req.comm = comm;
    auto ret = MirrorServer::Instance()->MirrorServerCheckRemoteUpdateReady(ctx, &req);
    EXPECT_EQ(ret, BIO_OK);
}
TEST_F(TestBioServer, test_alloc_quota_error)
{
    LOG_INFO("test_alloc_quota_error");
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "QUOTA_HOLDER_SIZE_MAX", 0, 1, userParam);
    QuotaHolder holder{NO_256, NO_1024};
    uint64_t expectAllocSize = NO_1024;
    auto ret = CacheOverloadCtrl::Instance().AllocQuota(holder, NO_128, expectAllocSize);
    BioHvsDeactiveTracePoint(0, "QUOTA_HOLDER_SIZE_MAX");
    EXPECT_EQ(ret, BIO_QUOTA_NOT_ENOUGH);
}

TEST_F(TestBioServer, test_large_node_list)
{
    LOG_INFO("test_large_node_list");
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "LARGE_NODE_LIST", 0, 1, userParam);
    std::map<CmNodeId, CmNodeInfo, CmNodeIdCmp> nodeInfos;
    auto ret = BioServer::Instance()->HandleCmNodeEvent(nodeInfos);
    BioHvsDeactiveTracePoint(0, "LARGE_NODE_LIST");
    EXPECT_EQ(ret, BIO_ERR);
}

TEST_F(TestBioServer, test_handle_update_ready)
{
    LOG_INFO("test_handle_update_ready");
    CheckUpdateReadyRequest *req = new CheckUpdateReadyRequest();
    req->comm.magic = NO_1;
    auto ret = MirrorServer::Instance()->CheckUpdateReadyReq(req);
    EXPECT_EQ(ret, false);
}

TEST_F(TestBioServer, test_handle_notify_update)
{
    LOG_INFO("test_handle_notify_update");
    ServiceContext ctx;
    NotifyUpdateRequest *req = new NotifyUpdateRequest();
    RequestComm comm;
    comm.magic = NO_1;
    req->comm = comm;
    auto ret = MirrorServer::Instance()->CheckNotifyUpdateReq(req);
    EXPECT_EQ(ret, false);
}

TEST_F(TestBioServer, test_handle_check_magic)
{
    LOG_INFO("test_handle_check_magic");
    RequestComm req;
    req.magic = MESSAGE_MAGIC;
    auto ret = MirrorServer::Instance()->CheckMagic(req);
    EXPECT_EQ(ret, true);
}

TEST_F(TestBioServer, test_handle_get_slice)
{
    LOG_INFO("test_handle_get_slice");
    GetSliceRequest *req = new GetSliceRequest();
    req->length = IO_SIZE_128M;
    auto ret = MirrorServer::Instance()->CheckGetSliceReq(req);
    EXPECT_EQ(ret, false);
    req->length = IO_SIZE_1M;
    ret = MirrorServer::Instance()->CheckGetSliceReq(req);
    EXPECT_EQ(ret, true);
}

TEST_F(TestBioServer, test_handle_free_mem)
{
    LOG_INFO("test_handle_free_mem");
    FreeMemRequest *req = new FreeMemRequest();
    RequestComm comm;
    comm.magic = NO_1;
    req->comm = comm;
    auto ret = MirrorServer::Instance()->CheckFreeMemReq(req);
    EXPECT_EQ(ret, false);

    req->num = NO_10;
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "MIRRIR_SERVER_CHECK_FREE_MEM_REQ_PASS_CHECK", 0, 1, userParam);
    ret = MirrorServer::Instance()->CheckFreeMemReq(req);
    EXPECT_EQ(ret, false);
    BioHvsDeactiveTracePoint(0, "MIRRIR_SERVER_CHECK_FREE_MEM_REQ_PASS_CHECK");

    req->num = NO_3;
    BioHvsActiveTracePoint(0, "MIRRIR_SERVER_CHECK_FREE_MEM_REQ_PASS_CHECK", 0, 1, userParam);
    ret = MirrorServer::Instance()->CheckFreeMemReq(req);
    EXPECT_EQ(ret, true);
    BioHvsDeactiveTracePoint(0, "MIRRIR_SERVER_CHECK_FREE_MEM_REQ_PASS_CHECK");
}

TEST_F(TestBioServer, test_handle_interceptor_write)
{
    LOG_INFO("test_handle_interceptor_write");
    ServiceContext ctx;
    auto ret = InterceptorServer::GetInstance().HandleInterceptorWrite(ctx);
    EXPECT_EQ(ret, BIO_OK);
}


TEST_F(TestBioServer, test_start_server_cacl_cache_hit_local)
{
    LOG_INFO("test_start_server_get_cache_hit_local");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    auto ret = mirror->HandleGetCacheHit(ctx);
    EXPECT_EQ(ret, BIO_OK);

    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "GET_CACHE_HIT", 0, 1, userParam);
    ret = mirror->HandleGetCacheHit(ctx);
    BioHvsDeactiveTracePoint(0, "GET_CACHE_HIT");
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_start_server_get_cache_resource_local)
{
    LOG_INFO("test_start_server_cacl_cache_resource_local");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    auto ret = mirror->HandleQueryCacheResource(ctx);
    EXPECT_EQ(ret, BIO_OK);

    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "CALC_CACHE_RESOURCE", 0, 1, userParam);
    ret = mirror->HandleQueryCacheResource(ctx);
    BioHvsDeactiveTracePoint(0, "CALC_CACHE_RESOURCE");
    EXPECT_EQ(ret, BIO_OK);
}

TEST_F(TestBioServer, test_calc_cache_resource_local_without_disk_info)
{
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    CacheResourceRequest req{};
    CacheResourceResponse rsp{};

    EXPECT_EQ(mirror->CalcCacheResourceLocal(nullptr, &rsp), BIO_INVALID_PARAM);
    EXPECT_EQ(mirror->CalcCacheResourceLocal(&req, nullptr), BIO_INVALID_PARAM);
    EXPECT_EQ(mirror->CalcCacheResourceLocal(&req, &rsp), BIO_OK);
    EXPECT_EQ(rsp.diskNum, 0);
}

TEST_F(TestBioServer, test_get_trace_data_success)
{
    LOG_INFO("test_get_trace_data_success");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    TraceDatabase traceDatabase = mirror->GetTraceData();
    auto ret = traceDatabase.count != 0;
    EXPECT_EQ(ret, true);
}

TEST_F(TestBioServer, test_get_trace_data_fail)
{
    LOG_INFO("test_get_trace_data_fail");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "MIRROR_SERVER_TRACE_EXCEED_ARRAY_SIZE", 0, 1, userParam);
    TraceDatabase traceDatabase = mirror->GetTraceData();
    BioHvsDeactiveTracePoint(0, "MIRROR_SERVER_TRACE_EXCEED_ARRAY_SIZE");
    EXPECT_EQ(traceDatabase.count, TRACE_MAX_NUM);
}

TEST_F(TestBioServer, test_get_trace_data_null)
{
    LOG_INFO("test_get_trace_data_null");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    BioTracepointParam userParam;
    BioHvsActiveTracePoint(0, "SERVER_GET_TRACEDATA_ERR", 0, 1, userParam);
    TraceDatabase traceDatabase = mirror->GetTraceData();
    BioHvsDeactiveTracePoint(0, "SERVER_GET_TRACEDATA_ERR");
    EXPECT_EQ(traceDatabase.count, 0);
}

TEST_F(TestBioServer, test_bio_server_get_cli_flag)
{
    LOG_INFO("test_bio_server_get_cli_flag");
    auto instance = BioServer::Instance();
    bool flag = instance->GetCliFlag();
    EXPECT_EQ(flag, false);
}

TEST_F(TestBioServer, test_start_server_add_disk)
{
    LOG_INFO("test_start_server_add_disk");
    MirrorServerPtr mirror = BioServer::Instance()->GetMirrorServer();
    ServiceContext ctx;
    auto ret = mirror->HandleAddDisk(ctx);
    EXPECT_EQ(ret, BIO_OK);
}
