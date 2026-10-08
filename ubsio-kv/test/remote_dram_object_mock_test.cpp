#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <map>
#include <string>
#include <vector>
#include "remote_dram_object.h"
#include "remote_dram_object_mock_api.h"
#include "ubsio_kvc_err.h"

namespace ock { namespace ubsio { namespace remote_dram {
uint32_t Crc32ForTest(const void *, size_t);
uint32_t Crc32SoftwareForTest(const void *, size_t);
bool HasHardwareCrc32ForTest();
}}}
using namespace ock::ubsio;
namespace {
std::map<std::string, std::vector<char>> objects;
bool lateManifestPublish = false;
bool shortRead = false;
bool failNextChunk = false;
std::map<std::string, std::vector<char>> delayedWrites;
std::vector<std::string> delayedDeletes;
bool delayDelete = false;

bool IsManifest(const std::string &key)
{
    return key.size() >= 8 && key.substr(key.size() - 8) == "ffffffff";
}

void CompleteDelayed()
{
    for (auto &item : delayedWrites) objects[item.first] = item.second;
    for (const auto &key : delayedDeletes) objects.erase(key);
    delayedWrites.clear();
    delayedDeletes.clear();
}
}

uint32_t mockBatchGetCalls = 0;
uint32_t mockBatchGetFreeCalls = 0;
bool mockBatchGetShortRead = false;
bool mockBatchGetGlobalFailure = false;

CResult DlBioSdkApi::Stat(uint64_t, const char *key, ObjLocation, ObjStat *stat)
{
    auto found = objects.find(key);
    if (found == objects.end()) return RET_CACHE_NOT_FOUND;
    stat->size = static_cast<uint32_t>(found->second.size());
    return RET_CACHE_OK;
}

CResult DlBioSdkApi::Get(uint64_t, const char *key, uint64_t, uint64_t length, ObjLocation,
                         char *value, uint64_t *actual)
{
    auto found = objects.find(key);
    if (found == objects.end()) return RET_CACHE_NOT_FOUND;
    *actual = std::min<uint64_t>(length, found->second.size());
    if (shortRead && *actual > 0) --*actual;
    std::memcpy(value, found->second.data(), *actual);
    return RET_CACHE_OK;
}

CResult DlBioSdkApi::BatchGet(uint64_t, const char **keys, const uint32_t count, uint64_t *offsets,
                              uint64_t *lengths, ObjLocation *, uintptr_t *addresses,
                              uint64_t *actualLengths, int32_t *results)
{
    ++mockBatchGetCalls;
    for (uint32_t i = 0; i < count; ++i) {
        addresses[i] = 0;
        actualLengths[i] = 0;
        auto found = objects.find(keys[i]);
        if (found == objects.end() || offsets[i] > found->second.size()) {
            results[i] = bio::BIO_NOT_EXISTS;
            continue;
        }
        uint64_t actual = std::min<uint64_t>(lengths[i], found->second.size() - offsets[i]);
        if (mockBatchGetShortRead && actual > 0) --actual;
        char *buffer = static_cast<char *>(std::malloc(actual == 0 ? 1 : actual));
        assert(buffer != nullptr);
        if (actual > 0) std::memcpy(buffer, found->second.data() + offsets[i], actual);
        addresses[i] = reinterpret_cast<uintptr_t>(buffer);
        actualLengths[i] = actual;
        results[i] = bio::BIO_OK;
    }
    if (mockBatchGetGlobalFailure) {
        for (uint32_t i = 0; i < count; ++i) {
            std::free(reinterpret_cast<void *>(addresses[i]));
            addresses[i] = 0;
        }
        return RET_CACHE_UNAVAILABLE;
    }
    return RET_CACHE_OK;
}

CResult DlBioSdkApi::BatchGetFree(uint64_t, uintptr_t *addresses, const uint32_t count)
{
    ++mockBatchGetFreeCalls;
    for (uint32_t i = 0; i < count; ++i) {
        std::free(reinterpret_cast<void *>(addresses[i]));
        addresses[i] = 0;
    }
    return RET_CACHE_OK;
}

CResult DlBioSdkApi::Put(uint64_t, const char *key, const char *value, uint64_t length, ObjLocation)
{
    if (failNextChunk && std::string(key).find("__kvc_tcp_v1_") == 0) {
        failNextChunk = false;
        return RET_CACHE_UNAVAILABLE;
    }
    if (lateManifestPublish && IsManifest(key)) {
        delayedWrites[key] = std::vector<char>(value, value + length);
        lateManifestPublish = false;
        return RET_CACHE_UNAVAILABLE;
    }
    objects[key] = std::vector<char>(value, value + length);
    return RET_CACHE_OK;
}

CResult DlBioSdkApi::Delete(uint64_t, const char *key, ObjLocation)
{
    if (delayDelete) {
        delayedDeletes.emplace_back(key);
        return RET_CACHE_UNAVAILABLE;
    }
    return objects.erase(key) ? RET_CACHE_OK : RET_CACHE_NOT_FOUND;
}


void TestCompatibleCrc()
{
    using namespace remote_dram;
    const char *known = "123456789";
    assert(Crc32ForTest(nullptr, 0) == 0);
    assert(Crc32SoftwareForTest(nullptr, 0) == 0);
    assert(Crc32ForTest(known, 9) == 0xcbf43926U);
    assert(Crc32SoftwareForTest(known, 9) == 0xcbf43926U);
    std::vector<uint8_t> data(41943040U + 8);
    uint32_t state = 0x7183U;
    for (auto &b : data) { state = state * 1664525U + 1013904223U; b = state >> 24; }
    std::vector<size_t> lengths;
    for (size_t n = 0; n < 130; ++n) lengths.push_back(n);
    for (size_t n : {1048575U, 1048576U, 1048577U, 21463040U, 41943040U}) lengths.push_back(n);
    for (size_t n : lengths) {
        for (size_t offset = 0; offset < 8; ++offset) {
            const auto expected = Crc32SoftwareForTest(data.data() + offset, n);
            assert(Crc32ForTest(data.data() + offset, n) == expected);
        }
    }
    std::cout << "CRC32 IEEE known vector, software fallback, tails, unaligned and large payload PASS; hardware="
              << HasHardwareCrc32ForTest() << '\n';
}

int main()
{
    TestCompatibleCrc();
    std::vector<char> data(remote_dram::CHUNK_BYTES + 13);
    for (size_t i = 0; i < data.size(); ++i) data[i] = static_cast<char>(i * 37);
    std::vector<char> output(data.size());
    uint32_t length = 0;
    assert(remote_dram::Put("large", data.data(), data.size()) == UBSIO_KVC_OK);
    for (auto &entry : objects) {
        if (!IsManifest(entry.first)) continue;
        uint32_t writtenCrc;
        assert(entry.second.size() == 48);
        std::memcpy(&writtenCrc, entry.second.data() + 44, sizeof(writtenCrc));
        assert(writtenCrc == remote_dram::Crc32SoftwareForTest(data.data(), data.size()));
        // Read a manifest CRC produced by the old implementation with the new reader.
        const uint32_t oldCrc = remote_dram::Crc32SoftwareForTest(data.data(), data.size());
        std::memcpy(entry.second.data() + 44, &oldCrc, sizeof(oldCrc));
    }
    assert(remote_dram::Length("large", length) == UBSIO_KVC_OK && length == data.size());
    assert(remote_dram::Get("large", output.data(), output.size()) == UBSIO_KVC_OK);
    assert(output == data);
    const uint32_t batchCallsBefore = mockBatchGetCalls;
    const uint32_t freesBefore = mockBatchGetFreeCalls;
    assert(remote_dram::BatchGet("large", output.data(), output.size()) == UBSIO_KVC_OK);
    assert(output == data);
    assert(mockBatchGetCalls - batchCallsBefore == 1);
    assert(mockBatchGetFreeCalls - freesBefore == 1);
    assert(remote_dram::PhysicalBatchGetCallCount() == mockBatchGetCalls);
    mockBatchGetShortRead = true;
    const uint32_t shortReadFreeBefore = mockBatchGetFreeCalls;
    assert(remote_dram::BatchGet("large", output.data(), output.size()) == UBSIO_KVC_CORRUPT);
    assert(mockBatchGetFreeCalls - shortReadFreeBefore == 1);
    mockBatchGetShortRead = false;
    mockBatchGetGlobalFailure = true;
    const uint32_t globalFailureFreeBefore = mockBatchGetFreeCalls;
    assert(remote_dram::BatchGet("large", output.data(), output.size()) == UBSIO_KVC_UNAVAILABLE);
    assert(mockBatchGetFreeCalls == globalFailureFreeBefore);
    mockBatchGetGlobalFailure = false;
    std::vector<char> manyChunks(17 * remote_dram::CHUNK_BYTES + 7);
    for (size_t i = 0; i < manyChunks.size(); ++i) manyChunks[i] = static_cast<char>(i * 19);
    assert(remote_dram::Put("many-chunks", manyChunks.data(), manyChunks.size()) == UBSIO_KVC_OK);
    std::vector<char> manyChunksOutput(manyChunks.size());
    const uint32_t multiCallsBefore = mockBatchGetCalls;
    const uint32_t multiFreesBefore = mockBatchGetFreeCalls;
    assert(remote_dram::BatchGet("many-chunks", manyChunksOutput.data(), manyChunksOutput.size()) == UBSIO_KVC_OK);
    assert(manyChunksOutput == manyChunks);
    assert(mockBatchGetCalls - multiCallsBefore == 2);
    assert(mockBatchGetFreeCalls - multiFreesBefore == 2);
    assert(remote_dram::Delete("many-chunks") == UBSIO_KVC_OK);
    assert(remote_dram::Get("large", output.data(), 10) == UBSIO_KVC_INVALID_PARAM);
    std::vector<char> oversized(data.size() + 1);
    assert(remote_dram::Get("large", oversized.data(), oversized.size()) == UBSIO_KVC_CORRUPT);
    failNextChunk = true;
    std::vector<char> replacement(data.size(), 'x');
    assert(remote_dram::Put("large", replacement.data(), replacement.size()) == UBSIO_KVC_UNAVAILABLE);
    assert(remote_dram::Get("large", output.data(), output.size()) == UBSIO_KVC_OK);
    assert(output == data);

    shortRead = true;
    assert(remote_dram::Get("large", output.data(), output.size()) == UBSIO_KVC_CORRUPT);
    shortRead = false;
    auto chunk = std::find_if(objects.begin(), objects.end(), [](const auto &item) {
        return item.first.find("__kvc_tcp_v1_") == 0 && !IsManifest(item.first);
    });
    assert(chunk != objects.end());
    chunk->second[0] ^= 1;
    assert(remote_dram::Get("large", output.data(), output.size()) == UBSIO_KVC_CORRUPT);
    assert(remote_dram::BatchGet("large", output.data(), output.size()) == UBSIO_KVC_CORRUPT);
    chunk->second[0] ^= 1;
    objects.erase(chunk->first);
    assert(remote_dram::Exist("large") == UBSIO_KVC_CORRUPT);
    assert(remote_dram::Delete("large") == UBSIO_KVC_OK);
    assert(remote_dram::Exist("large") == UBSIO_KVC_NOT_FOUND);

    lateManifestPublish = true;
    assert(remote_dram::Put("late", data.data(), data.size()) == UBSIO_KVC_UNAVAILABLE);
    assert(remote_dram::Get("late", output.data(), output.size()) == UBSIO_KVC_NOT_FOUND);
    assert(remote_dram::Put("late", replacement.data(), replacement.size()) == UBSIO_KVC_OK);
    CompleteDelayed(); // Old manifest arrives after the successor has been acknowledged.
    assert(remote_dram::Get("late", output.data(), output.size()) == UBSIO_KVC_OK && output == replacement);

    delayDelete = true;
    assert(remote_dram::Delete("late") == UBSIO_KVC_UNAVAILABLE);
    delayDelete = false;
    assert(remote_dram::Get("late", output.data(), output.size()) == UBSIO_KVC_NOT_FOUND);
    assert(remote_dram::Put("late", data.data(), data.size()) == UBSIO_KVC_OK);
    CompleteDelayed(); // An old delete must not remove the recreated key.
    assert(remote_dram::Get("late", output.data(), output.size()) == UBSIO_KVC_OK && output == data);
    assert(remote_dram::Delete("late") == UBSIO_KVC_OK);
    assert(remote_dram::Get("late", output.data(), output.size()) == UBSIO_KVC_NOT_FOUND);

    lateManifestPublish = true;
    assert(remote_dram::Put("deleted", data.data(), data.size()) == UBSIO_KVC_UNAVAILABLE);
    assert(remote_dram::Delete("deleted") == UBSIO_KVC_NOT_FOUND);
    CompleteDelayed();
    assert(remote_dram::Exist("deleted") == UBSIO_KVC_NOT_FOUND);
    assert(remote_dram::Put("restart", data.data(), data.size()) == UBSIO_KVC_OK);
    remote_dram::ResetDirectory();
    assert(remote_dram::Exist("restart") == UBSIO_KVC_NOT_FOUND);
    assert(remote_dram::Put("restart", replacement.data(), replacement.size()) == UBSIO_KVC_OK);
    assert(remote_dram::Get("restart", output.data(), output.size()) == UBSIO_KVC_OK && output == replacement);
    std::cout << "remote_dram_object: integrity, late write/delete, rebuild and restart PASS\n";
}
