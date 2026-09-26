#include <algorithm>
#include <cassert>
#include <cstring>
#include <map>
#include <string>
#include <vector>
#include "remote_dram_object.h"
#include "remote_dram_object_mock_api.h"
#include "ubsio_kvc_err.h"

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

int main()
{
    std::vector<char> data(remote_dram::CHUNK_BYTES + 13);
    for (size_t i = 0; i < data.size(); ++i) data[i] = static_cast<char>(i * 37);
    std::vector<char> output(data.size());
    uint32_t length = 0;
    assert(remote_dram::Put("large", data.data(), data.size()) == UBSIO_KVC_OK);
    assert(remote_dram::Length("large", length) == UBSIO_KVC_OK && length == data.size());
    assert(remote_dram::Get("large", output.data(), output.size()) == UBSIO_KVC_OK);
    assert(output == data);
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
