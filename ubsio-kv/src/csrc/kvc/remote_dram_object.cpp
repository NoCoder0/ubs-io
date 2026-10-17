/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * ubs-io is licensed under the Mulan PSL v2.
 */
#include "remote_dram_object.h"
#include "remote_read_limits.h"

#include <array>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <limits>
#include <map>
#include <mutex>
#include <random>
#include <unistd.h>
#if defined(__aarch64__) && defined(__linux__)
#include <arm_acle.h>
#include <asm/hwcap.h>
#include <sys/auxv.h>
#endif
#ifdef UBSIO_REMOTE_DRAM_OBJECT_MOCK
#include "remote_dram_object_mock_api.h"
#else
#include "bio_c.h"
#include "bio_err.h"
#include "dl_biosdk_api.h"
#include "ubsio_kvc_log.h"
#endif
#include "ubsio_kvc_err.h"

namespace ock {
namespace ubsio {
namespace remote_dram {
namespace {

constexpr uint64_t MANIFEST_MAGIC = 0x314d41524443564bULL; // KVCDRAM1, little endian
constexpr uint32_t MANIFEST_VERSION = 1;
constexpr size_t KEY_LOCK_COUNT = 256;
constexpr size_t MAX_KEYS_PER_SHARD = 4096; // At most 1M logical directory entries per SDK.
constexpr uint32_t BATCH_GET_WIDTH = 16;
std::array<std::mutex, KEY_LOCK_COUNT> keyLocks;
std::atomic<uint64_t> generationCounter{1};

struct Manifest {
    uint64_t magic;
    uint64_t generation;
    uint64_t keyHash;
    uint64_t length;
    uint32_t version;
    uint32_t chunkBytes;
    uint32_t chunkCount;
    uint32_t dataCrc;
};
static_assert(sizeof(Manifest) == 48, "Remote manifest ABI changed");
// This is a volatile, process-owned cache directory, never a remotely overwritten
// logical-key manifest. A timed-out write cannot publish itself into this directory.
std::array<std::map<std::string, Manifest>, KEY_LOCK_COUNT> committed;

std::string ManifestKey(const Manifest &manifest);

uint64_t HashKey(const std::string &key)
{
    uint64_t hash = 14695981039346656037ULL;
    for (unsigned char c : key) {
        hash = (hash ^ c) * 1099511628211ULL;
    }
    return hash;
}

uint32_t Crc32Software(const void *data, size_t length)
{
    static const auto table = []() {
        std::array<uint32_t, 256> values{};
        for (uint32_t i = 0; i < values.size(); ++i) {
            uint32_t crc = i;
            for (uint32_t bit = 0; bit < 8; ++bit) {
                crc = (crc >> 1) ^ ((crc & 1U) ? 0xedb88320U : 0U);
            }
            values[i] = crc;
        }
        return values;
    }();
    uint32_t crc = ~0U;
    auto *bytes = static_cast<const uint8_t *>(data);
    for (size_t i = 0; i < length; ++i) {
        crc = table[(crc ^ bytes[i]) & 0xffU] ^ (crc >> 8);
    }
    return ~crc;
}


#if defined(__aarch64__) && defined(__linux__)
// IEEE CRC32 (not CRC32C): preserve the existing manifest and data CRC format.
__attribute__((target("+crc")))
uint32_t Crc32Hardware(const void *data, size_t length)
{
    uint32_t crc = ~0U;
    const auto *bytes = static_cast<const uint8_t *>(data);
    while (length >= sizeof(uint64_t)) {
        uint64_t word;
        // Safe for unaligned buffers and strict aliasing; no read past length.
        std::memcpy(&word, bytes, sizeof(word));
#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
        word = __builtin_bswap64(word);
#endif
        crc = __crc32d(crc, word);
        bytes += sizeof(word);
        length -= sizeof(word);
    }
    while (length-- != 0) crc = __crc32b(crc, *bytes++);
    return ~crc;
}
#endif

bool HasHardwareCrc32()
{
#if defined(__aarch64__) && defined(__linux__)
    static const bool available = (getauxval(AT_HWCAP) & HWCAP_CRC32) != 0;
    return available;
#else
    return false;
#endif
}

uint32_t Crc32(const void *data, size_t length)
{
#if defined(__aarch64__) && defined(__linux__)
    if (HasHardwareCrc32()) return Crc32Hardware(data, length);
#endif
    return Crc32Software(data, length);
}

uint64_t NewGeneration()
{
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    uint64_t entropy = static_cast<uint64_t>(now) ^ (static_cast<uint64_t>(getpid()) << 32);
    try {
        std::random_device random;
        entropy ^= (static_cast<uint64_t>(random()) << 32) | random();
    } catch (...) {
        LOG_ERROR("random_device failed; using clock and process ID for remote generation");
    }
    return entropy ^ generationCounter.fetch_add(1);
}

std::string ChunkKey(const Manifest &manifest, uint32_t index)
{
    char suffix[96];
    std::snprintf(suffix, sizeof(suffix), "__kvc_tcp_v1_%016llx_%016llx_%08x",
                  static_cast<unsigned long long>(manifest.keyHash),
                  static_cast<unsigned long long>(manifest.generation), index);
    return suffix;
}

int32_t MapBio(CResult result)
{
    switch (result) {
        case RET_CACHE_OK: return UBSIO_KVC_OK;
        case RET_CACHE_NOT_FOUND:
        case RET_CACHE_MISS: return UBSIO_KVC_NOT_FOUND;
        case RET_CACHE_EPERM: return UBSIO_KVC_INVALID_PARAM;
        case RET_CACHE_BUSY:
        case RET_CACHE_NEED_RETRY:
        case RET_CACHE_NOT_READY:
        case RET_CACHE_NO_SPACE:
        case RET_CACHE_UNAVAILABLE:
        case RET_CACHE_EXCEED_QUOTA:
        case RET_CACHE_PT_FAULT: return UBSIO_KVC_UNAVAILABLE;
        default: return UBSIO_KVC_BIO_ERR;
    }
}

int32_t MapBioBatchResult(int32_t result)
{
    switch (result) {
        case bio::BIO_OK: return UBSIO_KVC_OK;
        case bio::BIO_NOT_EXISTS: return UBSIO_KVC_NOT_FOUND;
        case bio::BIO_CRC_ERR: return UBSIO_KVC_CORRUPT;
        case bio::BIO_INVALID_PARAM: return UBSIO_KVC_INVALID_PARAM;
        case bio::BIO_NOT_READY:
        case bio::BIO_ALLOC_FAIL:
        case bio::BIO_CHECK_PT_FAIL:
        case bio::BIO_INNER_RETRY:
        case bio::BIO_NET_RETRY:
        case bio::BIO_QUOTA_NOT_ENOUGH:
        case bio::BIO_QUOTA_TIMEOUT: return UBSIO_KVC_UNAVAILABLE;
        default: return UBSIO_KVC_BIO_ERR;
    }
}

class BatchGetBufferGuard {
public:
    explicit BatchGetBufferGuard(std::vector<uintptr_t> &addresses)
        : mAddresses(addresses), mOwned(true) {}

    ~BatchGetBufferGuard()
    {
        if (mOwned && DlBioSdkApi::BatchGetFree(DlBioSdkApi::TenantId(), mAddresses.data(),
            static_cast<uint32_t>(mAddresses.size())) != RET_CACHE_OK) {
            LOG_ERROR("Remote batch-get buffer release failed");
        }
    }

    int32_t Free()
    {
        if (!mOwned) return UBSIO_KVC_OK;
        mOwned = false;
        return MapBio(DlBioSdkApi::BatchGetFree(DlBioSdkApi::TenantId(), mAddresses.data(),
            static_cast<uint32_t>(mAddresses.size())));
    }

private:
    std::vector<uintptr_t> &mAddresses;
    bool mOwned;
};

int32_t Location(const std::string &key, ObjLocation &location)
{
    return MapBio(DlBioSdkApi::CalcLocation(DlBioSdkApi::TenantId(), HashKey(key), &location));
}

int32_t Stat(const std::string &key, uint32_t &size)
{
    ObjLocation location{};
    int32_t result = Location(key, location);
    if (result != UBSIO_KVC_OK) return result;
    ObjStat stat{};
    result = MapBio(DlBioSdkApi::Stat(DlBioSdkApi::TenantId(), key.c_str(), location, &stat));
    if (result == UBSIO_KVC_OK) size = stat.size;
    return result;
}

int32_t Read(const std::string &key, void *value, uint32_t length)
{
    ObjLocation location{};
    int32_t result = Location(key, location);
    if (result != UBSIO_KVC_OK) return result;
    uint64_t actual = 0;
    result = MapBio(DlBioSdkApi::Get(DlBioSdkApi::TenantId(), key.c_str(), 0, length, location,
                                      static_cast<char *>(value), &actual));
    if (result != UBSIO_KVC_OK) return result;
    return actual == length ? UBSIO_KVC_OK : UBSIO_KVC_CORRUPT;
}

int32_t Write(const std::string &key, const void *value, uint32_t length)
{
    ObjLocation location{};
    int32_t result = Location(key, location);
    if (result != UBSIO_KVC_OK) return result;
    return MapBio(DlBioSdkApi::Put(DlBioSdkApi::TenantId(), key.c_str(), static_cast<const char *>(value),
                                   length, location));
}

int32_t Remove(const std::string &key)
{
    ObjLocation location{};
    int32_t result = Location(key, location);
    if (result != UBSIO_KVC_OK) return result;
    return MapBio(DlBioSdkApi::Delete(DlBioSdkApi::TenantId(), key.c_str(), location));
}

bool Valid(const Manifest &manifest, const std::string &key)
{
    if (manifest.magic != MANIFEST_MAGIC || manifest.version != MANIFEST_VERSION ||
        manifest.keyHash != HashKey(key) || manifest.chunkBytes != CHUNK_BYTES ||
        manifest.length == 0 || manifest.length > std::numeric_limits<uint32_t>::max()) return false;
    return manifest.chunkCount == (manifest.length + CHUNK_BYTES - 1) / CHUNK_BYTES;
}

int32_t ReadManifest(const std::string &key, Manifest &manifest)
{
    ObjLocation location{};
    int32_t result = Location(key, location);
    if (result != UBSIO_KVC_OK) return result;
    // Request one extra byte to reject oversized manifests while accepting SDK short reads.
    std::array<char, sizeof(Manifest) + 1> bytes{};
    uint64_t actual = 0;
    result = MapBio(DlBioSdkApi::Get(DlBioSdkApi::TenantId(), key.c_str(), 0, bytes.size(), location,
                                    bytes.data(), &actual));
    if (result != UBSIO_KVC_OK) return result;
    if (actual != sizeof(Manifest)) return UBSIO_KVC_CORRUPT;
    std::memcpy(&manifest, bytes.data(), sizeof(manifest));
    return UBSIO_KVC_OK;
}

int32_t LoadManifest(const std::string &key, Manifest &manifest)
{
    const auto &directory = committed[HashKey(key) % KEY_LOCK_COUNT];
    const auto entry = directory.find(key);
    if (entry == directory.end()) return UBSIO_KVC_NOT_FOUND;
    const auto expected = entry->second;
    const auto physicalKey = ManifestKey(expected);
    int32_t result = UBSIO_KVC_OK;
    if (DlBioSdkApi::ManifestOneGet()) {
        result = ReadManifest(physicalKey, manifest);
    } else {
        uint32_t size = 0;
        result = Stat(physicalKey, size);
        if (result != UBSIO_KVC_OK) return result;
        if (size != sizeof(manifest)) return UBSIO_KVC_CORRUPT;
        result = Read(physicalKey, &manifest, sizeof(manifest));
    }
    if (result != UBSIO_KVC_OK) return result;
    return Valid(manifest, key) && std::memcmp(&manifest, &expected, sizeof(manifest)) == 0 ?
        UBSIO_KVC_OK : UBSIO_KVC_CORRUPT;
}

std::string ManifestKey(const Manifest &manifest)
{
    return ChunkKey(manifest, UINT32_MAX);
}

int32_t CheckChunks(const Manifest &manifest)
{
    for (uint32_t index = 0; index < manifest.chunkCount; ++index) {
        uint32_t actual = 0;
        int32_t result = Stat(ChunkKey(manifest, index), actual);
        const uint32_t expected = static_cast<uint32_t>(std::min<uint64_t>(
            CHUNK_BYTES, manifest.length - static_cast<uint64_t>(index) * CHUNK_BYTES));
        if (result == UBSIO_KVC_NOT_FOUND) return UBSIO_KVC_CORRUPT;
        if (result != UBSIO_KVC_OK) return result;
        if (actual != expected) return UBSIO_KVC_CORRUPT;
    }
    return UBSIO_KVC_OK;
}

void RemoveChunks(const Manifest &manifest)
{
    for (uint32_t index = 0; index < manifest.chunkCount; ++index) {
        int32_t result = Remove(ChunkKey(manifest, index));
        if (result != UBSIO_KVC_OK && result != UBSIO_KVC_NOT_FOUND) {
            LOG_ERROR("Remote chunk cleanup failed, index:" << index << " result:" << result);
            // Avoid one full network timeout per remaining chunk during an outage.
            // Unreachable fragments are bounded by WCache and can be evicted later.
            break;
        }
    }
}

} // namespace

#ifdef UBSIO_REMOTE_DRAM_OBJECT_MOCK
uint32_t Crc32ForTest(const void *data, size_t length) { return Crc32(data, length); }
uint32_t Crc32SoftwareForTest(const void *data, size_t length) { return Crc32Software(data, length); }
bool HasHardwareCrc32ForTest() { return HasHardwareCrc32(); }
#endif

int32_t Put(const std::string &key, const void *value, size_t length)
{
    if (value == nullptr || length == 0 || length > std::numeric_limits<uint32_t>::max()) {
        LOG_ERROR("Invalid remote put length:" << length);
        return UBSIO_KVC_INVALID_PARAM;
    }
    std::lock_guard<std::mutex> guard(keyLocks[HashKey(key) % KEY_LOCK_COUNT]);
    auto &directory = committed[HashKey(key) % KEY_LOCK_COUNT];
    const auto prior = directory.find(key);
    if (prior == directory.end() && directory.size() >= MAX_KEYS_PER_SHARD) return UBSIO_KVC_UNAVAILABLE;
    const Manifest previous = prior == directory.end() ? Manifest{} : prior->second;
    Manifest manifest{MANIFEST_MAGIC, NewGeneration(), HashKey(key), length, MANIFEST_VERSION,
                      CHUNK_BYTES, static_cast<uint32_t>((length + CHUNK_BYTES - 1) / CHUNK_BYTES),
                      Crc32(value, length)};
    for (uint32_t index = 0; index < manifest.chunkCount; ++index) {
        const size_t offset = static_cast<size_t>(index) * CHUNK_BYTES;
        const uint32_t size = static_cast<uint32_t>(std::min<size_t>(CHUNK_BYTES, length - offset));
        int32_t result = Write(ChunkKey(manifest, index), static_cast<const uint8_t *>(value) + offset, size);
        if (result != UBSIO_KVC_OK) {
            LOG_ERROR("Remote chunk put failed, index:" << index << " result:" << result);
            auto partial = manifest;
            partial.chunkCount = index + 1; // Include the possibly late current write, not unsubmitted chunks.
            RemoveChunks(partial);
            return result;
        }
    }
    int32_t result = Write(ManifestKey(manifest), &manifest, sizeof(manifest));
    if (result != UBSIO_KVC_OK) {
        LOG_ERROR("Remote manifest publish failed, key:" << key << " result:" << result);
        // Late completion can only leave an unreachable physical generation.
        RemoveChunks(manifest);
        Remove(ManifestKey(manifest));
        return result;
    }
    directory[key] = manifest; // The only publication point, after all successful acknowledgements.
    if (previous.magic == MANIFEST_MAGIC) {
        RemoveChunks(previous);
        Remove(ManifestKey(previous));
    }
    return UBSIO_KVC_OK;
}

int32_t Get(const std::string &key, void *value, size_t capacity)
{
    if (value == nullptr) return UBSIO_KVC_INVALID_PARAM;
    std::lock_guard<std::mutex> guard(keyLocks[HashKey(key) % KEY_LOCK_COUNT]);
    Manifest manifest{};
    int32_t result = LoadManifest(key, manifest);
    if (result != UBSIO_KVC_OK) return result;
    if (capacity < manifest.length) return UBSIO_KVC_INVALID_PARAM;
    if (capacity != manifest.length) return UBSIO_KVC_CORRUPT;
    for (uint32_t index = 0; index < manifest.chunkCount; ++index) {
        const size_t offset = static_cast<size_t>(index) * CHUNK_BYTES;
        const uint32_t size = static_cast<uint32_t>(std::min<uint64_t>(CHUNK_BYTES, manifest.length - offset));
        result = Read(ChunkKey(manifest, index), static_cast<uint8_t *>(value) + offset, size);
        if (result == UBSIO_KVC_NOT_FOUND) return UBSIO_KVC_CORRUPT;
        if (result != UBSIO_KVC_OK) return result;
    }
    if (Crc32(value, manifest.length) != manifest.dataCrc) return UBSIO_KVC_CORRUPT;
    Manifest current{};
    result = LoadManifest(key, current);
    if (result != UBSIO_KVC_OK) return result;
    return current.generation == manifest.generation ? UBSIO_KVC_OK : UBSIO_KVC_UNAVAILABLE;
}

int32_t BatchGet(const std::string &key, void *value, size_t capacity)
{
    if (value == nullptr) return UBSIO_KVC_INVALID_PARAM;
    std::lock_guard<std::mutex> guard(keyLocks[HashKey(key) % KEY_LOCK_COUNT]);
    Manifest manifest{};
    int32_t result = LoadManifest(key, manifest);
    if (result != UBSIO_KVC_OK) return result;
    if (capacity < manifest.length) return UBSIO_KVC_INVALID_PARAM;
    if (capacity != manifest.length) return UBSIO_KVC_CORRUPT;

    for (uint32_t first = 0; first < manifest.chunkCount;) {
        const uint32_t count = std::min<uint32_t>(BATCH_GET_WIDTH, manifest.chunkCount - first);
        std::vector<std::string> physicalKeys(count);
        std::vector<const char *> keys(count);
        std::vector<ObjLocation> locations(count);
        std::vector<uint64_t> offsets(count, 0);
        std::vector<uint64_t> lengths(count);
        std::vector<uintptr_t> addresses(count, 0);
        std::vector<uint64_t> realLengths(count, 0);
        std::vector<int32_t> itemResults(count, bio::BIO_OK);

        for (uint32_t i = 0; i < count; ++i) {
            const uint32_t index = first + i;
            const size_t offset = static_cast<size_t>(index) * CHUNK_BYTES;
            physicalKeys[i] = ChunkKey(manifest, index);
            keys[i] = physicalKeys[i].c_str();
            lengths[i] = std::min<uint64_t>(CHUNK_BYTES, manifest.length - offset);
            result = Location(physicalKeys[i], locations[i]);
            if (result != UBSIO_KVC_OK) return result;
        }

        // Reserve the entire window before any SDK allocation. The buffer guard
        // below is destroyed first, so credits return only after buffers are freed.
        ReadWindowCredits credits(count, DlBioSdkApi::ReadBlockLimit());
        if (!credits.Valid()) return UBSIO_KVC_INVALID_PARAM;
        const CResult batchResult = DlBioSdkApi::BatchGet(DlBioSdkApi::TenantId(), keys.data(), count,
            offsets.data(), lengths.data(), locations.data(), addresses.data(), realLengths.data(),
            itemResults.data());
        // BatchGetImpl releases every allocated address when the overall call fails.
        if (batchResult != RET_CACHE_OK) return MapBio(batchResult);

        BatchGetBufferGuard buffers(addresses);
        int32_t itemError = UBSIO_KVC_OK;
        for (uint32_t i = 0; i < count; ++i) {
            int32_t itemResult = MapBioBatchResult(itemResults[i]);
            if (itemResult == UBSIO_KVC_NOT_FOUND) itemResult = UBSIO_KVC_CORRUPT;
            if (itemResult == UBSIO_KVC_OK &&
                (addresses[i] == 0 || realLengths[i] != lengths[i])) {
                itemResult = UBSIO_KVC_CORRUPT;
            }
            if (itemError == UBSIO_KVC_OK && itemResult != UBSIO_KVC_OK) itemError = itemResult;
        }
        if (itemError != UBSIO_KVC_OK) return itemError;

        for (uint32_t i = 0; i < count; ++i) {
            const size_t offset = static_cast<size_t>(first + i) * CHUNK_BYTES;
            std::memcpy(static_cast<uint8_t *>(value) + offset,
                reinterpret_cast<const void *>(addresses[i]), static_cast<size_t>(lengths[i]));
        }
        result = buffers.Free();
        if (result != UBSIO_KVC_OK) return result;
        first += count;
    }

    if (Crc32(value, manifest.length) != manifest.dataCrc) return UBSIO_KVC_CORRUPT;
    Manifest current{};
    result = LoadManifest(key, current);
    if (result != UBSIO_KVC_OK) return result;
    return current.generation == manifest.generation ? UBSIO_KVC_OK : UBSIO_KVC_UNAVAILABLE;
}

int32_t Delete(const std::string &key)
{
    std::lock_guard<std::mutex> guard(keyLocks[HashKey(key) % KEY_LOCK_COUNT]);
    auto &directory = committed[HashKey(key) % KEY_LOCK_COUNT];
    const auto entry = directory.find(key);
    if (entry == directory.end()) return UBSIO_KVC_NOT_FOUND;
    const auto manifest = entry->second;
    directory.erase(entry); // Invalidate even when the physical delete times out.
    const auto result = Remove(ManifestKey(manifest));
    RemoveChunks(manifest);
    return result == UBSIO_KVC_NOT_FOUND ? UBSIO_KVC_OK : result;
}

int32_t Length(const std::string &key, uint32_t &length)
{
    std::lock_guard<std::mutex> guard(keyLocks[HashKey(key) % KEY_LOCK_COUNT]);
    Manifest manifest{};
    int32_t result = LoadManifest(key, manifest);
    if (result != UBSIO_KVC_OK) return result;
    result = CheckChunks(manifest);
    if (result != UBSIO_KVC_OK) return result;
    Manifest current{};
    result = LoadManifest(key, current);
    if (result != UBSIO_KVC_OK) return result;
    if (current.generation != manifest.generation) return UBSIO_KVC_UNAVAILABLE;
    length = static_cast<uint32_t>(manifest.length);
    return UBSIO_KVC_OK;
}

int32_t Exist(const std::string &key)
{
    uint32_t length = 0;
    return Length(key, length);
}

void ResetDirectory()
{
    for (size_t index = 0; index < KEY_LOCK_COUNT; ++index) {
        std::lock_guard<std::mutex> guard(keyLocks[index]);
        committed[index].clear();
    }
}

} // namespace remote_dram
} // namespace ubsio
} // namespace ock
