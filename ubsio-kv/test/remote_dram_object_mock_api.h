#ifndef REMOTE_DRAM_OBJECT_MOCK_API_H
#define REMOTE_DRAM_OBJECT_MOCK_API_H

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>
#include "ubsio_kvc.h"

enum CResult {
    RET_CACHE_OK,
    RET_CACHE_EPERM,
    RET_CACHE_BUSY,
    RET_CACHE_NEED_RETRY,
    RET_CACHE_NOT_READY,
    RET_CACHE_NOT_FOUND,
    RET_CACHE_MISS,
    RET_CACHE_NO_SPACE,
    RET_CACHE_UNAVAILABLE,
    RET_CACHE_EXCEED_QUOTA,
    RET_CACHE_PT_FAULT
};
struct ObjLocation {};
struct ObjStat { uint32_t size = 0; };
#define LOG_ERROR(message) do { std::cerr << message << '\n'; } while (0)
namespace bio {
enum {
    BIO_OK = 0,
    BIO_NOT_EXISTS = 1,
    BIO_CRC_ERR = 2,
    BIO_INVALID_PARAM = 3,
    BIO_NOT_READY = 4,
    BIO_ALLOC_FAIL = 5,
    BIO_CHECK_PT_FAIL = 6,
    BIO_INNER_RETRY = 7,
    BIO_NET_RETRY = 8,
    BIO_QUOTA_NOT_ENOUGH = 9,
    BIO_QUOTA_TIMEOUT = 10
};
}
extern uint32_t mockBatchGetCalls;
extern uint32_t mockBatchGetFreeCalls;
extern bool mockBatchGetShortRead;
extern bool mockBatchGetGlobalFailure;
inline std::vector<std::string> mockEvictKeys;
inline CResult mockEvictResult = RET_CACHE_OK;
inline bool mockRemoteMode = true;

namespace ock {
namespace ubsio {
class DlBioSdkApi {
public:
    static uint64_t TenantId() { return 17; }
    static bool RemoteMode() { return mockRemoteMode; }
    static CResult PollEvictEvents(UbsioMetaEventCallbackC callback, void *context, uint32_t limit)
    {
        std::vector<UbsioMetaEventC> events;
        for (const auto &key : mockEvictKeys) {
            events.push_back({UBSIO_META_DELETE_C, key.data(), static_cast<uint32_t>(key.size())});
        }
        if (limit != 0 && !events.empty()) callback(context, events.data(), static_cast<uint32_t>(events.size()));
        return mockEvictResult;
    }
    static uint32_t ReadBlockLimit() { return 64; }
    static bool ManifestOneGet() { return mockManifestOneGet; }
    static bool mockManifestOneGet;
    static CResult CalcLocation(uint64_t, uint64_t, ObjLocation *) { return RET_CACHE_OK; }
    static CResult Stat(uint64_t, const char *, ObjLocation, ObjStat *);
    static CResult Get(uint64_t, const char *, uint64_t, uint64_t, ObjLocation, char *, uint64_t *);
    static CResult BatchGet(uint64_t, const char **, const uint32_t, uint64_t *, uint64_t *,
                            ObjLocation *, uintptr_t *, uint64_t *, int32_t *);
    static CResult BatchGetFree(uint64_t, uintptr_t *, const uint32_t);
    static CResult Put(uint64_t, const char *, const char *, uint64_t, ObjLocation);
    static CResult Delete(uint64_t, const char *, ObjLocation);
};
} // namespace ubsio
} // namespace ock
#endif
