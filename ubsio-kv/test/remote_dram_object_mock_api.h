#ifndef REMOTE_DRAM_OBJECT_MOCK_API_H
#define REMOTE_DRAM_OBJECT_MOCK_API_H

#include <cstdint>
#include <iostream>
#include <string>

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

namespace ock {
namespace ubsio {
class DlBioSdkApi {
public:
    static uint64_t TenantId() { return 17; }
    static CResult CalcLocation(uint64_t, uint64_t, ObjLocation *) { return RET_CACHE_OK; }
    static CResult Stat(uint64_t, const char *, ObjLocation, ObjStat *);
    static CResult Get(uint64_t, const char *, uint64_t, uint64_t, ObjLocation, char *, uint64_t *);
    static CResult Put(uint64_t, const char *, const char *, uint64_t, ObjLocation);
    static CResult Delete(uint64_t, const char *, ObjLocation);
};
} // namespace ubsio
} // namespace ock
#endif
