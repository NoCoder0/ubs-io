/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * ubs-io is licensed under the Mulan PSL v2.
 */
#ifndef REMOTE_READ_LIMITS_H
#define REMOTE_READ_LIMITS_H
#include <condition_variable>
#include <cstddef>
#include <mutex>

namespace ock {
namespace ubsio {
namespace remote_dram {
// One DlBioSdkApi singleton and SDK data-message pool per process. All remote
// BatchGet callers share this budget; a slot costs one actual SDK data-message block.
// Every caller uses the same immutable limit loaded at process startup.
struct ReadBlockBudget {
    std::mutex mutex;
    std::condition_variable available;
    size_t used = 0;
};
inline ReadBlockBudget &SdkReadBlockBudget()
{
    static ReadBlockBudget budget;
    return budget;
}
class ReadWindowCredits {
public:
    ReadWindowCredits(size_t blocks, size_t blockLimit) : blocks_(blocks), valid_(blocks > 0 && blocks <= blockLimit)
    {
        if (!valid_) return;
        auto &budget = SdkReadBlockBudget();
        std::unique_lock<std::mutex> guard(budget.mutex);
        budget.available.wait(guard, [&budget, blocks, blockLimit] { return budget.used <= blockLimit - blocks; });
        budget.used += blocks;
    }
    ~ReadWindowCredits()
    {
        if (!valid_) return;
        auto &budget = SdkReadBlockBudget();
        std::lock_guard<std::mutex> guard(budget.mutex);
        budget.used -= blocks_;
        budget.available.notify_all();
    }
    bool Valid() const { return valid_; }
    ReadWindowCredits(const ReadWindowCredits &) = delete;
    ReadWindowCredits &operator=(const ReadWindowCredits &) = delete;
private:
    size_t blocks_;
    bool valid_;
};
} // namespace remote_dram
} // namespace ubsio
} // namespace ock
#endif // REMOTE_READ_LIMITS_H
