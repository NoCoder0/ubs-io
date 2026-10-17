#include "remote_read_limits.h"
#include <atomic>
#include <cassert>
#include <chrono>
#include <future>
#include <iostream>
#include <thread>
#include <vector>
using namespace ock::ubsio::remote_dram;
using namespace std::chrono_literals;
void CheckReadBudget(size_t blockLimit, size_t blocksPerWindow)
{
    const size_t holderCount = blockLimit / blocksPerWindow;
    std::mutex mutex;
    std::condition_variable changed;
    size_t holders = 0;
    bool release = false;
    std::vector<std::thread> threads;
    for (size_t index = 0; index < holderCount; ++index) {
        threads.emplace_back([&] {
            ReadWindowCredits credits(blocksPerWindow, blockLimit);
            assert(credits.Valid());
            std::unique_lock<std::mutex> guard(mutex);
            ++holders;
            changed.notify_all();
            changed.wait(guard, [&] { return release; });
        });
    }
    {
        std::unique_lock<std::mutex> guard(mutex);
        assert(changed.wait_for(guard, 3s, [&] { return holders == holderCount; }));
    }
    std::promise<void> entered, acquired;
    auto entry = entered.get_future();
    auto done = acquired.get_future();
    std::thread waiter([&] {
        entered.set_value();
        ReadWindowCredits credits(blocksPerWindow, blockLimit);
        assert(credits.Valid());
        acquired.set_value();
    });
    entry.wait();
    assert(done.wait_for(100ms) == std::future_status::timeout);
    {
        std::lock_guard<std::mutex> guard(mutex);
        release = true;
        changed.notify_all();
    }
    for (auto &thread : threads) thread.join();
    assert(done.wait_for(3s) == std::future_status::ready);
    waiter.join();
    { ReadWindowCredits invalid(blockLimit + 1, blockLimit); assert(!invalid.Valid()); }
    { ReadWindowCredits zero(0, blockLimit); assert(!zero.Valid()); }
    try { ReadWindowCredits credits(blockLimit, blockLimit); throw 7; } catch (int) {}
    { ReadWindowCredits afterException(blockLimit, blockLimit); assert(afterException.Valid()); }
}

int main()
{
    CheckReadBudget(64, 16);
    // Eight objects of the current workload each need twelve 1 MiB chunks.
    CheckReadBudget(96, 12);
    CheckReadBudget(128, 16);
    std::cout << "runtime budgets 64/96/128, bounded concurrency, wait and release PASS\n";
    return 0;
}
