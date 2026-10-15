#include "remote_read_limits.h"
#include <atomic>
#include <cassert>
#include <chrono>
#include <future>
#include <thread>
#include <vector>
using namespace ock::ubsio::remote_dram;
using namespace std::chrono_literals;
int main()
{
    std::mutex mutex;
    std::condition_variable changed;
    size_t holders = 0;
    bool release = false;
    std::vector<std::thread> threads;
    for (size_t index = 0; index < 4; ++index) {
        threads.emplace_back([&] {
            ReadWindowCredits credits(16);
            assert(credits.Valid());
            std::unique_lock<std::mutex> guard(mutex);
            ++holders;
            changed.notify_all();
            changed.wait(guard, [&] { return release; });
        });
    }
    {
        std::unique_lock<std::mutex> guard(mutex);
        assert(changed.wait_for(guard, 3s, [&] { return holders == 4; }));
    }
    std::promise<void> entered, acquired;
    auto entry = entered.get_future();
    auto done = acquired.get_future();
    std::thread fifth([&] {
        entered.set_value();
        ReadWindowCredits credits(16);
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
    fifth.join();
    { ReadWindowCredits invalid(65); assert(!invalid.Valid()); }
    try { ReadWindowCredits credits(64); throw 7; } catch (int) {}
    { ReadWindowCredits afterException(64); assert(afterException.Valid()); }
    return 0;
}
