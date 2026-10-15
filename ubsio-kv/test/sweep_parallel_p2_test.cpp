#include "remote_tuning.h"
#define LOG_INFO(message) do { } while(false)
#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>
#include "ubsio_kvc_err.h"
#define LOG_ERROR(...) do {} while (false)
using namespace std::chrono_literals;
namespace ock { namespace ubsio {
class ExecutorService {
public:
    size_t rejectAt = 0, throwAt = 0;
    std::function<void()> beforeReject;
    std::atomic<size_t> running{0};
    std::atomic<bool> stopCalled{false};
    explicit ExecutorService(size_t count = 4)
    {
        for (size_t index=0; index<count; ++index) threads.emplace_back([this] {
            while (true) {
                std::function<void()> task;
                {
                    std::unique_lock<std::mutex> guard(mutex);
                    available.wait(guard,[this] { return stopped || !queue.empty(); });
                    if (stopped) return; // Deliberately discard queued work, like Stop tasks prepended.
                    task=std::move(queue.front()); queue.pop_front();
                    ++running;
                }
                task();
                --running;
            }
        });
    }
    ~ExecutorService() { Stop(); }
    bool Execute(const std::function<void()> &task)
    {
        const size_t index=++submitted;
        if (index==rejectAt || index==throwAt) {
            if (beforeReject) beforeReject();
            if (index==throwAt) throw std::runtime_error("injected enqueue exception");
            return false;
        }
        std::lock_guard<std::mutex> guard(mutex);
        if (stopped) return false;
        queue.push_back(task); available.notify_one(); return true;
    }
    void Stop()
    {
        if (stopCalled.exchange(true)) return;
        { std::lock_guard<std::mutex> guard(mutex); stopped=true; available.notify_all(); }
        for (auto &thread:threads) thread.join();
    }
private:
    std::atomic<size_t> submitted{0};
    bool stopped=false;
    std::mutex mutex;
    std::condition_variable available;
    std::deque<std::function<void()>> queue;
    std::vector<std::thread> threads;
};
using ExecutorServicePtr=std::shared_ptr<ExecutorService>;
class KvOperation {
public:
    explicit KvOperation(ExecutorServicePtr executor):mKvExecutor(std::move(executor)) {}
    int32_t RunRemoteReads(size_t,std::vector<int>&,const std::function<int32_t(size_t)>&);
    bool Closing() { std::lock_guard<std::mutex> guard(mBatchMutex); return mReadExecutorClosing; }
    size_t Active() { std::lock_guard<std::mutex> guard(mBatchMutex); return mActiveReadBatches; }
inline void ExitKvExecutor(void)
    {
        std::unique_lock<std::mutex> guard(mBatchMutex);
        mReadExecutorClosing = true;
        mReadBatchesDone.wait(guard, [this] { return mActiveReadBatches == 0; });
        if (mKvExecutor != nullptr) {
            mKvExecutor->Stop();
            mKvExecutor = nullptr;
        }
    }
private:
    std::mutex mBatchMutex;
    std::condition_variable mReadBatchesDone;
    size_t mActiveReadBatches{0};
    bool mReadExecutorClosing{false};
    ExecutorServicePtr mKvExecutor;
};
constexpr size_t REMOTE_GET_WORKERS = 2;
int32_t KvOperation::RunRemoteReads(size_t count, std::vector<int> &results,
                                    const std::function<int32_t(size_t)> &task)
{
    if (results.size() < count) return UBSIO_KVC_INVALID_PARAM;
    std::fill_n(results.begin(), count, UBSIO_KVC_UNAVAILABLE);
    if (count == 0) return UBSIO_KVC_OK;
    ExecutorServicePtr executor;
    {
        std::lock_guard<std::mutex> guard(mBatchMutex);
        if (mKvExecutor == nullptr || mReadExecutorClosing) return UBSIO_KVC_UNAVAILABLE;
        executor = mKvExecutor;
        ++mActiveReadBatches;
    }
    // Keep the executor alive and forbid Stop (which prepends stop tasks) until
    // every accepted worker completes. Do not hold the common batch mutex here.
    struct ReadLease {
        KvOperation &operation;
        ~ReadLease()
        {
            std::lock_guard<std::mutex> guard(operation.mBatchMutex);
            --operation.mActiveReadBatches;
            operation.mReadBatchesDone.notify_all();
        }
    } lease{*this};
    std::atomic<size_t> next{0};
    std::atomic<bool> abort{false};
    std::mutex mutex;
    std::condition_variable completed;
    size_t pending = 0;
    bool submitFailed = false;
    RemoteTuning tuning;
    if (!ReadRemoteTuning(tuning, REMOTE_GET_WORKERS)) return UBSIO_KVC_INVALID_PARAM;
    const size_t workers = std::min<size_t>(tuning.workers, count);
    LOG_INFO("Sweep remote object workers:" << workers << " count:" << count);
    for (size_t worker = 0; worker < workers; ++worker) {
        {
            std::lock_guard<std::mutex> guard(mutex);
            ++pending;
        }
        bool accepted = false;
        try {
            accepted = executor->Execute([&]() {
                while (!abort.load()) {
                    const size_t index = next.fetch_add(1);
                    if (index >= count) break;
                    try {
                        results[index] = task(index);
                    } catch (...) {
                        LOG_ERROR("Remote read task threw, index:" << index);
                        results[index] = UBSIO_KVC_ERR;
                    }
                }
                std::lock_guard<std::mutex> guard(mutex);
                --pending;
                // Notify while holding the completion mutex: the waiter must
                // not destroy this condition variable before the final notify.
                completed.notify_all();
            });
        } catch (...) {
            LOG_ERROR("Remote read worker submission threw");
        }
        if (!accepted) {
            abort.store(true);
            std::lock_guard<std::mutex> guard(mutex);
            --pending;
            submitFailed = true;
            break;
        }
    }
    std::unique_lock<std::mutex> guard(mutex);
    completed.wait(guard, [&pending] { return pending == 0; });
    return submitFailed ? UBSIO_KVC_UNAVAILABLE : UBSIO_KVC_OK;
}
} } // namespace ock::ubsio
using namespace ock::ubsio;
int main()
{
    // Force index 1 to complete before index 0; preserve indices and per-item errors.
    {
        auto executor=std::make_shared<ExecutorService>(); KvOperation operation(executor);
        std::promise<void> first,second; auto firstEntered=first.get_future().share(); auto secondDone=second.get_future().share();
        std::vector<int> results(9,-77); std::vector<std::atomic<int>> calls(8);
        for (auto &call:calls) call=0;
        std::atomic<size_t> running{0},peak{0};
        auto task=[&](size_t index)->int32_t {
            ++calls[index]; const size_t active=++running;
            size_t prior=peak.load(); while (prior<active && !peak.compare_exchange_weak(prior,active)) {}
            if (index==0) { first.set_value(); assert(secondDone.wait_for(3s)==std::future_status::ready); }
            if (index==1) { assert(firstEntered.wait_for(3s)==std::future_status::ready); second.set_value(); }
            --running;
            if (index==2) throw std::runtime_error("per-object exception");
            if (index==3) return UBSIO_KVC_CORRUPT;
            return 100+static_cast<int>(index);
        };
        assert(operation.RunRemoteReads(8,results,task)==UBSIO_KVC_OK);
        for (size_t index=0;index<8;++index) {
            assert(calls[index]==1);
            const int expected=index==2 ? UBSIO_KVC_ERR : index==3 ? UBSIO_KVC_CORRUPT : 100+static_cast<int>(index);
            assert(results[index]==expected);
        }
        assert(results[8]==-77 && peak>=2 && peak<=REMOTE_GET_WORKERS && operation.Active()==0);
    }
    // Partial enqueue rejection/throw must wait for the already executing worker.
    for (bool throws:{false,true}) {
        auto executor=std::make_shared<ExecutorService>(); KvOperation operation(executor);
        std::promise<void> entered,release; auto enteredFuture=entered.get_future().share(); auto releaseFuture=release.get_future().share();
        if (throws) executor->throwAt=2; else executor->rejectAt=2;
        executor->beforeReject=[&] { assert(enteredFuture.wait_for(3s)==std::future_status::ready); };
        std::vector<int> results(8,-77); std::vector<std::atomic<size_t>> calls(8);
        for (auto &call:calls) call=0;
        auto request=std::async(std::launch::async,[&] {
            return operation.RunRemoteReads(8,results,[&](size_t index) {
                ++calls[index];
                if (index==0) { entered.set_value(); assert(releaseFuture.wait_for(3s)==std::future_status::ready); }
                return 123+static_cast<int>(index);
            });
        });
        assert(enteredFuture.wait_for(3s)==std::future_status::ready);
        assert(request.wait_for(100ms)==std::future_status::timeout);
        release.set_value(); assert(request.wait_for(3s)==std::future_status::ready);
        assert(request.get()==UBSIO_KVC_UNAVAILABLE && calls[0]==1 && results[0]==123);
        for (size_t index=1;index<results.size();++index) {
            assert(calls[index]<=1);
            assert(results[index]==(calls[index]==1 ? 123+static_cast<int>(index) : UBSIO_KVC_UNAVAILABLE));
        }
        assert(operation.Active()==0);
    }
    // Shutdown cannot prepend stop tasks until all accepted reads have finished.
    {
        auto executor=std::make_shared<ExecutorService>(); KvOperation operation(executor);
        std::promise<void> entered,release; auto enteredFuture=entered.get_future().share(); auto releaseFuture=release.get_future().share();
        std::vector<int> results(8,-77);
        auto request=std::async(std::launch::async,[&] {
            return operation.RunRemoteReads(8,results,[&](size_t index) {
                if (index==0) { entered.set_value(); assert(releaseFuture.wait_for(3s)==std::future_status::ready); }
                return 100+static_cast<int>(index);
            });
        });
        assert(enteredFuture.wait_for(3s)==std::future_status::ready);
        auto stop=std::async(std::launch::async,[&] { operation.ExitKvExecutor(); });
        const auto deadline=std::chrono::steady_clock::now()+3s;
        while (!operation.Closing() && std::chrono::steady_clock::now()<deadline) std::this_thread::yield();
        assert(operation.Closing() && !executor->stopCalled);
        assert(stop.wait_for(100ms)==std::future_status::timeout);
        std::vector<int> other(1,-77);
        assert(operation.RunRemoteReads(1,other,[](size_t) { return 0; })==UBSIO_KVC_UNAVAILABLE);
        assert(other[0]==UBSIO_KVC_UNAVAILABLE);
        release.set_value(); assert(request.wait_for(3s)==std::future_status::ready);
        assert(request.get()==UBSIO_KVC_OK); assert(stop.wait_for(3s)==std::future_status::ready); stop.get();
        for (size_t index=0;index<8;++index) assert(results[index]==100+static_cast<int>(index));
        assert(executor->stopCalled && executor->running==0 && operation.Active()==0);
    }
    {
        auto executor=std::make_shared<ExecutorService>(); KvOperation operation(executor); executor->rejectAt=1;
        std::vector<int> results(2,-77);
        assert(operation.RunRemoteReads(2,results,[](size_t){ return 0; })==UBSIO_KVC_UNAVAILABLE);
        assert(results[0]==UBSIO_KVC_UNAVAILABLE && results[1]==UBSIO_KVC_UNAVAILABLE);
        assert(operation.RunRemoteReads(3,results,[](size_t){ return 0; })==UBSIO_KVC_INVALID_PARAM);
        std::vector<int> empty; assert(operation.RunRemoteReads(0,empty,[](size_t){ return 0; })==UBSIO_KVC_OK);
    }
    std::cout << "parallel indices, bounded workers, exceptions, rejection, wait-all and shutdown PASS\n";
    return 0;
}
