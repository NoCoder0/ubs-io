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

#ifndef UBSIO_KVC_EXECUTION_H
#define UBSIO_KVC_EXECUTION_H

#include <unistd.h>
#include <atomic>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>
#include "ubsio_kvc_log.h"
#include "ubsio_kvc_ref.h"
#include "ubsio_kvc_ring_buffer.h"

namespace ock {
namespace ubsio {

enum RunnableType {
    NORMAL = 0,
    STOP = 1,
};

/*
 * @brief Base class of runnable task
 */
class Runnable {
public:
    Runnable() : mTask{ nullptr } {}

    explicit Runnable(const std::function<void()> &task) : mTask{ task } {}
    virtual ~Runnable() = default;

    virtual void Run()
    {
        if (mTask != nullptr) {
            mTask();
        }
    }

    DEFINE_REF_COUNT_FUNCTIONS
private:
    inline void Type(RunnableType type)
    {
        mType = type;
    }

    inline RunnableType Type() const
    {
        return mType;
    }

private:
    RunnableType mType = RunnableType::NORMAL;
    std::function<void()> mTask{ nullptr };

    DEFINE_REF_COUNT_VARIABLE

    friend class ExecutorService;
};
using RunnablePtr = Ref<Runnable>;

constexpr uint32_t ES_MAX_THR_NUM = 256;

class ExecutorService;
using ExecutorServicePtr = Ref<ExecutorService>;

/*
 * @brief Execution service is fixed thread pool to task execution
 */
class ExecutorService {
public:
    /*
     * @brief Create an execution service with fixed number of threads
     *
     * @param threadNum          [in] number of threads
     * @param queueCapacity      [in] capacity of inner queue to store tasks
     *
     * @return executor ptr if successfully, otherwise return null
     */
    static ExecutorServicePtr Create(uint16_t threadNum, uint32_t queueCapacity = 10000)
    {
        if (threadNum > ES_MAX_THR_NUM || threadNum == 0) {
            LOG_ERROR("The num of thread must 1-" << ES_MAX_THR_NUM);
            return nullptr;
        }

        return new (std::nothrow) ExecutorService(threadNum, queueCapacity);
    }

public:
    ~ExecutorService()
    {
        if (!mStopped) {
            Stop();
        }

        while (!mThreads.empty()) {
            delete (mThreads.back());
            mThreads.pop_back();
        }
    }

    /*
     * @brief Start the execution service, wait for all threads started
     *
     * @return true if successfully
     */
    bool Start();

    /*
     * @brief Stop the execution service, wait for all threads exited
     */
    void Stop();

    /*
     * @brief Enqueue a task to thread pool, need to ensure this has been started
     *
     * The ref count of runnable will be increased and will be decreased after executed
     *
     * @return true if enqueue successfully, otherwise the queue is full
     */
    inline bool Execute(const RunnablePtr &runnable)
    {
        auto tmp = runnable.Get();
        if (UNLIKELY(tmp == nullptr)) {
            return false;
        }

        tmp->IncreaseRef();
        if (mRunnableQueue.Enqueue(tmp)) {
            return true;
        }
        tmp->DecreaseRef();
        return false;
    }

    /*
     * @brief Enqueue a task to thread pool, need to ensure this has been started
     *
     * @param task a lambda expression or function no parameter no return value
     *
     * @return true if enqueue successfully, otherwise the queue is full
     */
    inline bool Execute(const std::function<void()> &task)
    {
        return Execute(MakeRef<Runnable>(task));
    }

    /*
     * @brief Set the thread name prefix
     *
     * @param name             [in] prefix name of execute service working thread
     */
    inline void SetThreadName(const std::string &name)
    {
        mThreadName = name;
    }

    /*
     * @brief Bind the cpu for working threads
     *
     * @param idx              [in] starting index cpu id to bind to working threads
     */
    inline void SetCpuSetStartIndex(int16_t idx)
    {
        mCpuSetStartIdx = idx;
    }

    DEFINE_REF_COUNT_FUNCTIONS

private:
    ExecutorService(uint16_t threadNum, uint32_t queueCapacity)
        : mRunnableQueue(queueCapacity),
          mThreadNum(threadNum),
          mThreads(0),
          mStarted(false),
          mStopped(false),
          mStartedThreadNum(0)
    {}

    void RunInThread(int16_t cpuId);
    void DoRunnable(bool &flag);

private:
    RingBufferBlockingQueue<Runnable *> mRunnableQueue;
    uint16_t mThreadNum = 0;
    int16_t mCpuSetStartIdx = -1;
    std::vector<std::thread *> mThreads;

    std::atomic<bool> mStarted;
    std::atomic<bool> mStopped;
    std::atomic<uint16_t> mStartedThreadNum;

    std::string mThreadName;

    DEFINE_REF_COUNT_VARIABLE
};

inline bool ExecutorService::Start()
{
    if (mStarted) {
        return true;
    }

    /* init ring buffer blocking queue */
    int result = 0;
    result = mRunnableQueue.Initialize();
    if (result != 0) {
        LOG_ERROR("Failed to initialize queue, result " << result);
        return false;
    }

    for (uint16_t i = 0; i < mThreadNum; i++) {
        auto cpuId = mCpuSetStartIdx < 0 ? -1 : mCpuSetStartIdx + i;
        std::thread *thr = nullptr;
        thr = new (std::nothrow) std::thread(&ExecutorService::RunInThread, this, cpuId);
        if (thr == nullptr) {
            LOG_ERROR("Failed to create executor thread " << i);
            return false;
        }

        mThreads.push_back(thr);
    }

    while (mStartedThreadNum < mThreadNum) {
        usleep(1);
    }

    mStarted = true;
    return true;
}

inline void ExecutorService::Stop()
{
    if (!mStarted || mStopped) {
        return;
    }

    for (uint32_t i = 0; i < mThreads.size(); ++i) {
        RunnablePtr stopTask = new (std::nothrow) Runnable();
        if (stopTask == nullptr) {
            LOG_ERROR("Failed to new stop task, probably out of memory");
            break;
        }
        stopTask->Type(RunnableType::STOP);

        Runnable *tmp = stopTask.Get();
        tmp->IncreaseRef();
        if (!mRunnableQueue.EnqueueFirst(tmp)) {
            continue;
        }
    }

    for (auto &thr : mThreads) {
        if (thr != nullptr) {
            thr->join();
        }
    }

    mStopped = true;
    mRunnableQueue.UnInitialize();
}

inline void ExecutorService::DoRunnable(bool &flag)
{
    try {
        Runnable *task = nullptr;
        mRunnableQueue.Dequeue(task);
        if (task != nullptr) {
            RunnablePtr runnable = task;
            task->DecreaseRef();
            if (runnable->Type() == RunnableType::NORMAL) {
                runnable->Run();
            } else if (runnable->Type() == RunnableType::STOP) {
                flag = false;
            } else {
                LOG_ERROR("Un-reachable path");
            }
        } else {
            LOG_ERROR("Task is null");
        }
    } catch (std::runtime_error &ex) {
        LOG_ERROR("Caught error " << ex.what() << " when execute a task, continue");
    } catch (...) {
        LOG_ERROR("Caught unknown error when execute a task, continue");
    }
}

inline void ExecutorService::RunInThread(int16_t cpuId)
{
    bool runFlag = true;
    uint16_t threadIndex = mStartedThreadNum++;

    auto threadName = mThreadName.empty() ? "executor" : mThreadName;
    threadName += std::to_string(threadIndex);
    if (cpuId != -1) {
        cpu_set_t cpuSet;
        CPU_ZERO(&cpuSet);
        CPU_SET(cpuId, &cpuSet);
        if (pthread_setaffinity_np(pthread_self(), sizeof(cpuSet), &cpuSet) != 0) {
            LOG_WARN("Failed to bind executor thread" << threadName << " << to cpu " << cpuId);
        }
    }

    pthread_setname_np(pthread_self(), threadName.c_str());

    while (runFlag) {
        DoRunnable(runFlag);
    }
    LOG_INFO("Thread for executor service <" << threadName << "> cpuId " << cpuId << "> exiting");
}

}
}
#endif // UBSIO_KVC_EXECUTION_H
