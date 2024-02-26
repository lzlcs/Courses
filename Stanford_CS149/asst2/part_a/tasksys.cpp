#include "tasksys.h"

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char *TaskSystemSerial::name()
{
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads) : ITaskSystem(num_threads)
{
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable *runnable, int num_total_tasks)
{
    for (int i = 0; i < num_total_tasks; i++)
    {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                          const std::vector<TaskID> &deps)
{
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync()
{
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelSpawn::name()
{
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads) : ITaskSystem(num_threads)
{
    this->num_threads = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable *runnable, int num_total_tasks)
{
    std::shared_ptr<std::mutex> mu = std::make_shared<std::mutex>();
    int count = 0;

    auto func = [&count, mu, num_total_tasks, runnable]()
    {
        while (true)
        {
            mu->lock();
            int tmp = count++;
            mu->unlock();

            if (tmp >= num_total_tasks)
                break;
            runnable->runTask(tmp, num_total_tasks);
        }
    };

    std::vector<std::thread> threadId(num_threads);
    for (int i = 1; i < num_threads; ++i)
        threadId[i] = std::thread(func);

    func();

    for (int i = 1; i < num_threads; ++i)
        threadId[i].join();
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                 const std::vector<TaskID> &deps)
{
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync()
{
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char *TaskSystemParallelThreadPoolSpinning::name()
{
    return "Parallel + Thread Pool + Spin";
}

#include <iostream>
using std::cout;
using std::endl;
TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads) : ITaskSystem(num_threads)
{
    auto func = [&]()
    {
        while (!killed)
        {
            mu->lock();
            int cur_toBeDone = num_toBeDone;
            int cur_total_tasks = num_total_tasks;
            if (cur_toBeDone < cur_total_tasks) 
                num_toBeDone++;                
            // cout << cur_toBeDone << ' ' << cur_total_tasks << endl;
            mu->unlock();

            if (cur_toBeDone >= cur_total_tasks) continue;

            runnable->runTask(cur_toBeDone, cur_total_tasks);

            mu_finished->lock();
            int cur_finished = ++num_finished;
            mu_finished->unlock();

            if (cur_finished == cur_total_tasks) se->notify_one();
        }
    };

    mu = std::make_shared<std::mutex>();
    mu_finished = std::make_shared<std::mutex>();
    se = std::make_shared<std::condition_variable>();

    killed = false;
    this->num_total_tasks = -1;
    this->num_finished = -1;
    this->num_toBeDone = -1;
    this->num_threads = num_threads;

    threads = std::vector<threadPtr>(num_threads);
    for (int i = 0; i < num_threads; i++)
        threads[i] = std::make_shared<std::thread>(func);
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning()
{
    killed = true;
    for (int i = 0; i < num_threads; i++)
        threads[i]->join();
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable *runnable, int num_total_tasks)
{
    std::unique_lock<std::mutex> lk(*mu_finished);

    mu->lock();
    this->runnable = runnable;
    num_toBeDone = 0;
    this->num_total_tasks = num_total_tasks;
    mu->unlock();
    
    num_finished = 0;
    while (num_finished != num_total_tasks) se->wait(lk);
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync()
{
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */



const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //

    auto func = [&]()
    {
        while (!killed)
        {

            std::unique_lock<std::mutex> lk(*mu);
            S->wait(lk, [this] { return killed || num_toBeDone < num_total_tasks; });
            if (killed) break;
            int cur_toBeDone = num_toBeDone++;
            int cur_total = num_total_tasks;
            lk.unlock();

            runnable->runTask(cur_toBeDone, cur_total);

            mu->lock();
            int cur_finished = ++num_finished;
            mu->unlock();

            if (cur_finished == cur_total) T->notify_one();
        }
    };

    killed = false;
    this->num_threads = num_threads;
    mu = std::make_shared<std::mutex>();
    mu_finished = std::make_shared<std::mutex>();
    S = std::make_shared<std::condition_variable>();
    T = std::make_shared<std::condition_variable>();

    num_finished = -1;
    num_toBeDone = -1;
    num_total_tasks = -1;

    threads = std::vector<threadPtr>(num_threads);
    for (int i = 0; i < num_threads; ++i)
        threads[i] = std::make_shared<std::thread>(func);
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //  
    killed = true;
    S->notify_all();

    for (int i = 0; i < num_threads; ++i)
        threads[i]->join();
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {

    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    std::unique_lock<std::mutex> lk(*mu);

    // mu->lock();
    this->runnable = runnable;
    this->num_total_tasks = num_total_tasks;
    num_toBeDone = 0;
    num_finished = 0;
    // mu->unlock();

    S->notify_all();
    T->wait(lk, [&]{ return num_finished == num_total_tasks; });
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable *runnable, int num_total_tasks,
                                                              const std::vector<TaskID> &deps)
{

    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync()
{

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
