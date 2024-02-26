#include <stdio.h>

#include <cuda.h>
#include <cuda_runtime.h>

#include <driver_functions.h>

#include <thrust/scan.h>
#include <thrust/device_ptr.h>
#include <thrust/device_malloc.h>
#include <thrust/device_free.h>

#include "CycleTimer.h"

#define THREADS_PER_BLOCK 256

// helper function to round an integer up to the next power of 2
static inline int nextPow2(int n)
{
    n--;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    n++;
    return n;
}

// exclusive_scan --
//
// Implementation of an exclusive scan on global memory array `input`,
// with results placed in global memory `result`.
//
// N is the logical size of the input and output arrays, however
// students can assume that both the start and result arrays we
// allocated with next power-of-two sizes as described by the comments
// in cudaScan().  This is helpful, since your parallel scan
// will likely write to memory locations beyond N, but of course not
// greater than N rounded up to the next power of 2.
//
// Also, as per the comments in cudaScan(), you can implement an
// "in-place" scan, since the timing harness makes a copy of input and
// places it in result

__global__ void upsweep(int *output, int two_d)
{
    int two_dplus1 = 2 * two_d;
    int i = (blockIdx.x * blockDim.x + threadIdx.x) * two_dplus1;
    output[i + two_dplus1 - 1] += output[i + two_d - 1];
}

__global__ void downsweep(int *output, int two_d)
{
    int two_dplus1 = 2 * two_d;
    int i = (blockIdx.x * blockDim.x + threadIdx.x) * two_dplus1;
    int t = output[i + two_d - 1];
    output[i + two_d - 1] = output[i + two_dplus1 - 1];
    output[i + two_dplus1 - 1] += t;
}

using std::cout;
using std::endl;

void exclusive_scan(int *input, int N, int *result)
{
    const int blockSize = 256;

    cudaMemcpy(result, input, N * sizeof(int), cudaMemcpyDeviceToDevice);

    N = nextPow2(N);

    auto getSize = [&](int two_d) -> std::pair<int, int>
    {
        int two_dplus1 = 2 * two_d;
        // 计算总共需要多少个线程
        int threads_count = N / two_dplus1;

        int blocks_count = 1;
        // 如果超过了一个线程块的大小, 就拆出来
        if (threads_count > blockSize)
        {
            blocks_count = threads_count / blockSize;
            threads_count = blockSize;
        }

        return {blocks_count, threads_count};
    };
    
    for (int two_d = 1; two_d <= N / 2; two_d *= 2)
    {
        auto [blocks_count, threads_count] = getSize(two_d);
        upsweep<<<blocks_count, threads_count>>>(result, two_d);
    }

    int *tmp = (int *)malloc(sizeof(int));
    *tmp = 0;
    cudaMemcpy(result + N - 1, tmp, sizeof(int), cudaMemcpyHostToDevice);

    for (int two_d = N / 2; two_d >= 1; two_d /= 2)
    {
        auto [blocks_count, threads_count] = getSize(two_d);
        downsweep<<<blocks_count, threads_count>>>(result, two_d);
    }
}

//
// cudaScan --
//
// This function is a timing wrapper around the student's
// implementation of scan - it copies the input to the GPU
// and times the invocation of the exclusive_scan() function
// above. Students should not modify it.
double cudaScan(int *inarray, int *end, int *resultarray)
{
    int *device_result;
    int *device_input;
    int N = end - inarray;

    // This code rounds the arrays provided to exclusive_scan up
    // to a power of 2, but elements after the end of the original
    // input are left uninitialized and not checked for correctness.
    //
    // Student implementations of exclusive_scan may assume an array's
    // allocated length is a power of 2 for simplicity. This will
    // result in extra work on non-power-of-2 inputs, but it's worth
    // the simplicity of a power of two only solution.

    int rounded_length = nextPow2(end - inarray);

    cudaMalloc((void **)&device_result, sizeof(int) * rounded_length);
    cudaMalloc((void **)&device_input, sizeof(int) * rounded_length);

    // For convenience, both the input and output vectors on the
    // device are initialized to the input values. This means that
    // students are free to implement an in-place scan on the result
    // vector if desired.  If you do this, you will need to keep this
    // in mind when calling exclusive_scan from find_repeats.
    cudaMemcpy(device_input, inarray, (end - inarray) * sizeof(int), cudaMemcpyHostToDevice);
    cudaMemcpy(device_result, inarray, (end - inarray) * sizeof(int), cudaMemcpyHostToDevice);

    double startTime = CycleTimer::currentSeconds();

    exclusive_scan(device_input, N, device_result);

    // Wait for completion
    cudaDeviceSynchronize();
    double endTime = CycleTimer::currentSeconds();

    cudaMemcpy(resultarray, device_result, (end - inarray) * sizeof(int), cudaMemcpyDeviceToHost);

    double overallDuration = endTime - startTime;
    return overallDuration;
}

// cudaScanThrust --
//
// Wrapper around the Thrust library's exclusive scan function
// As above in cudaScan(), this function copies the input to the GPU
// and times only the execution of the scan itself.
//
// Students are not expected to produce implementations that achieve
// performance that is competition to the Thrust version, but it is fun to try.
double cudaScanThrust(int *inarray, int *end, int *resultarray)
{

    int length = end - inarray;
    thrust::device_ptr<int> d_input = thrust::device_malloc<int>(length);
    thrust::device_ptr<int> d_output = thrust::device_malloc<int>(length);

    cudaMemcpy(d_input.get(), inarray, length * sizeof(int), cudaMemcpyHostToDevice);

    double startTime = CycleTimer::currentSeconds();

    thrust::exclusive_scan(d_input, d_input + length, d_output);

    cudaDeviceSynchronize();
    double endTime = CycleTimer::currentSeconds();

    cudaMemcpy(resultarray, d_output.get(), length * sizeof(int), cudaMemcpyDeviceToHost);

    thrust::device_free(d_input);
    thrust::device_free(d_output);

    double overallDuration = endTime - startTime;
    return overallDuration;
}

// find_repeats --
//
// Given an array of integers `device_input`, returns an array of all
// indices `i` for which `device_input[i] == device_input[i+1]`.
//
// Returns the total number of pairs found

__global__ void compare(int *input, int length, int *output)
{
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= length - 1)
        return;
    output[idx] = (input[idx] == input[idx + 1]);
}

__global__ void setIdx(int *input, int length, int *output)
{
    
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= length - 1)
        return;

    if (input[idx] + 1 == input[idx + 1])
        output[input[idx]] = idx;
}


int find_repeats(int *device_input, int length, int *device_output)
{

    auto getSize = [&]() -> std::pair<int, int>
    {
        const int block_size = 512;

        if (length < block_size)
            return {1, length};
        return {(length + block_size - 1) / block_size, block_size};
    };

    auto [blocks_count, threads_count] = getSize();

    compare<<<blocks_count, threads_count>>>(device_input, length, device_output);
    exclusive_scan(device_output, length, device_input);
    setIdx<<<blocks_count, threads_count>>>(device_input, length, device_output);


    int *res = (int *)malloc(sizeof(int));
    cudaMemcpy(res, device_input + length - 1, sizeof(int), cudaMemcpyDeviceToHost);

    return *res;
}

//
// cudaFindRepeats --
//
// Timing wrapper around find_repeats. You should not modify this function.
double cudaFindRepeats(int *input, int length, int *output, int *output_length)
{

    int *device_input;
    int *device_output;
    int rounded_length = nextPow2(length);

    cudaMalloc((void **)&device_input, rounded_length * sizeof(int));
    cudaMalloc((void **)&device_output, rounded_length * sizeof(int));
    cudaMemcpy(device_input, input, length * sizeof(int), cudaMemcpyHostToDevice);

    cudaDeviceSynchronize();
    double startTime = CycleTimer::currentSeconds();

    int result = find_repeats(device_input, length, device_output);

    cudaDeviceSynchronize();
    double endTime = CycleTimer::currentSeconds();

    // set output count and results array
    *output_length = result;
    cudaMemcpy(output, device_output, length * sizeof(int), cudaMemcpyDeviceToHost);

    cudaFree(device_input);
    cudaFree(device_output);

    float duration = endTime - startTime;
    return duration;
}

void printCudaInfo()
{
    int deviceCount = 0;
    cudaError_t err = cudaGetDeviceCount(&deviceCount);

    printf("---------------------------------------------------------\n");
    printf("Found %d CUDA devices\n", deviceCount);

    for (int i = 0; i < deviceCount; i++)
    {
        cudaDeviceProp deviceProps;
        cudaGetDeviceProperties(&deviceProps, i);
        printf("Device %d: %s\n", i, deviceProps.name);
        printf("   SMs:        %d\n", deviceProps.multiProcessorCount);
        printf("   Global mem: %.0f MB\n",
               static_cast<float>(deviceProps.totalGlobalMem) / (1024 * 1024));
        printf("   CUDA Cap:   %d.%d\n", deviceProps.major, deviceProps.minor);
    }
    printf("---------------------------------------------------------\n");
}
