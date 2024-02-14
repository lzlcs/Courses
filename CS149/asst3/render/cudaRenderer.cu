#include <string>
#include <algorithm>
#include <math.h>
#include <stdio.h>
#include <vector>

#include <cuda.h>
#include <cuda_runtime.h>
#include <driver_functions.h>

#include "cudaRenderer.h"
#include "cycleTimer.h"
#include "image.h"
#include "noise.h"
#include "sceneLoader.h"
#include "util.h"

#define SCAN_BLOCK_DIM 1024
#define NUM_CIRCLES_PROC SCAN_BLOCK_DIM
#define TILE_WIDTH 64
#define TILE_HEIGHT 64
static short *cudaDeviceCircleFlag = nullptr;

////////////////////////////////////////////////////////////////////////////////////////
// Putting all the cuda kernels here
///////////////////////////////////////////////////////////////////////////////////////

struct GlobalConstants
{
    SceneName sceneName;

    int numCircles;
    int numCirclesProc; // No. of circles to process at a time.
    int numPixels;      // No. of pixels in the image.
    short numTilesX;
    short numTilesY;

    float *position;
    float *velocity;
    float *color;
    float *radius;

    int imageWidth;
    int imageHeight;
    float invWidth;
    float invHeight;
    short tileWidth;
    short tileHeight;

    float *imageData;
    short *circleFlag; // Flags indicating circles relevant to a tile or a pixel.
};

// Global variable that is in scope, but read-only, for all cuda
// kernels.  The __constant__ modifier designates this variable will
// be stored in special "constant" memory on the GPU. (we didn't talk
// about this type of memory in class, but constant memory is a fast
// place to put read-only variables).
__constant__ GlobalConstants cuConstRendererParams;

// read-only lookup tables used to quickly compute noise (needed by
// advanceAnimation for the snowflake scene)
__constant__ int cuConstNoiseYPermutationTable[256];
__constant__ int cuConstNoiseXPermutationTable[256];
__constant__ float cuConstNoise1DValueTable[256];

// color ramp table needed for the color ramp lookup shader
#define COLOR_MAP_SIZE 5
__constant__ float cuConstColorRamp[COLOR_MAP_SIZE][3];

// including parts of the CUDA code from external files to keep this
// file simpler and to seperate code that should not be modified
#include "circleBoxTest.cu_inl"
#include "exclusiveScan.cu_inl"
#include "noiseCuda.cu_inl"
#include "lookupColor.cu_inl"

// kernelClearImageSnowflake -- (CUDA device code)
//
// Clear the image, setting the image to the white-gray gradation that
// is used in the snowflake image
__global__ void kernelClearImageSnowflake()
{
    int imageX = blockIdx.x * blockDim.x + threadIdx.x;
    int imageY = blockIdx.y * blockDim.y + threadIdx.y;

    int width = cuConstRendererParams.imageWidth;
    int height = cuConstRendererParams.imageHeight;

    if (imageX >= width || imageY >= height)
        return;

    int offset = 4 * (imageY * width + imageX);
    float shade = .4f + .45f * static_cast<float>(height - imageY) / height;
    float4 value = make_float4(shade, shade, shade, 1.f);

    // write to global memory: As an optimization, I use a float4
    // store, that results in more efficient code than if I coded this
    // up as four seperate fp32 stores.
    *(float4 *)(&cuConstRendererParams.imageData[offset]) = value;
}

// kernelClearImage --  (CUDA device code)
//
// Clear the image, setting all pixels to the specified color rgba
__global__ void kernelClearImage(float r, float g, float b, float a)
{
    int imageX = blockIdx.x * blockDim.x + threadIdx.x;
    int imageY = blockIdx.y * blockDim.y + threadIdx.y;

    int width = cuConstRendererParams.imageWidth;
    int height = cuConstRendererParams.imageHeight;

    if (imageX >= width || imageY >= height)
        return;

    int offset = 4 * (imageY * width + imageX);
    float4 value = make_float4(r, g, b, a);

    // write to global memory: As an optimization, I use a float4
    // store, that results in more efficient code than if I coded this
    // up as four seperate fp32 stores.
    *(float4 *)(&cuConstRendererParams.imageData[offset]) = value;
}

// kernelAdvanceFireWorks
//
// Update the position of the fireworks (if circle is firework)
__global__ void kernelAdvanceFireWorks()
{
    const float dt = 1.f / 60.f;
    const float pi = 3.14159;
    const float maxDist = 0.25f;

    float *velocity = cuConstRendererParams.velocity;
    float *position = cuConstRendererParams.position;
    float *radius = cuConstRendererParams.radius;

    int index = blockIdx.x * blockDim.x + threadIdx.x;
    if (index >= cuConstRendererParams.numCircles)
        return;

    if (0 <= index && index < NUM_FIREWORKS)
    { // firework center; no update
        return;
    }

    // determine the fire-work center/spark indices
    int fIdx = (index - NUM_FIREWORKS) / NUM_SPARKS;
    int sfIdx = (index - NUM_FIREWORKS) % NUM_SPARKS;

    int index3i = 3 * fIdx;
    int sIdx = NUM_FIREWORKS + fIdx * NUM_SPARKS + sfIdx;
    int index3j = 3 * sIdx;

    float cx = position[index3i];
    float cy = position[index3i + 1];

    // update position
    position[index3j] += velocity[index3j] * dt;
    position[index3j + 1] += velocity[index3j + 1] * dt;

    // fire-work sparks
    float sx = position[index3j];
    float sy = position[index3j + 1];

    // compute vector from firework-spark
    float cxsx = sx - cx;
    float cysy = sy - cy;

    // compute distance from fire-work
    float dist = sqrt(cxsx * cxsx + cysy * cysy);
    if (dist > maxDist)
    { // restore to starting position
        // random starting position on fire-work's rim
        float angle = (sfIdx * 2 * pi) / NUM_SPARKS;
        float sinA = sin(angle);
        float cosA = cos(angle);
        float x = cosA * radius[fIdx];
        float y = sinA * radius[fIdx];

        position[index3j] = position[index3i] + x;
        position[index3j + 1] = position[index3i + 1] + y;
        position[index3j + 2] = 0.0f;

        // travel scaled unit length
        velocity[index3j] = cosA / 5.0;
        velocity[index3j + 1] = sinA / 5.0;
        velocity[index3j + 2] = 0.0f;
    }
}

// kernelAdvanceHypnosis
//
// Update the radius/color of the circles
__global__ void kernelAdvanceHypnosis()
{
    int index = blockIdx.x * blockDim.x + threadIdx.x;
    if (index >= cuConstRendererParams.numCircles)
        return;

    float *radius = cuConstRendererParams.radius;

    float cutOff = 0.5f;
    // place circle back in center after reaching threshold radisus
    if (radius[index] > cutOff)
    {
        radius[index] = 0.02f;
    }
    else
    {
        radius[index] += 0.01f;
    }
}

// kernelAdvanceBouncingBalls
//
// Update the positino of the balls
__global__ void kernelAdvanceBouncingBalls()
{
    const float dt = 1.f / 60.f;
    const float kGravity = -2.8f; // sorry Newton
    const float kDragCoeff = -0.8f;
    const float epsilon = 0.001f;

    int index = blockIdx.x * blockDim.x + threadIdx.x;

    if (index >= cuConstRendererParams.numCircles)
        return;

    float *velocity = cuConstRendererParams.velocity;
    float *position = cuConstRendererParams.position;

    int index3 = 3 * index;
    // reverse velocity if center position < 0
    float oldVelocity = velocity[index3 + 1];
    float oldPosition = position[index3 + 1];

    if (oldVelocity == 0.f && oldPosition == 0.f)
    { // stop-condition
        return;
    }

    if (position[index3 + 1] < 0 && oldVelocity < 0.f)
    { // bounce ball
        velocity[index3 + 1] *= kDragCoeff;
    }

    // update velocity: v = u + at (only along y-axis)
    velocity[index3 + 1] += kGravity * dt;

    // update positions (only along y-axis)
    position[index3 + 1] += velocity[index3 + 1] * dt;

    if (fabsf(velocity[index3 + 1] - oldVelocity) < epsilon && oldPosition < 0.0f && fabsf(position[index3 + 1] - oldPosition) < epsilon)
    { // stop ball
        velocity[index3 + 1] = 0.f;
        position[index3 + 1] = 0.f;
    }
}

// kernelAdvanceSnowflake -- (CUDA device code)
//
// move the snowflake animation forward one time step.  Updates circle
// positions and velocities.  Note how the position of the snowflake
// is reset if it moves off the left, right, or bottom of the screen.
__global__ void kernelAdvanceSnowflake()
{
    int index = blockIdx.x * blockDim.x + threadIdx.x;

    if (index >= cuConstRendererParams.numCircles)
        return;

    const float dt = 1.f / 60.f;
    const float kGravity = -1.8f; // sorry Newton
    const float kDragCoeff = 2.f;

    int index3 = 3 * index;

    float *positionPtr = &cuConstRendererParams.position[index3];
    float *velocityPtr = &cuConstRendererParams.velocity[index3];

    // loads from global memory
    float3 position = *((float3 *)positionPtr);
    float3 velocity = *((float3 *)velocityPtr);

    // hack to make farther circles move more slowly, giving the
    // illusion of parallax
    float forceScaling = fmin(fmax(1.f - position.z, .1f), 1.f); // clamp

    // add some noise to the motion to make the snow flutter
    float3 noiseInput;
    noiseInput.x = 10.f * position.x;
    noiseInput.y = 10.f * position.y;
    noiseInput.z = 255.f * position.z;
    float2 noiseForce = cudaVec2CellNoise(noiseInput, index);
    noiseForce.x *= 7.5f;
    noiseForce.y *= 5.f;

    // drag
    float2 dragForce;
    dragForce.x = -1.f * kDragCoeff * velocity.x;
    dragForce.y = -1.f * kDragCoeff * velocity.y;

    // update positions
    position.x += velocity.x * dt;
    position.y += velocity.y * dt;

    // update velocities
    velocity.x += forceScaling * (noiseForce.x + dragForce.y) * dt;
    velocity.y += forceScaling * (kGravity + noiseForce.y + dragForce.y) * dt;

    float radius = cuConstRendererParams.radius[index];

    // if the snowflake has moved off the left, right or bottom of
    // the screen, place it back at the top and give it a
    // pseudorandom x position and velocity.
    if ((position.y + radius < 0.f) ||
        (position.x + radius) < -0.f ||
        (position.x - radius) > 1.f)
    {
        noiseInput.x = 255.f * position.x;
        noiseInput.y = 255.f * position.y;
        noiseInput.z = 255.f * position.z;
        noiseForce = cudaVec2CellNoise(noiseInput, index);

        position.x = .5f + .5f * noiseForce.x;
        position.y = 1.35f + radius;

        // restart from 0 vertical velocity.  Choose a
        // pseudo-random horizontal velocity.
        velocity.x = 2.f * noiseForce.y;
        velocity.y = 0.f;
    }

    // store updated positions and velocities to global memory
    *((float3 *)positionPtr) = position;
    *((float3 *)velocityPtr) = velocity;
}

__device__ __inline__ bool
shouldRender(int circleIndex, float2 &pixelCenterNorm, float *pixelDist)
{
    int index3 = 3 * circleIndex;

    // read position and radius
    float3 p = *(float3 *)(&cuConstRendererParams.position[index3]);
    float rad = cuConstRendererParams.radius[circleIndex];
    float maxDist = rad * rad;

    float diffX = p.x - pixelCenterNorm.x;
    float diffY = p.y - pixelCenterNorm.y;
    *pixelDist = diffX * diffX + diffY * diffY;
    return *pixelDist <= maxDist;
}

// Applies the changes induced by a batch of circles for a pixel.
__global__ void applyShadePixel(int offset, int numTrueCircles)
{
    int pixelIndex = blockIdx.x * blockDim.x + threadIdx.x;
    if (pixelIndex >= cuConstRendererParams.numPixels)
    {
        return;
    }

    short imageWidth = cuConstRendererParams.imageWidth;
    short pixelY = pixelIndex / imageWidth;
    short pixelX = pixelIndex % imageWidth;
    float invWidth = cuConstRendererParams.invWidth;
    float invHeight = cuConstRendererParams.invHeight;
    float2 pixelCenterNorm = make_float2(invWidth * (static_cast<float>(pixelX) + 0.5f),
                                         invHeight * (static_cast<float>(pixelY) + 0.5f));

    short tileWidth = cuConstRendererParams.tileWidth;
    short tileHeight = cuConstRendererParams.tileHeight;
    short numTilesX = cuConstRendererParams.numTilesX;
    short tileIndex = (pixelY / tileHeight) * numTilesX + (pixelX / tileWidth);

    // BEGIN SHOULD-BE-ATOMIC REGION
    float4 *imagePtr = (float4 *)(&cuConstRendererParams.imageData[4 * pixelIndex]);
    float4 newColor = *imagePtr;
    const int numCirclesProc = cuConstRendererParams.numCirclesProc;
    if (cuConstRendererParams.sceneName == SNOWFLAKES ||
        cuConstRendererParams.sceneName == SNOWFLAKES_SINGLE_FRAME)
    {
        const float kCircleMaxAlpha = .5f;
        const float falloffScale = 4.f;

        for (int idx = 0; idx < numTrueCircles; ++idx)
        {
            const int flagIndex = numCirclesProc * tileIndex + idx;
            const short flagValue = cuConstRendererParams.circleFlag[flagIndex];
            if (flagValue < 0)
            {
                break;
            }

            const int circleIndex = offset + flagValue;
            float pixelDist;
            if (!shouldRender(circleIndex, pixelCenterNorm, &pixelDist))
            {
                continue;
            }

            // Compute the RGBA of the circle for this pixel.
            float3 rgb;
            float alpha;

            int index3 = 3 * circleIndex;
            float p_z = *(float *)(&cuConstRendererParams.position[index3 + 2]);
            float rad = cuConstRendererParams.radius[circleIndex];
            float normPixelDist = sqrt(pixelDist) / rad;
            rgb = lookupColor(normPixelDist);

            float maxAlpha = .6f + .4f * (1.f - p_z);
            maxAlpha = kCircleMaxAlpha * fmaxf(fminf(maxAlpha, 1.f), 0.f); // kCircleMaxAlpha * clamped value
            alpha = maxAlpha * exp(-1.f * falloffScale * normPixelDist * normPixelDist);

            float oneMinusAlpha = 1.f - alpha;

            newColor.x = alpha * rgb.x + oneMinusAlpha * newColor.x;
            newColor.y = alpha * rgb.y + oneMinusAlpha * newColor.y;
            newColor.z = alpha * rgb.z + oneMinusAlpha * newColor.z;
            newColor.w = alpha + newColor.w;
        }
    }
    else
    {
        float alpha = .5f;
        float oneMinusAlpha = 1.f - alpha;
        for (int idx = 0; idx < numTrueCircles; ++idx)
        {
            const int flagIndex = numCirclesProc * tileIndex + idx;
            const short flagValue = cuConstRendererParams.circleFlag[flagIndex];
            if (flagValue < 0)
            {
                break;
            }

            const int circleIndex = offset + flagValue;
            float pixelDist;
            if (!shouldRender(circleIndex, pixelCenterNorm, &pixelDist))
            {
                continue;
            }

            // Compute the RGBA of the circle for this pixel.
            int index3 = 3 * circleIndex;
            float3 rgb = *(float3 *)(&cuConstRendererParams.color[index3]);

            newColor.x = alpha * rgb.x + oneMinusAlpha * newColor.x;
            newColor.y = alpha * rgb.y + oneMinusAlpha * newColor.y;
            newColor.z = alpha * rgb.z + oneMinusAlpha * newColor.z;
            newColor.w = alpha + newColor.w;
        }
    }
    *imagePtr = newColor;
    // END SHOULD-BE-ATOMIC REGION
}

__device__ __inline__ short isCircleInBox(int circleIndex, short tileX, short tileY)
{
    if (circleIndex >= cuConstRendererParams.numCircles)
    {
        return 0;
    }

    short tileWidth = cuConstRendererParams.tileWidth;
    short tileHeight = cuConstRendererParams.tileHeight;
    float invWidth = cuConstRendererParams.invWidth;
    float invHeight = cuConstRendererParams.invHeight;

    short boxL = tileWidth * tileX;
    short boxR = boxL + tileWidth;
    short boxB = tileHeight * tileY;
    short boxT = boxB + tileHeight;
    float nBoxL = invWidth * static_cast<float>(boxL);
    float nBoxR = invWidth * static_cast<float>(boxR);
    float nBoxB = invHeight * static_cast<float>(boxB);
    float nBoxT = invHeight * static_cast<float>(boxT);

    int index3 = 3 * circleIndex;
    float3 p = *(float3 *)(&cuConstRendererParams.position[index3]);
    float rad = cuConstRendererParams.radius[circleIndex];

    short circleFlag = circleInBox(p.x, p.y, rad, nBoxL, nBoxR, nBoxT, nBoxB);
    return circleFlag;
}
#include <assert.h>

__global__ void setTileCircles(int offset, int numTrueCircles)
{
    short circleIdx = threadIdx.x;
    const int circleIndex = offset + circleIdx;

    __shared__ uint prefixSumInput[SCAN_BLOCK_DIM];
    prefixSumInput[circleIdx] = isCircleInBox(circleIndex, blockIdx.x, blockIdx.y);

    __syncthreads(); // Wait until all circles for the tile finish.

    __shared__ uint prefixSumOutput[SCAN_BLOCK_DIM];
    __shared__ uint prefixSumScratch[2 * SCAN_BLOCK_DIM];
    sharedMemExclusiveScan(circleIdx, prefixSumInput, prefixSumOutput,
                           prefixSumScratch, SCAN_BLOCK_DIM);
    // Extra threads are only used for the scan.
    if (circleIndex >= cuConstRendererParams.numCircles)
        return;

    short tileIndex = blockIdx.y * cuConstRendererParams.numTilesX + blockIdx.x;

    const int numCirclesProc = cuConstRendererParams.numCirclesProc;
    int flagIndex = numCirclesProc * tileIndex + prefixSumOutput[circleIdx];
    
    if (circleIdx < numTrueCircles - 1 &&
        prefixSumOutput[circleIdx] != prefixSumOutput[circleIdx + 1])
    {
        cuConstRendererParams.circleFlag[flagIndex] = circleIdx;
    }
    else if (circleIdx == numTrueCircles - 1)
    {
        if (prefixSumInput[circleIdx] == 1)
            cuConstRendererParams.circleFlag[flagIndex] = circleIdx;
            
        int endIndex = flagIndex + prefixSumInput[circleIdx];
        if (endIndex < numCirclesProc * (tileIndex + 1))
            cuConstRendererParams.circleFlag[endIndex] = -1;
    }
}

////////////////////////////////////////////////////////////////////////////////////////

CudaRenderer::CudaRenderer()
{
    image = NULL;

    numCircles = 0;

    position = NULL;
    velocity = NULL;
    color = NULL;
    radius = NULL;

    cudaDevicePosition = NULL;
    cudaDeviceVelocity = NULL;
    cudaDeviceColor = NULL;
    cudaDeviceRadius = NULL;
    cudaDeviceImageData = NULL;
}

CudaRenderer::~CudaRenderer()
{
    if (image)
    {
        delete image;
    }

    if (position)
    {
        delete[] position;
        delete[] velocity;
        delete[] color;
        delete[] radius;
    }

    if (cudaDevicePosition)
    {
        cudaFree(cudaDevicePosition);
        cudaFree(cudaDeviceVelocity);
        cudaFree(cudaDeviceColor);
        cudaFree(cudaDeviceRadius);
        cudaFree(cudaDeviceImageData);
        cudaFree(cudaDeviceCircleFlag);
    }
}

const Image *CudaRenderer::getImage()
{
    // need to copy contents of the rendered image from device memory
    // before we expose the Image object to the caller
    printf("Copying image data from device\n");

    cudaMemcpy(image->data,
               cudaDeviceImageData,
               sizeof(float) * 4 * image->width * image->height,
               cudaMemcpyDeviceToHost);

    return image;
}

void CudaRenderer::loadScene(SceneName scene)
{
    sceneName = scene;
    loadCircleScene(sceneName, numCircles, position, velocity, color, radius);
}

void CudaRenderer::setup()
{
    int deviceCount = 0;
    std::string name;
    cudaError_t err = cudaGetDeviceCount(&deviceCount);

    printf("---------------------------------------------------------\n");
    printf("Initializing CUDA for CudaRenderer\n");
    printf("Found %d CUDA devices\n", deviceCount);

    for (int i = 0; i < deviceCount; i++)
    {
        cudaDeviceProp deviceProps;
        cudaGetDeviceProperties(&deviceProps, i);
        name = deviceProps.name;

        printf("Device %d: %s\n", i, deviceProps.name);
        printf("   SMs:        %d\n", deviceProps.multiProcessorCount);
        printf("   Global mem: %.0f MB\n", static_cast<float>(deviceProps.totalGlobalMem) / (1024 * 1024));
        printf("   CUDA Cap:   %d.%d\n", deviceProps.major, deviceProps.minor);
    }
    printf("---------------------------------------------------------\n");

    // By this time the scene should be loaded.  Now copy all the key
    // data structures into device memory so they are accessible to
    // CUDA kernels
    //
    // See the CUDA Programmer's Guide for descriptions of
    // cudaMalloc and cudaMemcpy

    cudaMalloc(&cudaDevicePosition, sizeof(float) * 3 * numCircles);
    cudaMalloc(&cudaDeviceVelocity, sizeof(float) * 3 * numCircles);
    cudaMalloc(&cudaDeviceColor, sizeof(float) * 3 * numCircles);
    cudaMalloc(&cudaDeviceRadius, sizeof(float) * numCircles);
    cudaMalloc(&cudaDeviceImageData, sizeof(float) * 4 * image->width * image->height);
    short numTilesX = (image->width + TILE_WIDTH - 1) / TILE_WIDTH;
    short numTilesY = (image->height + TILE_HEIGHT - 1) / TILE_HEIGHT;
    cudaMalloc(&cudaDeviceCircleFlag, sizeof(short) * NUM_CIRCLES_PROC * numTilesX * numTilesY);

    cudaMemcpy(cudaDevicePosition, position, sizeof(float) * 3 * numCircles, cudaMemcpyHostToDevice);
    cudaMemcpy(cudaDeviceVelocity, velocity, sizeof(float) * 3 * numCircles, cudaMemcpyHostToDevice);
    cudaMemcpy(cudaDeviceColor, color, sizeof(float) * 3 * numCircles, cudaMemcpyHostToDevice);
    cudaMemcpy(cudaDeviceRadius, radius, sizeof(float) * numCircles, cudaMemcpyHostToDevice);

    // Initialize parameters in constant memory.  We didn't talk about
    // constant memory in class, but the use of read-only constant
    // memory here is an optimization over just sticking these values
    // in device global memory.  NVIDIA GPUs have a few special tricks
    // for optimizing access to constant memory.  Using global memory
    // here would have worked just as well.  See the Programmer's
    // Guide for more information about constant memory.

    GlobalConstants params;
    params.sceneName = sceneName;
    params.numCircles = numCircles;
    params.numCirclesProc = NUM_CIRCLES_PROC;
    params.numPixels = image->width * image->height;
    params.numTilesX = numTilesX;
    params.numTilesY = numTilesY;
    params.tileWidth = TILE_WIDTH;
    params.tileHeight = TILE_HEIGHT;
    params.imageWidth = image->width;
    params.imageHeight = image->height;
    params.invWidth = 1.f / image->width;
    params.invHeight = 1.f / image->height;
    params.position = cudaDevicePosition;
    params.velocity = cudaDeviceVelocity;
    params.color = cudaDeviceColor;
    params.radius = cudaDeviceRadius;
    params.imageData = cudaDeviceImageData;
    params.circleFlag = cudaDeviceCircleFlag;

    cudaMemcpyToSymbol(cuConstRendererParams, &params, sizeof(GlobalConstants));

    // also need to copy over the noise lookup tables, so we can
    // implement noise on the GPU
    int *permX;
    int *permY;
    float *value1D;
    getNoiseTables(&permX, &permY, &value1D);
    cudaMemcpyToSymbol(cuConstNoiseXPermutationTable, permX, sizeof(int) * 256);
    cudaMemcpyToSymbol(cuConstNoiseYPermutationTable, permY, sizeof(int) * 256);
    cudaMemcpyToSymbol(cuConstNoise1DValueTable, value1D, sizeof(float) * 256);

    // last, copy over the color table that's used by the shading
    // function for circles in the snowflake demo

    float lookupTable[COLOR_MAP_SIZE][3] = {
        {1.f, 1.f, 1.f},
        {1.f, 1.f, 1.f},
        {.8f, .9f, 1.f},
        {.8f, .9f, 1.f},
        {.8f, 0.8f, 1.f},
    };

    cudaMemcpyToSymbol(cuConstColorRamp, lookupTable, sizeof(float) * 3 * COLOR_MAP_SIZE);
}

// allocOutputImage --
//
// Allocate buffer the renderer will render into.  Check status of
// image first to avoid memory leak.
void CudaRenderer::allocOutputImage(int width, int height)
{
    if (image)
        delete image;
    image = new Image(width, height);
}

// clearImage --
//
// Clear's the renderer's target image.  The state of the image after
// the clear depends on the scene being rendered.
void CudaRenderer::clearImage()
{
    // 256 threads per block is a healthy number
    dim3 blockDim(16, 16, 1);
    dim3 gridDim((image->width + blockDim.x - 1) / blockDim.x,
                 (image->height + blockDim.y - 1) / blockDim.y);

    if (sceneName == SNOWFLAKES || sceneName == SNOWFLAKES_SINGLE_FRAME)
    {
        kernelClearImageSnowflake<<<gridDim, blockDim>>>();
    }
    else
    {
        kernelClearImage<<<gridDim, blockDim>>>(1.f, 1.f, 1.f, 1.f);
    }
    cudaDeviceSynchronize();
}

// advanceAnimation --
//
// Advance the simulation one time step.  Updates all circle positions
// and velocities
void CudaRenderer::advanceAnimation()
{
    // 256 threads per block is a healthy number
    dim3 blockDim(256, 1);
    dim3 gridDim((numCircles + blockDim.x - 1) / blockDim.x);

    // only the snowflake scene has animation
    if (sceneName == SNOWFLAKES)
    {
        kernelAdvanceSnowflake<<<gridDim, blockDim>>>();
    }
    else if (sceneName == BOUNCING_BALLS)
    {
        kernelAdvanceBouncingBalls<<<gridDim, blockDim>>>();
    }
    else if (sceneName == HYPNOSIS)
    {
        kernelAdvanceHypnosis<<<gridDim, blockDim>>>();
    }
    else if (sceneName == FIREWORKS)
    {
        kernelAdvanceFireWorks<<<gridDim, blockDim>>>();
    }
    cudaDeviceSynchronize();
}

void CudaRenderer::render()
{
    // 256 threads per block is a healthy number
    dim3 blockDim(256, 1);
    dim3 pixelDim((image->width * image->height + blockDim.x - 1) / blockDim.x);

    short numTilesX = (image->width + TILE_WIDTH - 1) / TILE_WIDTH;
    short numTilesY = (image->height + TILE_HEIGHT - 1) / TILE_HEIGHT;
    dim3 tileDim(numTilesX, numTilesY);

    for (int offset = 0; offset < numCircles; offset += NUM_CIRCLES_PROC)
    {
        const int numTrueCircles = std::min(NUM_CIRCLES_PROC, numCircles - offset);
        setTileCircles<<<tileDim, NUM_CIRCLES_PROC>>>(offset, numTrueCircles);
        applyShadePixel<<<pixelDim, blockDim>>>(offset, numTrueCircles);
        cudaDeviceSynchronize();
    }
}