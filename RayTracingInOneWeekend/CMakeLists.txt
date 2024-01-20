#---------------------------------------------------------------------------------------------------
# CMake Build Configuration for the Ray Tracing Weekend Series
#
# See README.md for guidance.
#---------------------------------------------------------------------------------------------------

cmake_minimum_required ( VERSION 3.1.0...3.27.0 )

project (RayTracing)

# Set to C++11
set ( CMAKE_CXX_STANDARD          11 )
set ( CMAKE_CXX_STANDARD_REQUIRED ON )
set ( CMAKE_CXX_EXTENSIONS        OFF )

set ( SOURCE_ONE_WEEKEND
    camera.h
    rtweekend.h
    ray.h
    sphere.h
    hittable.h
    vec3.h
    color.h
    main.cpp
)

include_directories(src)

# Executables
add_executable(RayTracing ${SOURCE_ONE_WEEKEND})