#include <cmath>
#include <eigen3/Eigen/Core>
#include <eigen3/Eigen/Dense>
#include <iostream>

int main()
{

    Eigen::Matrix3f R, T;
    double theta = 45.0 / 180.0 * acos(-1);
    R << cos(theta), -sin(theta), 0, sin(theta), cos(theta), 0, 0, 0, 1;
    T << 1.0, 0.0, 1.0, 0.0, 1.0, 2.0, 0.0, 0.0, 1.0;
    Eigen::Vector3f P(2.0f, 1.0f, 1.0f);
    std::cout << R << std::endl;
    std::cout << R * P << std::endl;
    std::cout <<  T * R * P << std::endl;

    return 0;
}