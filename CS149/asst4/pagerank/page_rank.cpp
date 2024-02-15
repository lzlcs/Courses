#include "page_rank.h"

#include <stdlib.h>
#include <iostream>
#include <cmath>
#include <omp.h>
#include <utility>

#include "../common/CycleTimer.h"
#include "../common/graph.h"

// pageRank --
//
// g:           graph to process (see common/graph.h)
// solution:    array of per-vertex vertex scores (length of array is num_nodes(g))
// damping:     page-rank algorithm's damping parameter
// convergence: page-rank algorithm's convergence threshold
//
void pageRank(Graph g, double *solution, double damping, double convergence)
{

    // initialize vertex weights to uniform probability. Double
    // precision scores are used to avoid underflow for large graphs

    int numNodes = num_nodes(g);
    double equal_prob = 1.0 / numNodes;
    for (int i = 0; i < numNodes; ++i)
    {
        solution[i] = equal_prob;
    }

    double *score_old = solution;
    double *score_new = new double[numNodes];
    double *score_diff = new double[numNodes];

    bool converged = false;

    const double rest = (1.0 - damping) / numNodes;
    while (!converged)
    {

        double no_outgoing_ver_val = 0;

#pragma omp parallel for
        for (int i = 0; i < numNodes; ++i)
        {
            const Vertex *in_begin = incoming_begin(g, i);
            const Vertex *in_end = incoming_end(g, i);

            double tmp = 0;
            for (const Vertex *j = in_begin; j != in_end; ++j)
                tmp += score_old[*j] / outgoing_size(g, *j);

            score_new[i] = (damping * tmp) + rest;

            if (outgoing_size(g, i) == 0)
            {
#pragma omp atomic
                no_outgoing_ver_val += score_old[i];
            }
        }

        no_outgoing_ver_val = damping * no_outgoing_ver_val / numNodes;

#pragma omp parallel for
        for (int i = 0; i < numNodes; ++i)
        {
            score_new[i] += no_outgoing_ver_val;
            score_diff[i] = fabs(score_new[i] - score_old[i]);
            score_old[i] = score_new[i];
            score_new[i] = 0;
        }

        double global_diff = 0.0;
#pragma omp parallel for reduction(+ : global_diff)
        for (int i = 0; i < numNodes; ++i)
        {
            global_diff += score_diff[i];
        }

        converged = (global_diff < convergence);
    }

    delete[] score_new;
    delete[] score_diff;
}
