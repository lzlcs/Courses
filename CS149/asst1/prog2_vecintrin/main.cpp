#include <stdio.h>
#include <algorithm>
#include <getopt.h>
#include <math.h>
#include "CS149intrin.h"
#include "logger.h"
using namespace std;

#define EXP_MAX 10

Logger CS149Logger;

void usage(const char *progname);
void initValue(float *values, int *exponents, float *output, float *gold, unsigned int N);
void absSerial(float *values, float *output, int N);
void absVector(float *values, float *output, int N);
void clampedExpSerial(float *values, int *exponents, float *output, int N);
void clampedExpVector(float *values, int *exponents, float *output, int N);
float arraySumSerial(float *values, int N);
float arraySumVector(float *values, int N);
bool verifyResult(float *values, int *exponents, float *output, float *gold, int N);

int main(int argc, char *argv[])
{
	int N = 16;
	bool printLog = false;

	// parse commandline options ////////////////////////////////////////////
	int opt;
	static struct option long_options[] = {
		{"size", 1, 0, 's'},
		{"log", 0, 0, 'l'},
		{"help", 0, 0, '?'},
		{0, 0, 0, 0}};

	while ((opt = getopt_long(argc, argv, "s:l?", long_options, NULL)) != EOF)
	{

		switch (opt)
		{
		case 's':
			N = atoi(optarg);
			if (N <= 0)
			{
				printf("Error: Workload size is set to %d (<0).\n", N);
				return -1;
			}
			break;
		case 'l':
			printLog = true;
			break;
		case '?':
		default:
			usage(argv[0]);
			return 1;
		}
	}

	float *values = new float[N + VECTOR_WIDTH];
	int *exponents = new int[N + VECTOR_WIDTH];
	float *output = new float[N + VECTOR_WIDTH];
	float *gold = new float[N + VECTOR_WIDTH];
	initValue(values, exponents, output, gold, N);

	clampedExpSerial(values, exponents, gold, N);
	clampedExpVector(values, exponents, output, N);

	// absSerial(values, gold, N);
	// absVector(values, output, N);

	printf("\e[1;31mCLAMPED EXPONENT\e[0m (required) \n");
	bool clampedCorrect = verifyResult(values, exponents, output, gold, N);
	if (printLog)
		CS149Logger.printLog();
	CS149Logger.printStats();

	printf("************************ Result Verification *************************\n");
	if (!clampedCorrect)
	{
		printf("@@@ Failed!!!\n");
	}
	else
	{
		printf("Passed!!!\n");
	}

	printf("\n\e[1;31mARRAY SUM\e[0m (bonus) \n");
	if (N % VECTOR_WIDTH == 0)
	{
		float sumGold = arraySumSerial(values, N);
		float sumOutput = arraySumVector(values, N);
		float epsilon = 0.1;
		bool sumCorrect = abs(sumGold - sumOutput) < epsilon * 2;
		if (!sumCorrect)
		{
			printf("Expected %f, got %f\n.", sumGold, sumOutput);
			printf("@@@ Failed!!!\n");
		}
		else
		{
			printf("Passed!!!\n");
		}
	}
	else
	{
		printf("Must have N %% VECTOR_WIDTH == 0 for this problem (VECTOR_WIDTH is %d)\n", VECTOR_WIDTH);
	}

	delete[] values;
	delete[] exponents;
	delete[] output;
	delete[] gold;

	return 0;
}

void usage(const char *progname)
{
	printf("Usage: %s [options]\n", progname);
	printf("Program Options:\n");
	printf("  -s  --size <N>     Use workload size N (Default = 16)\n");
	printf("  -l  --log          Print vector unit execution log\n");
	printf("  -?  --help         This message\n");
}

void initValue(float *values, int *exponents, float *output, float *gold, unsigned int N)
{

	for (unsigned int i = 0; i < N + VECTOR_WIDTH; i++)
	{
		// random input values
		values[i] = -1.f + 4.f * static_cast<float>(rand()) / RAND_MAX;
		exponents[i] = rand() % EXP_MAX;
		output[i] = 0.f;
		gold[i] = 0.f;
	}
}

bool verifyResult(float *values, int *exponents, float *output, float *gold, int N)
{
	int incorrect = -1;
	float epsilon = 0.00001;
	for (int i = 0; i < N + VECTOR_WIDTH; i++)
	{
		if (abs(output[i] - gold[i]) > epsilon)
		{
			incorrect = i;
			break;
		}
	}

	if (incorrect != -1)
	{
		if (incorrect >= N)
			printf("You have written to out of bound value!\n");
		printf("Wrong calculation at value[%d]!\n", incorrect);
		printf("value  = ");
		for (int i = 0; i < N; i++)
		{
			printf("% f ", values[i]);
		}
		printf("\n");

		printf("exp    = ");
		for (int i = 0; i < N; i++)
		{
			printf("% 9d ", exponents[i]);
		}
		printf("\n");

		printf("output = ");
		for (int i = 0; i < N; i++)
		{
			printf("% f ", output[i]);
		}
		printf("\n");

		printf("gold   = ");
		for (int i = 0; i < N; i++)
		{
			printf("% f ", gold[i]);
		}
		printf("\n");
		return false;
	}
	printf("Results matched with answer!\n");
	return true;
}

// computes the absolute value of all elements in the input array
// values, stores result in output
void absSerial(float *values, float *output, int N)
{
	for (int i = 0; i < N; i++)
	{
		float x = values[i];
		if (x < 0)
		{
			output[i] = -x;
		}
		else
		{
			output[i] = x;
		}
	}
}

// implementation of absSerial() above, but it is vectorized using CS149 intrinsics
void absVector(float *values, float *output, int N)
{
	__cs149_vec_float x;
	__cs149_vec_float result;
	__cs149_vec_float zero = _cs149_vset_float(0.f);
	__cs149_mask maskAll, maskIsNegative, maskIsNotNegative;

	//  Note: Take a careful look at this loop indexing.  This example
	//  code is not guaranteed to work when (N % VECTOR_WIDTH) != 0.
	//  Why is that the case?
	for (int i = 0; i < N; i += VECTOR_WIDTH)
	{

		// All ones
		maskAll = _cs149_init_ones();

		// All zeros
		maskIsNegative = _cs149_init_ones(0);

		// Load vector of values from contiguous memory addresses
		_cs149_vload_float(x, values + i, maskAll); // x = values[i];

		// Set mask according to predicate
		_cs149_vlt_float(maskIsNegative, x, zero, maskAll); // if (x < 0) {

		// Execute instruction using mask ("if" clause)
		_cs149_vsub_float(result, zero, x, maskIsNegative); //   output[i] = -x;

		// Inverse maskIsNegative to generate "else" mask
		maskIsNotNegative = _cs149_mask_not(maskIsNegative); // } else {

		// Execute instruction ("else" clause)
		_cs149_vload_float(result, values + i, maskIsNotNegative); //   output[i] = x; }

		// Write results back to memory
		_cs149_vstore_float(output + i, result, maskAll);
	}
}

// accepts an array of values and an array of exponents
//
// For each element, compute values[i]^exponents[i] and clamp value to
// 9.999.  Store result in output.
void clampedExpSerial(float *values, int *exponents, float *output, int N)
{
	for (int i = 0; i < N; i++)
	{
		float x = values[i];
		int y = exponents[i];
		if (y == 0)
		{
			output[i] = 1.f;
		}
		else
		{
			float result = x;
			int count = y - 1;
			while (count > 0)
			{
				result *= x;
				count--;
			}
			if (result > 9.999999f)
			{
				result = 9.999999f;
			}
			output[i] = result;
		}
	}
}

void clampedExpVector(float *values, int *exponents, float *output, int N)
{

	__cs149_vec_float x;
	__cs149_vec_int y;

	__cs149_vec_float result;
	__cs149_vec_int count;

	__cs149_vec_int zero_int = _cs149_vset_int(0);
	__cs149_vec_float one_float = _cs149_vset_float(1.f);
	__cs149_vec_float limit = _cs149_vset_float(9.999999f);
	__cs149_vec_int one_int = _cs149_vset_int(1);

	__cs149_mask maskAll = _cs149_init_ones();
	__cs149_mask maskEqZero, maskNotEqZero;
	__cs149_mask overLimit, maskGtZero;

	int i = 0;
	for (i = 0; i + VECTOR_WIDTH - 1 < N; i += VECTOR_WIDTH)
	{
		// float x = values[i];
		_cs149_vload_float(x, values + i, maskAll);
		// int y = exponents[i];
		_cs149_vload_int(y, exponents + i, maskAll);
		// if (y == 0)
		_cs149_veq_int(maskEqZero, y, zero_int, maskAll);
		// {
		// 	output[i] = 1.f;
		_cs149_vstore_float(output + i, one_float, maskEqZero);
		// }
		// else
		maskNotEqZero = _cs149_mask_not(maskEqZero);
		// {
		// 	float result = x;

		_cs149_vload_float(result, values + i, maskNotEqZero);
		// 	int count = y - 1;
		_cs149_vsub_int(count, y, one_int, maskNotEqZero);

		// 	while (count > 0)
		// 	{
		while (true)
		{
			_cs149_vgt_int(maskGtZero, count, zero_int, maskNotEqZero);
			if (_cs149_cntbits(maskGtZero) == 0)
				break;
			// 		result *= x;
			_cs149_vmult_float(result, result, x, maskGtZero);
			// 		count--;
			_cs149_vsub_int(count, count, one_int, maskGtZero);
		}
		// 	}
		_cs149_vgt_float(overLimit, result, limit, maskNotEqZero);
		// 	if (result > 9.999999f)
		// 	{
		// 		result = 9.999999f;
		// 	}
		_cs149_vset_float(result, 9.999999f, overLimit);
		// 	output[i] = result;
		// }
		_cs149_vstore_float(output + i, result, maskNotEqZero);
	}
	//
	// CS149 STUDENTS TODO: Implement your vectorized version of
	// clampedExpSerial() here.
	//
	// Your solution should work for any value of
	// N and VECTOR_WIDTH, not just when VECTOR_WIDTH divides N
	//
	clampedExpSerial(values + i, exponents + i, output + i, N - i);
}

// returns the sum of all elements in values
float arraySumSerial(float *values, int N)
{
	float sum = 0;
	for (int i = 0; i < N; i++)
	{
		sum += values[i];
	}

	return sum;
}

// returns the sum of all elements in values
// You can assume N is a multiple of VECTOR_WIDTH
// You can assume VECTOR_WIDTH is a power of 2
float arraySumVector(float *values, int N)
{

	//
	// CS149 STUDENTS TODO: Implement your vectorized version of arraySumSerial here
	//
	// float gg = 0;
	// for (int i = 0; i < N; i++)
	// {
	// 	gg += values[i];
	// }
	// return gg;

	__cs149_vec_float sum = _cs149_vset_float(0.f);
	__cs149_vec_float x;

	__cs149_mask maskAll = _cs149_init_ones();

	int i;
	for (i = 0; i + VECTOR_WIDTH - 1 < N; i += VECTOR_WIDTH)
	{
		_cs149_vload_float(x, values + i, maskAll);
		_cs149_vadd_float(sum, sum, x, maskAll);
	}

	for (int t = 1; t < VECTOR_WIDTH; t *= 2)
	{
		_cs149_hadd_float(sum, sum);
		_cs149_interleave_float(sum, sum);
	}

	return sum.value[0];
}
