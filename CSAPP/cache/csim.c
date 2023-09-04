#include "cachelab.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>

int s, b, E;
int need_details = 0;

#define GETTAG(x) ((x) >> (s + b))
#define GETS(x) ((x) >> b & ((1 << s) - 1))
#define GETB(x) ((x) & ((1 << b) - 1))

typedef struct {
    unsigned valid, tag;
} Table;

Table **global_table;

int **global_stamp;

unsigned time_stamp = 0;

void cache_malloc() {

    int s_cnt = (1 << s);
    global_table = (Table **)malloc(s_cnt * sizeof(Table *));
    for (int i = 0; i < s_cnt; i++)
        global_table[i] = (Table *)malloc(E * sizeof(Table));

    for (int i = 0; i < s_cnt; i++) 
        for (int j = 0; j < E; j++)
            global_table[i][j] = (Table){ 0, 0 };

    global_stamp = (int **)malloc(s_cnt * sizeof(int *));
    for (int i = 0; i < s_cnt; i++)
        global_stamp[i] = (int *)malloc(E * sizeof(int));

    for (int i = 0; i < s_cnt; i++) 
        for (int j = 0; j < E; j++)
            global_stamp[i][j] = 0;
}

void cache_free() {

    int s_cnt = (1 << s);

    for (int i = 0; i < s_cnt; i++)
        free(global_table[i]);
    free(global_table);


    for (int i = 0; i < s_cnt; i++)
        free(global_stamp[i]);
    free(global_stamp);
}

void printUsage() {
    // 懒得写
}

int hits = 0, misses = 0, evictions = 0;

void inc(int *data, char *info) {
    if (need_details) printf("%s ", info);
    *data += 1;
}

int find(int addr) {
    int tag = GETTAG(addr), which_s = GETS(addr);

    for (int i = 0; i < E; i++) {
        Table tmp = global_table[which_s][i];
        if (tmp.valid && tmp.tag == tag) {
            global_stamp[which_s][i] = ++time_stamp;
            return inc(&hits, "hit"), 1;
        }
    }
    
    return inc(&misses, "miss"), 0;
}

void substitute(int addr) {
    int tag = GETTAG(addr), which_s = GETS(addr);

    int *min_aux = &global_stamp[which_s][0];
    Table *to_replace = &global_table[which_s][0];

    for (int i = 1; i < E; i++) {
        int *tmp = &global_stamp[which_s][i];
        if (*tmp < *min_aux) {
            min_aux = tmp;
            to_replace = &global_table[which_s][i];
        }
    }

    to_replace->valid = 1;
    to_replace->tag = tag;

    if (*min_aux != 0) inc(&evictions, "eviction");

   *min_aux = ++time_stamp;
}

void Load(int addr, int bytes) {
    if (!find(addr))
        substitute(addr);
}

void Store(int addr, int bytes) {
    Load(addr, bytes);
}

void Modify(int addr, int bytes) {
    
    Load(addr, bytes);
    find(addr);
}

int main(int argc, char *argv[])
{

    char opt;
	while(-1 != (opt = (getopt(argc, argv, "hvs:E:b:t:"))))
	{
        if (opt == 'h') printUsage();
        else if (opt == 'v') need_details = 1;
        else if (opt == 's') s = atoi(optarg);
        else if (opt == 'E') E = atoi(optarg);
        else if (opt == 'b') b = atoi(optarg);
        else if (opt == 't') freopen(optarg, "r", stdin);
	}

    cache_malloc();

    char type;
    int addr, bytes;

    getchar();
    while (~scanf("%c %x,%d\n", &type, &addr, &bytes)) {

        if (need_details) printf("%c %x,%d ", type, addr, bytes);
        if (type == 'L') Load(addr, bytes);
        else if (type == 'S') Store(addr, bytes);
        else if (type == 'M') Modify(addr, bytes);
        if (need_details) puts("");
    }

    printSummary(hits, misses, evictions);

    cache_free();
    fclose(stdin);
    return 0;
}
