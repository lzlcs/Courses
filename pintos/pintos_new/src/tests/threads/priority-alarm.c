#include <stdio.h>
#include "tests/threads/tests.h"
#include "threads/init.h"
#include "threads/malloc.h"
#include "threads/synch.h"
#include "threads/thread.h"
#include "devices/timer.h"

static void high_priority_sleeper(void *);

void test_priority_alarm(void) {
    int high_priority = PRI_DEFAULT + 10;
    thread_create("high_priority_sleeper", high_priority, high_priority_sleeper, NULL);

    msg("Main thread starts running.");

    int64_t start_tick = timer_ticks();
    while (timer_elapsed(start_tick) < 20);

    msg("Main thread thread changed its priority to 21");

    thread_set_priority(21);

    msg("Main thread completed execution.");

}

static void
high_priority_sleeper(void *aux UNUSED) {
    msg("High-priority thread starts and goes to sleep.");
    timer_sleep(10);
    msg("High-priority thread woke up at and preempts the main thread.");
    msg("High-priority thread changed its priority to 26");
    thread_set_priority(PRI_DEFAULT - 5);
    msg("High-priority thread exit.");
}