# -*- perl -*-
use strict;
use warnings;
use tests::tests;
check_expected ([<<'EOF']);
(priority-alarm) begin
(priority-alarm) High-priority thread starts and goes to sleep.
(priority-alarm) Main thread starts running.
(priority-alarm) High-priority thread woke up at and preempts the main thread.
(priority-alarm) High-priority thread changed its priority to 26
(priority-alarm) Main thread thread changed its priority to 21
(priority-alarm) High-priority thread exit.
(priority-alarm) Main thread completed execution.
(priority-alarm) end
EOF
pass;
