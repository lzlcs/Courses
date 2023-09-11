#include <stdio.h>
#include <unistd.h>
#include <sys/wait.h>

int main() {

    int rc = fork();

    if (rc == 0) {
        int tmp = wait(NULL);
        printf("child wait output: %d, pid: %d\n", tmp, getpid());
        _exit(0);
    } else {
        int tmp = wait(NULL);
        printf("parent wait output: %d\n", tmp);
    }

    return 0;
}
