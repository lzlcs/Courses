#include <stdio.h>
#include <unistd.h>
#include <sys/wait.h>

int main() {

    int rc = fork();

    if (rc == 0) {
        int tmp = waitpid(rc, NULL, 0);
        printf("child wait output: %d, pid: %d\n", tmp, getpid());
        _exit(0);
    } else {
        int tmp = waitpid(rc, NULL, 0);
        printf("parent wait output: %d\n", tmp);
    }

    return 0;
}
