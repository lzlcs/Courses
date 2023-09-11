#include <stdio.h>
#include <unistd.h>
#include <sys/wait.h>

int main() {

    int rc = fork();

    if (rc == 0) {
        close(STDOUT_FILENO);
        printf("child\n");
    } else {
        printf("parent\n");
    }

    return 0;
}
