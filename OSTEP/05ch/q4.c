#include <stdio.h>
#include <unistd.h>
#include <sys/wait.h>

int main() {

    int rc = fork();

    if (rc == 0) {
        execl("/bin/ls", "-l", NULL);
    } else {
        wait(NULL);
    }

    return 0;
}
