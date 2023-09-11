#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <sys/wait.h>
#include <stdlib.h>

int main() {

    int fd = open("example.txt", O_CREAT | O_WRONLY, 0644);

    int rc = fork();

    if (rc == 0) {
        char *s = "child write\n";
        int tmp = write(fd, s, strlen(s));
    } else {
        char *s = "parent write\n";
        int tmp = write(fd, s, strlen(s));
        wait(NULL);
        close(fd);
    }

    return 0;
}
