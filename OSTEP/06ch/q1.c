#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/time.h>

int main() {

    int fd = open("q1.c", O_RDONLY);

    char buf[256];

    struct timeval start, end;

    gettimeofday(&start, NULL);

    for (int i = 0; i < 1e7; i++) {
        read(fd, buf, 0);
    }

    gettimeofday(&end, NULL);

    printf("%d\n", (int)(end.tv_usec - start.tv_usec));

    return 0;
}
