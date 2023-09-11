#include <stdio.h>
#include <unistd.h>

int main() {

    int x = 100;

    int rc = fork();

    if (rc == 0) {
        printf("child: \n");
        x = 101;
        printf("after: %d\n", x);
        
    } else {

        printf("parent: \n");
        x = 102;
        printf("after: %d\n", x);
    }

    return 0;
}
