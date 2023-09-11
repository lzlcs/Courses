#include <stdio.h>
#include <unistd.h>
#include <sys/wait.h>
#include <stdlib.h>

int main() {

    int pipe_fd[2];
    int rc1, rc2;

    pipe(pipe_fd);

    rc1 = fork();

    if (rc1 == 0) {
        close(pipe_fd[0]);
        dup2(pipe_fd[1], STDOUT_FILENO);

        execlp("echo", "echo", "Hello", NULL);
        _exit(1);

    } else {

        waitpid(rc1, NULL, 0);

        rc2 = fork();

        if (rc2 == 0) {
            close(pipe_fd[1]);
            dup2(pipe_fd[0], STDIN_FILENO);

            int x = execlp("cat", "cat", NULL);
            
            _exit(1);
        } else {

            close(pipe_fd[0]), close(pipe_fd[1]);

            waitpid(rc2, NULL, 0);
        }
    }


    return 0;
}

