#include "couch.h"
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

int couch11_register(const couch_config_t *cfg);
int couch11_heartbeat(const couch_config_t *cfg);
int couch11_claim(const couch_config_t *cfg, char **body);
int couch11_log(const couch_config_t *cfg, int id, const char *data, size_t len);
int couch11_complete(const couch_config_t *cfg, int id, int exit_code, int signal_no);

static const char *json_string_local(const char *body, const char *key, char *out, size_t cap) {
    char needle[128];
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    const char *p = strstr(body, needle);
    if(!p) return NULL;
    p = strchr(p + strlen(needle), ':');
    if(!p) return NULL;
    p++;
    while(*p == ' ' || *p == '\t') p++;
    if(*p != '"') return NULL;
    p++;
    size_t i = 0;
    while(*p && *p != '"' && i + 1 < cap) { if(*p == '\\' && p[1]) p++; out[i++] = *p++; }
    out[i] = 0;
    return out;
}

static int json_int_local(const char *body, const char *key) {
    char needle[128];
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    const char *p = strstr(body, needle);
    if(!p) return -1;
    p = strchr(p, ':');
    return p ? atoi(p + 1) : -1;
}

static int run_job(const couch_config_t *cfg, int id, const char *command) {
    int pipefd[2];
    if(pipe(pipefd) != 0) return 127;
    pid_t pid = fork();
    if(pid < 0) { close(pipefd[0]); close(pipefd[1]); return 127; }
    if(pid == 0) {
        int nullfd = open("/dev/null", O_RDONLY);
        if(nullfd >= 0) dup2(nullfd, STDIN_FILENO);
        dup2(pipefd[1], STDOUT_FILENO);
        dup2(pipefd[1], STDERR_FILENO);
        close(pipefd[0]);
        close(pipefd[1]);
        if(nullfd > STDERR_FILENO) close(nullfd);
        char *const envp[] = {"PATH=/usr/local/sbin:/usr/local/bin:/usr/bin:/bin", "LC_ALL=C", NULL};
        char *const argv[] = {"/bin/sh", "-c", (char *)command, NULL};
        execve("/bin/sh", argv, envp);
        _exit(127);
    }

    close(pipefd[1]);
    char buf[8192];
    ssize_t n;
    while((n = read(pipefd[0], buf, sizeof(buf))) > 0) {
        if(couch11_log(cfg, id, buf, (size_t)n) != 0) fprintf(stderr, "couch11: failed to upload log for job %d\n", id);
        fwrite(buf, 1, (size_t)n, stdout);
        fflush(stdout);
    }
    close(pipefd[0]);

    int status = 0;
    if(waitpid(pid, &status, 0) < 0) return 127;
    int ec = WIFEXITED(status) ? WEXITSTATUS(status) : 128;
    int sig = WIFSIGNALED(status) ? WTERMSIG(status) : 0;
    if(couch11_complete(cfg, id, ec, sig) != 0) fprintf(stderr, "couch11: failed to report job %d\n", id);
    return ec;
}

int main(int argc, char **argv) {
    const char *config = argc > 1 ? argv[1] : "couch11.conf";
    couch_config_t cfg;
    if(couch_config_load(config, &cfg) != 0) { fprintf(stderr, "couch11: cannot read %s\n", config); return 1; }
    if(!cfg.couch_url[0] || !cfg.runner_name[0]) { fprintf(stderr, "couch11: couch_url and runner_name are required\n"); return 1; }

    if(couch11_register(&cfg) != 0) { fprintf(stderr, "couch11: cannot register with couch\n"); return 1; }
    printf("couch11: runner %s connected to %s\n", cfg.runner_name, cfg.couch_url);

    for(;;) {
        couch11_heartbeat(&cfg);
        char *job = NULL;
        int result = couch11_claim(&cfg, &job);
        if(result < 0) {
            fprintf(stderr, "couch11: unable to contact couch\n");
            usleep((useconds_t)cfg.poll_ms * 1000);
            continue;
        }
        if(result == 0) {
            usleep((useconds_t)cfg.poll_ms * 1000);
            continue;
        }

        int id = json_int_local(job, "id");
        char command[COUCH_MAX_COMMAND];
        if(id < 0 || !json_string_local(job, "command", command, sizeof(command))) {
            fprintf(stderr, "couch11: invalid job: %s\n", job);
            free(job);
            continue;
        }

        printf("couch11: running job %d\n", id);
        run_job(&cfg, id, command);
        free(job);
    }
    return 0;
}
