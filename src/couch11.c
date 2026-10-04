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
#include <time.h>
#include <limits.h>

extern char **environ;

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
    while(*p && *p != '"' && i + 1 < cap) {
        if(*p == '\\' && p[1]) p++;
        out[i++] = *p++;
    }
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

static int mkdir_one(const char *path) {
    if(mkdir(path, 0755) == 0) return 0;
    return errno == EEXIST ? 0 : -1;
}

static int mkdir_recursive(const char *path) {
    char tmp[PATH_MAX];
    size_t len = strlen(path);

    if(!len || len >= sizeof(tmp)) return -1;

    memcpy(tmp, path, len + 1);

    if(tmp[len - 1] == '/') tmp[len - 1] = 0;

    for(char *p = tmp + 1; *p; p++) {
        if(*p != '/') continue;
        *p = 0;
        if(mkdir_one(tmp) != 0) return -1;
        *p = '/';
    }

    return mkdir_one(tmp);
}

static int make_workspace(const couch_config_t *cfg, int id, char *path, size_t cap) {
    char jobs[PATH_MAX];

    if(snprintf(jobs, sizeof(jobs), "%s/jobs", cfg->workdir) >= (int)sizeof(jobs)) return -1;
    if(snprintf(path, cap, "%s/%d", jobs, id) >= (int)cap) return -1;
    if(mkdir_recursive(jobs) != 0) return -1;
    if(mkdir_one(path) != 0) return -1;

    return 0;
}

static int run_process(const couch_config_t *cfg, int id, const char *cwd, const char *const argv[]) {
    int pipefd[2];

    if(pipe(pipefd) != 0) return 127;

    pid_t pid = fork();

    if(pid < 0) {
        close(pipefd[0]);
        close(pipefd[1]);
        return 127;
    }

    if(pid == 0) {
        int nullfd = open("/dev/null", O_RDONLY);

        if(nullfd >= 0) dup2(nullfd, STDIN_FILENO);
        dup2(pipefd[1], STDOUT_FILENO);
        dup2(pipefd[1], STDERR_FILENO);

        close(pipefd[0]);
        close(pipefd[1]);

        if(nullfd > STDERR_FILENO) close(nullfd);

        if(cwd && chdir(cwd) != 0) _exit(127);

        execve(argv[0], (char *const *)argv, environ);

        if(!strcmp(argv[0], "/usr/bin/git")) {
            execve("/bin/git", (char *const *)argv, environ);
        }

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

    if(WIFEXITED(status)) return WEXITSTATUS(status);
    if(WIFSIGNALED(status)) return 128 + WTERMSIG(status);

    return 127;
}

static int checkout_repository(const couch_config_t *cfg, const couch_job_t *job, const char *workspace) {
    char repo_dir[PATH_MAX];

    if(snprintf(repo_dir, sizeof(repo_dir), "%s/repo", workspace) >= (int)sizeof(repo_dir)) {
        couch11_log(cfg, job->id, "couch11: repository workspace path is too long\n", strlen("couch11: repository workspace path is too long\n"));
        return -1;
    }

    {
        const char *argv[] = {"/usr/bin/git", "clone", "--no-checkout", job->repository, repo_dir, NULL};

        if(run_process(cfg, job->id, NULL, argv) != 0) {
            const char *msg = "couch11: git clone failed\n";
            couch11_log(cfg, job->id, msg, strlen(msg));
            return -1;
        }
    }

    {
        const char *argv[] = {"/usr/bin/git", "-C", repo_dir, "fetch", "--force", "origin", job->ref, NULL};

        if(run_process(cfg, job->id, NULL, argv) != 0) {
            const char *msg = "couch11: git fetch failed\n";
            couch11_log(cfg, job->id, msg, strlen(msg));
            return -1;
        }
    }

    {
        const char *argv[] = {"/usr/bin/git", "-C", repo_dir, "checkout", "--detach", job->commit, NULL};

        if(run_process(cfg, job->id, NULL, argv) != 0) {
            const char *msg = "couch11: git checkout failed\n";
            couch11_log(cfg, job->id, msg, strlen(msg));
            return -1;
        }
    }

    return 0;
}

static int run_job(const couch_config_t *cfg, const couch_job_t *job, const char *workspace) {
    char repo_dir[PATH_MAX];

    if(snprintf(repo_dir, sizeof(repo_dir), "%s/repo", workspace) >= (int)sizeof(repo_dir)) {
        couch11_log(cfg, job->id, "couch11: repository path is too long\n", strlen("couch11: repository path is too long\n"));
        couch11_complete(cfg, job->id, 127, 0);
        return 127;
    }

    if(checkout_repository(cfg, job, workspace) != 0) {
        couch11_complete(cfg, job->id, 127, 0);
        return 127;
    }

    printf("couch11: running job %d in %s\n", job->id, repo_dir);

    int pipefd[2];

    if(pipe(pipefd) != 0) {
        couch11_log(cfg, job->id, "couch11: pipe failed\n", strlen("couch11: pipe failed\n"));
        couch11_complete(cfg, job->id, 127, 0);
        return 127;
    }

    pid_t pid = fork();

    if(pid < 0) {
        close(pipefd[0]);
        close(pipefd[1]);
        couch11_log(cfg, job->id, "couch11: fork failed\n", strlen("couch11: fork failed\n"));
        couch11_complete(cfg, job->id, 127, 0);
        return 127;
    }

    if(pid == 0) {
        int nullfd = open("/dev/null", O_RDONLY);

        if(nullfd >= 0) dup2(nullfd, STDIN_FILENO);
        dup2(pipefd[1], STDOUT_FILENO);
        dup2(pipefd[1], STDERR_FILENO);

        close(pipefd[0]);
        close(pipefd[1]);

        if(nullfd > STDERR_FILENO) close(nullfd);

        if(chdir(repo_dir) != 0) _exit(127);

        char *const argv[] = {"/bin/sh", "-c", (char *)job->command, NULL};
        char *const envp[] = {"PATH=/usr/local/sbin:/usr/local/bin:/usr/bin:/bin", "LC_ALL=C", NULL};

        execve("/bin/sh", argv, envp);
        _exit(127);
    }

    close(pipefd[1]);

    char buf[8192];
    ssize_t n;

    while((n = read(pipefd[0], buf, sizeof(buf))) > 0) {
        if(couch11_log(cfg, job->id, buf, (size_t)n) != 0) fprintf(stderr, "couch11: failed to upload log for job %d\n", job->id);
        fwrite(buf, 1, (size_t)n, stdout);
        fflush(stdout);
    }

    close(pipefd[0]);

    int status = 0;

    if(waitpid(pid, &status, 0) < 0) {
        couch11_log(cfg, job->id, "couch11: waitpid failed\n", strlen("couch11: waitpid failed\n"));
        couch11_complete(cfg, job->id, 127, 0);
        return 127;
    }

    int ec = WIFEXITED(status) ? WEXITSTATUS(status) : 128;
    int sig = WIFSIGNALED(status) ? WTERMSIG(status) : 0;

    if(couch11_complete(cfg, job->id, ec, sig) != 0) fprintf(stderr, "couch11: failed to report job %d\n", job->id);

    return ec;
}

int main(int argc, char **argv) {
    const char *config = argc > 1 ? argv[1] : "couch11.conf";
    couch_config_t cfg;

    if(couch_config_load(config, &cfg) != 0) {
        fprintf(stderr, "couch11: cannot read %s\n", config);
        return 1;
    }

    if(!cfg.couch_url[0] || !cfg.runner_name[0]) {
        fprintf(stderr, "couch11: couch_url and runner_name are required\n");
        return 1;
    }

    if(mkdir_recursive(cfg.workdir) != 0) {
        fprintf(stderr, "couch11: cannot create workdir %s: %s\n", cfg.workdir, strerror(errno));
        return 1;
    }

    if(couch11_register(&cfg) != 0) {
        fprintf(stderr, "couch11: cannot register with couch\n");
        return 1;
    }

    printf("couch11: runner %s connected to %s\n", cfg.runner_name, cfg.couch_url);

    for(;;) {
        couch11_heartbeat(&cfg);

        char *body = NULL;
        int result = couch11_claim(&cfg, &body);

        if(result < 0) {
            fprintf(stderr, "couch11: unable to contact couch\n");
            struct timespec ts = {cfg.poll_ms / 1000, (cfg.poll_ms % 1000) * 1000000L};
            nanosleep(&ts, NULL);
            continue;
        }

        if(result == 0) {
            struct timespec ts = {cfg.poll_ms / 1000, (cfg.poll_ms % 1000) * 1000000L};
            nanosleep(&ts, NULL);
            continue;
        }

        couch_job_t job;
        memset(&job, 0, sizeof(job));

        job.id = json_int_local(body, "id");

        if(job.id < 0 || !json_string_local(body, "repository", job.repository, sizeof(job.repository)) || !json_string_local(body, "ref", job.ref, sizeof(job.ref)) || !json_string_local(body, "commit", job.commit, sizeof(job.commit)) || !json_string_local(body, "command", job.command, sizeof(job.command))) {
            fprintf(stderr, "couch11: invalid job: %s\n", body);
            free(body);
            continue;
        }

        char workspace[PATH_MAX];

        if(make_workspace(&cfg, job.id, workspace, sizeof(workspace)) != 0) {
            fprintf(stderr, "couch11: cannot create workspace for job %d: %s\n", job.id, strerror(errno));
            couch11_complete(&cfg, job.id, 127, 0);
            free(body);
            continue;
        }

        printf("couch11: claimed job %d\n", job.id);
        printf("couch11: repository: %s\n", job.repository);
        printf("couch11: ref: %s\n", job.ref);
        printf("couch11: commit: %s\n", job.commit);
        printf("couch11: workspace: %s\n", workspace);

        run_job(&cfg, &job, workspace);

        free(body);
    }

    return 0;
}
