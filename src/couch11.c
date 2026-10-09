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
#include <glob.h>

extern char **environ;

int couch11_register(const couch_config_t *cfg);
int couch11_heartbeat(const couch_config_t *cfg);
int couch11_claim(const couch_config_t *cfg, char **body);
int couch11_log(const couch_config_t *cfg, int id, const char *data, size_t len);
int couch11_complete(const couch_config_t *cfg, int id, int exit_code, int signal_no);
int couch11_artifact_begin(const couch_config_t *cfg, int job_id, const char *name, int *artifact_id);
int couch11_artifact_chunk(const couch_config_t *cfg, int artifact_id, int sequence, const void *data, size_t len);
int couch11_artifact_finish(const couch_config_t *cfg, int artifact_id, size_t size);

typedef struct {
    char command[COUCH_MAX_COMMAND];
} couch_stage_t;

typedef struct {
    couch_stage_t *stages;
    size_t count;
    size_t cap;
    char **artifacts;
    size_t artifact_count;
    size_t artifact_cap;
} couch_build_t;

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

static char *trim(char *s) {
    while(*s == ' ' || *s == '\t' || *s == '\r' || *s == '\n') s++;

    char *e = s + strlen(s);

    while(e > s && (e[-1] == ' ' || e[-1] == '\t' || e[-1] == '\r' || e[-1] == '\n')) *--e = 0;

    return s;
}

static void build_free(couch_build_t *build) {
    for(size_t i = 0; i < build->artifact_count; i++) free(build->artifacts[i]);

    free(build->artifacts);
    free(build->stages);

    memset(build, 0, sizeof(*build));
}

static int build_add_stage(couch_build_t *build, const char *command) {
    if(build->count == build->cap) {
        size_t cap = build->cap ? build->cap * 2 : 8;
        couch_stage_t *p = realloc(build->stages, cap * sizeof(*p));

        if(!p) return -1;

        build->stages = p;
        build->cap = cap;
    }

    snprintf(build->stages[build->count].command, sizeof(build->stages[build->count].command), "%s", command);

    build->count++;

    return 0;
}

static int build_add_artifact(couch_build_t *build, const char *path) {
    if(build->artifact_count == build->artifact_cap) {
        size_t cap = build->artifact_cap ? build->artifact_cap * 2 : 8;
        char **p = realloc(build->artifacts, cap * sizeof(*p));

        if(!p) return -1;

        build->artifacts = p;
        build->artifact_cap = cap;
    }

    build->artifacts[build->artifact_count] = strdup(path);

    if(!build->artifacts[build->artifact_count]) return -1;

    build->artifact_count++;

    return 0;
}

static int parse_couch_file(const char *path, couch_build_t *build) {
    FILE *f = fopen(path, "r");

    if(!f) return -1;

    char line[COUCH_MAX_COMMAND + 256];

    while(fgets(line, sizeof(line), f)) {
        char *p = trim(line);

        if(!*p || *p == '#') continue;

        char *eq = strchr(p, '=');

        if(!eq) continue;

        *eq++ = 0;

        char *key = trim(p);
        char *value = trim(eq);

        if(*value == '"' && value[strlen(value) - 1] == '"') {
            size_t len = strlen(value);

            if(len >= 2) {
                value[len - 1] = 0;
                value++;
            }
        }

        if(!strcmp(key, "run")) {
            if(build_add_stage(build, value) != 0) {
                fclose(f);
                return -1;
            }
        } else if(!strcmp(key, "path")) {
            if(build_add_artifact(build, value) != 0) {
                fclose(f);
                return -1;
            }
        }
    }

    fclose(f);

    return build->count || build->artifact_count ? 0 : -1;
}

static const char *tmpdir(void) {
    const char *tmp = getenv("TMPDIR");

    return tmp && *tmp ? tmp : "/tmp";
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
    char root[PATH_MAX];
    char jobs[PATH_MAX];

    if(cfg->workdir[0]) {
        snprintf(root, sizeof(root), "%s", cfg->workdir);
    } else {
        snprintf(root, sizeof(root), "%s/couch11", tmpdir());
    }

    if(snprintf(jobs, sizeof(jobs), "%s/jobs", root) >= (int)sizeof(jobs)) return -1;
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

        if(!strcmp(argv[0], "/usr/bin/git")) execve("/bin/git", (char *const *)argv, environ);

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

    if(snprintf(repo_dir, sizeof(repo_dir), "%s/repo", workspace) >= (int)sizeof(repo_dir)) return -1;

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

static int run_command(const couch_config_t *cfg, const couch_job_t *job, const char *repo_dir, const char *command) {
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

        if(chdir(repo_dir) != 0) _exit(127);

        char *const argv[] = {"/bin/sh", "-c", (char *)command, NULL};

        execve("/bin/sh", argv, environ);

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

    if(waitpid(pid, &status, 0) < 0) return 127;

    if(WIFEXITED(status)) return WEXITSTATUS(status);

    if(WIFSIGNALED(status)) return 128 + WTERMSIG(status);

    return 127;
}

static int upload_artifact(const couch_config_t *cfg, int job_id, const char *repo_dir, const char *path) {
    char pattern[PATH_MAX];

    if(path[0] == '/') {
        snprintf(pattern, sizeof(pattern), "%s", path);
    } else {
        if(snprintf(pattern, sizeof(pattern), "%s/%s", repo_dir, path) >= (int)sizeof(pattern)) return -1;
    }

    glob_t matches;

    memset(&matches, 0, sizeof(matches));

    int gr = glob(pattern, GLOB_TILDE, NULL, &matches);

    if(gr != 0) {
        globfree(&matches);
        return 0;
    }

    int failed = 0;

    for(size_t i = 0; i < matches.gl_pathc; i++) {
        struct stat st;

        if(stat(matches.gl_pathv[i], &st) != 0) {
            failed = 1;
            continue;
        }

        if(!S_ISREG(st.st_mode)) continue;

        FILE *f = fopen(matches.gl_pathv[i], "rb");

        if(!f) {
            failed = 1;
            continue;
        }

        const char *relative = matches.gl_pathv[i];

        if(!strncmp(relative, repo_dir, strlen(repo_dir))) {
            relative += strlen(repo_dir);

            while(*relative == '/') relative++;
        }

        int artifact_id = -1;

        if(couch11_artifact_begin(cfg, job_id, relative, &artifact_id) != 0) {
            fclose(f);
            failed = 1;
            continue;
        }

        char *buf = malloc(COUCH_ARTIFACT_CHUNK);

        if(!buf) {
            fclose(f);
            failed = 1;
            continue;
        }

        int sequence = 0;
        size_t total = 0;

        for(;;) {
            size_t n = fread(buf, 1, COUCH_ARTIFACT_CHUNK, f);

            if(n) {
                if(couch11_artifact_chunk(cfg, artifact_id, sequence++, buf, n) != 0) {
                    failed = 1;
                    break;
                }

                total += n;
            }

            if(n < COUCH_ARTIFACT_CHUNK) {
                if(ferror(f)) failed = 1;
                break;
            }
        }

        fclose(f);
        free(buf);

        if(!failed && couch11_artifact_finish(cfg, artifact_id, total) != 0) failed = 1;
    }

    globfree(&matches);

    return failed ? -1 : 0;
}

static int run_build(const couch_config_t *cfg, const couch_job_t *job, const char *repo_dir, const char *workspace) {
    char definition[PATH_MAX];

    if(snprintf(definition, sizeof(definition), "%s/.couch", repo_dir) >= (int)sizeof(definition)) return 127;

    couch_build_t build;

    memset(&build, 0, sizeof(build));

    if(access(definition, R_OK) != 0) {
        return run_command(cfg, job, repo_dir, job->command);
    }

    if(parse_couch_file(definition, &build) != 0) {
        const char *msg = "couch11: invalid .couch file\n";
        couch11_log(cfg, job->id, msg, strlen(msg));
        build_free(&build);
        return 127;
    }

    char message[1024];

    snprintf(message, sizeof(message), "couch11: loaded .couch with %zu stages and %zu artifact patterns\n", build.count, build.artifact_count);
    couch11_log(cfg, job->id, message, strlen(message));

    int result = 0;

    for(size_t i = 0; i < build.count; i++) {
        snprintf(message, sizeof(message), "\n== stage %zu/%zu ==\n$ %s\n\n", i + 1, build.count, build.stages[i].command);
        couch11_log(cfg, job->id, message, strlen(message));

        printf("%s", message);

        result = run_command(cfg, job, repo_dir, build.stages[i].command);

        if(result != 0) {
            snprintf(message, sizeof(message), "\ncouch11: stage %zu failed with exit code %d\n", i + 1, result);
            couch11_log(cfg, job->id, message, strlen(message));
            break;
        }
    }

    if(result == 0) {
        for(size_t i = 0; i < build.artifact_count; i++) {
            snprintf(message, sizeof(message), "\n== artifact ==\n$ %s\n", build.artifacts[i]);
            couch11_log(cfg, job->id, message, strlen(message));

            if(upload_artifact(cfg, job->id, repo_dir, build.artifacts[i]) != 0) {
                snprintf(message, sizeof(message), "couch11: failed to upload artifact pattern %s\n", build.artifacts[i]);
                couch11_log(cfg, job->id, message, strlen(message));
                result = 1;
                break;
            }
        }
    }

    build_free(&build);

    (void)workspace;

    return result;
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

    if(!cfg.workdir[0]) {
        snprintf(cfg.workdir, sizeof(cfg.workdir), "%s/couch11", tmpdir());
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
    printf("couch11: workdir %s\n", cfg.workdir);

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

        char repo_dir[PATH_MAX];

        snprintf(repo_dir, sizeof(repo_dir), "%s/repo", workspace);

        printf("couch11: claimed job %d\n", job.id);
        printf("couch11: repository: %s\n", job.repository);
        printf("couch11: ref: %s\n", job.ref);
        printf("couch11: commit: %s\n", job.commit);
        printf("couch11: workspace: %s\n", workspace);

        if(checkout_repository(&cfg, &job, workspace) != 0) {
            couch11_complete(&cfg, job.id, 127, 0);
            free(body);
            continue;
        }

        printf("couch11: running job %d in %s\n", job.id, repo_dir);

        int exit_code = run_build(&cfg, &job, repo_dir, workspace);

        if(couch11_complete(&cfg, job.id, exit_code, 0) != 0) fprintf(stderr, "couch11: failed to report job %d\n", job.id);

        free(body);
    }

    return 0;
}
