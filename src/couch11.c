#define _POSIX_C_SOURCE 200809L

#include <couch.h>
#include <tomlc17.h>

#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include <dirent.h>
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

#define PIPE_MAX_FILES 128
#define PIPE_MAX_STEPS 64
#define PIPE_MAX_ENV 64
#define PIPE_MAX_NEEDS 32
#define PIPE_MAX_LABELS 32
#define PIPE_MAX_RETRIES 10
#define PIPE_MAX_TIMEOUT 86400

typedef struct {
    char name[128];
    char command[COUCH_MAX_COMMAND];
    char cwd[COUCH_MAX_PATH];
    int timeout;
    char env_name[PIPE_MAX_ENV][128];
    char env_value[PIPE_MAX_ENV][1024];
    size_t env_count;
} couch_step_t;

typedef struct {
    char name[128];
    char when[16];
    int allow_failure;
    int timeout;
    int retries;
    char needs[PIPE_MAX_NEEDS][128];
    size_t need_count;
    char labels[PIPE_MAX_LABELS][128];
    size_t label_count;
    couch_step_t steps[PIPE_MAX_STEPS];
    size_t step_count;
    char **artifacts;
    size_t artifact_count;
    size_t artifact_cap;
    char **cache_paths;
    size_t cache_count;
    size_t cache_cap;
    char source[PATH_MAX];
    int status;
} couch_stage_t;

typedef struct {
    couch_stage_t *stages;
    size_t count;
    size_t cap;
} couch_build_t;

static const char *tmpdir(void) {
    const char *tmp = getenv("TMPDIR");
    return tmp && *tmp ? tmp : "/tmp";
}

static void log_text(const couch_config_t *cfg, int id, const char *text) {
    if(couch11_log(cfg, id, text, strlen(text)) != 0) fprintf(stderr, "couch11: failed to upload log for job %d\n", id);
}

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
        if(*p == '\\' && p[1]) {
            p++;
            if(*p == 'n') out[i++] = '\n';
            else if(*p == 'r') out[i++] = '\r';
            else if(*p == 't') out[i++] = '\t';
            else out[i++] = *p;
            p++;
        } else out[i++] = *p++;
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
    if(errno == EEXIST) {
        struct stat st;
        return stat(path, &st) == 0 && S_ISDIR(st.st_mode) ? 0 : -1;
    }
    return -1;
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
    if(cfg->workdir[0]) snprintf(root, sizeof(root), "%s", cfg->workdir);
    else snprintf(root, sizeof(root), "%s/couch11", tmpdir());
    if(snprintf(jobs, sizeof(jobs), "%s/jobs", root) >= (int)sizeof(jobs)) return -1;
    if(snprintf(path, cap, "%s/%d", jobs, id) >= (int)cap) return -1;
    return mkdir_recursive(jobs) == 0 && mkdir_one(path) == 0 ? 0 : -1;
}

static void stage_free(couch_stage_t *stage) {
    for(size_t i = 0; i < stage->artifact_count; i++) free(stage->artifacts[i]);
    for(size_t i = 0; i < stage->cache_count; i++) free(stage->cache_paths[i]);
    free(stage->artifacts);
    free(stage->cache_paths);
    memset(stage, 0, sizeof(*stage));
}

static void build_free(couch_build_t *build) {
    for(size_t i = 0; i < build->count; i++) stage_free(&build->stages[i]);
    free(build->stages);
    memset(build, 0, sizeof(*build));
}

static int add_string(char ***items, size_t *count, size_t *cap, const char *value) {
    if(*count == *cap) {
        size_t next = *cap ? *cap * 2 : 8;
        char **p = realloc(*items, next * sizeof(*p));
        if(!p) return -1;
        *items = p;
        *cap = next;
    }
    (*items)[*count] = strdup(value);
    if(!(*items)[*count]) return -1;
    (*count)++;
    return 0;
}

static int build_add_stage(couch_build_t *build, couch_stage_t *stage) {
    if(build->count == build->cap) {
        size_t next = build->cap ? build->cap * 2 : 8;
        couch_stage_t *p = realloc(build->stages, next * sizeof(*p));
        if(!p) return -1;
        build->stages = p;
        build->cap = next;
    }
    build->stages[build->count++] = *stage;
    memset(stage, 0, sizeof(*stage));
    return 0;
}

static int get_string(toml_datum_t table, const char *key, const char **out, const char *file) {
    toml_datum_t value = toml_get(table, key);
    if(value.type == TOML_UNKNOWN) return 0;
    if(value.type != TOML_STRING) {
        fprintf(stderr, "couch11: %s in %s must be a string\n", key, file);
        return -1;
    }
    *out = value.u.s;
    return 1;
}

static int get_bool(toml_datum_t table, const char *key, int *out, const char *file) {
    toml_datum_t value = toml_get(table, key);
    if(value.type == TOML_UNKNOWN) return 0;
    if(value.type != TOML_BOOLEAN) {
        fprintf(stderr, "couch11: %s in %s must be a boolean\n", key, file);
        return -1;
    }
    *out = value.u.boolean;
    return 1;
}

static int get_int(toml_datum_t table, const char *key, int *out, const char *file) {
    toml_datum_t value = toml_get(table, key);
    if(value.type == TOML_UNKNOWN) return 0;
    if(value.type != TOML_INT64 || value.u.int64 < 0 || value.u.int64 > INT_MAX) {
        fprintf(stderr, "couch11: %s in %s must be a non-negative integer\n", key, file);
        return -1;
    }
    *out = (int)value.u.int64;
    return 1;
}

static int parse_string_array(toml_datum_t array, char ***items, size_t *count, size_t *cap, const char *field, const char *file) {
    if(array.type == TOML_UNKNOWN) return 0;
    if(array.type != TOML_ARRAY) {
        fprintf(stderr, "couch11: %s in %s must be an array of strings\n", field, file);
        return -1;
    }
    for(int i = 0; i < array.u.arr.size; i++) {
        toml_datum_t item = array.u.arr.elem[i];
        if(item.type != TOML_STRING || !item.u.s[0] || add_string(items, count, cap, item.u.s) != 0) {
            fprintf(stderr, "couch11: invalid %s entry in %s\n", field, file);
            return -1;
        }
    }
    return 0;
}

static int parse_fixed_array(toml_datum_t array, char dst[][128], size_t *count, size_t max, const char *field, const char *file) {
    if(array.type == TOML_UNKNOWN) return 0;
    if(array.type != TOML_ARRAY) {
        fprintf(stderr, "couch11: %s in %s must be an array of strings\n", field, file);
        return -1;
    }
    for(int i = 0; i < array.u.arr.size; i++) {
        toml_datum_t item = array.u.arr.elem[i];
        if(item.type != TOML_STRING || !item.u.s[0] || *count >= max || strlen(item.u.s) >= sizeof(dst[0])) {
            fprintf(stderr, "couch11: invalid %s entry in %s\n", field, file);
            return -1;
        }
        snprintf(dst[*count], sizeof(dst[0]), "%s", item.u.s);
        (*count)++;
    }
    return 0;
}

static int parse_env(toml_datum_t env, couch_step_t *step, const char *file) {
    if(env.type == TOML_UNKNOWN) return 0;
    if(env.type != TOML_TABLE) {
        fprintf(stderr, "couch11: env in %s must be a table\n", file);
        return -1;
    }
    for(int i = 0; i < env.u.tab.size; i++) {
        const char *key = env.u.tab.key[i];
        toml_datum_t value = env.u.tab.value[i];
        if(value.type != TOML_STRING || step->env_count >= PIPE_MAX_ENV || strlen(key) >= sizeof(step->env_name[0]) || strlen(value.u.s) >= sizeof(step->env_value[0])) {
            fprintf(stderr, "couch11: invalid environment entry in %s\n", file);
            return -1;
        }
        for(const char *p = key; *p; p++) if(!( (*p >= 'A' && *p <= 'Z') || (*p >= 'a' && *p <= 'z') || (*p >= '0' && *p <= '9') || *p == '_')) {
            fprintf(stderr, "couch11: invalid environment variable name '%s' in %s\n", key, file);
            return -1;
        }
        snprintf(step->env_name[step->env_count], sizeof(step->env_name[0]), "%s", key);
        snprintf(step->env_value[step->env_count], sizeof(step->env_value[0]), "%s", value.u.s);
        step->env_count++;
    }
    return 0;
}

static int parse_stage_file(const char *path, couch_stage_t *stage) {
    memset(stage, 0, sizeof(*stage));
    stage->status = -1;
    snprintf(stage->when, sizeof(stage->when), "success");
    snprintf(stage->source, sizeof(stage->source), "%s", path);

    toml_result_t parsed = toml_parse_file_ex(path);
    if(!parsed.ok) {
        fprintf(stderr, "couch11: TOML parse error in %s: %s\n", path, parsed.errmsg);
        toml_free(parsed);
        return -1;
    }

    toml_datum_t root = parsed.toptab;
    toml_datum_t meta = toml_get(root, "stage");
    if(meta.type != TOML_TABLE) {
        fprintf(stderr, "couch11: %s needs a [stage] table\n", path);
        toml_free(parsed);
        return -1;
    }

    const char *name = NULL;
    const char *when = NULL;
    int rc = get_string(meta, "name", &name, path);
    if(rc <= 0 || !name || !*name || strlen(name) >= sizeof(stage->name)) {
        fprintf(stderr, "couch11: [stage].name is required in %s\n", path);
        toml_free(parsed);
        return -1;
    }
    snprintf(stage->name, sizeof(stage->name), "%s", name);

    rc = get_string(meta, "when", &when, path);
    if(rc < 0 || (when && strcmp(when, "success") && strcmp(when, "always"))) {
        fprintf(stderr, "couch11: [stage].when must be 'success' or 'always' in %s\n", path);
        toml_free(parsed);
        return -1;
    }
    if(when) snprintf(stage->when, sizeof(stage->when), "%s", when);

    rc = get_bool(meta, "allow_failure", &stage->allow_failure, path);
    if(rc < 0) { toml_free(parsed); return -1; }
    rc = get_int(meta, "timeout", &stage->timeout, path);
    if(rc < 0 || stage->timeout > PIPE_MAX_TIMEOUT) { toml_free(parsed); return -1; }
    rc = get_int(meta, "retries", &stage->retries, path);
    if(rc < 0 || stage->retries > PIPE_MAX_RETRIES) { toml_free(parsed); return -1; }

    if(parse_fixed_array(toml_get(meta, "needs"), stage->needs, &stage->need_count, PIPE_MAX_NEEDS, "stage.needs", path) != 0 || parse_fixed_array(toml_get(meta, "labels"), stage->labels, &stage->label_count, PIPE_MAX_LABELS, "stage.labels", path) != 0) {
        toml_free(parsed);
        return -1;
    }

    toml_datum_t steps = toml_get(root, "steps");
    if(steps.type != TOML_ARRAY || steps.u.arr.size < 1 || steps.u.arr.size > PIPE_MAX_STEPS) {
        fprintf(stderr, "couch11: %s needs 1-%d [[steps]] entries\n", path, PIPE_MAX_STEPS);
        toml_free(parsed);
        return -1;
    }

    for(int i = 0; i < steps.u.arr.size; i++) {
        toml_datum_t item = steps.u.arr.elem[i];
        if(item.type != TOML_TABLE) { toml_free(parsed); return -1; }
        couch_step_t *step = &stage->steps[stage->step_count];
        const char *step_name = NULL;
        const char *command = NULL;
        const char *cwd = NULL;

        rc = get_string(item, "name", &step_name, path);
        if(rc < 0 || (step_name && strlen(step_name) >= sizeof(step->name))) { toml_free(parsed); return -1; }
        rc = get_string(item, "run", &command, path);
        if(rc <= 0 || !command || !*command || strlen(command) >= sizeof(step->command)) {
            fprintf(stderr, "couch11: each step needs a valid run string in %s\n", path);
            toml_free(parsed);
            return -1;
        }

        snprintf(step->name, sizeof(step->name), "%s", step_name && *step_name ? step_name : "step");
        if(!step_name) snprintf(step->name, sizeof(step->name), "step-%d", i + 1);
        snprintf(step->command, sizeof(step->command), "%s", command);

        rc = get_string(item, "working_directory", &cwd, path);
        if(rc < 0 || (cwd && strlen(cwd) >= sizeof(step->cwd))) { toml_free(parsed); return -1; }
        if(cwd) snprintf(step->cwd, sizeof(step->cwd), "%s", cwd);

        rc = get_int(item, "timeout", &step->timeout, path);
        if(rc < 0 || step->timeout > PIPE_MAX_TIMEOUT || parse_env(toml_get(item, "env"), step, path) != 0) { toml_free(parsed); return -1; }
        if(!step->timeout) step->timeout = stage->timeout;
        stage->step_count++;
    }

    toml_datum_t artifacts = toml_get(root, "artifacts");
    if(artifacts.type != TOML_UNKNOWN && artifacts.type != TOML_TABLE) { toml_free(parsed); return -1; }
    if(artifacts.type == TOML_TABLE && parse_string_array(toml_get(artifacts, "paths"), &stage->artifacts, &stage->artifact_count, &stage->artifact_cap, "artifacts.paths", path) != 0) { toml_free(parsed); return -1; }

    toml_datum_t cache = toml_get(root, "cache");
    if(cache.type != TOML_UNKNOWN && cache.type != TOML_TABLE) { toml_free(parsed); return -1; }
    if(cache.type == TOML_TABLE && parse_string_array(toml_get(cache, "paths"), &stage->cache_paths, &stage->cache_count, &stage->cache_cap, "cache.paths", path) != 0) { toml_free(parsed); return -1; }

    toml_free(parsed);
    return 0;
}

static int path_compare(const void *a, const void *b) {
    return strcmp(*(char *const *)a, *(char *const *)b);
}

static int load_pipeline(const char *directory, couch_build_t *build) {
    DIR *dir = opendir(directory);
    if(!dir) return -1;

    char **files = NULL;
    size_t count = 0;
    size_t cap = 0;
    struct dirent *entry;

    while((entry = readdir(dir)) != NULL) {
        size_t len = strlen(entry->d_name);
        if(len <= 5 || strcmp(entry->d_name + len - 5, ".toml")) continue;
        if(count >= PIPE_MAX_FILES) {
            fprintf(stderr, "couch11: too many pipeline files (maximum %d)\n", PIPE_MAX_FILES);
            closedir(dir);
            for(size_t i = 0; i < count; i++) free(files[i]);
            free(files);
            return -1;
        }
        char path[PATH_MAX];
        if(snprintf(path, sizeof(path), "%s/%s", directory, entry->d_name) >= (int)sizeof(path)) continue;
        struct stat st;
        if(stat(path, &st) != 0 || !S_ISREG(st.st_mode)) continue;
        if(add_string(&files, &count, &cap, path) != 0) {
            closedir(dir);
            for(size_t i = 0; i < count; i++) free(files[i]);
            free(files);
            return -1;
        }
    }
    closedir(dir);

    if(!count) {
        fprintf(stderr, "couch11: no .toml files found in %s\n", directory);
        free(files);
        return -1;
    }

    qsort(files, count, sizeof(*files), path_compare);

    for(size_t i = 0; i < count; i++) {
        couch_stage_t stage;
        if(parse_stage_file(files[i], &stage) != 0 || build_add_stage(build, &stage) != 0) {
            stage_free(&stage);
            for(size_t j = 0; j < count; j++) free(files[j]);
            free(files);
            return -1;
        }
    }
    for(size_t i = 0; i < count; i++) free(files[i]);
    free(files);

    for(size_t i = 0; i < build->count; i++) {
        couch_stage_t *stage = &build->stages[i];
        for(size_t j = 0; j < i; j++) if(!strcmp(stage->name, build->stages[j].name)) {
            fprintf(stderr, "couch11: duplicate stage name '%s'\n", stage->name);
            return -1;
        }
        for(size_t n = 0; n < stage->need_count; n++) {
            int found = 0;
            for(size_t j = 0; j < i; j++) if(!strcmp(stage->needs[n], build->stages[j].name)) found = 1;
            if(!found) {
                fprintf(stderr, "couch11: stage '%s' depends on unknown or later stage '%s'\n", stage->name, stage->needs[n]);
                return -1;
            }
        }
    }
    return 0;
}

static int run_process(const couch_config_t *cfg, int id, const char *cwd, const char *const argv[]) {
    int pipefd[2];
    if(pipe(pipefd) != 0) return 127;
    pid_t pid = fork();
    if(pid < 0) { close(pipefd[0]); close(pipefd[1]); return 127; }

    if(pid == 0) {
        setpgid(0, 0);
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

    setpgid(pid, pid);
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
    while(waitpid(pid, &status, 0) < 0) if(errno != EINTR) return 127;
    if(WIFEXITED(status)) return WEXITSTATUS(status);
    if(WIFSIGNALED(status)) return 128 + WTERMSIG(status);
    return 127;
}

static int run_command(const couch_config_t *cfg, const couch_job_t *job, const char *repo_dir, const char *command, const char *cwd, int timeout, const couch_step_t *step) {
    int pipefd[2];
    if(pipe(pipefd) != 0) return 127;
    pid_t pid = fork();
    if(pid < 0) { close(pipefd[0]); close(pipefd[1]); return 127; }

    if(pid == 0) {
        setpgid(0, 0);
        int nullfd = open("/dev/null", O_RDONLY);
        if(nullfd >= 0) dup2(nullfd, STDIN_FILENO);
        dup2(pipefd[1], STDOUT_FILENO);
        dup2(pipefd[1], STDERR_FILENO);
        close(pipefd[0]);
        close(pipefd[1]);
        if(nullfd > STDERR_FILENO) close(nullfd);
        if(chdir(cwd && *cwd ? cwd : repo_dir) != 0) _exit(127);
        if(step) for(size_t i = 0; i < step->env_count; i++) if(setenv(step->env_name[i], step->env_value[i], 1) != 0) _exit(126);
        char *const args[] = {"/bin/sh", "-c", (char *)command, NULL};
        execve("/bin/sh", args, environ);
        _exit(127);
    }

    setpgid(pid, pid);
    close(pipefd[1]);
    int flags = fcntl(pipefd[0], F_GETFL, 0);
    if(flags >= 0) fcntl(pipefd[0], F_SETFL, flags | O_NONBLOCK);

    time_t start = time(NULL);
    int status = 0;
    int child_done = 0;
    int pipe_done = 0;
    int timed_out = 0;
    char buf[8192];

    while(!child_done || !pipe_done) {
        ssize_t n = read(pipefd[0], buf, sizeof(buf));
        if(n > 0) {
            if(couch11_log(cfg, job->id, buf, (size_t)n) != 0) fprintf(stderr, "couch11: failed to upload log for job %d\n", job->id);
            fwrite(buf, 1, (size_t)n, stdout);
            fflush(stdout);
        } else if(n == 0) pipe_done = 1;
        else if(errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) pipe_done = 1;

        if(!child_done) {
            pid_t rc = waitpid(pid, &status, WNOHANG);
            if(rc == pid) child_done = 1;
            else if(rc < 0 && errno != EINTR) { child_done = 1; status = 0; }
        }

        if(timeout > 0 && time(NULL) - start >= timeout && (!child_done || !pipe_done)) {
            timed_out = 1;
            kill(-pid, SIGTERM);
            struct timespec grace = {0, 250000000L};
            nanosleep(&grace, NULL);
            kill(-pid, SIGKILL);
            if(!child_done) while(waitpid(pid, &status, 0) < 0 && errno == EINTR) {}
            child_done = 1;
            pipe_done = 1;
            char message[256];
            snprintf(message, sizeof(message), "couch11: step timed out after %d seconds\n", timeout);
            log_text(cfg, job->id, message);
        }

        if(!child_done || !pipe_done) {
            struct timespec pause = {0, 20000000L};
            nanosleep(&pause, NULL);
        }
    }

    close(pipefd[0]);
    if(timed_out) return 124;
    if(WIFEXITED(status)) return WEXITSTATUS(status);
    if(WIFSIGNALED(status)) return 128 + WTERMSIG(status);
    return 127;
}

static int checkout_repository(const couch_config_t *cfg, const couch_job_t *job, const char *workspace) {
    char repo_dir[PATH_MAX];
    if(snprintf(repo_dir, sizeof(repo_dir), "%s/repo", workspace) >= (int)sizeof(repo_dir)) return -1;

    const char *clone_args[] = {"/usr/bin/git", "clone", "--no-checkout", job->repository, repo_dir, NULL};
    if(run_process(cfg, job->id, NULL, clone_args) != 0) { log_text(cfg, job->id, "couch11: git clone failed\n"); return -1; }

    const char *fetch_args[] = {"/usr/bin/git", "-C", repo_dir, "fetch", "--force", "origin", job->ref, NULL};
    if(run_process(cfg, job->id, NULL, fetch_args) != 0) { log_text(cfg, job->id, "couch11: git fetch failed\n"); return -1; }

    const char *checkout_args[] = {"/usr/bin/git", "-C", repo_dir, "checkout", "--detach", job->commit, NULL};
    if(run_process(cfg, job->id, NULL, checkout_args) != 0) { log_text(cfg, job->id, "couch11: git checkout failed\n"); return -1; }
    return 0;
}

static int upload_artifact(const couch_config_t *cfg, int job_id, const char *repo_dir, const char *path) {
    char pattern[PATH_MAX];
    if(path[0] == '/') snprintf(pattern, sizeof(pattern), "%s", path);
    else if(snprintf(pattern, sizeof(pattern), "%s/%s", repo_dir, path) >= (int)sizeof(pattern)) return -1;

    glob_t matches;
    memset(&matches, 0, sizeof(matches));
    int rc = glob(pattern, GLOB_TILDE, NULL, &matches);
    if(rc == GLOB_NOMATCH) { globfree(&matches); return 0; }
    if(rc != 0) { globfree(&matches); return -1; }

    int failed = 0;
    for(size_t i = 0; i < matches.gl_pathc; i++) {
        struct stat st;
        if(stat(matches.gl_pathv[i], &st) != 0 || !S_ISREG(st.st_mode)) continue;

        FILE *file = fopen(matches.gl_pathv[i], "rb");
        if(!file) { failed = 1; continue; }

        const char *name = matches.gl_pathv[i];
        if(!strncmp(name, repo_dir, strlen(repo_dir)) && (name[strlen(repo_dir)] == '/' || name[strlen(repo_dir)] == '\0')) {
            name += strlen(repo_dir);
            while(*name == '/') name++;
        }

        int artifact_id = -1;
        if(couch11_artifact_begin(cfg, job_id, name, &artifact_id) != 0) { fclose(file); failed = 1; continue; }

        char *buf = malloc(COUCH_ARTIFACT_CHUNK);
        if(!buf) { fclose(file); failed = 1; continue; }

        int sequence = 0;
        size_t total = 0;
        int file_failed = 0;

        for(;;) {
            size_t n = fread(buf, 1, COUCH_ARTIFACT_CHUNK, file);
            if(n) {
                if(couch11_artifact_chunk(cfg, artifact_id, sequence++, buf, n) != 0) { file_failed = 1; break; }
                total += n;
            }
            if(n < COUCH_ARTIFACT_CHUNK) {
                if(ferror(file)) file_failed = 1;
                break;
            }
        }

        fclose(file);
        free(buf);
        if(!file_failed && couch11_artifact_finish(cfg, artifact_id, total) != 0) file_failed = 1;
        if(file_failed) failed = 1;
    }

    globfree(&matches);
    return failed ? -1 : 0;
}

static int stage_needs_success(const couch_build_t *build, const couch_stage_t *stage) {
    for(size_t n = 0; n < stage->need_count; n++) {
        int found = 0;
        for(size_t i = 0; i < build->count; i++) if(!strcmp(build->stages[i].name, stage->needs[n])) {
            found = 1;
            if(build->stages[i].status != 0) return 0;
            break;
        }
        if(!found) return 0;
    }
    return 1;
}

static int runner_has_label(const char *label) {
    return !strcmp(label, "linux") || !strcmp(label, "x86_64");
}

static int stage_labels_match(const couch_stage_t *stage) {
    for(size_t i = 0; i < stage->label_count; i++) if(!runner_has_label(stage->labels[i])) return 0;
    return 1;
}

static int run_stage(const couch_config_t *cfg, const couch_job_t *job, const char *repo_dir, couch_stage_t *stage) {
    char message[1024];
    snprintf(message, sizeof(message), "\n== stage: %s ==\n", stage->name);
    log_text(cfg, job->id, message);
    printf("%s", message);

    for(size_t i = 0; i < stage->step_count; i++) {
        couch_step_t *step = &stage->steps[i];
        int result = 127;

        for(int attempt = 0; attempt <= stage->retries; attempt++) {
            snprintf(message, sizeof(message), "\n-- step %zu/%zu: %s (attempt %d/%d) --\n$ %s\n", i + 1, stage->step_count, step->name, attempt + 1, stage->retries + 1, step->command);
            log_text(cfg, job->id, message);
            printf("%s", message);

            char cwd[PATH_MAX];
            if(step->cwd[0] == '/') snprintf(cwd, sizeof(cwd), "%s", step->cwd);
            else if(step->cwd[0] && snprintf(cwd, sizeof(cwd), "%s/%s", repo_dir, step->cwd) >= (int)sizeof(cwd)) return 127;
            else if(step->cwd[0]) snprintf(cwd, sizeof(cwd), "%s/%s", repo_dir, step->cwd);
            else snprintf(cwd, sizeof(cwd), "%s", repo_dir);

            result = run_command(cfg, job, repo_dir, step->command, cwd, step->timeout, step);
            if(result == 0) break;

            snprintf(message, sizeof(message), "couch11: step '%s' failed with exit code %d\n", step->name, result);
            log_text(cfg, job->id, message);
        }
        if(result != 0) return result;
    }
    return 0;
}

static int run_pipeline(const couch_config_t *cfg, const couch_job_t *job, const char *repo_dir, couch_build_t *build) {
    int failed = 0;
    char message[1024];

    for(size_t i = 0; i < build->count; i++) {
        couch_stage_t *stage = &build->stages[i];

        if(!stage_labels_match(stage)) {
            snprintf(message, sizeof(message), "couch11: skipping stage '%s': this runner lacks a required label\n", stage->name);
            log_text(cfg, job->id, message);
            stage->status = 125;
            if(!stage->allow_failure) failed = 1;
            continue;
        }

        if(failed && strcmp(stage->when, "always")) {
            snprintf(message, sizeof(message), "couch11: skipping stage '%s' after an earlier failure\n", stage->name);
            log_text(cfg, job->id, message);
            stage->status = 125;
            continue;
        }

        if(!stage_needs_success(build, stage)) {
            snprintf(message, sizeof(message), "couch11: skipping stage '%s': a dependency did not succeed\n", stage->name);
            log_text(cfg, job->id, message);
            stage->status = 125;
            if(!stage->allow_failure) failed = 1;
            continue;
        }

        stage->status = run_stage(cfg, job, repo_dir, stage);
        if(stage->status != 0) {
            snprintf(message, sizeof(message), "couch11: stage '%s' failed with exit code %d%s\n", stage->name, stage->status, stage->allow_failure ? " (allowed)" : "");
            log_text(cfg, job->id, message);
            if(stage->allow_failure) stage->status = 0;
            else failed = 1;
        }

        for(size_t a = 0; a < stage->artifact_count; a++) {
            snprintf(message, sizeof(message), "couch11: collecting artifacts for '%s': %s\n", stage->name, stage->artifacts[a]);
            log_text(cfg, job->id, message);
            if(upload_artifact(cfg, job->id, repo_dir, stage->artifacts[a]) != 0) {
                snprintf(message, sizeof(message), "couch11: artifact upload failed: %s\n", stage->artifacts[a]);
                log_text(cfg, job->id, message);
                failed = 1;
            }
        }

        if(stage->cache_count) {
            snprintf(message, sizeof(message), "couch11: cache paths for stage '%s' are declared, but persistent caching is not implemented yet\n", stage->name);
            log_text(cfg, job->id, message);
        }
    }

    return failed ? 1 : 0;
}

static int run_build(const couch_config_t *cfg, const couch_job_t *job, const char *repo_dir, const char *workspace) {
    char definition[PATH_MAX];
    if(snprintf(definition, sizeof(definition), "%s/.couch", repo_dir) >= (int)sizeof(definition)) return 127;

    struct stat st;
    if(stat(definition, &st) != 0) return run_command(cfg, job, repo_dir, job->command, repo_dir, 0, NULL);
    if(!S_ISDIR(st.st_mode)) {
        log_text(cfg, job->id, "couch11: .couch is not a directory; falling back to the job command\n");
        return run_command(cfg, job, repo_dir, job->command, repo_dir, 0, NULL);
    }

    couch_build_t build;
    memset(&build, 0, sizeof(build));
    if(load_pipeline(definition, &build) != 0) {
        log_text(cfg, job->id, "couch11: pipeline validation failed; no stages were executed\n");
        build_free(&build);
        return 127;
    }

    char message[256];
    snprintf(message, sizeof(message), "couch11: validated %zu pipeline stage(s)\n", build.count);
    log_text(cfg, job->id, message);

    int result = run_pipeline(cfg, job, repo_dir, &build);
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
    if(!cfg.workdir[0]) snprintf(cfg.workdir, sizeof(cfg.workdir), "%s/couch11", tmpdir());
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

        if(result < 0) fprintf(stderr, "couch11: unable to contact couch\n");
        if(result <= 0) {
            struct timespec pause = {cfg.poll_ms / 1000, (cfg.poll_ms % 1000) * 1000000L};
            nanosleep(&pause, NULL);
            continue;
        }

        couch_job_t job;
        memset(&job, 0, sizeof(job));
        job.id = json_int_local(body, "id");

        if(job.id < 0 || !json_string_local(body, "repository", job.repository, sizeof(job.repository)) || !json_string_local(body, "ref", job.ref, sizeof(job.ref)) || !json_string_local(body, "commit", job.commit, sizeof(job.commit)) || !json_string_local(body, "command", job.command, sizeof(job.command))) {
            fprintf(stderr, "couch11: invalid job: %s\n", body ? body : "(empty response)");
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
        if(snprintf(repo_dir, sizeof(repo_dir), "%s/repo", workspace) >= (int)sizeof(repo_dir)) {
            couch11_complete(&cfg, job.id, 127, 0);
            free(body);
            continue;
        }

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
