#ifndef COUCH_H
#define COUCH_H

#include <stddef.h>
#include <sys/types.h>

#define COUCH_MAX_BODY (1024 * 1024)
#define COUCH_MAX_LINE 8192
#define COUCH_MAX_COMMAND 8192
#define COUCH_MAX_REPO 512
#define COUCH_MAX_REF 256
#define COUCH_MAX_RUNNER 128
#define COUCH_MAX_NAME 512
#define COUCH_MAX_PATH 4096
#define COUCH_ARTIFACT_CHUNK (512 * 1024)

typedef struct {
    char host[64];
    int port;
    char database[512];
    char workdir[512];
    char webdir[512];
    int poll_ms;
    int max_jobs;
    char couch_url[1024];
    char token[256];
    char runner_name[128];
} couch_config_t;

typedef struct {
    int fd;
    char method[16];
    char path[1024];
    char *body;
    size_t body_len;
} http_request_t;

typedef struct {
    int id;
    char repository[COUCH_MAX_REPO];
    char ref[COUCH_MAX_REF];
    char commit[128];
    char command[COUCH_MAX_COMMAND];
} couch_job_t;

typedef struct {
    int id;
    int job_id;
    char name[COUCH_MAX_NAME];
    size_t size;
} couch_artifact_t;

int couch_config_load(const char *path, couch_config_t *cfg);

int couch_http_read_request(int fd, http_request_t *req);
void couch_http_free_request(http_request_t *req);
int couch_http_send(int fd, int status, const char *type, const void *body, size_t len);
int couch_http_send_text(int fd, int status, const char *body);
int couch_http_send_json(int fd, int status, const char *body);
int couch_http_send_file(int fd, int status, const char *type, const char *path);

int couch_db_open(const char *path);
void couch_db_close(void);
int couch_db_init(void);
int couch_db_create_job(const char *repository, const char *ref, const char *commit, const char *command);
int couch_db_claim_job(const char *runner, couch_job_t *job);
int couch_db_append_log(int id, const char *data, size_t len);
int couch_db_complete_job(int id, int exit_code, int signal_no);
int couch_db_fail_job(int id, const char *reason);
int couch_db_register_runner(const char *name, const char *labels);
int couch_db_heartbeat_runner(const char *name);
int couch_db_list_jobs(char *out, size_t cap);
int couch_db_job_json(int id, char *out, size_t cap);
int couch_db_get_log(int id, char **out, size_t *len);

int couch_db_create_artifact(int job_id, const char *name);
int couch_db_append_artifact(int artifact_id, int sequence, const void *data, size_t len);
int couch_db_finish_artifact(int artifact_id, size_t size);
int couch_db_list_artifacts(int job_id, char *out, size_t cap);
int couch_db_get_artifact(int job_id, int artifact_id, char **name, void **data, size_t *len);

#endif
