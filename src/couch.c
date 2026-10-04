#include "couch.h"
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>

static couch_config_t cfg;

static int json_int(const char *body, const char *key, int *out) {
    char needle[128];
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    const char *p = strstr(body, needle);
    if(!p) return -1;
    p = strchr(p + strlen(needle), ':');
    if(!p) return -1;
    p++;
    while(*p == ' ' || *p == '\t') p++;
    char *end;
    long value = strtol(p, &end, 10);
    if(end == p) return -1;
    *out = (int)value;
    return 0;
}

static const char *json_string(const char *body, const char *key, char *out, size_t cap) {
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

static int path_id(const char *path, const char *prefix) {
    if(strncmp(path, prefix, strlen(prefix)) != 0) return -1;
    const char *p = path + strlen(prefix);
    if(!*p) return -1;
    return atoi(p);
}

static void *client_thread(void *arg) {
    int fd = *(int *)arg;
    free(arg);
    http_request_t req;
    if(couch_http_read_request(fd, &req) != 0) { close(fd); return NULL; }

    if(!strcmp(req.method, "GET") && !strcmp(req.path, "/health")) {
        couch_http_send_json(fd, 200, "{\"status\":\"ok\",\"service\":\"couch\"}");
    } else if(!strcmp(req.method, "GET") && !strcmp(req.path, "/api/v1/jobs")) {
        char *buf = malloc(256 * 1024);
        couch_db_list_jobs(buf, 256 * 1024);
        couch_http_send_json(fd, 200, buf);
        free(buf);
    } else if(!strcmp(req.method, "POST") && !strcmp(req.path, "/api/v1/jobs")) {
        char repo[COUCH_MAX_REPO], ref[COUCH_MAX_REF], commit[128], command[COUCH_MAX_COMMAND];
        if(!json_string(req.body, "repository", repo, sizeof(repo)) || !json_string(req.body, "ref", ref, sizeof(ref)) || !json_string(req.body, "commit", commit, sizeof(commit)) || !json_string(req.body, "command", command, sizeof(command))) {
            couch_http_send_json(fd, 400, "{\"error\":\"repository, ref, commit and command are required\"}");
        } else {
            int id = couch_db_create_job(repo, ref, commit, command);
            char out[256];
            snprintf(out, sizeof(out), "{\"id\":%d,\"status\":\"queued\"}", id);
            couch_http_send_json(fd, id > 0 ? 201 : 500, out);
        }
    } else if(!strcmp(req.method, "POST") && !strcmp(req.path, "/api/v1/runners/register")) {
        char name[COUCH_MAX_RUNNER], labels[512];
        if(!json_string(req.body, "name", name, sizeof(name))) couch_http_send_json(fd, 400, "{\"error\":\"name is required\"}");
        else {
            json_string(req.body, "labels", labels, sizeof(labels));
            couch_db_register_runner(name, labels);
            couch_http_send_json(fd, 200, "{\"status\":\"registered\"}");
        }
    } else if(!strcmp(req.method, "POST") && !strcmp(req.path, "/api/v1/runners/heartbeat")) {
        char name[COUCH_MAX_RUNNER];
        if(!json_string(req.body, "name", name, sizeof(name))) couch_http_send_json(fd, 400, "{\"error\":\"name is required\"}");
        else { couch_db_heartbeat_runner(name); couch_http_send_json(fd, 200, "{\"status\":\"ok\"}"); }
    } else if(!strcmp(req.method, "POST") && !strcmp(req.path, "/api/v1/jobs/claim")) {
        char runner[COUCH_MAX_RUNNER];
        if(!json_string(req.body, "runner", runner, sizeof(runner))) couch_http_send_json(fd, 400, "{\"error\":\"runner is required\"}");
        else {
            couch_job_t job;
            if(couch_db_claim_job(runner, &job)) {
                char out[COUCH_MAX_COMMAND + 1024];
                snprintf(out, sizeof(out), "{\"id\":%d,\"repository\":\"%s\",\"ref\":\"%s\",\"commit\":\"%s\",\"command\":\"%s\"}", job.id, job.repository, job.ref, job.commit, job.command);
                couch_http_send_json(fd, 200, out);
            } else couch_http_send_json(fd, 204, "");
        }
    } else {
        int id = path_id(req.path, "/api/v1/jobs/");
        if(id > 0 && !strcmp(req.method, "GET")) {
            char *log = NULL;
            size_t len = 0;
            if(strstr(req.path, "/log")) {
                if(couch_db_get_log(id, &log, &len) == 0) couch_http_send(fd, 200, "text/plain; charset=utf-8", log, len);
                else couch_http_send_json(fd, 404, "{\"error\":\"job not found\"}");
                free(log);
            } else {
                char out[4096];
                if(couch_db_job_json(id, out, sizeof(out)) == 0) couch_http_send_json(fd, 200, out);
                else couch_http_send_json(fd, 404, "{\"error\":\"job not found\"}");
            }
        } else if(id > 0 && !strcmp(req.method, "POST") && strstr(req.path, "/log")) {
            couch_db_append_log(id, req.body, req.body_len);
            couch_http_send_json(fd, 200, "{\"status\":\"ok\"}");
        } else if(id > 0 && !strcmp(req.method, "POST") && strstr(req.path, "/complete")) {
            char code[32], sig[32];
	    int ec = 127, sn = 0;
	    json_int(req.body, "exit_code", &ec);
	    json_int(req.body, "signal", &sn);
            couch_db_complete_job(id, ec, sn);
            couch_http_send_json(fd, 200, "{\"status\":\"recorded\"}");
        } else couch_http_send_json(fd, 404, "{\"error\":\"not found\"}");
    }

    couch_http_free_request(&req);
    close(fd);
    return NULL;
}

int main(int argc, char **argv) {
    const char *config = argc > 1 ? argv[1] : "couch.conf";
    if(couch_config_load(config, &cfg) != 0) { fprintf(stderr, "couch: cannot read %s\n", config); return 1; }
    if(couch_db_open(cfg.database) != 0 || couch_db_init() != 0) { fprintf(stderr, "couch: database initialization failed\n"); return 1; }

    int server = socket(AF_INET, SOCK_STREAM, 0);
    if(server < 0) { perror("socket"); return 1; }
    int yes = 1;
    setsockopt(server, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)cfg.port);
    if(inet_pton(AF_INET, cfg.host, &addr.sin_addr) != 1) { fprintf(stderr, "couch: invalid host\n"); return 1; }
    if(bind(server, (struct sockaddr *)&addr, sizeof(addr)) != 0 || listen(server, 64) != 0) { perror("bind/listen"); return 1; }

    printf("couch: listening on %s:%d\n", cfg.host, cfg.port);
    for(;;) {
        int fd = accept(server, NULL, NULL);
        if(fd < 0) { if(errno == EINTR) continue; perror("accept"); break; }
        int *p = malloc(sizeof(*p));
        if(!p) { close(fd); continue; }
        *p = fd;
        pthread_t thread;
        if(pthread_create(&thread, NULL, client_thread, p) == 0) pthread_detach(thread);
        else { free(p); close(fd); }
    }
    close(server);
    couch_db_close();
    return 0;
}
