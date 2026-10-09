#include "couch.h"
#include <curl/curl.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

typedef struct {
    char *data;
    size_t len;
} response_t;

static size_t write_cb(char *ptr, size_t size, size_t nmemb, void *userdata) {
    response_t *r = userdata;
    size_t n = size * nmemb;

    char *p = realloc(r->data, r->len + n + 1);

    if(!p) return 0;

    r->data = p;

    memcpy(r->data + r->len, ptr, n);

    r->len += n;
    r->data[r->len] = 0;

    return n;
}

static int request(const char *url, const char *method, const char *token, const char *body, const char *type, long *code, response_t *response) {
    CURL *c = curl_easy_init();

    if(!c) return -1;

    struct curl_slist *headers = NULL;

    if(type) {
        char content_type[256];
        snprintf(content_type, sizeof(content_type), "Content-Type: %s", type);
        headers = curl_slist_append(headers, content_type);
    }

    char auth[512];

    if(token && *token) {
        snprintf(auth, sizeof(auth), "Authorization: Bearer %s", token);
        headers = curl_slist_append(headers, auth);
    }

    memset(response, 0, sizeof(*response));

    curl_easy_setopt(c, CURLOPT_URL, url);
    curl_easy_setopt(c, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, write_cb);
    curl_easy_setopt(c, CURLOPT_WRITEDATA, response);
    curl_easy_setopt(c, CURLOPT_CONNECTTIMEOUT, 10L);
    curl_easy_setopt(c, CURLOPT_TIMEOUT, 120L);

    if(!strcmp(method, "POST")) {
        curl_easy_setopt(c, CURLOPT_POST, 1L);
        curl_easy_setopt(c, CURLOPT_POSTFIELDS, body ? body : "");
        curl_easy_setopt(c, CURLOPT_POSTFIELDSIZE, body ? (long)strlen(body) : 0L);
    }

    CURLcode rc = curl_easy_perform(c);

    curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, code);

    curl_slist_free_all(headers);
    curl_easy_cleanup(c);

    return rc == CURLE_OK ? 0 : -1;
}

static int request_binary(const char *url, const char *token, const void *data, size_t len, long *code, response_t *response) {
    CURL *c = curl_easy_init();

    if(!c) return -1;

    struct curl_slist *headers = NULL;

    headers = curl_slist_append(headers, "Content-Type: application/octet-stream");

    char auth[512];

    if(token && *token) {
        snprintf(auth, sizeof(auth), "Authorization: Bearer %s", token);
        headers = curl_slist_append(headers, auth);
    }

    memset(response, 0, sizeof(*response));

    curl_easy_setopt(c, CURLOPT_URL, url);
    curl_easy_setopt(c, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, write_cb);
    curl_easy_setopt(c, CURLOPT_WRITEDATA, response);
    curl_easy_setopt(c, CURLOPT_CONNECTTIMEOUT, 10L);
    curl_easy_setopt(c, CURLOPT_TIMEOUT, 120L);
    curl_easy_setopt(c, CURLOPT_POST, 1L);
    curl_easy_setopt(c, CURLOPT_POSTFIELDS, data);
    curl_easy_setopt(c, CURLOPT_POSTFIELDSIZE_LARGE, (curl_off_t)len);

    CURLcode rc = curl_easy_perform(c);

    curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, code);

    curl_slist_free_all(headers);
    curl_easy_cleanup(c);

    return rc == CURLE_OK ? 0 : -1;
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

static int post_json(const couch_config_t *cfg, const char *path, const char *body, response_t *r) {
    char url[2048];

    snprintf(url, sizeof(url), "%s%s", cfg->couch_url, path);

    long code = 0;

    int rc = request(url, "POST", cfg->token, body, "application/json", &code, r);

    return rc == 0 && code >= 200 && code < 300 ? 0 : -1;
}

int couch11_register(const couch_config_t *cfg) {
    char body[1024];

    snprintf(body, sizeof(body), "{\"name\":\"%s\",\"labels\":\"linux,x86_64\"}", cfg->runner_name);

    response_t r;

    int rc = post_json(cfg, "/api/v1/runners/register", body, &r);

    free(r.data);

    return rc;
}

int couch11_heartbeat(const couch_config_t *cfg) {
    char body[512];

    snprintf(body, sizeof(body), "{\"name\":\"%s\"}", cfg->runner_name);

    response_t r;

    int rc = post_json(cfg, "/api/v1/runners/heartbeat", body, &r);

    free(r.data);

    return rc;
}

int couch11_claim(const couch_config_t *cfg, char **body) {
    char req[512];

    snprintf(req, sizeof(req), "{\"runner\":\"%s\"}", cfg->runner_name);

    response_t r;

    if(post_json(cfg, "/api/v1/jobs/claim", req, &r) != 0) {
        free(r.data);
        return -1;
    }

    if(!r.data || !*r.data) {
        free(r.data);
        *body = NULL;
        return 0;
    }

    *body = r.data;

    return 1;
}

int couch11_log(const couch_config_t *cfg, int id, const char *data, size_t len) {
    char path[128];

    snprintf(path, sizeof(path), "/api/v1/jobs/%d/log", id);

    response_t r;

    char *copy = malloc(len + 1);

    if(!copy) return -1;

    memcpy(copy, data, len);
    copy[len] = 0;

    int rc = post_json(cfg, path, copy, &r);

    free(copy);
    free(r.data);

    return rc;
}

int couch11_complete(const couch_config_t *cfg, int id, int exit_code, int signal_no) {
    char path[128];
    char body[256];

    snprintf(path, sizeof(path), "/api/v1/jobs/%d/complete", id);
    snprintf(body, sizeof(body), "{\"exit_code\":%d,\"signal\":%d}", exit_code, signal_no);

    response_t r;

    int rc = post_json(cfg, path, body, &r);

    free(r.data);

    return rc;
}

int couch11_artifact_begin(const couch_config_t *cfg, int job_id, const char *name, int *artifact_id) {
    char *escaped = curl_easy_escape(NULL, name, 0);

    if(!escaped) return -1;

    char path[2048];

    snprintf(path, sizeof(path), "/api/v1/jobs/%d/artifacts?name=%s", job_id, escaped);

    curl_free(escaped);

    response_t r;
    long code = 0;

    char url[4096];

    snprintf(url, sizeof(url), "%s%s", cfg->couch_url, path);

    if(request(url, "POST", cfg->token, "", "application/json", &code, &r) != 0) {
        free(r.data);
        return -1;
    }

    if(code < 200 || code >= 300) {
        free(r.data);
        return -1;
    }

    char value[64];

    if(!r.data || !json_string(r.data, "id", value, sizeof(value))) {
        free(r.data);
        return -1;
    }

    *artifact_id = atoi(value);

    free(r.data);

    return *artifact_id > 0 ? 0 : -1;
}

int couch11_artifact_chunk(const couch_config_t *cfg, int artifact_id, int sequence, const void *data, size_t len) {
    char path[256];

    snprintf(path, sizeof(path), "/api/v1/artifacts/%d?sequence=%d", artifact_id, sequence);

    char url[4096];

    snprintf(url, sizeof(url), "%s%s", cfg->couch_url, path);

    response_t r;
    long code = 0;

    int rc = request_binary(url, cfg->token, data, len, &code, &r);

    free(r.data);

    return rc == 0 && code >= 200 && code < 300 ? 0 : -1;
}

int couch11_artifact_finish(const couch_config_t *cfg, int artifact_id, size_t size) {
    char path[256];
    char body[256];

    snprintf(path, sizeof(path), "/api/v1/artifacts/%d/finish", artifact_id);
    snprintf(body, sizeof(body), "{\"size\":%zu}", size);

    response_t r;

    int rc = post_json(cfg, path, body, &r);

    free(r.data);

    return rc;
}
