#define _GNU_SOURCE
#include "couch.h"
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <ctype.h>
#include <sys/socket.h>

static int read_all(int fd, char *buf, size_t cap, size_t *used) {
    *used = 0;
    while(*used + 1 < cap) {
        ssize_t n = recv(fd, buf + *used, cap - *used - 1, 0);
        if(n <= 0) return -1;
        *used += (size_t)n;
        buf[*used] = 0;
        if(strstr(buf, "\r\n\r\n")) return 0;
    }
    return -2;
}

static const char *header_value(const char *headers, const char *name) {
    static char value[128];
    char needle[128];
    snprintf(needle, sizeof(needle), "\r\n%s:", name);
    const char *p = strcasestr(headers, needle);
    if(!p) {
        if(strncasecmp(headers, name, strlen(name)) == 0 && headers[strlen(name)] == ':') p = headers;
        else return NULL;
    }
    p = strchr(p, ':');
    if(!p) return NULL;
    p++;
    while(*p == ' ') p++;
    size_t i = 0;
    while(*p && *p != '\r' && *p != '\n' && i + 1 < sizeof(value)) value[i++] = *p++;
    value[i] = 0;
    return value;
}

int couch_http_read_request(int fd, http_request_t *req) {
    memset(req, 0, sizeof(*req));
    req->fd = fd;
    char *buf = malloc(COUCH_MAX_BODY + COUCH_MAX_LINE);
    if(!buf) return -1;
    size_t used = 0;
    int r = read_all(fd, buf, COUCH_MAX_BODY + COUCH_MAX_LINE, &used);
    if(r) { free(buf); return r; }
    char *end = strstr(buf, "\r\n\r\n");
    size_t head_len = (size_t)(end - buf) + 4;
    char *line_end = strstr(buf, "\r\n");
    if(!line_end) { free(buf); return -1; }
    *line_end = 0;
    if(sscanf(buf, "%15s %1023s", req->method, req->path) != 2) { free(buf); return -1; }
    *line_end = '\r';
    const char *cl = header_value(buf, "Content-Length");
    size_t content_len = cl ? (size_t)strtoull(cl, NULL, 10) : 0;
    if(content_len > COUCH_MAX_BODY) { free(buf); return -1; }
    req->body = calloc(1, content_len + 1);
    if(!req->body) { free(buf); return -1; }
    size_t already = used - head_len;
    if(already > content_len) already = content_len;
    if(already) memcpy(req->body, buf + head_len, already);
    size_t got = already;
    while(got < content_len) {
        ssize_t n = recv(fd, req->body + got, content_len - got, 0);
        if(n <= 0) { free(buf); free(req->body); req->body = NULL; return -1; }
        got += (size_t)n;
    }
    req->body_len = content_len;
    free(buf);
    return 0;
}

void couch_http_free_request(http_request_t *req) {
    free(req->body);
    req->body = NULL;
}

int couch_http_send(int fd, int status, const char *type, const void *body, size_t len) {
    char head[512];
    const char *msg = status == 200 ? "OK" : status == 201 ? "Created" : status == 400 ? "Bad Request" : status == 401 ? "Unauthorized" : status == 404 ? "Not Found" : status == 409 ? "Conflict" : status == 500 ? "Internal Server Error" : "Error";
    int n = snprintf(head, sizeof(head), "HTTP/1.1 %d %s\r\nContent-Type: %s\r\nContent-Length: %zu\r\nConnection: close\r\n\r\n", status, msg, type, len);
    if(send(fd, head, (size_t)n, 0) < 0) return -1;
    if(len && send(fd, body, len, 0) < 0) return -1;
    return 0;
}

int couch_http_send_text(int fd, int status, const char *body) { return couch_http_send(fd, status, "text/plain; charset=utf-8", body, strlen(body)); }
int couch_http_send_json(int fd, int status, const char *body) { return couch_http_send(fd, status, "application/json; charset=utf-8", body, strlen(body)); }
