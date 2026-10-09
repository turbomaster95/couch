#include "couch.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>

static char *trim(char *s) {
    while(isspace((unsigned char)*s)) s++;
    char *e = s + strlen(s);
    while(e > s && isspace((unsigned char)e[-1])) *--e = 0;
    return s;
}

int couch_config_load(const char *path, couch_config_t *cfg) {
    FILE *f = fopen(path, "r");

    if(!f) return -1;

    memset(cfg, 0, sizeof(*cfg));

    snprintf(cfg->host, sizeof(cfg->host), "127.0.0.1");
    cfg->port = 8080;
    snprintf(cfg->database, sizeof(cfg->database), "couch.db");
    cfg->poll_ms = 1000;
    cfg->max_jobs = 1;

    char line[4096];

    while(fgets(line, sizeof(line), f)) {
        char *p = trim(line);

        if(!*p || *p == '#') continue;

        char *eq = strchr(p, '=');

        if(!eq) continue;

        *eq++ = 0;

        char *key = trim(p);
        char *val = trim(eq);
        size_t len = strlen(val);

        if(len >= 2 && val[0] == '"' && val[len - 1] == '"') {
            val[len - 1] = 0;
            val++;
        }

        if(!strcmp(key, "host")) snprintf(cfg->host, sizeof(cfg->host), "%s", val);
        else if(!strcmp(key, "port")) cfg->port = atoi(val);
        else if(!strcmp(key, "database")) snprintf(cfg->database, sizeof(cfg->database), "%s", val);
        else if(!strcmp(key, "workdir")) snprintf(cfg->workdir, sizeof(cfg->workdir), "%s", val);
        else if(!strcmp(key, "poll_ms")) cfg->poll_ms = atoi(val);
        else if(!strcmp(key, "max_jobs")) cfg->max_jobs = atoi(val);
        else if(!strcmp(key, "couch_url")) snprintf(cfg->couch_url, sizeof(cfg->couch_url), "%s", val);
        else if(!strcmp(key, "token")) snprintf(cfg->token, sizeof(cfg->token), "%s", val);
        else if(!strcmp(key, "runner_name")) snprintf(cfg->runner_name, sizeof(cfg->runner_name), "%s", val);
    }

    fclose(f);
    return 0;
}
