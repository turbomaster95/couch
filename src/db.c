#include "couch.h"
#include <sqlite3.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <pthread.h>
#include <stddef.h>
#include <ctype.h>

static sqlite3 *db;
static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

static int couch_json_escape(const char* src, char *dst, size_t size) {
    size_t used = 0;

    if(size == 0) return -1;
    dst[0] = '\0';

    if(src == NULL) src = "";

    for(const unsigned char *p = (const unsigned char *)src; *p; p++) {
        char escaped[7];
        size_t len = 1;

        switch(*p) {
            case '"': escaped[0] = '\\'; escaped[1] = '"'; len = 2; break;
            case '\\': escaped[0] = '\\'; escaped[1] = '\\'; len = 2; break;
            case '\b': escaped[0] = '\\'; escaped[1] = 'b'; len = 2; break;
            case '\f': escaped[0] = '\\'; escaped[1] = 'f'; len = 2; break;
            case '\n': escaped[0] = '\\'; escaped[1] = 'n'; len = 2; break;
            case '\r': escaped[0] = '\\'; escaped[1] = 'r'; len = 2; break;
            case '\t': escaped[0] = '\\'; escaped[1] = 't'; len = 2; break;
            default:
                if(*p < 0x20) {
                    snprintf(escaped, sizeof(escaped), "\\u%04x", *p);
                    len = 6;
                } else {
                    escaped[0] = (char)*p;
                }
                break;
        }

        if(used + len >= size) return -1;
        memcpy(dst + used, escaped, len);
        used += len;
    }

    dst[used] = '\0';
    return 0;
}

static int exec_sql(const char *sql) {
    char *err = NULL;
    int rc = sqlite3_exec(db, sql, NULL, NULL, &err);

    if(rc != SQLITE_OK) {
        fprintf(stderr, "couch: sqlite: %s\n", err ? err : sqlite3_errmsg(db));
        sqlite3_free(err);
    }

    return rc;
}

int couch_db_open(const char *path) {
    if(sqlite3_open(path, &db) != SQLITE_OK) {
        if(db) sqlite3_close(db);
        db = NULL;
        return -1;
    }

    sqlite3_busy_timeout(db, 5000);

    return 0;
}

void couch_db_close(void) {
    if(db) sqlite3_close(db);
    db = NULL;
}

static int couch_db_ensure_job_labels(void) {
    sqlite3_stmt *st = NULL;
    int found = 0;

    if(sqlite3_prepare_v2(db, "PRAGMA table_info(jobs)", -1, &st, NULL) != SQLITE_OK) return -1;

    while(sqlite3_step(st) == SQLITE_ROW) {
        const char *name = (const char *)sqlite3_column_text(st, 1);
        if(name && !strcmp(name, "required_labels")) {
            found = 1;
            break;
        }
    }

    sqlite3_finalize(st);

    if(found) return 0;

    return exec_sql("ALTER TABLE jobs ADD COLUMN required_labels TEXT NOT NULL DEFAULT ''") == SQLITE_OK ? 0 : -1;
}

int couch_db_init(void) {
    if(exec_sql(
        "PRAGMA journal_mode=WAL;"
        "PRAGMA foreign_keys=ON;"
        "CREATE TABLE IF NOT EXISTS jobs("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "repository TEXT NOT NULL,"
        "ref TEXT NOT NULL,"
        "commit_hash TEXT NOT NULL,"
        "command TEXT NOT NULL,"
        "status TEXT NOT NULL DEFAULT 'queued',"
        "runner TEXT,"
        "exit_code INTEGER,"
        "signal INTEGER,"
        "created_at INTEGER NOT NULL,"
        "started_at INTEGER,"
        "finished_at INTEGER"
        ");"
        "CREATE TABLE IF NOT EXISTS logs("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "job_id INTEGER NOT NULL,"
        "data BLOB NOT NULL,"
        "FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE"
        ");"
        "CREATE TABLE IF NOT EXISTS runners("
        "name TEXT PRIMARY KEY,"
        "labels TEXT,"
        "heartbeat INTEGER NOT NULL"
        ");"
        "CREATE TABLE IF NOT EXISTS artifacts("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "job_id INTEGER NOT NULL,"
        "name TEXT NOT NULL,"
        "size INTEGER NOT NULL DEFAULT 0,"
        "finished INTEGER NOT NULL DEFAULT 0,"
        "created_at INTEGER NOT NULL,"
        "FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE"
        ");"
        "CREATE TABLE IF NOT EXISTS artifact_chunks("
        "id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "artifact_id INTEGER NOT NULL,"
        "sequence INTEGER NOT NULL,"
        "data BLOB NOT NULL,"
        "FOREIGN KEY(artifact_id) REFERENCES artifacts(id) ON DELETE CASCADE,"
        "UNIQUE(artifact_id,sequence)"
        ");"
    ) != SQLITE_OK) return -1;

    return couch_db_ensure_job_labels();
}

int couch_db_create_job_with_labels(const char *repository, const char *ref, const char *commit, const char *command, const char *required_labels) {
    pthread_mutex_lock(&lock);

    sqlite3_stmt *st = NULL;
    int id = -1;

    if(sqlite3_prepare_v2(db, "INSERT INTO jobs(repository,ref,commit_hash,command,required_labels,status,created_at) VALUES(?,?,?,?,?,?,?)", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, repository, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 2, ref, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 3, commit, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 4, command, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 5, required_labels ? required_labels : "", -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 6, "queued", -1, SQLITE_STATIC);
        sqlite3_bind_int64(st, 7, (sqlite3_int64)time(NULL));

        if(sqlite3_step(st) == SQLITE_DONE) id = (int)sqlite3_last_insert_rowid(db);
    }

    sqlite3_finalize(st);
    pthread_mutex_unlock(&lock);

    return id;
}

int couch_db_create_job(const char *repository, const char *ref, const char *commit, const char *command) {
    return couch_db_create_job_with_labels(repository, ref, commit, command, "");
}

static int label_list_contains(const char *list, const char *wanted, size_t wanted_len) {
    const char *p = list ? list : "";

    while(*p) {
        const char *start = p;

        while(*p && *p != ',') p++;

        const char *end = p;

        while(start < end && isspace((unsigned char)*start)) start++;
        while(end > start && isspace((unsigned char)end[-1])) end--;

        if((size_t)(end - start) == wanted_len && !strncmp(start, wanted, wanted_len)) return 1;

        if(*p == ',') p++;
    }

    return 0;
}

static int requirements_match(const char *required, const char *available) {
    const char *p = required ? required : "";

    while(*p) {
        const char *start = p;

        while(*p && *p != ',') p++;

        const char *end = p;

        while(start < end && isspace((unsigned char)*start)) start++;
        while(end > start && isspace((unsigned char)end[-1])) end--;

        if(end > start && !label_list_contains(available, start, (size_t)(end - start))) return 0;

        if(*p == ',') p++;
    }

    return 1;
}

int couch_db_claim_job(const char *runner, couch_job_t *job) {
    pthread_mutex_lock(&lock);

    sqlite3_stmt *st = NULL;
    int ok = 0;
    char runner_labels[512] = "";

    if(sqlite3_exec(db, "BEGIN IMMEDIATE", NULL, NULL, NULL) != SQLITE_OK) {
        pthread_mutex_unlock(&lock);
        return 0;
    }

    if(sqlite3_prepare_v2(db, "SELECT labels FROM runners WHERE name=?", -1, &st, NULL) != SQLITE_OK) goto rollback;

    sqlite3_bind_text(st, 1, runner, -1, SQLITE_TRANSIENT);

    if(sqlite3_step(st) != SQLITE_ROW) goto rollback;

    snprintf(runner_labels, sizeof(runner_labels), "%s", (const char *)sqlite3_column_text(st, 0) ? (const char *)sqlite3_column_text(st, 0) : "");

    sqlite3_finalize(st);
    st = NULL;

    if(sqlite3_prepare_v2(db, "SELECT id,repository,ref,commit_hash,command,required_labels FROM jobs WHERE status='queued' ORDER BY id", -1, &st, NULL) != SQLITE_OK) goto rollback;

    while(sqlite3_step(st) == SQLITE_ROW) {
        const char *required = (const char *)sqlite3_column_text(st, 5);

        if(!requirements_match(required, runner_labels)) continue;

        job->id = sqlite3_column_int(st, 0);
        snprintf(job->repository, sizeof(job->repository), "%s", (const char *)sqlite3_column_text(st, 1));
        snprintf(job->ref, sizeof(job->ref), "%s", (const char *)sqlite3_column_text(st, 2));
        snprintf(job->commit, sizeof(job->commit), "%s", (const char *)sqlite3_column_text(st, 3));
        snprintf(job->command, sizeof(job->command), "%s", (const char *)sqlite3_column_text(st, 4));

        ok = 1;
        break;
    }

    sqlite3_finalize(st);
    st = NULL;

    if(!ok) goto rollback;

    if(sqlite3_prepare_v2(db, "UPDATE jobs SET status='running',runner=?,started_at=? WHERE id=? AND status='queued'", -1, &st, NULL) != SQLITE_OK) {
        ok = 0;
        goto rollback;
    }

    sqlite3_bind_text(st, 1, runner, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(st, 2, (sqlite3_int64)time(NULL));
    sqlite3_bind_int(st, 3, job->id);

    ok = sqlite3_step(st) == SQLITE_DONE && sqlite3_changes(db) == 1;

    sqlite3_finalize(st);
    st = NULL;

    if(ok) sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
    else sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);

    pthread_mutex_unlock(&lock);

    return ok;

rollback:
    sqlite3_finalize(st);
    sqlite3_exec(db, "ROLLBACK", NULL, NULL, NULL);
    pthread_mutex_unlock(&lock);

    return 0;
}

int couch_db_append_log(int id, const char *data, size_t len) {
    pthread_mutex_lock(&lock);

    sqlite3_stmt *st = NULL;
    int ok = 0;

    if(sqlite3_prepare_v2(db, "INSERT INTO logs(job_id,data) VALUES(?,?)", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_int(st, 1, id);
        sqlite3_bind_blob(st, 2, data, (int)len, SQLITE_TRANSIENT);
        ok = sqlite3_step(st) == SQLITE_DONE;
    }

    sqlite3_finalize(st);
    pthread_mutex_unlock(&lock);

    return ok ? 0 : -1;
}

int couch_db_complete_job(int id, int exit_code, int signal_no) {
    pthread_mutex_lock(&lock);

    sqlite3_stmt *st = NULL;
    int ok = 0;

    if(sqlite3_prepare_v2(db, "UPDATE jobs SET status=?,exit_code=?,signal=?,finished_at=? WHERE id=?", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, exit_code == 0 && signal_no == 0 ? "success" : "failed", -1, SQLITE_STATIC);
        sqlite3_bind_int(st, 2, exit_code);
        sqlite3_bind_int(st, 3, signal_no);
        sqlite3_bind_int64(st, 4, (sqlite3_int64)time(NULL));
        sqlite3_bind_int(st, 5, id);
        ok = sqlite3_step(st) == SQLITE_DONE;
    }

    sqlite3_finalize(st);
    pthread_mutex_unlock(&lock);

    return ok ? 0 : -1;
}

int couch_db_fail_job(int id, const char *reason) {
    if(couch_db_append_log(id, reason, strlen(reason)) != 0) return -1;
    return couch_db_complete_job(id, 127, 0);
}

int couch_db_register_runner(const char *name, const char *labels) {
    pthread_mutex_lock(&lock);

    sqlite3_stmt *st = NULL;
    int ok = 0;

    if(sqlite3_prepare_v2(db, "INSERT INTO runners(name,labels,heartbeat) VALUES(?,?,?) ON CONFLICT(name) DO UPDATE SET labels=excluded.labels,heartbeat=excluded.heartbeat", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, name, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 2, labels, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(st, 3, (sqlite3_int64)time(NULL));
        ok = sqlite3_step(st) == SQLITE_DONE;
    }

    sqlite3_finalize(st);
    pthread_mutex_unlock(&lock);

    return ok ? 0 : -1;
}

int couch_db_heartbeat_runner(const char *name) {
    pthread_mutex_lock(&lock);

    sqlite3_stmt *st = NULL;
    int ok = 0;

    if(sqlite3_prepare_v2(db, "UPDATE runners SET heartbeat=? WHERE name=?", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_int64(st, 1, (sqlite3_int64)time(NULL));
        sqlite3_bind_text(st, 2, name, -1, SQLITE_TRANSIENT);
        ok = sqlite3_step(st) == SQLITE_DONE;
    }

    sqlite3_finalize(st);
    pthread_mutex_unlock(&lock);

    return ok ? 0 : -1;
}


int couch_db_list_jobs(char *out, size_t cap) {
    pthread_mutex_lock(&lock);

    sqlite3_stmt *st = NULL;
    size_t used = 0;
    int first = 1;
    int ok = -1;

    if(!out || cap == 0) {
        pthread_mutex_unlock(&lock);
        return -1;
    }

    out[0] = '\0';

    if(sqlite3_prepare_v2(db, "SELECT id,repository,ref,commit_hash,status,runner FROM jobs ORDER BY id DESC LIMIT 100", -1, &st, NULL) != SQLITE_OK) {
        pthread_mutex_unlock(&lock);
        return -1;
    }

    int n = snprintf(out, cap, "[");
    if(n < 0 || (size_t)n >= cap) goto cleanup;
    used = (size_t)n;

    while(sqlite3_step(st) == SQLITE_ROW) {
        char repository[1024];
        char ref[512];
        char commit[512];
        char status[256];
        char runner[512];
        const char *r = (const char *)sqlite3_column_text(st, 5);

        if(couch_json_escape((const char *)sqlite3_column_text(st, 1), repository, sizeof(repository)) != 0 ||
           couch_json_escape((const char *)sqlite3_column_text(st, 2), ref, sizeof(ref)) != 0 ||
           couch_json_escape((const char *)sqlite3_column_text(st, 3), commit, sizeof(commit)) != 0 ||
           couch_json_escape((const char *)sqlite3_column_text(st, 4), status, sizeof(status)) != 0 ||
           (r && couch_json_escape(r, runner, sizeof(runner)) != 0)) goto cleanup;

        n = snprintf(out + used, cap - used, "%s{\"id\":%d,\"repository\":\"%s\",\"ref\":\"%s\",\"commit\":\"%s\",\"status\":\"%s\",\"runner\":%s%s%s}", first ? "" : ",", sqlite3_column_int(st, 0), repository, ref, commit, status, r ? "\"" : "null", r ? runner : "", r ? "\"" : "");

        if(n < 0 || (size_t)n >= cap - used) goto cleanup;

        used += (size_t)n;
        first = 0;
    }

    if(sqlite3_errcode(db) != SQLITE_DONE && sqlite3_errcode(db) != SQLITE_OK) goto cleanup;

    if(used + 2 > cap) goto cleanup;

    out[used++] = ']';
    out[used] = '\0';
    ok = (int)used;

cleanup:
    sqlite3_finalize(st);

    if(ok < 0) out[0] = '\0';

    pthread_mutex_unlock(&lock);
    return ok;
}

int couch_db_job_json(int id, char *out, size_t cap) {
    pthread_mutex_lock(&lock);

    sqlite3_stmt *st = NULL;
    int ok = -1;

    if(!out || cap == 0) {
        pthread_mutex_unlock(&lock);
        return -1;
    }

    out[0] = '\0';

    if(sqlite3_prepare_v2(db, "SELECT id,repository,ref,commit_hash,status,runner,exit_code,signal FROM jobs WHERE id=?", -1, &st, NULL) != SQLITE_OK) goto cleanup;

    sqlite3_bind_int(st, 1, id);

    if(sqlite3_step(st) != SQLITE_ROW) goto cleanup;

    char repository[1024];
    char ref[512];
    char commit[512];
    char status[256];
    char runner[512];
    const char *r = (const char *)sqlite3_column_text(st, 5);

    if(couch_json_escape((const char *)sqlite3_column_text(st, 1), repository, sizeof(repository)) != 0 ||
       couch_json_escape((const char *)sqlite3_column_text(st, 2), ref, sizeof(ref)) != 0 ||
       couch_json_escape((const char *)sqlite3_column_text(st, 3), commit, sizeof(commit)) != 0 ||
       couch_json_escape((const char *)sqlite3_column_text(st, 4), status, sizeof(status)) != 0 ||
       (r && couch_json_escape(r, runner, sizeof(runner)) != 0)) goto cleanup;

    int n = snprintf(out, cap, "{\"id\":%d,\"repository\":\"%s\",\"ref\":\"%s\",\"commit\":\"%s\",\"status\":\"%s\",\"runner\":%s%s%s,\"exit_code\":%d,\"signal\":%d}", sqlite3_column_int(st, 0), repository, ref, commit, status, r ? "\"" : "null", r ? runner : "", r ? "\"" : "", sqlite3_column_int(st, 6), sqlite3_column_int(st, 7));

    if(n >= 0 && (size_t)n < cap) ok = 0;

cleanup:
    sqlite3_finalize(st);
    pthread_mutex_unlock(&lock);

    if(ok != 0) out[0] = '\0';

    return ok;
}

int couch_db_get_log(int id, char **out, size_t *len) {
    pthread_mutex_lock(&lock);

    sqlite3_stmt *st = NULL;
    size_t total = 0;

    if(sqlite3_prepare_v2(db, "SELECT length(data) FROM logs WHERE job_id=? ORDER BY id", -1, &st, NULL) != SQLITE_OK) {
        pthread_mutex_unlock(&lock);
        return -1;
    }

    sqlite3_bind_int(st, 1, id);

    while(sqlite3_step(st) == SQLITE_ROW) total += (size_t)sqlite3_column_int(st, 0);

    sqlite3_finalize(st);

    *out = malloc(total + 1);

    if(!*out) {
        pthread_mutex_unlock(&lock);
        return -1;
    }

    size_t pos = 0;

    if(sqlite3_prepare_v2(db, "SELECT data,length(data) FROM logs WHERE job_id=? ORDER BY id", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_int(st, 1, id);

        while(sqlite3_step(st) == SQLITE_ROW) {
            int n = sqlite3_column_int(st, 1);

            memcpy(*out + pos, sqlite3_column_blob(st, 0), (size_t)n);

            pos += (size_t)n;
        }
    }

    sqlite3_finalize(st);

    (*out)[pos] = 0;
    *len = pos;

    pthread_mutex_unlock(&lock);

    return 0;
}

int couch_db_create_artifact(int job_id, const char *name) {
    pthread_mutex_lock(&lock);

    sqlite3_stmt *st = NULL;
    int id = -1;
    int rc = sqlite3_prepare_v2(db, "INSERT INTO artifacts(job_id,name,created_at) VALUES(?,?,?)", -1, &st, NULL);

    if(rc == SQLITE_OK) {
        sqlite3_bind_int(st, 1, job_id);
        sqlite3_bind_text(st, 2, name, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(st, 3, (sqlite3_int64)time(NULL));

        rc = sqlite3_step(st);

        if(rc == SQLITE_DONE) {
            id = (int)sqlite3_last_insert_rowid(db);
        }
    }

    if(id < 0) fprintf(stderr, "couch: cannot create artifact for job %d ('%s'): %s\n", job_id, name ? name : "", sqlite3_errmsg(db));

    sqlite3_finalize(st);
    pthread_mutex_unlock(&lock);

    return id;
}

int couch_db_append_artifact(int artifact_id, int sequence, const void *data, size_t len) {
    pthread_mutex_lock(&lock);

    sqlite3_stmt *st = NULL;
    int ok = 0;

    if(sqlite3_prepare_v2(db, "INSERT INTO artifact_chunks(artifact_id,sequence,data) VALUES(?,?,?)", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_int(st, 1, artifact_id);
        sqlite3_bind_int(st, 2, sequence);
        sqlite3_bind_blob(st, 3, data, (int)len, SQLITE_TRANSIENT);
        ok = sqlite3_step(st) == SQLITE_DONE;
    }

    sqlite3_finalize(st);
    pthread_mutex_unlock(&lock);

    return ok ? 0 : -1;
}

int couch_db_finish_artifact(int artifact_id, size_t size) {
    pthread_mutex_lock(&lock);

    sqlite3_stmt *st = NULL;
    int ok = 0;

    if(sqlite3_prepare_v2(db, "UPDATE artifacts SET size=?,finished=1 WHERE id=?", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_int64(st, 1, (sqlite3_int64)size);
        sqlite3_bind_int(st, 2, artifact_id);
        ok = sqlite3_step(st) == SQLITE_DONE;
    }

    sqlite3_finalize(st);
    pthread_mutex_unlock(&lock);

    return ok ? 0 : -1;
}

int couch_db_list_artifacts(int job_id, char *out, size_t cap) {
    pthread_mutex_lock(&lock);

    sqlite3_stmt *st = NULL;
    size_t used = 0;

    if(sqlite3_prepare_v2(db, "SELECT id,name,size FROM artifacts WHERE job_id=? AND finished=1 ORDER BY id", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_int(st, 1, job_id);

        used += (size_t)snprintf(out + used, cap > used ? cap - used : 0, "[");

        int first = 1;

        while(sqlite3_step(st) == SQLITE_ROW && used < cap) {
            char name[1024];

            couch_json_escape((const char *)sqlite3_column_text(st, 1), name, sizeof(name));

            used += (size_t)snprintf(out + used, cap > used ? cap - used : 0,
                "%s{\"id\":%d,\"name\":\"%s\",\"size\":%lld}",
                first ? "" : ",",
                sqlite3_column_int(st, 0),
                name,
                (long long)sqlite3_column_int64(st, 2));

            first = 0;
        }

        if(used < cap) used += (size_t)snprintf(out + used, cap - used ? cap - used : 0, "]");
    }

    sqlite3_finalize(st);
    pthread_mutex_unlock(&lock);

    return (int)used;
}

int couch_db_get_artifact(int job_id, int artifact_id, char **name, void **data, size_t *len) {
    pthread_mutex_lock(&lock);

    sqlite3_stmt *st = NULL;
    size_t total = 0;

    if(sqlite3_prepare_v2(db, "SELECT name,size FROM artifacts WHERE id=? AND job_id=? AND finished=1", -1, &st, NULL) != SQLITE_OK) {
        pthread_mutex_unlock(&lock);
        return -1;
    }

    sqlite3_bind_int(st, 1, artifact_id);
    sqlite3_bind_int(st, 2, job_id);

    if(sqlite3_step(st) != SQLITE_ROW) {
        sqlite3_finalize(st);
        pthread_mutex_unlock(&lock);
        return -1;
    }

    const char *artifact_name = (const char *)sqlite3_column_text(st, 0);
    total = (size_t)sqlite3_column_int64(st, 1);

    *name = strdup(artifact_name ? artifact_name : "");

    sqlite3_finalize(st);

    if(!*name) {
        pthread_mutex_unlock(&lock);
        return -1;
    }

    *data = malloc(total ? total : 1);

    if(!*data) {
        free(*name);
        *name = NULL;
        pthread_mutex_unlock(&lock);
        return -1;
    }

    size_t pos = 0;

    if(sqlite3_prepare_v2(db, "SELECT data FROM artifact_chunks WHERE artifact_id=? ORDER BY sequence", -1, &st, NULL) != SQLITE_OK) {
        free(*name);
        free(*data);
        *name = NULL;
        *data = NULL;
        pthread_mutex_unlock(&lock);
        return -1;
    }

    sqlite3_bind_int(st, 1, artifact_id);

    while(sqlite3_step(st) == SQLITE_ROW) {
        const void *chunk = sqlite3_column_blob(st, 0);
        int n = sqlite3_column_bytes(st, 0);

        if(pos + (size_t)n > total) {
            sqlite3_finalize(st);
            free(*name);
            free(*data);
            *name = NULL;
            *data = NULL;
            pthread_mutex_unlock(&lock);
            return -1;
        }

        memcpy((char *)*data + pos, chunk, (size_t)n);
        pos += (size_t)n;
    }

    sqlite3_finalize(st);

    *len = pos;

    pthread_mutex_unlock(&lock);

    return 0;
}
