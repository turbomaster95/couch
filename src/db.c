#include "couch.h"
#include <sqlite3.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <pthread.h>

static sqlite3 *db;
static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

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
    if(sqlite3_open(path, &db) != SQLITE_OK) return -1;
    sqlite3_busy_timeout(db, 5000);
    return 0;
}

void couch_db_close(void) {
    if(db) sqlite3_close(db);
    db = NULL;
}

int couch_db_init(void) {
    return exec_sql("PRAGMA journal_mode=WAL;"
                    "CREATE TABLE IF NOT EXISTS jobs("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                    "repository TEXT NOT NULL,ref TEXT NOT NULL,commit_hash TEXT NOT NULL,"
                    "command TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'queued',"
                    "runner TEXT,exit_code INTEGER,signal INTEGER,"
                    "created_at INTEGER NOT NULL,started_at INTEGER,finished_at INTEGER);"
                    "CREATE TABLE IF NOT EXISTS logs("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT,job_id INTEGER NOT NULL,data BLOB NOT NULL);"
                    "CREATE TABLE IF NOT EXISTS runners("
                    "name TEXT PRIMARY KEY,labels TEXT,heartbeat INTEGER NOT NULL);");
}

int couch_db_create_job(const char *repository, const char *ref, const char *commit, const char *command) {
    pthread_mutex_lock(&lock);
    sqlite3_stmt *st = NULL;
    int id = -1;
    if(sqlite3_prepare_v2(db, "INSERT INTO jobs(repository,ref,commit_hash,command,status,created_at) VALUES(?,?,?,?,?,?)", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, repository, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 2, ref, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 3, commit, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 4, command, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 5, "queued", -1, SQLITE_STATIC);
        sqlite3_bind_int64(st, 6, (sqlite3_int64)time(NULL));
        if(sqlite3_step(st) == SQLITE_DONE) id = (int)sqlite3_last_insert_rowid(db);
    }
    sqlite3_finalize(st);
    pthread_mutex_unlock(&lock);
    return id;
}

int couch_db_claim_job(const char *runner, couch_job_t *job) {
    pthread_mutex_lock(&lock);
    sqlite3_stmt *st = NULL;
    int ok = 0;

    if(sqlite3_exec(db, "BEGIN IMMEDIATE", NULL, NULL, NULL) != SQLITE_OK) {
        pthread_mutex_unlock(&lock);
        return 0;
    }

    if(sqlite3_prepare_v2(db, "SELECT id,repository,ref,commit_hash,command FROM jobs WHERE status='queued' ORDER BY id LIMIT 1", -1, &st, NULL) == SQLITE_OK) {
        if(sqlite3_step(st) == SQLITE_ROW) {
            job->id = sqlite3_column_int(st, 0);
            snprintf(job->repository, sizeof(job->repository), "%s", (const char *)sqlite3_column_text(st, 1));
            snprintf(job->ref, sizeof(job->ref), "%s", (const char *)sqlite3_column_text(st, 2));
            snprintf(job->commit, sizeof(job->commit), "%s", (const char *)sqlite3_column_text(st, 3));
            snprintf(job->command, sizeof(job->command), "%s", (const char *)sqlite3_column_text(st, 4));
            ok = 1;
        }
    }

    sqlite3_finalize(st);
    st = NULL;

    if(ok) {
        if(sqlite3_prepare_v2(db, "UPDATE jobs SET status='running',runner=?,started_at=? WHERE id=?", -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, runner, -1, SQLITE_TRANSIENT);
            sqlite3_bind_int64(st, 2, (sqlite3_int64)time(NULL));
            sqlite3_bind_int(st, 3, job->id);
            ok = sqlite3_step(st) == SQLITE_DONE;
        } else ok = 0;
    }

    sqlite3_finalize(st);
    sqlite3_exec(db, ok ? "COMMIT" : "ROLLBACK", NULL, NULL, NULL);
    pthread_mutex_unlock(&lock);
    return ok;
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
    couch_db_append_log(id, reason, strlen(reason));
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
    if(sqlite3_prepare_v2(db, "SELECT id,repository,ref,commit_hash,status,runner FROM jobs ORDER BY id DESC LIMIT 100", -1, &st, NULL) == SQLITE_OK) {
        used += (size_t)snprintf(out + used, cap > used ? cap - used : 0, "[");
        int first = 1;
        while(sqlite3_step(st) == SQLITE_ROW && used < cap) {
            char row[1024];
            snprintf(row, sizeof(row), "%s{\"id\":%d,\"repository\":\"%s\",\"ref\":\"%s\",\"commit\":\"%s\",\"status\":\"%s\",\"runner\":%s}",
                     first ? "" : ",", sqlite3_column_int(st, 0), (const char *)sqlite3_column_text(st, 1),
                     (const char *)sqlite3_column_text(st, 2), (const char *)sqlite3_column_text(st, 3),
                     (const char *)sqlite3_column_text(st, 4), sqlite3_column_text(st, 5) ? "\"" : "null");
            if(sqlite3_column_text(st, 5)) {
                size_t l = strlen(row);
                if(l && row[l-1] == '"') strncat(row, (const char *)sqlite3_column_text(st, 5), sizeof(row)-strlen(row)-1), strncat(row, "\"", sizeof(row)-strlen(row)-1);
            }
            used += (size_t)snprintf(out + used, cap > used ? cap - used : 0, "%s", row);
            first = 0;
        }
    }
    sqlite3_finalize(st);
    if(used < cap) snprintf(out + used, cap - used, "]");
    pthread_mutex_unlock(&lock);
    return (int)used;
}

int couch_db_job_json(int id, char *out, size_t cap) {
    pthread_mutex_lock(&lock);
    sqlite3_stmt *st = NULL;
    int ok = -1;
    if(sqlite3_prepare_v2(db, "SELECT id,repository,ref,commit_hash,status,runner,exit_code,signal FROM jobs WHERE id=?", -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_int(st, 1, id);
        if(sqlite3_step(st) == SQLITE_ROW) {
            const char *runner = (const char *)sqlite3_column_text(st, 5);
            snprintf(out, cap, "{\"id\":%d,\"repository\":\"%s\",\"ref\":\"%s\",\"commit\":\"%s\",\"status\":\"%s\",\"runner\":%s,\"exit_code\":%d,\"signal\":%d}",
                     sqlite3_column_int(st,0),(const char *)sqlite3_column_text(st,1),(const char *)sqlite3_column_text(st,2),(const char *)sqlite3_column_text(st,3),
                     (const char *)sqlite3_column_text(st,4), runner ? "\"" : "null", sqlite3_column_int(st,6), sqlite3_column_int(st,7));
            if(runner) {
                char tmp[1024];
                snprintf(tmp, sizeof(tmp), "{\"id\":%d,\"repository\":\"%s\",\"ref\":\"%s\",\"commit\":\"%s\",\"status\":\"%s\",\"runner\":\"%s\",\"exit_code\":%d,\"signal\":%d}",
                         sqlite3_column_int(st,0),(const char *)sqlite3_column_text(st,1),(const char *)sqlite3_column_text(st,2),(const char *)sqlite3_column_text(st,3),
                         (const char *)sqlite3_column_text(st,4),runner,sqlite3_column_int(st,6),sqlite3_column_int(st,7));
                snprintf(out, cap, "%s", tmp);
            }
            ok = 0;
        }
    }
    sqlite3_finalize(st);
    pthread_mutex_unlock(&lock);
    return ok;
}

int couch_db_get_log(int id, char **out, size_t *len) {
    pthread_mutex_lock(&lock);
    sqlite3_stmt *st = NULL;
    size_t total = 0;
    if(sqlite3_prepare_v2(db, "SELECT length(data) FROM logs WHERE job_id=? ORDER BY id", -1, &st, NULL) != SQLITE_OK) { pthread_mutex_unlock(&lock); return -1; }
    sqlite3_bind_int(st, 1, id);
    while(sqlite3_step(st) == SQLITE_ROW) total += (size_t)sqlite3_column_int(st, 0);
    sqlite3_finalize(st);
    *out = malloc(total + 1);
    if(!*out) { pthread_mutex_unlock(&lock); return -1; }
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
