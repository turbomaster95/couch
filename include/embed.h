#ifndef EMBEDDED_WEB_H
#define EMBEDDED_WEB_H

#include <stddef.h>

typedef struct {
    const char *path;
    const unsigned char *data;
    size_t size;
    const char *content_type;
} couch_web_asset_t;

extern const couch_web_asset_t couch_web_assets[];
extern const size_t couch_web_asset_count;

#endif
