#include <stdint.h>

void *db_init(int fd);
void db_free(void *ctx);
char db_next(void *ctx, char **k, size_t *ksize, char **v, size_t *vsize);
char db_int_key_next(void *ctx, uint32_t *k, char **v, size_t *vsize);
