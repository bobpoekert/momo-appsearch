#define CHUNK_SIZE 4096*4096
#define TREE_SIZE 5000000
#define HASH_SEED 0xdeadbeef

typedef struct inserter_ctx {
    queue *pool;
    queue *outq;
} inserter_ctx;

typedef struct job {
    char buf[CHUNK_SIZE];
    size_t buf_size;
} job;

#ifdef BIG_HASH

typedef uint64_t[2] hash;
inline char hash_compare(hash a, hash b) {
    if (a[0] == b[0]) {
        return a[1] - b[1];
    } else {
        return a[0] - b[0];
    }
}
#define murmur_hash(line, end, outp) MurmurHash3_x64_128(line, end, HASH_SEED, outp)

#else

typedef uint32_t hash;
inline char hash_compare(hash a, hash b) {
    return a - b;
}
#define murmur_hash(line, end, outp) MurmurHash3_x86_32(line, end, HASH_SEED, outp)

#endif

typedef struct string_row {
    hash hash;
    uint64_t count; /* starts at 1 */
    uint8_t file_id;
    uint64_t file_offset;
} string_row;

typedef struct string_tree_node {
    string_row row;
    struct string_tree_node *right;
    struct string_tree_node *left;
} string_tree_node;

typedef struct string_tree {
    string_tree_node nodes[TREE_SIZE];
    struct string_tree *next;
    size_t length;
} string_tree;


typedef struct worker_ctx {
    string_tree *head;
    string_tree *tail;
    queue *inq;
    queue *pool;
    size_t strings_offset;
    int strings_fd;
    char strings_buffer[CHUNK_SIZE];
    size_t strings_buffer_offset;
} worker_ctx;
