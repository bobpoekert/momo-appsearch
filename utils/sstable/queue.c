#pragma once
#include <pthread.h>
#include <stdint.h>
#include <unistd.h>
#include <stdlib.h>

typedef struct queue {
    size_t max_size;
    size_t write_idx;
    size_t read_idx;
    void **buffer;
    pthread_mutex_t lock;
    pthread_cond_t can_produce;
    pthread_cond_t can_consume;
} queue;

queue *queue_alloc(size_t max_size) {
    queue *res = malloc(sizeof(queue));
    res->buffer = malloc(sizeof(void *) * max_size);
    return res;
}

void queue_free(queue *q) {
    free(q->buffer);
    free(q);
}

inline size_t queue_size(queue *q) {
    if (q->write_idx == q->read_idx) {
        return 0;
    } else if (q->write_idx > q->read_idx) {
        return q->write_idx - q->read_idx;
    } else {
        return (q->max_size - q->read_idx) + q->write_idx;
    }
}

void queue_put(queue *q, void *item) {
    pthread_mutex_lock(&q->lock);
    while(queue_size(q) >= q->max_size) {
        pthread_cond_wait(&q->can_produce, &q->lock);
    }
    q->buffer[q->write_idx] = item;
    if (q->write_idx >= q->max_size) {
        q->write_idx = 0;
    } else {
        q->write_idx++;
    }
    pthread_cond_signal(&q->can_consume);
    pthread_mutex_unlock(&q->lock);
}

void *queue_take(queue *q) {
    pthread_mutex_lock(&q->lock);

    while (queue_size(q) < 1) {
        pthread_cond_wait(&q->can_consume, &q->lock);
    }

    void *res = q->buffer[q->read_idx];
    if (q->read_idx > 0) {
        q->read_idx--;
    } else {
        q->read_idx = q->max_size;
    }

    pthread_mutex_unlock(&q->lock);
    return res;
}
