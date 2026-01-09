#include <liburing.h>
#include <stdio.h>

int main() {
    struct io_uring ring;
    int ret = io_uring_queue_init(8, &ring, 0);
    if (ret) {
        printf("io_uring init failed: %d\n", ret);
        return 1;
    }
    printf("io_uring OK\n");
    getchar();
}
