CC = gcc
CFLAGS = -Wall -Wextra -std=c99 -O2
LDFLAGS = -libverbs

SOURCES = pg.c RDMA_api.c pg_net.c
OBJECTS = $(SOURCES:.c=.o)
TARGET = pg_test
TEST_TARGET = test_connect

.PHONY: all clean install help test

all: $(TARGET) $(TEST_TARGET)

$(TARGET): $(OBJECTS)
	$(CC) $(OBJECTS) -o $(TARGET) $(LDFLAGS)

$(TEST_TARGET): test_connect.o $(OBJECTS)
	$(CC) test_connect.o $(OBJECTS) -o $(TEST_TARGET) $(LDFLAGS)

test_connect.o: test_connect.c
	$(CC) $(CFLAGS) -c test_connect.c -o test_connect.o

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

test: $(TEST_TARGET)
	@echo "Test program built successfully!"
	@echo "Usage: ./$(TEST_TARGET) -myindex <index> -list <host1> <host2> ..."
	@echo "Example: ./$(TEST_TARGET) -myindex 2 -list mlx-stud-01 mlx-stud-02 mlx-stud-03 mlx-stud-04"

clean:
	rm -f $(OBJECTS) test_connect.o $(TARGET) $(TEST_TARGET)

install:
	sudo apt-get update
	sudo apt-get install -y libibverbs-dev librdmacm-dev

help:
	@echo "Available targets:"
	@echo "  all     - Build both the library and test program"
	@echo "  test    - Build the test program and show usage"
	@echo "  clean   - Remove built files"
	@echo "  install - Install required dependencies"
	@echo "  help    - Show this help message"
	@echo ""
	@echo "Usage examples:"
	@echo "  make install  # Install dependencies"
	@echo "  make test     # Build test program"
	@echo "  ./test_connect -myindex 2 -list mlx-stud-01 mlx-stud-02 mlx-stud-03 mlx-stud-04"
