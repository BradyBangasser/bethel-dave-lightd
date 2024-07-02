lightd: lightd.c
	clang -std=c17 -o $@ $^ -lpthread -Wall -O3