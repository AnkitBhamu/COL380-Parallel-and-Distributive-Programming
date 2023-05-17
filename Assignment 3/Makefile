CC=mpic++
CFLAGS=-std=c++11 -fopenmp -O2 -g

a2: main.o
	$(CC) $(CFLAGS) main.o -o a3

main.o: main.cpp
	$(CC) $(CFLAGS) -c main.cpp -o main.o

clean:
	rm a3 main.o
