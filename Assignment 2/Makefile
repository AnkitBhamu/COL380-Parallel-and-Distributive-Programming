CC=mpic++
CFLAGS=-std=c++11 -O2 -g

a2: main.o
	$(CC) $(CFLAGS) main.o -o a2

main.o: main.cpp
	$(CC) $(CFLAGS) -c main.cpp -o main.o

clean:
	rm a2 main.o
