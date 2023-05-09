CXXFLAGS=-w -std=c++11 -g -O3 
#CXXFLAGS=-Wall -std=c++11 -g -pg
#CXXFLAGS=-Wall -std=c++11 -g -pg -DDEBUG
CC=g++

test: test.cpp betree.hpp swap_space.hpp backing_store.hpp


clean:
	$(RM) *.o test
