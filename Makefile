CXXFLAGS=-w
#CXXFLAGS=-Wall -std=c++11 -g -pg
#CXXFLAGS=-Wall -std=c++11 -g -pg -DDEBUG
CC=g++

test: test.cpp betree.hpp


clean:
	$(RM) *.o test
