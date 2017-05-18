all:
	clang++ -std=c++1y -O0 -g pgclientlib.cpp -o pgclientlib

debug:
	clang++ -std=c++1y -O0 -g pgclientlib.cpp -o pgclientlib

clean:
	rm -f *.o pgclientlib

