all:
	clang++ -std=c++1y -O3 pgclientlib.cpp -o pgclientlib

debug:
	clang++ -std=c++1y -O0 -g pgclientlib.cpp -o pgclientlib

doc:
	/Applications/Doxygen.app/Contents/Resources/doxygen Doxyfile

clean:
	rm -f *.o pgclientlib

