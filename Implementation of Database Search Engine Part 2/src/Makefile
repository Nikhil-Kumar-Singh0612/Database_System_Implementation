CC = g++ -O2 -Wno-deprecated

tag = -i

ifdef linux
tag = -n
endif

all: test.out gTest.out

test.out:  Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o Pipe.o ExtendedDBFile.o DBFile.o  y.tab.o lex.yy.o test.o 
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o Pipe.o ExtendedDBFile.o DBFile.o  y.tab.o lex.yy.o test.o -lfl -lpthread

a1test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o a1-test.o
	$(CC) -o a1test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o DBFile.o Pipe.o y.tab.o lex.yy.o a1-test.o -lfl

gTest.out: gTest.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o ExtendedDBFile.o Pipe.o y.tab.o lex.yy.o 
	$(CC) -o gTest.out gTest.o Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o ExtendedDBFile.o Pipe.o y.tab.o lex.yy.o -lfl -lpthread -lgtest

test.o: test.cc
	$(CC) -g -c test.cc

a1-test.o: a1-test.cc
	$(CC) -g -c a1-test.cc

test-p1.o: test-p1.cc
	$(CC) -g -c test-p1.cc

test-p2.o: test-p2.cc
	$(CC) -g -c test-p2.cc

ExtendedDBFile.o : ExtendedDBFile.cc
	$(CC) -g -c ExtendedDBFile.cc

gTest.o: gTest.cc
	$(CC) -g -c gTest.cc
	
Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc
	
Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc
	
y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c


clean: 
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h
