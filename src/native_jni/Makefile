
libblkrdr.so : blkrdr.o CblkRdr.o
	gcc -shared -o libblkrdr.so blkrdr.o CblkRdr.o
CblkRdr.o : CblkRdr.c CblkRdr.h
	gcc -c -fPIC -o CblkRdr.o CblkRdr.c
blkrdr.o : blkrdr.c
	gcc -c -fPIC -o blkrdr.o blkrdr.c

