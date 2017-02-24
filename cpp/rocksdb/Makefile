# Makefile for GNU make

# For compiling in OSX
#UNAME_S := $(shell uname -s)
#ifeq ($(UNAME_S),Darwin)
#  CC = clang++
#endif

ROCKS_DIR = /home/marenato/Documents/workspace/workspacePhd/rocksdb

#CFLAGS = -std=c++11  -O2 -Wall -I${ROCKS_DIR}/include/  -I${RAMC_DIR}/obj.master -I${RAMC_DIR}/gtest/include/ -L${RAMC_DIR}/obj.master -Wl,-R${RAMC_DIR}/obj.master '-Wl,-R$$ORIGIN' -g
CFLAGS = $(ROCKS_DIR)/librocksdb.a -I$(ROCKS_DIR)/include -O2 -std=c++11 
#RELEASE_CFLAGS = -std=c++11  -O2 -Wall $(ROCKS_DIR)/librocksdb.a -I${ROCKS_DIR}/include/  -O3 -DNDEBUG -march=native
RELEASE_CFLAGS = $(ROCKS_DIR)/librocksdb.a -I$(ROCKS_DIR)/include -std=c++11  -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX  -DOS_LINUX -fno-builtin-memcmp -DROCKSDB_FALLOCATE_PRESENT -DSNAPPY -DGFLAGS=gflags -DZLIB -DBZIP2 -DZSTD -DNUMA -DTBB -DROCKSDB_MALLOC_USABLE_SIZE -DROCKSDB_PTHREAD_ADAPTIVE_MUTEX -DROCKSDB_BACKTRACE -march=native  "-DROCKSDB_JEMALLOC"

#g++  main.cpp -omainn 

#LDLIBS = -pthread -lrt -lramcloud -lboost_system
LDLIBS = $(ROCKS_DIR)/librocksdb.a -I$(ROCKS_DIR)/include -lpthread -lrt -lsnappy -lgflags -lz -lbz2 -lzstd -lnuma -ltbb -ljemalloc 
#LDLIBS = -lpthread -lrt -lsnappy -lgflags -lz -lbz2 -lzstd -lnuma -ltbb -ljemalloc 

OBJECTS = main.o Util.o

%.o : %.cpp
	$(CXX) $(RELEASE_CFLAGS) -c $<

main: $(OBJECTS) 
	$(CXX) $(RELEASE_CFLAGS) -o $@ $(OBJECTS) $(LDLIBS) 

clean:
	rm -rf *.o main Util *~ /tmp/testdb
