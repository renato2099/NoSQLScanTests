# Makefile for GNU make

# For compiling in OSX
#UNAME_S := $(shell uname -s)
#ifeq ($(UNAME_S),Darwin)
#  CC = clang++
#endif
#RAMC_DIR = /home/marenato/Documents/workspace/workspacePhd/RAMCloud
RAMC_DIR = /home/marenato/NoSQLTests/RAMCloud

LDLIBS = -pthread -lrt -lramcloud -lboost_system
CFLAGS = -std=c++11  -O2 -Wall -I${RAMC_DIR}/src/  -I${RAMC_DIR}/obj.master -I${RAMC_DIR}/gtest/include/ -L${RAMC_DIR}/obj.master -Wl,-R${RAMC_DIR}/obj.master '-Wl,-R$$ORIGIN' -g
RELEASE_CFLAGS = -std=c++11  -O2 -Wall -I${RAMC_DIR}/src/  -I${RAMC_DIR}/obj.master -I${RAMC_DIR}/gtest/include/ -L${RAMC_DIR}/obj.master -Wl,-R${RAMC_DIR}/obj.master '-Wl,-R$$ORIGIN' -O3 -DNDEBUG -march=native

OBJECTS = main.o Util.o

%.o : %.cpp
	$(CXX) $(RELEASE_CFLAGS) -c $<


main: $(OBJECTS) 
	$(CXX) $(RELEASE_CFLAGS) -o $@ $(OBJECTS) $(LDLIBS) 

clean:
	rm -f *.o main Util *~
