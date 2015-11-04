# Makefile for GNU make

LDLIBS = -pthread -lrt -lramcloud -lboost_system

# For compiling in OSX
#UNAME_S := $(shell uname -s)
#ifeq ($(UNAME_S),Darwin)
#  CC = clang++
#endif
RAMC_DIR = /home/marenato/Documents/workspace/workspacePhd/RAMCloud

CFLAGS = -std=c++11  -O2 -Wall -I${RAMC_DIR}/src/ -I${RAMC_DIR}/obj.master -L${RAMC_DIR}/obj.master 



OBJECTS = main.o Util.o

main: $(OBJECTS) 
	$(CXX) $(CFLAGS) -o $@ $(OBJECTS) $(LDLIBS) 

%.o : %.cpp
	$(CXX) $(CFLAGS) -c $<

clean:
	rm -f *.o main Util *~
