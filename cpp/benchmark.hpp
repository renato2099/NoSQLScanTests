#include <stdlib.h>
#include <iostream>
#include <thread>
#include <chrono>
#include <random>
#include <limits>
#include <assert.h>
#include "Util.hpp"

#include "ClusterMetrics.h"
#include "Context.h"
#include "Cycles.h"
#include "Dispatch.h"
#include "ShortMacros.h"
#include "Crc32C.h"
#include "ObjectFinder.h"
#include "RamCloud.h"
#include "Tub.h"
#include "IndexLookup.h"

using namespace RAMCloud;

const int session_timeout = 100000;
const std::string tName = "employees";

   class Benchmark
   {
   	private:
   		void rcInsert(int nInserts, long idRange, uint64_t tableId, std::string locator, std::string clName, bool verbose);
   		
   		uint64_t createTable(RamCloud* client);
   	public:
   		void load(int nThreads, int nInserts, std::string locator, std::string clName, bool verbose);
   		void scan(double percentage, std::string locator, std::string clName);

   };

   void Benchmark::scan(double percentage, std::string locator, std::string clName) {
   	Context context(false);
   	context.transportManager->setSessionTimeout(session_timeout);
   	RamCloud client(&context, locator.c_str(), clName.c_str());
	uint64_t tableId = 0;
	try {
		tableId = client.getTableId(tName.c_str());
		Buffer objects, state;
		uint64_t currentTablet = 0ul;
		size_t receivedSize = 0ul;
		auto beginTime = std::chrono::system_clock::now();
		do {
			currentTablet = client.enumerateTable(tableId, false, currentTablet, state, objects);
			receivedSize += objects.size();
#ifndef NDEBUG
			std::cout << "Got " << objects.size() << " in " << objects.getNumberChunks() << " Chunks" << std::endl;
#endif
		} while (currentTablet);
		auto duration = std::chrono::system_clock::now() - beginTime;
		std::cout << "Recieved " << receivedSize << " bytes\n";
		std::cout << "Scan took " << std::chrono::duration_cast<std::chrono::milliseconds>(duration).count() << " ms\n";
	} catch (ClientException &e) {
		std::cout << "[Scan] Something went wrong while trying to scan." << std::endl;
	}
   }

   uint64_t Benchmark::createTable(RamCloud* client) {
   	uint64_t tableId = 0;
	std::string tableName = "employees";
	try
	{
   		tableId = client->getTableId(tName.c_str());
	} catch (ClientException &e) 
	{
		std::cout<<"[Load] Table doesn't exist. It will be created." << std::endl;
	}
   	if (tableId == 0) {
   		std::cout<<"[Load] Creating table" << tName << std::endl;
   		tableId = client->createTable(tName.c_str());
   	} else {
   		std::cout<<"[Load] Table exists. Reusing it." << std::endl;
   		//client->dropTable(tableName);
   	}
   	return tableId;
   }

   void Benchmark::load(int nThreads, int nInserts, std::string coordLocator, std::string clusterName, bool verbose) {
   	typedef std::chrono::high_resolution_clock clock;
	typedef std::chrono::milliseconds milliseconds;

   	if (verbose) 
	{
		std::cout << "[Load] Cluster: " << clusterName << " ServiceLocator: " << coordLocator <<  std::endl;
   		std::cout << "[Load] TotalOps: " << nInserts << " with " << nThreads << " threads." <<  std::endl;
	}

	Context context(false);
   	context.transportManager->setSessionTimeout(session_timeout);

	RamCloud client(&context, coordLocator.c_str(), clusterName.c_str());
	uint64_t tableId = 0;
	tableId = createTable(&client);

   	clock::time_point t0 = clock::now();
   	std::thread* tids = new std::thread[nThreads];

   	int threadOp = nInserts/nThreads;
   	long idRange = 1;
   	for (int i = 0; i < nThreads-1; i++)
	{
		tids[i] = std::thread(&Benchmark::rcInsert, this, threadOp, idRange, tableId, coordLocator, clusterName, verbose);
		idRange += threadOp;
	}
	tids[nThreads-1] = std::thread(&Benchmark::rcInsert, this, (threadOp+nInserts%nThreads), idRange, tableId, coordLocator, clusterName, verbose);

   	for (int i = 0; i < nThreads; i++)
	{
		tids[i].join();
	}
   	clock::time_point t1 = clock::now();
   	milliseconds total_ms = std::chrono::duration_cast<milliseconds>(t1 - t0);
	std::cout << "[Load] Elapsed ms: " << total_ms.count() << std::endl;
   }

   void Benchmark::rcInsert(int nInserts, long idRange, uint64_t tableId, std::string locator, std::string clName, bool verbose) 
   {
   	int cnt = 0;
   	tpcc::Random_t random;
	std::string table = "employees";
   	Context context(false);
   	context.transportManager->setSessionTimeout(100000);
   	RamCloud client(&context, locator.c_str(), clName.c_str());

   	while(cnt < nInserts) {
   		//std::cout << "Inserted element: " << idRange << std::endl;
   		std::stringstream val;
   		float salary = random.randomWithin(0.0, 1.0);
   		if (salary < 0) salary *=-1;
		val << random.astring(500, 500) << random.astring(515, 515) <<  salary  << random.random(1, 100) << random.astring(5,5);
		std::string value = val.str();
		const char * vv = value.c_str();
   		client.write(tableId, (const void*)&idRange, sizeof(idRange), vv, uint32_t(value.size()));
   		//client.write(tableId, vali.c_str(), downCast<uint16_t>(idRange), val.c_str(), downCast<uint32_t>(strlen(val.c_str())+1));
   		if (verbose)
   			if ((cnt*100 % nInserts)/10 == 0)
   				std::cout << "[Load] Inserted " << idRange << std::endl;
   		idRange ++;
   		cnt ++;
   	}
   }
