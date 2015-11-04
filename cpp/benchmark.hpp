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

   class Benchmark
   {
   	private:
   		void rcInsert(int nInserts, long idRange, uint64_t tableId, std::string locator, std::string clName, bool verbose);
   		
   		// uint64_t createTable(RamCloud client);
   	public:
   		void load(int nThreads, int nInserts, std::string locator, std::string clName, bool verbose);
   		void scan(double percentage);

   };

   /*uint64_t Benchmark::createTable(RamCloud client) {
   	std::string locator = "";
   	std::string clName = "";
   	std::string tableName = "employees";
   	uint64_t tableId = 0;
   	// tableId = client->getTableId(tableName);
   	if (tableId == 0) {
   		std::cout<<"[Load] Table non-existing" << std::endl;
   		std::cout<<"[Load] Creating table" << tableName << std::endl;
   		// tableId = client->createTable(tableName);
   	} else {
   		std::cout<<"[Load] Table exists. Reusing it." << std::endl;
   		// cient->dropTable(tableName);
   	}
   	return tableId;
   }*/

   void Benchmark::load(int nThreads, int nInserts, std::string locator, std::string clName, bool verbose) {
   	typedef std::chrono::high_resolution_clock clock;
	typedef std::chrono::milliseconds milliseconds;

	// Context context(false);
   	// context.transportManager->setSessionTimeout(100000);
	// RamCloud client(&context, locator.c_str(), clName.c_str());
	uint64_t tableId = 0;
	// tableId = createTable(client);

   	clock::time_point t0 = clock::now();
   	std::thread* tids = new std::thread[nThreads];

   	if (verbose)
   		std::cout << "[Load] " << nInserts << " with " << nThreads << " threads." << std::endl;

   	int threadOp = nInserts/nThreads;
   	long idRange = 1;
   	for (int i = 0; i < nThreads-1; i++)
	{
		tids[i] = std::thread(&Benchmark::rcInsert, this, threadOp, idRange, tableId, locator, clName, verbose);
		idRange += threadOp;
	}
	tids[nThreads-1] = std::thread(&Benchmark::rcInsert, this, (threadOp+nInserts%nThreads), idRange, tableId, locator, clName, verbose);

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
   	std::cout<<std::endl;

   	Context context(false);
   	context.transportManager->setSessionTimeout(100000);
   	RamCloud client(&context, locator.c_str(), clName.c_str());

   	std::stringstream record;
   	tpcc::Random_t random;
   	float salary = random.randomWithin(0.0, 1.0);
   	if (salary < 0) salary *=-1;

   	record << random.astring(15, 15) << random.astring(20, 20) <<  salary  << random.random(1, 100) << random.astring(5,5); 
   	// std::cout << record.str();

   	while(cnt < nInserts) {
   		std::cout << "Inserted element: " << idRange << std::endl;
   		std::string val = "";
   		// client.write(table, idRange, downCast<uint16_t>(idRange), val, downCast<uint32_t>(strlen(val)+1));
   		if (verbose)
   			if ((cnt*100 % nInserts)/10 == 0)
   				std::cout << "[Load] Inserted " << idRange << std::endl;
   		idRange ++;
   		cnt ++;
   	}
   }