#include <stdlib.h>
#include <iostream>
#include <thread>
#include <chrono>
#include <random>
#include <limits>
#include <assert.h>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
//#include "ClusterMetrics.h"
//#include "Context.h"
//#include "Cycles.h"
//#include "Dispatch.h"
//#include "ShortMacros.h"
//#include "Crc32C.h"
//#include "ObjectFinder.h"
//#include "RamCloud.h"
//#include "Tub.h"
//#include "IndexLookup.h"
//#include "TableEnumerator.h"

//using namespace RAMCloud;

const int session_timeout = 100000;
const std::string tName = "employees";
const int ln_sz = 500;
const int fn_sz = 515;
const int cn_sz = 5;
const std::string tenStr = "1234567890";
const std::string fiveStr = "12345";
const uint32_t server_span = 4;

   class Benchmark
   {
   	private:
   		void rcInsert(int nInserts, long idRange, std::string dbPath,  std::string dbName, bool verbose);
   		std::string genString(int sz);		
   		uint64_t createTable();
                void setupDb();
                rocksdb::Options createDefOpts();
                rocksdb::DB* db;
                std::string dbPath;
                std::string dbName;
   	public:
                int dbStat;
   		void load(int nThreads, int nInserts, bool verbose);
   		void scan(double percentage, std::string locator, std::string clName);
		void idxScan(std::string locator, std::string clName);
                Benchmark(std::string dp, std::string dn);

   };

Benchmark::Benchmark(std::string dp, std::string dn) {
    dbPath = dp;
    dbName = dn;
}

void Benchmark::setupDb() {
//    rocksdb::Status s = rocksdb::DB::Open(createDefOpts, dbPath + dbName, &this->db);
//    dbStat = 0;
//    if (!s.ok()) 
//        dbStat = -1;
}
   void Benchmark::idxScan(std::string locator, std::string clName)
 {
/*	Context context(false);
	context.transportManager->setSessionTimeout(session_timeout);
	RamCloud client(&context, locator.c_str(), clName.c_str());
	uint64_t tableId = 0;
	try {
		tableId = client.getTableId(tName.c_str());
		// checkCreated index if not
		// scan index
	} catch (ClientException &e) {
		std::cout << "[IdxScan] Error while index scanning." << std::endl;
		std::cout << "[IdxScan] TableId " << tableId << std::endl;
	}
        */
 }
   void Benchmark::scan(double percentage, std::string locator, std::string clName) {
   	/*Context context(false);
   	context.transportManager->setSessionTimeout(session_timeout);
   	RamCloud client(&context, locator.c_str(), clName.c_str());
	uint64_t tableId = 0;
	try {
		tableId = client.getTableId(tName.c_str());
		Buffer objects, state;
		size_t receivedSize = 0ul;
		TableEnumerator tEnum(client, tableId, false);
		auto beginTime = std::chrono::system_clock::now();
		while (tEnum.hasNext()) {
			uint32_t size;
			const void* objs = 0;
			tEnum.next(&size, &objs);
			//currentTablet = client.enumerateTable(tableId, false, currentTablet, state, objects);
			receivedSize += size;
#ifndef NDEBUG
			//std::cout << "Got " << objects.size() << " in " << objects.getNumberChunks() << " Chunks" << std::endl;
#endif
		} //while (currentTablet);
		auto duration = std::chrono::system_clock::now() - beginTime;
		std::cout << "[Scan] Received " << receivedSize << " bytes\n";
		std::cout << "[Scan] Elapsed " << std::chrono::duration_cast<std::chrono::milliseconds>(duration).count() << " ms\n";
	} catch (ClientException &e) {
		std::cout << "[Scan] Something went wrong while trying to scan." << std::endl;
		std::cout << "[Scan] TableId:" << tableId << std::endl;
	}*/
   }

   void Benchmark::load(int nThreads, int nInserts, bool verbose) {
   	typedef std::chrono::high_resolution_clock clock;
	typedef std::chrono::milliseconds milliseconds;

   	if (verbose) 
	{
		std::cout << "[Load] Datapath: " << dbPath << dbName <<  std::endl;
   		std::cout << "[Load] TotalOps: " << nInserts << " with " << nThreads << " threads." <<  std::endl;
	}
        
	/*Context context(false);
   	context.transportManager->setSessionTimeout(session_timeout);

	RamCloud client(&context, coordLocator.c_str(), clusterName.c_str());
	uint64_t tableId = 0;
	tableId = createTable(&client);*/

   	clock::time_point t0 = clock::now();
   	std::thread* tids = new std::thread[nThreads];

   	int threadOp = nInserts/nThreads;
   	long idRange = 1;
   	for (int i = 0; i < nThreads-1; i++)
	{
		tids[i] = std::thread(&Benchmark::rcInsert, this, threadOp, idRange, dbPath, dbName, verbose);
		idRange += threadOp;
	}
	tids[nThreads-1] = std::thread(&Benchmark::rcInsert, this, (threadOp+nInserts%nThreads), idRange, dbPath, dbName, verbose);

   	for (int i = 0; i < nThreads; i++)
	{
		tids[i].join();
	}
   	clock::time_point t1 = clock::now();
   	milliseconds total_ms = std::chrono::duration_cast<milliseconds>(t1 - t0);
	std::cout << "[Load] Elapsed ms: " << total_ms.count() << std::endl;
       /* */
   }

rocksdb::Options Benchmark::createDefOpts() {
    rocksdb::Options options;
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
    options.create_if_missing = true;
    return options;
}
   void Benchmark::rcInsert(int nInserts, long idRange, std::string dbPath, std::string dbName, bool verbose) 
   {
	std::cout << "[Load] Start loading " << nInserts << " range " << idRange << std::endl;
   	int cnt = 0;
        rocksdb::DB* db2;
        std::string fDbPath = dbPath + dbName;
        
        rocksdb::Status s = rocksdb::DB::Open(createDefOpts(), fDbPath, &db2);
        if (!s.ok()) {
            std::cout<< "Error while opening db " << fDbPath << std::endl;
            return;
        }
   	/*tpcc::Random_t random;
	std::string table = "employees";
   	Context context(false);
   	context.transportManager->setSessionTimeout(session_timeout);
   	RamCloud client(&context, locator.c_str(), clName.c_str());

   	while(cnt < nInserts) {
   		std::stringstream val;
   		float salary = random.randomWithin(0.0, 1.0);
   		if (salary < 0) salary *=-1;
		int yrs = random.random(1,100);
		std::string ln = genString(ln_sz);
		std::string fn = genString(fn_sz);
		std::string cn = genString(5);
		val << ln ;
		val << fn ;
		val <<  salary;
		val << yrs << cn;
		std::string value = val.str();
		//std::cout << idRange << std::endl;
		const char * vv = value.c_str();
   		client.write(tableId, (const void*)&idRange, sizeof(idRange), vv, uint32_t(value.size()));
   		//client.write(tableId, vali.c_str(), downCast<uint16_t>(idRange), val.c_str(), downCast<uint32_t>(strlen(val.c_str())+1));
   		if (verbose)
   			if ((cnt*100 / nInserts)%10 == 0)
   				std::cout << "[Load] Inserted tuples " << cnt << std::endl;
		if (cnt%1000000 == 0)
			std::cout << "[Load] inserted " << cnt/1000000 + 1 << " millions." << std::endl;
   		idRange ++;
   		cnt ++;
   	}*/
   }

   std::string Benchmark::genString(int sz) {
	std::stringstream ss;
	int cur_sz = 0;
	while (cur_sz < sz) {
		if (cur_sz + 10 < sz )
			ss << tenStr;
		else
			ss << fiveStr;	
		cur_sz = ss.str().size();
	}
	return ss.str();
   }
