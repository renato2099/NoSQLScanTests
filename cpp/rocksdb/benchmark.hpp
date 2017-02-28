#include <stdlib.h>
#include <iostream>
#include <thread>
#include <chrono>
#include <random>
#include <limits>
#include <assert.h>

#include "rocksdb/db.h"
#include "rocksdb/options.h"

const int session_timeout = 100000;
const std::string tName = "employees";
const int ln_sz = 500;
const int fn_sz = 515;
const int cn_sz = 5;
const std::string tenStr = "1234567890";
const std::string fiveStr = "12345";
const uint32_t server_span = 4;
using Clock = std::chrono::steady_clock;

   class Benchmark
   {
   	private:
   		void rcInsert(int nInserts, long idRange, rocksdb::DB* db2, bool verbose);
   		std::string genString(int sz);		
   		uint64_t createTable();
                rocksdb::Options createDefOpts();
                rocksdb::DB* getDB(std::string, std::string);
                rocksdb::DB* db;
                std::string dbPath;
                std::string dbName;
   	public:
                int dbStat;
   		void load(int nThreads, int nInserts, bool verbose);
   		void scan(double percentage, bool verbose);
                Benchmark(std::string dp, std::string dn);

   };

Benchmark::Benchmark(std::string dp, std::string dn) {
    dbPath = dp;
    dbName = dn;
    db = getDB(dbPath, dbName);
}

   void Benchmark::scan(double percentage, bool verbose) {
        rocksdb::ReadOptions roptions;
        roptions.verify_checksums = false;
        auto begin = Clock::now();
        std::unique_ptr<rocksdb::Iterator> iter(db->NewIterator(roptions));
        int32_t max = std::numeric_limits<int32_t>::min();
        int32_t nRecords = 0;
        for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
            //TODO deserialize?
            std::string val = iter->value().ToString();
            nRecords ++;
            if (verbose)
                std::cout << val << std::endl;
        }
        auto end = Clock::now();
        std::cout << "[Scan] Elapsed " << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() << " ms\n";
        std::cout << "[Scan] Records read " << nRecords << std::endl;
   }

   void Benchmark::load(int nThreads, int nInserts, bool verbose) {
   	typedef std::chrono::high_resolution_clock clock;
	typedef std::chrono::milliseconds milliseconds;

   	if (verbose) 
	{
		std::cout << "[Load] Datapath: " << dbPath << dbName <<  std::endl;
   		std::cout << "[Load] TotalOps: " << nInserts << " with " << nThreads << " threads." <<  std::endl;
	}
        

   	clock::time_point t0 = clock::now();
   	std::thread* tids = new std::thread[nThreads];

   	int threadOp = nInserts/nThreads;
   	long idRange = 1;
   	for (int i = 0; i < nThreads-1; i++)
	{
		tids[i] = std::thread(&Benchmark::rcInsert, this, threadOp, idRange, db, verbose);
		idRange += threadOp;
	}
	tids[nThreads-1] = std::thread(&Benchmark::rcInsert, this, (threadOp+nInserts%nThreads), idRange, db, verbose);

   	for (int i = 0; i < nThreads; i++)
	{
		tids[i].join();
	}
   	clock::time_point t1 = clock::now();
   	milliseconds total_ms = std::chrono::duration_cast<milliseconds>(t1 - t0);
	std::cout << "[Load] Elapsed ms: " << total_ms.count() << std::endl;
   }

    rocksdb::Options Benchmark::createDefOpts() {
        rocksdb::Options options;
        options.IncreaseParallelism();
        options.OptimizeLevelStyleCompaction();
        options.create_if_missing = true;
        return options;
    }

   rocksdb::DB* Benchmark::getDB(std::string dbPath, std::string dbName) {
        rocksdb::DB* db2;
        std::string fDbPath = dbPath + dbName;
        rocksdb::Status s = rocksdb::DB::Open(createDefOpts(), fDbPath, &db2);
        if (!s.ok()) {
            std::cout<< "Error while opening db " << fDbPath << "\t" << s.ToString() << std::endl;
            return NULL; 
        }
        return db2;
   }

   void Benchmark::rcInsert(int nInserts, long idRange, rocksdb::DB* db2, bool verbose) 
   {
	std::cout << "[Load] Start loading " << nInserts << " range " << idRange << std::endl;
   	int cnt = 0;
        std::random_device rd;
        std::mt19937 rng(rd());
        std::uniform_int_distribution<int> rYears(1, 100);
        std::uniform_real_distribution<> rSal(0.0, 1.0);
        std::string table = "employees";
        rocksdb::WriteOptions wOpts;

   	while(cnt < nInserts) {
   		std::stringstream val;
   		float salary = rSal(rng); 
   		if (salary < 0) salary *=-1;
		int yrs = rYears(rng);
		std::string ln = genString(ln_sz);
		std::string fn = genString(fn_sz);
		std::string cn = genString(5);
		val << ln ;
		val << fn ;
		val << salary;
		val << yrs << cn;
		std::string value = val.str();
                db2->Put(wOpts, rocksdb::Slice(reinterpret_cast<const char*>(&idRange), sizeof(idRange)), rocksdb::Slice(value));
   		if (verbose)
   			if ((cnt*100 / nInserts)%10 == 0)
   				std::cout << "[Load] Inserted tuples " << cnt << std::endl;
		if ((cnt > 0) && (cnt%1000000 == 0))
			std::cout << "[Load] inserted " << cnt/1000000 + 1 << " millions." << std::endl;
   		idRange ++;
   		cnt ++;
   	}
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
