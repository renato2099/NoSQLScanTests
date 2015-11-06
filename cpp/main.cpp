#include <stdlib.h>
#include <sstream>
#include "optionparser.h"
#include "benchmark.hpp"

using namespace std;

const int DEFAULT_OPS = 1000;
const int DEFAULT_THREADS = 4;

struct Arg: public option::Arg
{
  static void printError(const char* msg1, const option::Option& opt, const char* msg2)
  {
    fprintf(stderr, "%s", msg1);
    fwrite(opt.name, opt.namelen, 1, stderr);
    fprintf(stderr, "%s", msg2);
  }

  static option::ArgStatus Unknown(const option::Option& option, bool msg)
  {
    if (msg) printError("Unknown option '", option, "'\n");
    return option::ARG_ILLEGAL;
  }

  static option::ArgStatus Required(const option::Option& option, bool msg)
  {
    if (option.arg != 0)
      return option::ARG_OK;

    if (msg) printError("Option '", option, "' requires an argument\n");
    return option::ARG_ILLEGAL;
  }

  static option::ArgStatus NonEmpty(const option::Option& option, bool msg)
  {
    if (option.arg != 0 && option.arg[0] != 0)
      return option::ARG_OK;

    if (msg) printError("Option '", option, "' requires a non-empty argument\n");
    return option::ARG_ILLEGAL;
  }

  static option::ArgStatus Numeric(const option::Option& option, bool msg)
  {
    char* endptr = 0;
    if (option.arg != 0 && strtol(option.arg, &endptr, 10)){};
    if (endptr != option.arg && *endptr == 0)
      return option::ARG_OK;

    if (msg) printError("Option '", option, "' requires a numeric argument\n");
    return option::ARG_ILLEGAL;
  }
};

enum  optionIndex { UNKNOWN, HELP, VERBOSE, THREADS, OPERATIONS, LOCATOR, CLUSTER, INSERT, PERCENTAGE };
const option::Descriptor usage[] =
{
	{UNKNOWN,		0,	"", 	"",		 		Arg::None, 		"USAGE: example [options]\n\n"
																"Options:" },
	{HELP,    		0,	"h", 	"help",			Arg::None, 		"  --help \t \t -h \t \t Print usage and exit." },
	{VERBOSE,  		0,	"v", 	"verbose",		Arg::None, 		"  --verbose \t \t -v \t \t Print information about the benchmark." },
	{THREADS,   	0,	"t", 	"threads",		Arg::Numeric, 	"  --threads <num> \t \t -t <num>\t \t Number of threads." },
	{OPERATIONS,	0,	"o",	"operations",	Arg::Numeric,	"  --operations <num> \t \t -o <num> \t \t Number of total operations."},
	{LOCATOR,	0,	"l",	"locator",	Arg::Required,	"  --locator <locator> \t \t -l <locator> \t \t Locator name."},
	{INSERT,	0,	"i",	"insert",	Arg::None,	"  --insert <insert> \t \t -i <insert> \t \t To insert data into table."},
	{PERCENTAGE,	0,	"p",	"percentage",	Arg::Numeric,	"  --percentage <percentage> \t \t -p <percentage> \t \t Percentage in terms of salary."},
	{CLUSTER,	0,	"c",	"cluster",	Arg::Required,	"  --cluster <clusterName> \t \t -c <clusterName> \t \t Cluster name."},
	{0,0,0,0,0,0}
};

int readCmdLine(int argc, char** argv, int &numThreads, int &numOperations, std::string &tableName, std::string &clusterName, double &percentage, bool &load, bool &verbose)
{
	// program options
	// program options
	stringstream strValue, strVal;
	argc-=(argc>0); argv+=(argc>0); // skip program name argv[0] if present
	option::Stats stats(usage, argc, argv);
	option::Option options[stats.options_max], buffer[stats.buffer_max];
	option::Parser parse(usage, argc, argv, options, buffer);

	if (parse.error())
		return 1;

	if (options[HELP] || argc == 0)
	{
		int columns = getenv("COLUMNS")? atoi(getenv("COLUMNS")) : 80;
		option::printUsage(fwrite, stdout, usage, columns);
		return 2;
	}

	
	for (int i = 0; i < parse.optionsCount(); ++i)
	{
		option::Option& opt = buffer[i];
		// std::cout<<opt.index()<<opt.arg<<std::endl;
		switch (opt.index())
		{
			// UNKNOWN, HELP, BENCHMARK, POP, RM, THRDS, INSERTS, FIXED, VERBOSE, LOCK_FREE, CNC_TBB
			case HELP:
				// not possible, because handled further above and exits the program
			case INSERT:
				load = true;
				break;
			case PERCENTAGE:
				char *endptr;
				percentage = std::strtod(opt.arg, &endptr);
				break;
			case VERBOSE:
				verbose = true;
				//istringstream(opt.arg) >> std::boolalpha >> verbose;
				break;
			case LOCATOR:
				tableName = opt.arg;
				break;
			case CLUSTER:
				clusterName = opt.arg;
				break;
			case THREADS:
				strValue << opt.arg;
				strValue >> numThreads;
				break;
			case OPERATIONS:
				numOperations = std::stoi(opt.arg);
				break;
			case UNKNOWN:
				// not possible because Arg::Unknown returns ARG_ILLEGAL
				// which aborts the parse with an error
				break;
		}
	}
	return 0;
}

int check_parameters(int numThreads, int numOperations, std::string locator, std::string clusterName, double percentage, bool load, bool verbose)
{
	//todo some validation over percentage and load
	if (locator.empty()) 
	{
		cerr << "A locator must be specified." << endl;
		return 1;
	}
	if (clusterName.empty())
	{
		cerr << "A clusterName must be specified." << endl;
		return 1;
	}
	if (numThreads < 1)
	{
		cerr << "The number of threads has be a positive integer number." << endl;
		return 1;
	}

	if (numOperations == 0)
	{
		cerr << "Using a default number of operations." << DEFAULT_OPS << endl;
		return 1;
	}
	return 0;
}


int main(int argc, char** argv) 
{
	bool verbose = false, load = false;
	int numThreads = DEFAULT_THREADS, numOperations = DEFAULT_OPS;
	std::string locator = "";
	std::string clusterName = "cluster";
	double percentage = 0.5;

	if (!readCmdLine(argc, argv, numThreads, numOperations, locator, clusterName, percentage, load, verbose))
	{
		if (check_parameters(numThreads, numOperations, locator, clusterName, percentage, load, verbose)) {
			exit(1);
		}
		Benchmark bench;
		if (load) 
		{
			std::cout << "Loading database" << std::endl;
			bench.load(numThreads, numOperations, locator, clusterName, verbose);
		}
		std::cout << "Scanning database" << std::endl;
		bench.scan(percentage, locator, clusterName);
	}
	return 0;
}
