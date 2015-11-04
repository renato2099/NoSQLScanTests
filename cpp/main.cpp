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

enum  optionIndex { UNKNOWN, HELP, VERBOSE, THREADS, OPERATIONS, TABLE, CLUSTER };
const option::Descriptor usage[] =
{
	{UNKNOWN,		0,	"", 	"",		 		Arg::None, 		"USAGE: example [options]\n\n"
																"Options:" },
	{HELP,    		0,	"h", 	"help",			Arg::None, 		"  --help \t \t -h \t \t Print usage and exit." },
	{VERBOSE,  		0,	"v", 	"verbose",		Arg::None, 		"  --verbose \t \t -v \t \t Print information about the benchmark." },
	{THREADS,   	0,	"t", 	"threads",		Arg::Numeric, 	"  --threads <num> \t \t -t <num>\t \t Number of threads." },
	{OPERATIONS,	0,	"o",	"operations",	Arg::Numeric,	"  --operations <num> \t \t -o <num> \t \t Number of total operations."},
	{TABLE,	0,	"tb",	"table",	Arg::Numeric,	"  --table <tableName> \t \t -tb <tableName> \t \t Table name."},
	{CLUSTER,	0,	"c",	"cluster",	Arg::Numeric,	"  --cluster <clusterName> \t \t -c <clusterName> \t \t Table name."},
	{0,0,0,0,0,0}
};

int readCmdLine(int argc, char** argv, int &numThreads, int &numOperations, bool &verbose)
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
			case VERBOSE:
				verbose = true;
				// std::cout<<opt.arg<<std::endl;
				//istringstream(opt.arg) >> std::boolalpha >> verbose;
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

int check_parameters(int numThreads, int numOperations, bool verbose)
{
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
	bool verbose = false;
	int numThreads = DEFAULT_THREADS, numOperations = DEFAULT_OPS;
	std::string tName = "employees";
	std::string clName = "cluster";

	if (!readCmdLine(argc, argv, numThreads, numOperations, verbose))
	{
		if (check_parameters(numThreads, numOperations, verbose)) {
			exit(1);
		}
		Benchmark bench;
		std::cout << "Loading database" << std::endl;
		bench.load(numThreads, numOperations, tName, clName, verbose);
		std::cout << "Scanning database" << std::endl;
		// bench.scan();
	}
	std::cout << "verbose" << std::endl;
	return 0;
}