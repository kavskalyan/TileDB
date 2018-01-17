#include <tiledb.h>
#include <chrono>
#include <iostream>
#include <cstring>
#include <sstream>
using std::chrono::steady_clock;
using namespace std;

#include<fstream>
#ifdef __cplusplus
extern "C" {
#endif

#if !defined(__i386__) && !defined(__x86_64__) && !defined(__sparc__)
#warning No supported architecture found -- timers will return junk.
#endif

static __inline__ unsigned long long curtick() {
	unsigned long long tick;
#if defined(__i386__)
	unsigned long lo, hi;
	__asm__ __volatile__ (".byte 0x0f, 0x31" : "=a" (lo), "=d" (hi));
	tick = (unsigned long long) hi << 32 | lo;
#elif defined(__x86_64__)
	unsigned long lo, hi;
	__asm__ __volatile__ ("rdtsc" : "=a" (lo), "=d" (hi));
	tick = (unsigned long long) hi << 32 | lo;
#elif defined(__sparc__)
	__asm__ __volatile__ ("rd %%tick, %0" : "=r" (tick));
#endif
	return tick;
}

static __inline__ void startTimer(unsigned long long* t) {
	*(volatile unsigned long long*)t = curtick();
}

static __inline__ void stopTimer(unsigned long long* t) {
	*(volatile unsigned long long*)t = curtick() - *(volatile unsigned long long*)t;
}
#ifdef __cplusplus
}
#endif


#define TRACELOG 1
#ifdef TRACELOG
struct TraceEntry
{
	unsigned short threadid;
	unsigned char label;
	unsigned long tick;
};

static const unsigned long long TraceLogSize = 0x100000;
static TraceEntry TraceLog[TraceLogSize];
static volatile unsigned long TraceLogTail;

void append_to_log(unsigned char label, unsigned short threadid = 0)
{
	// static_assert(sizeof(unsigned long) == sizeof(void*));
  cout <<"Appendign to log" <<endl;
	unsigned long oldval;
	unsigned long newval;

	newval = TraceLogTail;
//Currently only single threadid

	// do
	// {
		oldval = newval;
		newval = (oldval + 1) % TraceLogSize;
    TraceLogTail = newval;
	// 	newval = (unsigned long) atomic_compare_and_swap (
	// 			(void**)&TraceLogTail, (void*)oldval, (void*)newval);
  //
	// } while (newval != oldval);

	TraceLog[newval].threadid = 1;
	TraceLog[newval].label = label;
	TraceLog[newval].tick = curtick();
}


void dbgDumpTraceToFile(const char* filename)
{
	ofstream of(filename);
	for (unsigned long i=1; i<TraceLogSize; ++i)
	{
		if (TraceLog[i].threadid == static_cast<unsigned short>(-1))
			break;
		of << TraceLog[i].threadid << " ";
		of << TraceLog[i].label << " ";
		of << TraceLog[i].tick << endl;
	}
	of.close();
}

#define TRACE( x ) append_to_log( x )
#define TRACEID( x, y ) append_to_log( x , y )
#else
#define TRACE( x )
#define TRACEID( x, y )
#endif

const std::string URI_PREFIX = "hdfs://";
const std::string DIRNAME = "/tmp";
const std::string FILENAME = "/dense_array_1";
void initialize_buffers(int* buffer1, int block_size){
  for (int i = 0; i < block_size; i++){
      buffer1[i] = rand() % 100;
  }
}

int main(int argc, char** argv) {
#ifdef TRACELOG
	memset(TraceLog, -1, sizeof(TraceEntry)*TraceLogSize);
	TraceLogTail = 0;
#endif
  TRACE('1');
  tiledb_ctx_t* ctx;
  tiledb_ctx_create(&ctx);
  TRACE('2');
  std::string array_name_string = URI_PREFIX + DIRNAME + FILENAME;
  int number_of_iterations = 32;
  int block_size = 128;
  if(argc > 1){
    int file_size = atoi(argv[1]);
    block_size = atoi(argv[2]);
    number_of_iterations = file_size/block_size;
    array_name_string = URI_PREFIX + argv[3];
  }

  // Prepare cell buffers
  int buffer_count = (block_size * 1024 * 1024)/4;
  int * buffer_a1 = new int[buffer_count];
  uint64_t buffer_size = buffer_count * sizeof(int);
  std::cout<<buffer_count <<std::endl;
  initialize_buffers(buffer_a1, buffer_count);
  TRACE('3');
  void* buffers[] = {buffer_a1};
  uint64_t buffer_sizes[] = {
  	buffer_size
  };

  const char* array_name = array_name_string.c_str();

  auto start = steady_clock::now();
  // Create query
  TRACE('4');
  tiledb_query_t* query;
  tiledb_query_create(
    ctx,
    &query,
    array_name,
    TILEDB_WRITE,
    TILEDB_GLOBAL_ORDER,
    nullptr,
    nullptr,
    0,
    buffers,
    buffer_sizes);
  TRACE('5');
  // Submit query
  tiledb_query_submit(ctx, query);
  TRACE('6');
  for(int i = 1; i < number_of_iterations; i++){
    tiledb_query_reset_buffers(ctx, query, buffers, buffer_sizes);
    tiledb_query_submit(ctx, query);
  }
  TRACE('7');
  auto end = steady_clock::now();
  double elapsedSeconds = ((end - start).count()) * steady_clock::period::num / static_cast<double>(steady_clock::period::den);
  std::cout << "Time taken to write (in seconds):" << elapsedSeconds << std::endl;
  // Clean up
  tiledb_query_free(ctx, query);
  tiledb_ctx_free(ctx);
  TRACE('8');
  delete [] buffer_a1;
#ifdef TRACELOG
	dbgDumpTraceToFile("HighLevelTrace");
#endif
  return 0;
}
