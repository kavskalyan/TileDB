#include <cstring>
#include <sstream>

#include<fstream>
using namespace std;
#ifdef TRACELOG

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

struct TraceEntry
{
	unsigned short threadid;
	unsigned char label;
	unsigned long tick;
};

static const unsigned long long TraceLogSize = 0x100000;
static TraceEntry TraceLog[TraceLogSize];
static volatile unsigned long TraceLogTail;

void initialize_tracing(){
  memset(TraceLog, -1, sizeof(TraceEntry)*TraceLogSize);
	TraceLogTail = 0;
}


void append_to_log(unsigned char label, unsigned short threadid = 0)
{
	unsigned long oldval;
	unsigned long newval;

	newval = TraceLogTail;

	oldval = newval;
	newval = (oldval + 1) % TraceLogSize;
  TraceLogTail = newval;

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
