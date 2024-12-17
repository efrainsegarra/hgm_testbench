#ifndef __SIMPLE_LOG_H
#define __SIMPLE_LOG_H

#ifdef __cplusplus
extern "C" {
#endif

#ifdef _CVI_
#pragma iso_9899_1999		// Keep __FUNCTION__ as a macro
#endif

// Various log levels
#define SL_ERROR	0x8
#define SL_WARNING	0x4
#define SL_NOTICE	0x2
#define SL_DEBUG	0x1

#define SL_ALL		(SL_ERROR|SL_WARNING|SL_NOTICE|SL_DEBUG)
#define SL_QUIET	0x0

#define SL_ORIGIN	__FILE__, __func__, __LINE__

// Use this as shortcut when calling SimpleLog_Write, or see below in C99
// Do not mistake SL_ERR and SL_ERROR and co
#define SL_ERR	SL_ERROR,  SL_ORIGIN
#define SL_WRN	SL_WARNING,SL_ORIGIN
#define SL_NTC	SL_NOTICE, SL_ORIGIN
#define SL_DBG	SL_DEBUG,  SL_ORIGIN
// For instance SimpleLog_Write(SL_ERR, "Error %d with %s", R, Msg);
// Or better, see below and use SLOG(SWRN, "msg %d", i)... instead as a shortcut

#define SL_MSG_MAX 1024	// Max size of messages written to SimpleLog_Write

extern void SimpleLog_Setup(const char *PathName, 
							const char *TimeFormat, 
							const int NoRepeatLastN, 
							const int RepeatMaxCount,
							const int RepeatMaxSeconds,
							const char *Separator);

extern int SimpleLog_FilterLevel(const int LogLevel);

extern void SimpleLog_RegisterCallback(void (*cb)(char*, int));

extern void SimpleLog_Write(const int Level, 
							const char *OrigFile, const char *OrigFunc, const int LineNb,
							const char *fmt, ... ) __attribute__ (( __format__( __printf__, 5, 6 ) ));

extern void SimpleLog_Flush(void);
extern void SimpleLog_Free(void);

#if __STDC_VERSION__ >= 199901L
	// Even simpler: This is a possible shortcut in C99 only
	#define SLOG(Level, ...) SimpleLog_Write(Level, SL_ORIGIN, __VA_ARGS__)
	// The above code becomes SLOG(SERR, "Error %d with %s", R, Msg);
	// The advantage is that you can do conditionals on the level (which you cannot with SL_ERR):
	//	SLOG(R==0?SDBG:SERR, "Value %d with %s", R, Msg);
	
	// Those are just shorter and same length synonyms
	#define SERR SL_ERROR
	#define SWRN SL_WARNING
	#define SNTC SL_NOTICE
	#define SDBG SL_DEBUG
#endif


#ifdef __cplusplus
}
#endif

#endif
