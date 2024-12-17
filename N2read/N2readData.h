#ifndef __N2_READ_DATA_H
#define __N2_READ_DATA_H

// NOTE: this library is multithreadable thanks to thread specific static variables. No locks are used.

#ifdef __cplusplus
extern "C" {
#endif


typedef struct sColumn {
	char *Name, *Description, *DataType;
} tColumn;

// This is for one file. No, I don't want to make this an opaque struct
typedef struct sN2data {
	// Content coming from config file (.hd)
	char* Name;
	char* ConfigPathname;
	char* DataPathname;
	
	unsigned long long EOLidentifier;	// If 0, nothing has been read yet.
	int RunNo, CycNo;	// Set from the config file. Should match the filename
	int HdrVer;			// Not currently used. TODO?
	int NbCol;			// The timestamp is counted, the EOL is not

	tColumn *Columns;	// Contains the column headers. Size NbCol
	char **Labels;		// Simplified version of Columns. Size NbCol
	
	long long FirstTimeStamp, LastTimeStamp, LastWrite;	// Not sure what last one is for
	
	// Content coming from data file (.EDMdat)
	int NbRow, 						// Number of rows in TimeStamp[] and Data[][]
		ReservedSize;				// Number of reserved rows in the array, to optimize realloc
	/*unsigned*/ long long *TimeStamp;	// [NbRow] in ns -> reduplicated in Data[][0] in s
	void **Data;					// Array of [NbRow][NbCol], with some columns being uint64 and others being double,
									// depending on Columns[].DataType. All items 8 bytes anyway
	//unsigned long long *EOL;		// [NbRow]
	// Other unneeded parameters include githash, compileDate, compileTime, swVersionMajor, swVersionMinor, 
	//                                   nodeGithash, nodeCompileDate, nodeCompileTime, nodeSwVersionMajor, nodeSwVersionMinor...
	
	struct sFilter {	// For use by AMIn2_ReadSeriesJSON
		int MaxRows, Decimation;
		long long StartTimeStamp, EndTimeStamp;
	} tFilter;
} tN2data;


// Those functions don't open any files, they only explore the directories
extern int  N2_GetRunNumbers  (const char* RootDirName, int Direct, int *RunNoList[], const int PartialRunNo);
extern int  N2_GetSubsystems  (const char* RootDirName, int Direct, int  RunNo,       char**SubsList[]);
extern int  N2_GetCycleNumbers(const char* RootDirName, int Direct, int  RunNo, const char* Subsystem, int *CycNoList[]);

extern int  N2_GetMinMaxRunNumbers  (const char* RootDirName, int Direct, 
									 int *RunNoMin, int *RunNoMax, const int StartFromRunNo);
extern int  N2_GetMinMaxCycleNumbers(const char* RootDirName, int Direct, int  RunNo, const char* Subsystem, 
									 int *CycNoMin, int *CycNoMax);
extern void N2_ClearStuff(void);
	
//extern int N2_GetRunNumbersBetweenDates(const char* RootDirName, int Direct, long long StartTimeStamp, 
//										long long EndTimeStamp, int *RunNoMin, int *RunNoMax,
//										long long *TimeStampOfStartRun, long long *TimeStampOfEndRun);
int N2_GetRunNumbersTimeStamps(const char* RootDirName, int Direct, 
							   const char* OptionalSubsystem, 
							   int *RunNumbers[], 
							   long long *RunStarts[], long long *RunEnds[],
							   const int StartFromRunNo);
	
// Those functions only open the header file
extern int  N2_ReadConfig(const char* ConfigPathName, tN2data *N2data, int Quick);
extern void N2_ClearConfig(tN2data *N2data);
extern void N2_CopyConfig(tN2data *N2dest, const tN2data *N2source);
extern int  N2_AddDataWithFilter(tN2data *N2dest, tN2data *N2source, int* Remaining, 
						int Decimation, int MaxRows, 
						long long TimeStampLow, long long TimeStampHigh);

// Those functions open both header and data files
extern int  N2_ReadFile(const char* ConfigPathName, tN2data *N2data);
// If Direct=0, the RootDirName is partly based on RunNo...
extern int  N2_ReadData(const char* RootDirName, int Direct, 
						int RunNo, int CycNo, int SizeIdx, const char* Subsystem, int HdrVer, 
						tN2data *N2data, long long AltFirstTimeStamp);


// Conversions
extern const char*N2_NanoToDateStr(long long TimeStamp, const char* TimeFrmt);
extern       void N2_NanoToDate   (long long TimeStamp, int* Year, int* Month, int* Day, 
								   int* Hour, int* Min, int* Sec, int* Nano);
extern const char*N2_MakePathName(int IsConfig, const char* RootDirName, 
								  int Direct, int RunNo, int CycNo, int SizeIdx,
								  const char* Subsystem, int HdrVer);
	

// Glue code for Java, see SimpleLog.h for syntax
extern int  N2_LogFilterLevel(const int LogLevel);
extern void N2_LogSetup(const char *PathName, 
						const char *TimeFormat, 
						const int NoRepeatLastN, 
						const int RepeatMaxCount,
						const int RepeatMaxSeconds,
						const char *Separator);

#ifdef __cplusplus
}
#endif


#endif
