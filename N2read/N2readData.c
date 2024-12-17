#include <stdlib.h>
#include <stdio.h>
#include <math.h>
#include <errno.h>
#include <string.h>
#include <time.h>
#include <iso646.h>
//#include <threads.h>	// Only for thread_local variable definitions (C11). Better use __thread instead
//#include <sys/stat.h>	// Only for stat.h in case FirstTimeStamp is missing

#if 0
	#include <linux/limits.h>	// Can't find it easily on the Mac
#else
	#define PATH_MAX 4096
#endif

#include <libconfig.h>

#include "SimpleLog.h"
#include "N2readData.h"

///////////////////////////////////////////////////////////////////////////////

// Forward declarations
static const char* ConfigToDataName(const char* ConfigPathName);
static long long GetMissingFirstTimeStamp(const char* DataPathName);
static long long GetMissingLastTimeStamp (const char* DataPathName, int NbCols);
static int ReadData(const char* PathName, tN2data *N2data, long long AltFirstTimeStamp);


#define NANO_TO_SEC(TimeStamp) ((TimeStamp)/1e9)
#define ST "%Y%m%d-%H%M%S"
int NbAlloc=0, NbFree=0;	// Debug

///////////////////////////////////////////////////////////////////////////////
/// HIFN	Equivalent to strftime()
/// HIPAR	TimeFrmt / Format string in strftime format (optional, you can pass NULL for default)
/// HIPAR	TimeFrmt / Only NULL will return nanosec info
/// HIRET	Default string will be YYYYMMDD-HHMMSS.nanosec
///////////////////////////////////////////////////////////////////////////////
const char* N2_NanoToDateStr(long long TimeStamp, const char* TimeFrmt) {
	time_t Secs=TimeStamp/1000000000;
//	time_t Time=time(&Secs);
	/*thread_local*/ static __thread char TimeStr[80]="";
	int UseDefaultFrmt=(TimeFrmt==NULL or !*TimeFrmt);
	struct tm t_result;
//	SLOG(SDBG, "%d %d", Secs, Time);
	strftime(TimeStr, 79, UseDefaultFrmt ? "%Y%m%d-%H%M%S" : TimeFrmt, /*local*/gmtime_r(&Secs, &t_result));
	if (UseDefaultFrmt) sprintf(TimeStr+strlen(TimeStr), ".%09lli", TimeStamp%1000000000);
	return TimeStr;
}

///////////////////////////////////////////////////////////////////////////////
/// HIFN	Convert a timestamp to explicit date, similar to gmtime()
///////////////////////////////////////////////////////////////////////////////
void N2_NanoToDate(long long TimeStamp, int* Year, int* Month, int* Day, int* Hour, int* Min, int* Sec, int* Nano) {
	time_t Secs=TimeStamp/1000000000;
//	time_t Time=time(&Secs);
	struct tm TM;
	/*local*/gmtime_r(&Secs, &TM);	// localtime is not thread-safe
	*Year =TM.tm_year+1900;
	*Month=TM.tm_mon+1;	// 0-based
	*Day  =TM.tm_mday;	// 1-based
	*Hour =TM.tm_hour;
	*Min  =TM.tm_min;
	*Sec  =TM.tm_sec;
	*Nano=(int)(TimeStamp%1000000000);
}

///////////////////////////////////////////////////////////////////////////////
/// HIFN	Read a configuration file and fill in the N2data structure
/// HIPAR	ConfigPathName / To a .hd file
/// HIPAR	Quick / 1: does not open the data file at all. 0: open it to guess missing NbRows
/// HIRET	-errno or number of columns (including TimeStamp)
///////////////////////////////////////////////////////////////////////////////
int N2_ReadConfig(const char* ConfigPathName, tN2data *N2data, int Quick) {
	N2_ClearConfig(N2data);

	config_t Config;
	SLOG(SDBG, "Enter: %s", ConfigPathName);

	char *P=strrchr(ConfigPathName, '_');
	if (P==NULL) return -(errno=ENOENT);	// Invalid filename
	N2data->HdrVer=atoi(P+1);

	N2data->ConfigPathname=strdup(ConfigPathName);
	if (N2data->ConfigPathname==NULL) return -(errno=ENOMEM);

	errno=0;
	config_init (&Config);
	if (!config_read_file (&Config, ConfigPathName)) {
		SLOG(SDBG, "%s line %d when reading config file %s", // This will happen when searching for possible successive files
			 config_error_text(&Config), config_error_line(&Config), ConfigPathName);
		config_destroy(&Config);
		free(N2data->ConfigPathname); N2data->ConfigPathname=NULL; 
		return errno ? -errno : -ENOENT;
	}

	const char *tmp=NULL;
	if (config_lookup_string(&Config, "name", &tmp)) {
		N2data->Name=strdup(tmp);
		SLOG(SNTC, "Store name:\t%s", N2data->Name);
	} else {
		N2data->Name=malloc(10); strcpy(N2data->Name, "-Missing-");
		SLOG(SERR, "No 'name' setting in configuration file.");
	}
	if (N2data->Name==NULL) { SLOG(SERR, "Out of memory"); return -(errno=ENOMEM); }

	const char *EOLid=NULL;
	if (config_lookup_string(&Config, "EOLidentifier", &EOLid))
		N2data->EOLidentifier=strtoul(EOLid, NULL, 0);
	SLOG(SNTC, "EOLidentifier:\t0x%llX", N2data->EOLidentifier);

	config_lookup_int(&Config, "runNo", &N2data->RunNo);
	SLOG(SNTC, "RunNo:\t%d", N2data->RunNo);

	config_lookup_int(&Config, "cycNo", &N2data->CycNo);
	SLOG(SNTC, "CycNo:\t%d", N2data->CycNo);

	N2data->FirstTimeStamp=N2data->LastTimeStamp=1;
	if (!config_lookup_int64(&Config, "firstTimeStamp", &N2data->FirstTimeStamp))	// Not always here (very rare), see below
		SLOG(SWRN, "Missing firstTimeStamp");
	if (!config_lookup_int64(&Config, "lastTimeStamp",  &N2data->LastTimeStamp ))	// Not always here, see below
		SLOG(SNTC, "Missing lastTimeStamp");
	if (config_lookup_int64(&Config, "lastWrite",       &N2data->LastWrite))		// Not used
		SLOG(SNTC, "LastWrite:\t%.3fs / %s", NANO_TO_SEC(N2data->LastWrite), N2_NanoToDateStr(N2data->LastWrite, ""));
//			printf("Timestamps R%d-C%d: %lld ~ %lld, %s ~ ", 
//					N2data->RunNo, N2data->CycNo, 
//					N2data->FirstTimeStamp, N2data->LastTimeStamp, 
//					N2_NanoToDateStr(N2data->FirstTimeStamp, ST));
//			printf("%s\n", N2_NanoToDateStr(N2data->LastTimeStamp, ST));
	int Count=0;
	config_setting_t *setting  = config_lookup(&Config, "columns");
	if (setting != NULL) Count = config_setting_length(setting);
	SLOG(SNTC, "Columns:\t%d", Count);

	N2data->Columns=calloc(Count, sizeof(tColumn));
	N2data->Labels =calloc(Count, sizeof(char*));
	if (N2data->Columns==NULL or N2data->Labels==NULL) { SLOG(SERR, "Out of mem"); return -(errno=ENOMEM); }


	const char *tmp1=NULL, *tmp2=NULL, *tmp3=NULL;;
	while (N2data->NbCol<Count) {
		char Path1[80], Path2[80], Path3[80];
		sprintf(Path1, "columns/column_%03d/columnName",        N2data->NbCol); 
		sprintf(Path2, "columns/column_%03d/columnDescription", N2data->NbCol); 
		sprintf(Path3, "columns/column_%03d/columnDataType",    N2data->NbCol); 
		if (!config_lookup_string(&Config, Path1, &tmp1) or
			!config_lookup_string(&Config, Path2, &tmp2) or
			!config_lookup_string(&Config, Path3, &tmp3)) { 
				SLOG(SWRN, "Config column read failure: %s/%s/%s", tmp1, tmp2, tmp3); 
				break; 
		}

		N2data->Columns[N2data->NbCol].Name       =strdup(tmp1);	// We need to do this because config_destroy will remove the references
		N2data->Columns[N2data->NbCol].Description=strdup(tmp2);
		N2data->Columns[N2data->NbCol].DataType   =strdup(tmp3);
		N2data->Labels [N2data->NbCol]=malloc(strlen(N2data->Columns[N2data->NbCol].Name) + 
		                                      strlen(N2data->Columns[N2data->NbCol].Description) + 4);
		if (NULL==N2data->Columns[N2data->NbCol].Name        or
			NULL==N2data->Columns[N2data->NbCol].Description or
			NULL==N2data->Columns[N2data->NbCol].DataType    or
			NULL==N2data->Labels [N2data->NbCol]) { SLOG(SERR, "Out of mem"); return -(errno=ENOMEM); }

		if (N2data->Columns[N2data->NbCol].Description[0]=='\0')
			strcpy(  N2data->Labels [N2data->NbCol], N2data->Columns[N2data->NbCol].Name);
		else sprintf(N2data->Labels [N2data->NbCol], "%s (%s)", 
				N2data->Columns[N2data->NbCol].Name, N2data->Columns[N2data->NbCol].Description);

		SLOG(SNTC, "Col %d:\t%s\t%s\t%s", N2data->NbCol, 
				N2data->Columns[N2data->NbCol].Name, 
				N2data->Columns[N2data->NbCol].Description, 
				N2data->Columns[N2data->NbCol].DataType);	// NOTE: can be double or uint64, but all 8 bytes. Others maybe ???
		N2data->NbCol++;
	}

	config_destroy (&Config); 	// this destroys the allocations, in particular the config_lookup_string

	// Suboptimal, but better than nothing
	if (N2data->FirstTimeStamp==1) N2data->FirstTimeStamp=0;	// Don't remember why I had to do this
	if (N2data->LastTimeStamp ==1) N2data->LastTimeStamp=0;

	const char* DataPath=ConfigToDataName(ConfigPathName);
/**/if (!Quick) { N2data->NbRow=-1; ReadData(DataPath, N2data, 0); }
	if (N2data->FirstTimeStamp==0) 
		N2data->FirstTimeStamp = GetMissingFirstTimeStamp(DataPath);
	if (N2data->LastTimeStamp==0) 
		N2data->LastTimeStamp = GetMissingLastTimeStamp(DataPath, N2data->NbCol);
	if (N2data->LastTimeStamp==0) 	// Still !!! It happens on some improper data files.
		N2data->LastTimeStamp = N2data->FirstTimeStamp;

	SLOG(SNTC, "FirstTimeStamp:\t%.3fs / %s", NANO_TO_SEC(N2data->FirstTimeStamp), N2_NanoToDateStr(N2data->FirstTimeStamp, ""));
	SLOG(SNTC, "LastTimeStamp:\t%.3fs / %s",  NANO_TO_SEC(N2data->LastTimeStamp),  N2_NanoToDateStr(N2data->LastTimeStamp,  ""));
	if (N2data->FirstTimeStamp==0) {	// Still 0, we invalidate the file
		N2_ClearConfig(N2data);
		return -(errno=ENOENT);
	}
		
	if (N2data->LastTimeStamp)
		SLOG(SNTC, "Time range:\t%.3fs",     NANO_TO_SEC(N2data->LastTimeStamp-N2data->FirstTimeStamp));

	// This seems to trigger spurious error on a Mac
//	if (errno) SLOG(SERR, "%s", strerror(errno)); 
//	else SLOG(SDBG, "Config success");
//	return errno ? -errno : N2data->NbCol;
	errno=0;	// We ignore some errors because some files may be missing

	return N2data->NbCol;
}

///////////////////////////////////////////////////////////////////////////////
/// HIFN	Free the data present in a tN2data structure and zero all parameters
///////////////////////////////////////////////////////////////////////////////
void N2_ClearConfig(tN2data *N2data) {
	SLOG(SDBG, "Enter. Currently %sNULL", N2data?"NOT ":"");
	if (N2data==NULL) return;
	if (N2data->Name          ) free(N2data->Name          );; N2data->Name          =NULL;
	if (N2data->ConfigPathname) free(N2data->ConfigPathname);; N2data->ConfigPathname=NULL;
	if (N2data->DataPathname  ) free(N2data->DataPathname  );; N2data->DataPathname  =NULL;

	if (N2data->Labels) {
		for (int i=0; i<N2data->NbCol; i++)
			if (N2data->Labels[i]) free(N2data->Labels[i]);
		free(   N2data->Labels   );     N2data->Labels=NULL;
	}
	if (N2data->Columns) {
		for (int i=0; i<N2data->NbCol; i++) {
			if (N2data->Columns[i].Name       ) free(N2data->Columns[i].Name);
			if (N2data->Columns[i].Description) free(N2data->Columns[i].Description);
			if (N2data->Columns[i].DataType   ) free(N2data->Columns[i].DataType);
		}
		free(   N2data->Columns               );     N2data->Columns  =NULL;
	}
	if (N2data->TimeStamp) free(N2data->TimeStamp);; N2data->TimeStamp=NULL;
	if (N2data->Data     ) {
		for (int i=0; i<N2data->NbRow/*ReservedSize*/; i++) 
			if (N2data->Data[i]) { free(N2data->Data[i]); NbFree++; }
		free(   N2data->Data   );       N2data->Data=NULL;
	}
	N2data->FirstTimeStamp = N2data->LastTimeStamp = 
	N2data->EOLidentifier  = N2data->RunNo = N2data->CycNo = 
	N2data->HdrVer = N2data->NbCol = N2data->NbRow = N2data->ReservedSize = 0;
	N2data->tFilter.StartTimeStamp=N2data->tFilter.Decimation=
	N2data->tFilter.EndTimeStamp=  N2data->tFilter.MaxRows   =0;
	bzero(N2data, sizeof(tN2data));	// Redundant, Just make sure everything is at 0
}

///////////////////////////////////////////////////////////////////////////////
/// HIFN	Copy the configuration from one struct to the next (but not the timestamp/data)
/// HIPAR	N2dest / Assumed to be empty/undefined on entry
///////////////////////////////////////////////////////////////////////////////
extern void N2_CopyConfig(tN2data *N2dest, const tN2data *N2source) {
	if (N2dest==NULL or N2source==NULL) return;
	N2dest->Name          =strdup(N2source->Name);
	N2dest->ConfigPathname=strdup(N2source->ConfigPathname);
	N2dest->DataPathname  =strdup(N2source->DataPathname);
	if (N2dest->Name==NULL or N2dest->ConfigPathname==NULL or N2dest->DataPathname==NULL) SLOG(SERR, "Out of memory");
	
	N2dest->EOLidentifier=N2source->EOLidentifier;
	N2dest->RunNo =N2source->RunNo;
	N2dest->CycNo =N2source->CycNo;
	N2dest->HdrVer=N2source->HdrVer;
	N2dest->NbCol =N2source->NbCol;
	
	if (N2source->Labels!=NULL) {
		N2dest->Labels=calloc(N2dest->NbCol, sizeof(char*));
		for (int i=0; i<N2dest->NbCol; i++)
			N2dest->Labels[i] = (N2source->Labels[i]==NULL ? NULL : strdup(N2source->Labels[i]));	// See https://stackoverflow.com/questions/6432384/strdup-dumping-core-on-passing-null
	} else N2dest=NULL;
	
	if (N2source->Columns!=NULL) {
		if (N2dest->NbCol<=0) SLOG(SERR, "NbCols=%d", N2dest->NbCol);
		N2dest->Columns=calloc(N2dest->NbCol, sizeof(tColumn));
		for (int i=0; i<N2dest->NbCol; i++) {
			N2dest->Columns[i].Name       = (N2source->Columns[i].Name       ==NULL ? NULL : strdup(N2source->Columns[i].Name));
			N2dest->Columns[i].Description= (N2source->Columns[i].Description==NULL ? NULL : strdup(N2source->Columns[i].Description));
			N2dest->Columns[i].DataType   = (N2source->Columns[i].DataType   ==NULL ? NULL : strdup(N2source->Columns[i].DataType));
		}
	} else N2dest->Columns=NULL;
	
	N2dest->FirstTimeStamp=N2source->FirstTimeStamp;
	N2dest->LastTimeStamp =N2source->LastTimeStamp;
	N2dest->LastWrite     =N2source->LastWrite;
	
	N2dest->NbRow=N2dest->ReservedSize=0;	// Not copying data yet, this will be done by N2_AddDataWithFilter()
	N2dest->TimeStamp=NULL;
	N2dest->Data=NULL;

	N2dest->tFilter=N2source->tFilter;
}


///////////////////////////////////////////////////////////////////////////////
/// HIFN	Copy the copy the timestamp+data by applying the filter parameters
/// HIPAR	Remaining / When using decimation on multiple files, 
/// HIPAR	Remaining / number of points remaining since last decimation of previous file (0 on start)
/// HIPAR	Remaining / This avoids the problem of files having less points than the decimation
/// HIRET	Number of effectively added rows (or -errno)
///////////////////////////////////////////////////////////////////////////////
int N2_AddDataWithFilter(tN2data *N2dest, tN2data *N2source, int* Remaining, 
				int Decimation, int MaxRows, 
				long long TimeStampLow, long long TimeStampHigh) {
	if (Decimation==0) Decimation=1;
	int R=0, CopiedRows=0, AddedRows=0,
		MaxES=N2source->NbRow/Decimation; // Max Expected Size
	
	if (N2source->NbRow==0) return 0;
	if (N2source->NbRow>0 and MaxES==0) MaxES=1;	// TODO: This should be rather fixed by using an offset
	if (MaxES==0 or (MaxRows>0 and N2dest->NbRow>=MaxRows)) return 0;	// No data or already over
	
	unsigned long long* TempTimeStamp=calloc(MaxES, sizeof(unsigned long long));
	void**              TempData     =calloc(MaxES, 8);	// double or uint64
	if (TempTimeStamp==NULL or TempData==NULL) { R=-ENOMEM; goto End; }

	int SourceR=(Decimation-*Remaining)%Decimation;	// Skip the 1st points
	for (int NbR=0; NbR<MaxES; NbR++, SourceR+=Decimation)
		if (SourceR<N2source->NbRow and
			(TimeStampLow ==0 or TimeStampLow<=N2source->TimeStamp[SourceR]               ) and 
			(TimeStampHigh==0 or               N2source->TimeStamp[SourceR]<=TimeStampHigh)) {
			TempTimeStamp[CopiedRows] =  N2source->TimeStamp[SourceR];
			TempData     [CopiedRows] =  N2source->Data     [SourceR];	// Point to same thing
			N2source->Data[SourceR] = NULL;	// So that ClearConfig won't the transfered destination
//			memcpy(TempData[CopiedRows], N2source->Data     [SourceR], N2source->NbCol*sizeof(double));
			CopiedRows++;
		}
	*Remaining=(N2source->NbRow /*-Decimation*/ + *Remaining)%Decimation;	// Number of remaining points before next decimation
	
	if (CopiedRows>0) {
		if (N2dest->ReservedSize<N2dest->NbRow+CopiedRows) {
			N2dest->ReservedSize=N2dest->NbRow+CopiedRows+1000;	// Too big, but doesn't matter. The constant is to limit the reallocations. Value is pulled out of my ass
			N2dest->TimeStamp=realloc(N2dest->TimeStamp, N2dest->ReservedSize*sizeof(unsigned long long));
			N2dest->Data     =realloc(N2dest->Data,      N2dest->ReservedSize*8);
			if (N2dest->TimeStamp==NULL or N2dest->Data==NULL) { R=-ENOMEM; goto End; }
			for (int i=N2dest->NbRow+1; i<N2dest->ReservedSize; i++) {
				N2dest->TimeStamp[i]=0;	// This loop shouldn't be necessary
				N2dest->Data[i]=NULL;
			}
		}
		for (int i=0; i<CopiedRows; i++) {
			if (MaxRows>0 and N2dest->NbRow>=MaxRows) { 
				// Because we need to free the N2source->Data[..] which we aren't moving to N2dest
				if (TempData[i]) { free(TempData[i]); TempData[i]=NULL; NbFree++; }
				continue;
			}
			N2dest->TimeStamp[N2dest->NbRow] = TempTimeStamp[i];
			N2dest->Data     [N2dest->NbRow] = TempData     [i];
			TempData[i]=NULL;	// So that ClearConfig won't free the transfered destination
//			memcpy(N2dest->Data[N2dest->NbRow+i],TempData     [i], N2source->NbCol*sizeof(double));
			AddedRows++;
			N2dest->NbRow++;
		}
		R=AddedRows;
	}
	
End:
	if (TempTimeStamp!=NULL) free(TempTimeStamp);
	if (TempData     !=NULL) free(TempData);
	return R;
}



///////////////////////////////////////////////////////////////////////////////
/// HIFN	Read the actual EDMdat file
/// HIPAR	PathName / Name of .EDMdat file
/// HIPAR	N2data / Structure already read and set by ReadConfigData()
/// HIPAR	N2data / Set N2data->NbRow to -1 if you want this function to JUST return the expected number of rows without reading any data
/// HIPAR	AltFirstTimeStamp / Pass 0 to use the 1st timestamp of the file as the zero reference for relative time
/// HIPAr	AltFirstTimeStamp / Or when merging multiple cycle files, pass the timestamp of the 1st file
/// HIRET	-errno or number of rows
///////////////////////////////////////////////////////////////////////////////
static int ReadData(const char* PathName, tN2data *N2data, long long AltFirstTimeStamp) {
	SLOG(SDBG, "Enter: %s", PathName);

	errno=0;
	FILE* fd=fopen(PathName, "rb");
	if (fd==NULL) {
		SLOG(SERR, "Could not open data file %s: %s", PathName, strerror(errno));
		return -errno;
	}

	// Get file size
	fseek(fd, 0, SEEK_END);
	int Size = ftell(fd);
	int ExpectRows=Size/((N2data->NbCol+1)*sizeof(double));	// Includes Reltime and EOF marker
	SLOG(SNTC, "ExpectRows=%d", ExpectRows);
	if (N2data->NbRow==-1) { fclose(fd); return N2data->NbRow=ExpectRows; }

	rewind(fd);

	N2data->DataPathname=strdup(PathName);
	if (N2data->DataPathname==NULL) { SLOG(SERR, "Out of memory"); return -(errno=ENOMEM); }

	// Print presentation header
	//	printf("%s", N2data->Labels[0]);
	char Buf[1024*1024]="RelTime (s)";	// WARNING: default java stack size is very small, only 320Kb !!! Use -Xss4m
	if (N2data->Columns[0].DataType) free(N2data->Columns[0].DataType);
	N2data->Columns[0].DataType=strdup("double");	// Converted to seconds (string is already allocated as uint64)
	if (N2data->Columns[0].DataType==NULL) { SLOG(SERR, "Out of memory"); return -(errno=ENOMEM); }
	//	if (N2data->Columns[0].Description) free(N2data->Columns[0].Description);
	strcpy(N2data->Columns[0].Description, "[s]");	// Change unit ns->s
	
	// Skip this block if not in debug mode
	int ShowDebug=SimpleLog_FilterLevel(-1)&SL_DEBUG;
	if (ShowDebug) {
		for (int i=1; i<N2data->NbCol; i++) sprintf(Buf+strlen(Buf), ", %s", N2data->Labels[i]);
		SLOG(SDBG, "%s", Buf);
	}
		 
	N2data->TimeStamp=calloc(ExpectRows, sizeof(long long));
//	N2data->Data     =calloc((N2data->NbCol-1)*ExpectRows, sizeof(double));
	N2data->Data     =calloc(ExpectRows, sizeof(void*));
	if (N2data->TimeStamp==NULL or N2data->Data==NULL) return -(errno=ENOMEM);
	int R=0;
	unsigned long long Eol;
	N2data->NbRow=N2data->ReservedSize=0;
	do {
		// First the timestamp
		R=fread(&N2data->TimeStamp[N2data->NbRow],   sizeof(long long), 1, fd);
		if (R!=1) { SLOG(SWRN, "Unexpected end of file: R=%i (expecting %i)", R, N2data->NbCol-1); break; }
		N2data->Data[N2data->NbRow]=calloc(N2data->NbCol, 8); NbAlloc++;
		if (N2data->Data[N2data->NbRow]==NULL) return -(errno=ENOMEM);
		((double**)N2data->Data)[N2data->NbRow][0] = NANO_TO_SEC(N2data->TimeStamp[N2data->NbRow] - (AltFirstTimeStamp==0?N2data->FirstTimeStamp:AltFirstTimeStamp));	// convert to seconds

		// Then the rest of the data
		R=fread((long long*)(N2data->Data[N2data->NbRow])+1, 8, N2data->NbCol-1, fd);	// either double or uint64
		if (R!=N2data->NbCol-1) { SLOG(SWRN, "Unexpected end of file: R=%i (expecting %i)", R, N2data->NbCol-1); break; }

		R=fread(&Eol, sizeof(Eol), 1, fd);
		if (R!=1) { SLOG(SWRN, "Unexpected end of file: R=%i (expecting %i)", R, N2data->NbCol-1); break; }

		if (ShowDebug) {	// Skip this block if not in debug mode
			sprintf(Buf, "%.3f", ((double**)N2data->Data)[N2data->NbRow][0]);
			for (int i=1; i<N2data->NbCol; i++) 
				if (0==strcmp(N2data->Columns[i].DataType, "double")) 
					 sprintf(Buf+strlen(Buf),  ", %.3g", ((   double**)N2data->Data)[N2data->NbRow][i]);
				else sprintf(Buf+strlen(Buf),  ", %lli", ((long long**)N2data->Data)[N2data->NbRow][i]);
			sprintf(Buf+strlen(Buf), ", 0x%llX", Eol);
			SLOG(SDBG, "%.1000s", Buf);
		}
		
		if (Eol!=N2data->EOLidentifier)
			SLOG(SERR, "EOL is wrong: 0x%llX (expecting 0x%llX)", Eol, N2data->EOLidentifier);

		N2data->NbRow++;
	} while (N2data->NbRow<ExpectRows);
	
	N2data->ReservedSize=N2data->NbRow;
	
	if (N2data->NbRow!=ExpectRows) 
		 SLOG(SERR, "Row number discrepancy: %d!=%d", N2data->NbRow, ExpectRows);
	else SLOG(SNTC, "NbRow=%d", N2data->NbRow);
	
	fclose(fd); fd=NULL;
	
	if (errno) SLOG(SERR, "Error on data file %s: %s", PathName, strerror(errno));
	return errno ? -errno : N2data->NbRow;
}


///////////////////////////////////////////////////////////////////////////////
/// HIFN	To be used when the first time stamp is missing from the hd files
/// HIFN	Read the timestamp of the 1st sample
/// HIPAR	DataPathName / Path to .EDMdat file
/// HIRET	0 if error or first timestamp (1970 ns)
///////////////////////////////////////////////////////////////////////////////
static long long GetMissingFirstTimeStamp(const char* DataPathName) {
	SLOG(SDBG, "Enter: %s", DataPathName);
	
	errno=0;
	FILE* fd=fopen(DataPathName, "rb");
	if (fd==NULL) {
		SLOG(SERR, "Could not fopen %s: %s", DataPathName, strerror(errno));
		return 0;	//-errno;
	}
	
	long long TimeStamp=0;
	int R=fread(&TimeStamp,   sizeof(long long), 1, fd);
	if (R!=1) SLOG(SERR, "Cannot fread %s: %s", DataPathName, strerror(errno));
	fclose(fd);

#if 0	// Just give up already and return 0	
	if (TimeStamp==0) {	// The timestamp of the data is 0. Why ? It happens in 000702_000054_000_TrimCoils_000.hd
		#ifdef HAVE_ST_BIRTHTIME
			#define birthtime(x) x.st_birthtime
		#else
			#define birthtime(x) x.st_ctime
		#endif
		struct stat st;				// Needs sys/stat.h
		R=stat(DataPathName, &st);
		if (R) SLOG(SERR, "Cannot stat %s: %s", DataPathName, strerror(errno));
		else TimeStamp=1000000000LL*birthtime(st);
	}
#endif

	return TimeStamp;
}

///////////////////////////////////////////////////////////////////////////////
/// HIFN	To be used when the last time stamp is missing from the hd files
/// HIFN	Skim through a data file to read the timestamp of last data row
/// HIPAR	DataPathName / Path to .EDMdat file
/// HIPAR	NbCols / Number of data columns (including timestamp, excluding DEADBEEF)
/// HIRET	0 if error or last timestamp (1970 ns)
///////////////////////////////////////////////////////////////////////////////
static long long GetMissingLastTimeStamp(const char* DataPathName, int NbCols) {
	SLOG(SDBG, "Enter: %s", DataPathName);
	
	errno=0;
	FILE* fd=fopen(DataPathName, "rb");
	if (fd==NULL) {
		SLOG(SERR, "Could not open data file %s: %s", DataPathName, strerror(errno));
		return 0;	//-errno;
	}
	
	int RowSize=(NbCols+1)*sizeof(double);	// Includes Reltime and EOF marker
	//	int ExpectRows=Size/RowSize;
	fseek(fd, -RowSize, SEEK_END);	// Position on last row
	
	long long TimeStamp=0;
	int R=fread(&TimeStamp,   sizeof(long long), 1, fd);
	if (R!=1) SLOG(SERR, "%s: %s", strerror(errno), DataPathName);
	fclose(fd);
	return R==1 ? TimeStamp : 0;	//-errno;
}

///////////////////////////////////////////////////////////////////////////////
/// HIFN	Convert the pathname of a configuration file to the corresponding data pathname
///////////////////////////////////////////////////////////////////////////////
static const char* ConfigToDataName(const char* ConfigPathName) {
	/*thread_local*/ static __thread char DataName[PATH_MAX];	//strlen(ConfigPathName)+1];	// Valid in C99
	strcpy(DataName, ConfigPathName);
	strcpy(DataName+strlen(ConfigPathName)-7, ".EDMdat");
	SLOG(SNTC, "%s %s", ConfigPathName, DataName);
	return DataName;
}

///////////////////////////////////////////////////////////////////////////////
/// HIFN	Read the config and the associated data
/// HIPAR	ConfigPathName / Name of the config file (.hd). The name of the data file is derived from there
/// HIPAR	ConfigOnly / 1: does not read associated data
/// HIRET	<0 is error, or number of rows read
///////////////////////////////////////////////////////////////////////////////
int N2_ReadFile(const char* ConfigPathName, tN2data *N2data) {
	SLOG(SDBG, "Enter");
	int R=N2_ReadConfig(ConfigPathName, N2data , 1);
	if (R<0) return R;
	return ReadData(ConfigToDataName(ConfigPathName), N2data, 0);
}

///////////////////////////////////////////////////////////////////////////////
/// HIFN	Returns a config or data pathname based on parameter
///////////////////////////////////////////////////////////////////////////////
const char* N2_MakePathName(int IsConfig, const char* RootDirName, int Direct, int RunNo, int CycNo, int SizeIdx, const char* Subsystem, int HdrVer) {
	/*thread_local*/ static __thread char Str[PATH_MAX];
	if (IsConfig)
		if (Direct) sprintf(Str, "%s"        "/%06d_%06d_%03d_%s_%03d.hd", RootDirName,                         RunNo, CycNo, SizeIdx, Subsystem, HdrVer);
		else        sprintf(Str, "%s/%03d/%03d/%06d_%06d_%03d_%s_%03d.hd", RootDirName, RunNo/1000, RunNo%1000, RunNo, CycNo, SizeIdx, Subsystem, HdrVer);
	else
		if (Direct) sprintf(Str, "%s"        "/%06d_%06d_%03d_%s.EDMdat",  RootDirName,                         RunNo, CycNo, SizeIdx, Subsystem);
		else        sprintf(Str, "%s/%03d/%03d/%06d_%06d_%03d_%s.EDMdat",  RootDirName, RunNo/1000, RunNo%1000, RunNo, CycNo, SizeIdx, Subsystem);
	return Str;
}

///////////////////////////////////////////////////////////////////////////////
/// HIFN	Read the header AND data associated with given parameters
/// HIPAR	Direct / 0: read file in dir, 1:the DirName is partly based on RunNo as well: RootDirName/RunNo/RunNo_CycNo_SizeIdx_Subsystem_HdrVer.hd
///////////////////////////////////////////////////////////////////////////////
int N2_ReadData(const char* RootDirName, int Direct, 
				int RunNo, int CycNo, int SizeIdx, const char* Subsystem, int HdrVer, 
				tN2data *N2data, long long AltFirstTimeStamp) {
	SLOG(SDBG, "Enter");
	int R=N2_ReadConfig(N2_MakePathName(1, RootDirName, Direct, RunNo, CycNo, SizeIdx, Subsystem, HdrVer), N2data, 1);
	return R<0 ? R :
			ReadData(   N2_MakePathName(0, RootDirName, Direct, RunNo, CycNo, SizeIdx, Subsystem, 0     ), N2data, AltFirstTimeStamp);
}

///////////////////////////////////////////////////////////////////////////////
/// Glue code. See SimpleLog.c for sytax
///////////////////////////////////////////////////////////////////////////////
void N2_LogSetup(const char *PathName, 
					const char *TimeFormat, 
					const int NoRepeatLastN, 
					const int RepeatMaxCount,
					const int RepeatMaxSeconds,
					const char *Separator) {
	SimpleLog_Setup(PathName, TimeFormat, NoRepeatLastN, RepeatMaxCount, RepeatMaxSeconds, Separator);
}

int N2_LogFilterLevel(const int LogLevel) {
	return SimpleLog_FilterLevel(LogLevel);
}



///////////////////////////////////////////////////////////////////////////////
// And now for the features to find the data files
///////////////////////////////////////////////////////////////////////////////

#include <dirent.h>	// For directory-related stuff on Linux
//#include <config.h>
//#include <gl_oset.h>

#define DEF_ALLOC 1000000		// NOTE: Should be 10^6 for number of possible Cycle numbers

static int CompareInt(const void *A, const void *B) { 
	return *(int*)A-*(int*)B; 
}

///////////////////////////////////////////////////////////////////////////////
// WARNING: A and B are not equivalent. You figure it out...
///////////////////////////////////////////////////////////////////////////////
static int CompareStr(const void *A, const void *B) { 
//	printf("\nA:%p, B:%p", A, B);
//	printf("\nA:%p, B:%p", (char*)A, (char*)B);
//	printf("\nA:%p, B:%p\n", *(char**)A, *(char**)B);
	//	return strcmp(*(char**)A, *(char**)B);	// Nope
	//	return strcmp((char*)A, (char*)B); 		// Nope
	return strcmp((char*)A, *(char**)B); 		// Yup
}

///////////////////////////////////////////////////////////////////////////////
/// HIFN	Remove duplicates from array
/// HIFN	Compar / Comparision function
/// HIRET	New number of elements
///////////////////////////////////////////////////////////////////////////////
static int RmDups(void* Array, int Nb, int size, int (*Compar)(const void*, const void*)) {
	if (Nb==0 or Nb==1) return Nb;
	char temp[Nb*size];
	int j = 0, i;
	for (i=0; i<Nb-1; i++)
		if (0!=Compar((char*)Array+i*size, (char*)Array+(i+1)*size))
			memcpy(temp+j++*size, (char*)Array+i*size, size);
	memcpy(temp+j++*size, (char*)Array+(Nb-1)*size, size);

	for (i=0; i<j; i++)
		memcpy(Array, temp, j*size);

	return j;
}

///////////////////////////////////////////////////////////////////////////////
/// HIFN	Look for an item in an array
/// HIFN	Compar / Comparision function
/// HIRET	1 if item is found in (unsorted) array
///////////////////////////////////////////////////////////////////////////////
static int FindInArray(void* Item, void* Array, int Nb, int Size, int (*Compar)(const void*,const void*)) {
//	printf("\nEnter Item:%p, Nb:%d, Size:%d", Item, Nb, Size);
	for (int i=0; i<Nb; i++) {
		if (0==Compar(Item, (char*)Array+Size*i)) 
			return 1;
	}
	return 0;
}



///////////////////////////////////////////////////////////////////////////////
/// HIFN	Find the run numbers available in a given directory
/// HIPAR	Direct / 0: Look for directories names /RunNo/1000/RunNo%1000/ in RootDirName
/// HIPAR	Direct / 1: Look for files named RunNo_... in RootDirName
/// HIPAR	RunNoList / Returned array of RunNo, passed by reference, 
/// HIPAR	RunNoList / initially NULL, you should free it at the end of the prog, otherwise it's reused
/// HIPAR	StartFromRunNo / Start search from this run (0 by default) to speed cache refresh
/// HIRET	Number of runs found (or -errno)
///////////////////////////////////////////////////////////////////////////////
int N2_GetRunNumbers(const char* RootDirName, int Direct, int *RunNoList[], 
					 const int StartFromRunNo) {
	int Nb=0;
	//	SLOG(SDBG, "Enter");
	errno=0;
	//	if (*RunNoList) free(*RunNoList);
	//	*RunNoList=calloc(DEF_ALLOC, sizeof(int));	// Max size with 3 digits
	*RunNoList=realloc(*RunNoList, DEF_ALLOC*sizeof(int));	// Max size with 6 digits (direct) or 3 (indirect)
	if (*RunNoList==NULL) return -(errno=ENOMEM);
	
	#pragma GCC diagnostic push
	#pragma GCC diagnostic ignored "-Wpedantic"
	/// HIFN	Return 1 if the name is a header file %6d_%6d_%3d_%[^.].hd
	int FilterHeaderNames(const struct dirent* dir) {
		int Rn=0, Cn, Si, n, nc=-1;
		char SubS[80];
		return strlen(dir->d_name)>22 and 
		4==(n=sscanf(dir->d_name, "%6d_%6d_%3d_%[^.].hd%n", &Rn, &Cn, &Si, SubS, &nc)) and
		nc!=-1 and	// This way we match only header files
		Rn>=StartFromRunNo;
	}
	
	/// HIFN	Return 1 if the string is exactly 3 digits assumed to be thousands
	int Filter3digitsK(const struct dirent* dir) {
		return strlen(dir->d_name)==3 and 
		'0'<=dir->d_name[0] and dir->d_name[0]<='9' and 
		'0'<=dir->d_name[1] and dir->d_name[1]<='9' and 
		'0'<=dir->d_name[2] and dir->d_name[2]<='9' and
		atoi(dir->d_name)>=StartFromRunNo/1000; 
	}
	
	/// HIFN	Return 1 if the string is exactly 3 digits assumed to be units
	int Filter3digitsU(const struct dirent* dir) {
		return strlen(dir->d_name)==3 and 
		'0'<=dir->d_name[0] and dir->d_name[0]<='9' and 
		'0'<=dir->d_name[1] and dir->d_name[1]<='9' and 
		'0'<=dir->d_name[2] and dir->d_name[2]<='9' and
		atoi(dir->d_name)>=StartFromRunNo%1000; 
	}
	#pragma GCC diagnostic pop
	
	if (Direct) {
		struct dirent **hlist;
		int h = scandir(RootDirName, &hlist, FilterHeaderNames, alphasort);
		if (h == -1) { perror("scandir"); return -errno; }
		
		for (int i=0; i<h; i++) {
			//			printf("%s\n", hlist[i]->d_name);
			(*RunNoList)[Nb++]=atoi(hlist[i]->d_name);
			free(hlist[i]);
		}
		free(hlist);
	} else {
		struct dirent **klist=NULL;
		int k = scandir(RootDirName, &klist, Filter3digitsK, alphasort);
		if (k == -1) { perror("scandir"); return -errno; }
		
		for (int i=0; i<k; i++) {
			//			printf("%s\n", klist[i]->d_name);
			
			// Recurse 2nd level
			struct dirent **plist=NULL;
			int p;
			
			char Sub[1024];
			sprintf(Sub, "%s/%s", RootDirName, klist[i]->d_name);
			p = scandir(Sub, &plist, Filter3digitsU, alphasort);
			if (p == -1) { perror("scandir2"); return -errno; }
			
			for (int j=0; j<p; j++) {
				//				printf("%s/%s\n", klist[i]->d_name, plist[j]->d_name);
				(*RunNoList)[Nb++] = atoi(klist[i]->d_name)*1000 + atoi(plist[j]->d_name);
				free(plist[j]); plist[j]=NULL;
			}
			free(plist);    plist   =NULL;
			free(klist[i]); klist[i]=NULL;
		}
		free(klist); klist=NULL;
	}
	// NOTE: RunNoList should already be sorted by the alphasort function
	qsort(    *RunNoList, Nb, sizeof(int), CompareInt);	// qsort *is* thread safe if you don't use global variables in the comparision function
	Nb=RmDups(*RunNoList, Nb, sizeof(int), CompareInt);
	//	*RunNoList=realloc(*RunNoList, Nb*sizeof(int));		// Optional
	//	if (*RunNoList==NULL) return -(errno=ENOMEM);
	return errno ? -errno : Nb;
}

#if 0
///////////////////////////////////////////////////////////////////////////////
/// HIFN	Find the run numbers available in a given directory
/// HIPAR	Direct / 0: Look for directories names /RunNo/1000/RunNo%1000/ in RootDirName
/// HIPAR	Direct / 1: Look for files named RunNo_... in RootDirName
/// HIPAR	RunNoList / Returned array of RunNo, passed by reference, 
/// HIPAR	RunNoList / initially NULL, you should free it at the end of the prog, otherwise it's reused
///////////////////////////////////////////////////////////////////////////////
int N2_GetRunNumbers_Simple(const char* RootDirName, int Direct, int *RunNoList[]) {
	int Nb=0;
//	SLOG(SDBG, "Enter");
	errno=0;
//	if (*RunNoList) free(*RunNoList);
//	*RunNoList=calloc(DEF_ALLOC, sizeof(int));	// Max size with 3 digits
	*RunNoList=realloc(*RunNoList, DEF_ALLOC*sizeof(int));	// Max size with 3 digits
	if (*RunNoList==NULL) return -(errno=ENOMEM);
	
	if (Direct) {
		struct dirent **hlist;
		int h = scandir(RootDirName, &hlist, FilterHeaderNames, alphasort);
		if (h == -1) { perror("scandir"); return -errno; }
		
		for (int i=0; i<h; i++) {
//			printf("%s\n", hlist[i]->d_name);
			(*RunNoList)[Nb++]=atoi(hlist[i]->d_name);
			free(hlist[i]);
		}
		free(hlist);
	} else {
		struct dirent **klist=NULL;
		int k = scandir(RootDirName, &klist, Filter3digits, alphasort);
		if (k == -1) { perror("scandir"); return -errno; }
		
		for (int i=0; i<k; i++) {
//			printf("%s\n", klist[i]->d_name);
			
			// Recurse 2nd level
			struct dirent **plist=NULL;
			int p;
			
			char Sub[1024];
			sprintf(Sub, "%s/%s", RootDirName, klist[i]->d_name);
			p = scandir(Sub, &plist, Filter3digits, alphasort);
			if (p == -1) { perror("scandir2"); return -errno; }
			
			for (int j=0; j<p; j++) {
//				printf("%s/%s\n", klist[i]->d_name, plist[j]->d_name);
				(*RunNoList)[Nb++] = atoi(klist[i]->d_name)*1000 + atoi(plist[j]->d_name);
				free(plist[j]); plist[j]=NULL;
			}
			free(plist);    plist   =NULL;
			
			free(klist[i]); klist[i]=NULL;
		}
		free(klist); klist=NULL;
	}
	// NOTE: RunNoList should already be sorted by the alphasort function
	qsort(    *RunNoList, Nb, sizeof(int), CompareInt);	// qsort *is* thread safe if you don't use global variables in the comparision function
	Nb=RmDups(*RunNoList, Nb, sizeof(int), CompareInt);
//	*RunNoList=realloc(*RunNoList, Nb*sizeof(int));		// Optional
//	if (*RunNoList==NULL) return -(errno=ENOMEM);
	return errno ? -errno : Nb;
}
#endif

///////////////////////////////////////////////////////////////////////////////
/// HIFN	Find the subsystems available in a certain directory for a given run number
/// HIPAR	SubsList / Returned array of CycNo, passed by reference, initially NULL, 
/// HIPAR	SubsList / you should free it and free every substring (function will not clean it for you on re-entry)
/// HIRET	Number of Subsystems in the SubsList array, or -errno
///////////////////////////////////////////////////////////////////////////////
int N2_GetSubsystems(const char* RootDirName, int Direct, int RunNo, char **SubsList[]) {
	int Nb=0;
	SLOG(SDBG, "Enter");
	errno=0;
//	if (*SubsList) SLOG(SWRN, "SubsList is not NULL");
//	*SubsList=calloc(DEF_ALLOC, sizeof(char*));	/// How many ? Probably not very many total, but temporary ?
	*SubsList=realloc(*SubsList, DEF_ALLOC*sizeof(char*));	/// How many ? Probably not very many total, but temporary ?
	if (*SubsList==NULL) { SLOG(SERR, "Out of memory"); return -(errno=ENOMEM); }
	
	#pragma GCC diagnostic push
	#pragma GCC diagnostic ignored "-Wpedantic"
	/// HIFN	Return 1 if the name is a header file %6d_%6d_%3d_%[^.].hd matching RunNo
	int FilterHeaderNamesForR(const struct dirent* dir) {
		int Rn, Cn, Si, n, nc=-1;
		char SubS[80], *P;
		if (!(strlen(dir->d_name)>22 and 
			4==(n=sscanf(dir->d_name, "%6d_%6d_%3d_%[^.].hd%n", &Rn, &Cn, &Si, SubS, &nc)) and
			nc!=-1 and	// This way we match only header files
			Rn==RunNo))	// Only match wanted runnumber
			return 0;
		if (NULL==(P=strrchr(SubS, '_'))) return 0;
		*P='\0';	// Because SubS is like coils_000 or some_other_stuff_000
		if (!FindInArray(SubS, *SubsList, Nb, sizeof(char*), CompareStr)) {
			(*SubsList)[Nb]=strdup(SubS);	// NOTE: Use N2_FreeSubsystems() to free cleanly
			//					SLOG(SDBG, "Added: %s", SubS);
			if ((*SubsList)[Nb]==NULL) SLOG(SERR, "Out of memory"); 
			Nb++;
			return 1;
		} 
		return 0;
	}
	#pragma GCC diagnostic pop
	
	char DirPath[PATH_MAX];
	if (Direct) sprintf(DirPath, "%s",           RootDirName);
	else        sprintf(DirPath, "%s/%03i/%03i", RootDirName, RunNo/1000, RunNo%1000);
	
	struct dirent **hlist;
	int h = scandir(DirPath, &hlist, FilterHeaderNamesForR, alphasort);
	if (h == -1) { perror("scandir"); return -errno; }
	
	for (int i=0; i<h; i++) // SubsList already filled by Filter function
		free(hlist[i]);
	free(hlist);
	
	//	qsort(    *SubsList, Nb, sizeof(int), (int (*)(const void*,const void*))strcmp);
	//	Nb=RmDups(*SubsList, Nb, sizeof(int), (int (*)(const void*,const void*))strcmp);
	//	*SubsList=realloc(*SubsList, Nb*sizeof(char*));	// Optional
	if (errno) SLOG(SERR, "errno %d", errno); 
	if (Nb==0) SLOG(SWRN, "No match in %s", DirPath);
	return errno ? -errno : Nb;
}

void N2_FreeSubsystems(int N, char *SubsList[]) {
	if (SubsList==NULL or *SubsList==NULL) return;
	for (int i=0; i<N; i++) free(SubsList[i]);
//	free(*SubsList); *SubsList=NULL;
}

///////////////////////////////////////////////////////////////////////////////
/// HIFN	Find the cycle numbers available in a directory for for a given run number and subsystem 
/// HIPAR	CycNoList / Returned array of CycNo, passed by reference, initially NULL, you should free it, or give it again to the function which will free it
///////////////////////////////////////////////////////////////////////////////
int N2_GetCycleNumbers(const char* RootDirName, int Direct, int RunNo, const char* Subsystem, int *CycNoList[]) {
	int Nb=0;
	char DirPath[PATH_MAX];
	SLOG(SDBG, "Enter R=%d %s\n", RunNo, Subsystem);
	errno=0;
//	if (*CycNoList) free(*CycNoList);
//	*CycNoList=calloc(DEF_ALLOC, sizeof(int));	// Max size with 6 digits - FIXME: that's 6 digits here
	*CycNoList=realloc(*CycNoList, DEF_ALLOC*sizeof(int));	// Max size with 6 digits - FIXME: that's 6 digits here
	if (*CycNoList==NULL) { SLOG(SERR, "Out of memory"); return -(errno=ENOMEM); }

	#pragma GCC diagnostic push
	#pragma GCC diagnostic ignored "-Wpedantic"
	/// HIFN	Return 1 if the name is a header file %6d_%6d_%3d_%[^.].hd
	int FilterHeaderNamesForRandS(const struct dirent* dir) {
		int Rn, Cn, Si, n, nc=-1;
		char SubS[80], *P;
		int Res=(strlen(dir->d_name)>22 and 
				4==(n=sscanf(dir->d_name, "%6d_%6d_%3d_%[^.].hd%n", &Rn, &Cn, &Si, SubS, &nc)) and
				nc!=-1 and	// This way we match only header files
				Rn==RunNo);	// Only match wanted runnumber
		if (!Res) return 0;
		if (NULL==(P=strrchr(SubS, '_'))) return 0;
		*P='\0';	// Because SubS is like coils_000 or some_other_stuff_000
		if (strcmp(Subsystem, SubS)==0) { (*CycNoList)[Nb++]=Cn; return 1; }	// There will be duplicates
		return 0;
	}
	#pragma GCC diagnostic pop

	if (Direct) sprintf(DirPath, "%s",           RootDirName);
	else        sprintf(DirPath, "%s/%03i/%03i", RootDirName, RunNo/1000, RunNo%1000);
	
	struct dirent **hlist;
	int h = scandir(DirPath, &hlist, FilterHeaderNamesForRandS, alphasort);
	if (h == -1) { perror("scandir"); return -errno; }
	
	if (Nb==0) SLOG(SWRN, "No match in %s", DirPath); 
	
	for (int i=0; i<h; i++) free(hlist[i]);
	free(hlist);

	// NOTE: CycNoList should already be sorted by the alphasort function
	qsort(    *CycNoList, Nb, sizeof(int), CompareInt);
	Nb=RmDups(*CycNoList, Nb, sizeof(int), CompareInt);
	//	*CycNoList=realloc(*CycNoList, Nb*sizeof(int));	// Optional
	//	if (*CycNoList==NULL) return -(errno=ENOMEM);
	if (errno) SLOG(SERR, "errno %d", errno);
	if (Nb==0) SLOG(SWRN, "No match in %s for %s", DirPath, Subsystem);
	return errno ? -errno : Nb;
}



///////////////////////////////////////////////////////////////////////////////
/// HIFN	Return the min and max run numbers found in the directory
/// HIPAR	RootDirName / Call with NULL to free memory at the end
/// HIPAR	StartFromRunNo / Start search from this run (0 by default)
/// HIRET	Number of runs found
///////////////////////////////////////////////////////////////////////////////
int  N2_GetMinMaxRunNumbers  (const char* RootDirName, int Direct, int *RunNoMin, int *RunNoMax, const int StartFromRunNo) {
	/*thread_local*/ static __thread int *RunNoList=NULL;	// Could be local, but we just reuse for speed
	if (RootDirName==NULL) { SLOG(SNTC, "Free RunNoList"); if (RunNoList) free(RunNoList);; RunNoList=NULL; return 0; }
	*RunNoMin=999999;
	*RunNoMax=0;
	int R=N2_GetRunNumbers(RootDirName, Direct, &RunNoList, StartFromRunNo);
	if (R>0) { 
		//*RunNoMin=RunNoList[0]; *RunNoMax=RunNoList[R-1];  // May not be in order
		for (int i=0; i<R; i++) {
			if (*RunNoMin>=RunNoList[i]) *RunNoMin=RunNoList[i]; 
			if (*RunNoMax<=RunNoList[i]) *RunNoMax=RunNoList[i]; 
		}
	} else { *RunNoMin=0; SLOG(SWRN, "Empty RunNoList"); }
//	free(RunNoList);
	return R;
}

///////////////////////////////////////////////////////////////////////////////
int  N2_GetMinMaxCycleNumbers(const char* RootDirName, int Direct, int  RunNo, const char* Subsystem, int *CycNoMin, int *CycNoMax) {
	/*thread_local*/ static __thread int *CycNoList=NULL;	// Could be local, but we just reuse for speed
	if (RootDirName==NULL) { SLOG(SNTC, "Free CycNoList"); if (CycNoList) free(CycNoList);; CycNoList=NULL; return 0; }
	*CycNoMin=999999;
	*CycNoMax=0;

	// Find any subsystem
//	char **SubsList=NULL;
//	int N=N2_GetSubsystems(RootDirName, Direct, RunNo, &SubsList);
//	if (N<=0) { N2_FreeSubsystems(N, &SubsList); return N; }

	int R=N2_GetCycleNumbers(RootDirName, Direct, RunNo, Subsystem/*SubsList[0]*/, &CycNoList);
	if (R>0) { 
		//*CycNoMin=CycNoList[0]; *CycNoMax=CycNoList[R-1];  // May not be in order
		for (int i=0; i<R; i++) {
			if (*CycNoMin>=CycNoList[i]) *CycNoMin=CycNoList[i]; 
			if (*CycNoMax<=CycNoList[i]) *CycNoMax=CycNoList[i]; 
		}
	} else { *CycNoMin=0; SLOG(SWRN, "R=%d Rn=%d, S=%s\n", R, RunNo, Subsystem); }
//	free(CycNoList);	// TODO: realloc to save time
	SLOG(SNTC, "Using %s: run %d, cycles %d..%d\n", Subsystem, RunNo, *CycNoMin, *CycNoMax);
//	N2_FreeSubsystems(N, &SubsList);
	return R;
}

///////////////////////////////////////////////////////////////////////////////
void N2_ClearStuff(void) {
	SLOG(SNTC, "Enter");
	N2_GetMinMaxRunNumbers(NULL, 0, NULL, NULL, 0);
	N2_GetMinMaxCycleNumbers(NULL, 0, 0, NULL, NULL, NULL);
	N2_GetRunNumbersTimeStamps(NULL, 0, NULL, NULL, NULL, NULL, 0);
}

///////////////////////////////////////////////////////////////////////////////
/// HIFN	Returns the list of runs with their start and end timestamps
/// HIFN	WARNING: this function is very slow, its output should be cached
/// HIPAR	OptionalSubsystem / Pass NULL or "" to use the 1st available subsystem
/// HIPAR	RunNumbers / List of run numbers. You need to free this after use
/// HIPAR	RunStarts / Timestamp of start of corresponding run. You need to free this after use
/// HIPAR	RunEnds / Timestamp of end of corresponding run. You need to free this after use
/// HIPAR	StartFromRunNo / By default 0 to read the entire store
/// OUT		NbOfRuns, RunNumbers, RunStarts, RunEnds
/// HIRET	Possible error code (don't free in that case) or number of runs (size of arrays)
///////////////////////////////////////////////////////////////////////////////
int N2_GetRunNumbersTimeStamps(const char* RootDirName, int Direct, 
								const char* OptionalSubsystem, 
								int *RunNumbers[], 
								long long *RunStarts[], long long *RunEnds[], 
								const int StartFromRunNo) {
	/*thread_local*/ static __thread char **Subsystems=NULL;	// Find 1st available subsystem - WARNING: may not be all identical (see run 006)
	if (RootDirName==NULL) { SLOG(SNTC, "Free Subsystems"); if (Subsystems) free(Subsystems);; Subsystems=NULL; return 0; }
	SLOG(SDBG, "Enter");
	tN2data N2data={0};
//	N2_ClearConfig(&N2data);
	
	int NR, NS, NC;
	NR=N2_GetRunNumbers(RootDirName, Direct, RunNumbers, StartFromRunNo);
	if (NR<=0) return NR;
	*RunStarts=calloc(NR, sizeof(long long));
	*RunEnds  =calloc(NR, sizeof(long long));
	if (*RunStarts==NULL or *RunEnds==NULL) { SLOG(SERR, "Out of mem"); return -(errno=ENOMEM); }
		
	for (int iR=0; iR<NR; iR++) {
		const char *Subs;
		if (OptionalSubsystem==NULL or *OptionalSubsystem=='\0') {
			NS=N2_GetSubsystems(RootDirName, Direct, (*RunNumbers)[iR], &Subsystems);
			if (NS<=0) { (*RunStarts)[iR]=(*RunEnds)[iR]=-9998/*(NS==0?-9998:NS)*/; goto Skip; }
			Subs=Subsystems[0];
		} else Subs=OptionalSubsystem;
	
		int CycNoMin=-1, CycNoMax=-1;
		NC=N2_GetMinMaxCycleNumbers(RootDirName, Direct, (*RunNumbers)[iR], Subs, &CycNoMin, &CycNoMax);
		if (NC<=0 or CycNoMin<0 or CycNoMax<0) {
			(*RunStarts)[iR]=(*RunEnds)[iR]=(NC<0?NC:CycNoMin<0?CycNoMin:CycNoMax<0?CycNoMax:-9997); 
			goto Skip; }
		
		// First Cycle. 
		// FIXME: Sometimes the number returned if >0 for some subsystems. Should we ignore it / try another subsystem ?
		int R=N2_ReadConfig(N2_MakePathName(1, RootDirName, Direct, (*RunNumbers)[iR], CycNoMin, 0, Subs, 0), &N2data, 1);
		(*RunStarts)[iR]= (R>0 ? N2data.FirstTimeStamp : R==0 ? -9999 : R);
//		printf("iR %d, Run %d, start cycle %d: %.15s ~ ", iR, (*RunNumbers)[iR], CycNoMin, 
//						  N2_NanoToDateStr(N2data.FirstTimeStamp, NULL)); 
//		printf("%.15s\n", N2_NanoToDateStr(N2data.LastTimeStamp, NULL));

		N2_ClearConfig(&N2data);
		
		// Last cycle
		R=N2_ReadConfig(N2_MakePathName(1, RootDirName, Direct, (*RunNumbers)[iR], CycNoMax, 0, Subs, 0), &N2data, 1);
		(*RunEnds)  [iR]= (R>0 ? N2data.LastTimeStamp : R==0 ? -9999 : R);
//		printf("iR %d, Run %d, end cycle %d: %.15s ~ ", iR, (*RunNumbers)[iR], CycNoMax, 
//						  N2_NanoToDateStr(N2data.FirstTimeStamp, NULL));
//		printf("%.15s\n", N2_NanoToDateStr(N2data.LastTimeStamp, NULL));

		N2_ClearConfig(&N2data);
		
//		printf("iR %d, %d cycles: %.15s ~ %.15s\n", (*RunNumbers)[iR], NC, 
//			   N2_NanoToDateStr(ThisRunStart, NULL), 
//			   N2_NanoToDateStr(ThisRunEnd, NULL));
Skip:
		if (Subsystems!=NULL) N2_FreeSubsystems(NS, Subsystems);
	}

	return NR;
}

///////////////////////////////////////////////////////////////////////////////


#ifdef STANDALONE_N2TEST_PROG

// Optional parameter is single header file path
int main(int argc, char *argv[]) {
// "%06d_%06d_%03d_%s.EDMdat" % (run_number, cycle_number, size_index, subsystem_name) 
	//	N2_ReadConfig("Data/000002_000008_000_degauss_000.hd");
//	ReadData(      "Data/000002_000008_000_degauss.EDMdat");

	SimpleLog_Setup(NULL, NULL, 0, 0, 0, "\t");
	SimpleLog_FilterLevel(SL_ERROR|SL_WARNING /*|SL_NOTICE SL_ALL*/); // Default is SL_ALL

	tN2data N2data={0};
	const char* Dir="./Data";
	int *RunNo=NULL, NR=0, NS=0, NC=0, *CycNo=NULL;
	char **Subsystems=NULL;
	
	if (argc==2 and 0==strcmp(argv[1]+strlen(argv[1])-3, ".hd")) {
		N2_ReadFile(argv[1], &N2data);
		int NbCol, NbRow;
		NbCol = N2data.NbCol;
		NbRow = N2data.NbRow;
		printf("%s %i %i %i \n",N2data.Name,N2data.CycNo,N2data.NbRow,N2data.NbCol);
		int thisCol = 9;
		for( int row = 0 ; row < NbRow ; ++row ){
			double val = (double**) N2data.Data[row][9];
			printf("%f \n",val);
		}
		//for( int col = 0 ; col < NbCol ; ++col ){
		//	printf("%s \n",N2data.Columns[col].Name);
		//	printf("%s \n",N2data.Columns[col].DataType);
		//}
		goto End;
	}

////	N2_ReadFile("Data/000002_000008_000_degauss_000.hd", &N2data);

	// When there was no subdir - does not work anymore
//	N2_ReadData("Data", 1, 2, 8, 0, "degauss", 0, &N2data);
////	N2_ReadData("LiveData", 1, 78, 0, 0, "temperature", 0, &N2data, 0);

	// With Data/RunNo/RunNo_... subdir
//	N2_ReadData("Data", 0, 5, 61, 0, "shutterInput", 0, &N2data);	// Small
//	N2_ReadData("Data", 0, 6, 151, 0, "coils", 0, &N2data);	// Very large

	N2_ClearConfig(&N2data);

	int *RunNumbers=NULL;
	long long *RunStarts=NULL, *RunEnds=NULL;
	int NbR=N2_GetRunNumbersTimeStamps("./Data", 0, NULL, 
								   &RunNumbers, &RunStarts, &RunEnds, 0);
	printf("%d run %d - %lld %lld\n", 0, RunNumbers[0], RunStarts[0], RunEnds[0]);
	printf("%d run %d - %lld %lld\n", NbR-1, RunNumbers[NbR-1], RunStarts[NbR-1], RunEnds[NbR-1]);
	if (NbR>0) free(RunNumbers), free(RunStarts), free(RunEnds);
		
#if 0
	long long ActualStart, ActualEnd;
	int R0, Rn, R=N2_GetRunNumbersBetweenDates("./Data", 0, 0, 0, &R0, &Rn, &ActualStart, &ActualEnd);
	printf("\nGetRunNumbersBetweenDates (all): R=%d, runs %d~%d\n\n", R, R0, Rn);

	
	long long Mid=(9*ActualStart+ActualEnd)/10; // Find a random valid date
	R=N2_GetRunNumbersBetweenDates("./Data", 0, Mid, Mid, &R0, &Rn, NULL, NULL);
	printf("\nGetRunNumbersBetweenDates for dates %s~", N2_NanoToDateStr(Mid, NULL));
	printf("%s within available range ", N2_NanoToDateStr(Mid, NULL));
	printf("%s~",
		   N2_NanoToDateStr(ActualStart, NULL));	// Needed deserialization of static return 
	printf("%s: R=%d, RunNo range: %d~%d\n\n", N2_NanoToDateStr(ActualEnd, NULL), R, R0, Rn);
	
	Mid=(ActualStart+9*ActualEnd)/10; // Find a random valid date
	R=N2_GetRunNumbersBetweenDates("./Data", 0, Mid, Mid, &R0, &Rn, NULL, NULL);
	printf("\nGetRunNumbersBetweenDates for dates %s~", N2_NanoToDateStr(Mid, NULL));
	printf("%s within available range ", N2_NanoToDateStr(Mid, NULL));
	printf("%s~",
		   N2_NanoToDateStr(ActualStart, NULL));	// Needed deserialization of static return 
	printf("%s: R=%d, RunNo range: %d~%d\n\n", N2_NanoToDateStr(ActualEnd, NULL), R, R0, Rn);
	
	Mid=(ActualStart+ActualEnd)/2; // Find a random valid date
	R=N2_GetRunNumbersBetweenDates("./Data", 0, Mid, Mid, &R0, &Rn, NULL, NULL);
	printf("\nGetRunNumbersBetweenDates for dates %s~", N2_NanoToDateStr(Mid, NULL));
	printf("%s within available range ", N2_NanoToDateStr(Mid, NULL));
	printf("%s~",
		   N2_NanoToDateStr(ActualStart, NULL));	// Needed deserialization of static return 
	printf("%s: R=%d, RunNo range: %d~%d\n\n", N2_NanoToDateStr(ActualEnd, NULL), R, R0, Rn);
#endif	
	

	goto End;
	

	
	// Tests with hierarchy
	printf("\nFound %d Run Numbers in %s\n", NR=N2_GetRunNumbers(Dir, 0, &RunNo, 0), Dir);
	if (NR<=0) { printf("NO RunNo FOUND\n"); goto Direct; }

	printf("\nRunNo:%d, found %d Subsystems:", RunNo[0], NS=N2_GetSubsystems(Dir, 0, RunNo[0], &Subsystems));
	if (NS<=0) { printf("NO SubSystems FOUND\n"); goto Next; }
	for (int i=0; i<NS; i++) printf(" %s", Subsystems[i]);

	printf("\nRunNo:%d, subs:%s, found %d Cycle Numbers\n", RunNo[0], Subsystems[0], NC=N2_GetCycleNumbers(Dir, 0, RunNo[0], Subsystems[0], &CycNo));
	free(CycNo); CycNo=NULL;
//	N2_FreeSubsystems(NS, Subsystems); free(Subsystems); Subsystems=NULL;

Next:
	printf("\nRunNo:%d, found %d Subsystems:", RunNo[1], NS=N2_GetSubsystems(Dir, 0, RunNo[1], &Subsystems));
	if (NR==0) { printf("NO Run Numbers FOUND\n"); goto Direct; }
	for (int i=0; i<NS; i++) printf(", %s", Subsystems[i]);
	printf("\nRunNo:%d, subs:%s, found %d Cycle Numbers\n", RunNo[1], Subsystems[0], NC=N2_GetCycleNumbers(Dir, 0, RunNo[1], Subsystems[0], &CycNo));


Direct:
	// Tests with direct files in folder
	Dir="LiveData";
	printf("\nFound %d Run Numbers in %s\n", NR=N2_GetRunNumbers(Dir, 1, &RunNo, 0), Dir);
	if (NR==0) { printf("NO Run Numbers FOUND\n"); goto End; }

	printf("\nRunNo:%d, found %d Subsystems:", RunNo[1], NS=N2_GetSubsystems(Dir, 1, RunNo[1], &Subsystems));
	for (int i=0; i<NS; i++) printf(" %s", Subsystems[i]);

	printf("\nRunNo:%d, subs:%s, found %d Cycle Numbers\n", RunNo[1], Subsystems[0], NC=N2_GetCycleNumbers(Dir, 1, RunNo[1], Subsystems[0], &CycNo));

End:
	if (CycNo) free(CycNo);; CycNo=NULL;
	if (Subsystems) { N2_FreeSubsystems(NS, Subsystems); free(Subsystems); Subsystems=NULL; }
	if (RunNo) free(RunNo);; RunNo=NULL;

/*	int IntList[4]={1, 2, 4, 8}, A=4, B=5;
	printf("\nFound 4: %d\n", FindInArray(&A, IntList, 4, sizeof(int), CompareInt));
	printf("\nFound 5: %d\n", FindInArray(&B, IntList, 4, sizeof(int), CompareInt));

	char *StrList[4]={"Toto", "Prout", "Hello", "No"};
	printf("\nFound Hello: %d\n", FindInArray("Hello", StrList, 4, sizeof(char*), CompareStr));
	printf("\nFound Bye: %d\n",   FindInArray("Bye",   StrList, 4, sizeof(char*), CompareStr));
*/
	N2_ClearStuff();
	SimpleLog_Free();
	printf("\nNbAlloc=%d, NbFree=%d\n", NbAlloc, NbFree);
	return 0;
}

#endif
