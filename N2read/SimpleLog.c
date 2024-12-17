///////////////////////////////////////////////////////////////////////////////
// MODULE	SimpleLog
// AUTHOR	Guillaume Dargaud
// PURPOSE	Very simple logging facility. Requirements:
//			Standard C, compatible Linux/Windows
//			Uses sprintf syntax for write messages
//			Optional time tagging
//			Works in append mode:
//				- files can be manipulated externally
//				- no need to catch HUP
//				- reliable, if the program crashes, everything is already written 
//				  (except possible repeated messages)
//				- Not highly fast (use ElasticSearch for that)
//			Use log level filters
//			Can write to stderr or files
//			Doesn't repeat last N identical messages
//			Doesn't repeat them unless an optional count or a optional duration has been reached
//
//			Message have the following form with [brakets] being optional
//			[TimeStamp] Severity [OrigFunc[-line]] [repeat count] Message
///////////////////////////////////////////////////////////////////////////////

#include <iso646.h>
#include <ctype.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stdarg.h>
#include <time.h>
//#include <snprintf.h>	// Or <vsnprintf.h>
#ifdef SL_STANDALONE	// See at the end
	#include <unistd.h>	// Just for dup2()
#endif

#include "SimpleLog.h"

// Those are the tags that are written with the message depending on the severity
#define SL_MSG_ERROR	"ERROR"	// ERR, alternatively
#define SL_MSG_WARNING	"WARNG"	// WRN
#define SL_MSG_NOTICE	"NTICE"	// NTC
#define SL_MSG_DEBUG	"DEBUG"	// DBG

// Retain setup variables
static char *SL_PathName=NULL;			// Defaults to stderr
static char *SL_TimeFormat=NULL;		// Not used by default
static int   SL_NoRepeatLastN=0;		// Write all messages by default
static int   SL_RepeatMaxCount=1;		// Write the messages after this many times. 0 to disable, 1 to print all
static int   SL_RepeatMaxSeconds=1;		// Write the messages after this many seconds. 0 to keep them as long as possible
static int   SL_FilterLevel=SL_ALL;		// Write all messages by default
static char  SL_Separator[10]=" ";		// Space by default
static void (*SL_Callback)(char*, int)=NULL;	// Optional callback function

// Retain repeated messages
typedef struct sMessage {
	time_t TimeStampFirst, TimeStampLast;
	int Severity;	// Of the 1st message, we don't care if this changes
	char *Origin;	// Of the 1st message, we don't care if this changes
	char *Message;
	int Count;		// Number of times this message has been logged since start or last being printed
} tMessage;
static tMessage *Messages=NULL;


///////////////////////////////////////////////////////////////////////////////
#define SL_THREAD_LOCK	// Optional multithread locking

#ifdef SL_THREAD_LOCK
	#ifdef _CVI_
		#include <utility.h>
		static int Lock=0;	// Optional multithread locking
		#define LOCK	if (Lock==0) CmtNewLock (NULL, 0, &Lock);\
	 					CmtGetLock(Lock)
		#define UNLOCK	CmtReleaseLock(Lock)
	#else // Linux POSIX
		#include <pthread.h>
		static pthread_mutex_t mutex;
		#define LOCK   pthread_mutex_lock(&mutex)
		#define UNLOCK pthread_mutex_unlock(&mutex)
	#endif
#else
	#define LOCK
	#define UNLOCK
#endif
		
#pragma GCC diagnostic ignored "-Wmisleading-indentation"

///////////////////////////////////////////////////////////////////////////////
/// HIFN	Set the name of the log file and other parameters. Should be called only once.
/// HIFN	The file is only opened in append mode when we write to it, and then closed.
/// HIFN	This allows an external app to reset it or move it at will.
/// HIFN	Call to this function is optional same as (NULL, NULL, 0, 1, 0, " ")
/// HIPAR	PathName/Pathname of the file to write to. Pass NULL to use stderr.
/// HIPAR	TimeFormat/Optional strftime format string. Pass NULL to disable. 
/// HIPAR	TimeFormat/Suggested "%Y%m%d-%H%M%S" or "%Y/%m/%d-%H:%M:%S" (note the space at the end)
/// HIPAR	NoRepeatLastN/Will avoid repeating the last N messages if they are identical
/// HIPAR	NoRepeatLastN/You can call this function multiple times except to change this parameter
/// HIPAR	RepeatMaxCount/Display the message after it's been sent so many times
/// HIPAR	RepeatMaxCount/0 to keep them forever in memory, 1 to print them all
/// HIPAR	RepeatMaxSeconds/0 to keep them forever in memory, or number of seconds before a flush
/// HIPAR	Separator/Character(s) used as separators between fields. NULL to not print level nor source line
// EXAMPLES	(NULL, NULL, 0, 1, 0, " ") print all messages to stderr, no time stamp
//			("Msg.log", "%H:%M ", 1, 0, 0, ", ") Avoid repeating the last message
//			("Msg.log", "%Y%m%d-%H%M%S ", 10, 20, 0, "\t") Among the last 10 different messages, avoid repeating them at most 20 times
//			(NULL, "%Y%m%d-%H%M%S ", 100, 0, 60, " ") Among the last 100 different messages, avoid repeating them but print them at least once per minute
///////////////////////////////////////////////////////////////////////////////
void SimpleLog_Setup(const char *PathName, 
		const char *TimeFormat, 
		const int NoRepeatLastN, 
		const int RepeatMaxCount,
		const int RepeatMaxSeconds,
		const char *Separator) {
	int i;
	if (PathName and PathName[0]!='\0') {
		if (SL_PathName) free(SL_PathName); SL_PathName=NULL;
		if (NULL==(SL_PathName=malloc(strlen(PathName)+1))) 
			return;
		strcpy(SL_PathName, PathName);
	}

	if (TimeFormat and TimeFormat[0]!='\0') {
		if (SL_TimeFormat) free(SL_TimeFormat); SL_TimeFormat=NULL;
		if (NULL==(SL_TimeFormat=malloc(strlen(TimeFormat)+1))) 
			return;
		strcpy(SL_TimeFormat, TimeFormat);
	}
	
	if (Separator) {
		strncpy(SL_Separator, Separator, 9);
		SL_Separator[9]='\0';
	} else SL_Separator[0]='\0';
	
	SL_RepeatMaxCount=RepeatMaxCount;	
	SL_RepeatMaxSeconds=RepeatMaxSeconds;	
	
	if (Messages==NULL and NoRepeatLastN>0) {	// Can be allocated only once
		SL_NoRepeatLastN=NoRepeatLastN;	
		if (NULL==(Messages=realloc(Messages, (size_t)SL_NoRepeatLastN*sizeof(tMessage)))) 
			return;
		for (i=0; i<SL_NoRepeatLastN; i++) {
			Messages[i].TimeStampFirst=
			Messages[i].TimeStampLast=
			Messages[i].Severity=
			Messages[i].Count=0;
			Messages[i].Origin=
			Messages[i].Message=NULL;
		}			
	}
	
	// Log itself ! (this may be annoying)
/*	SLOG(SNTC, "Start of logging facility to file %s with%s timestamp, "
		"avoid repeat of last %d identical messages among the last %d, "
		"with a flush at least every %d seconds.",
		SL_PathName==NULL?"stderr":SL_PathName, SL_TimeFormat==NULL?"out":"",
		SL_RepeatMaxCount, SL_NoRepeatLastN, SL_RepeatMaxSeconds);
*/
}

///////////////////////////////////////////////////////////////////////////////
/// HIFN	Set the current filter level
/// HIPAR	FilterLevel/Combination of the various SL_ defines. Messages that don't match will be ignored
/// HIPAR	FilterLevel/Pass -1 to simply get the current filter level
/// HIRET	Returns the previous level (so you can do temporary swaps)
// EXAMPLE	(SL_WARNING|SL_NOTICE) from now on will only display warnings and notices (not a good idea to leave SL_ERROR out)
///////////////////////////////////////////////////////////////////////////////
int SimpleLog_FilterLevel(const int FilterLevel) {
	int Previous  =SL_FilterLevel;
	if (FilterLevel>=0)
		SL_FilterLevel=   FilterLevel;
	return Previous;
}


///////////////////////////////////////////////////////////////////////////////
/// HIFN	Display a message (and its count if more than 1)
/// HIFN	If the file proves non-writable, stderr is used as a fallback
/// HIPAR	Level/Log level to use. If it doesn't match the filter, it's discarded
/// HIPAR	OrigFunc/Optional part of the message that doesn't need to be different 
/// HIPAR	OrigFunc/Suggested value is __func__ to pass the origin function name
/// HIPAR	LineNb/Line number as given by __LINE__. Pass 0 for no display
/// HIPAR	Message/Message to display
/// HIPAR	Count/If more than 1, assumes the message has already been printed once before
// NOTE		When printing a repeated message, 
//			the time is of the last print 
//			while the level and origin correspond to the first instance of this message
///////////////////////////////////////////////////////////////////////////////
static void DisplayMessage(time_t MsgTime, const int Level, const char *Origin,
						const char *Message, const int Count) {
	static int FirstError=1;
	char OutMsg[SL_MSG_MAX+2000]="\n";	// Start with newline
	FILE *File=stderr;					// This is the default destination...
	
	if (Message==NULL) return;	// But if it's empty we display a blank line
	
	if (SL_PathName and SL_PathName[0]!='\0') 
		if ((File=fopen(SL_PathName, "a"))==NULL) {	// ...override default destination
			File=stderr;
			if (FirstError) {	// Notify this error only once
				FirstError=0;
				fprintf(stderr, "\nSimpleLog Error %d (%s) when trying to append to file %s", 
					errno, strerror(errno), SL_PathName);
			}
		}
  
	
	// Optional time string
	if (SL_TimeFormat and SL_TimeFormat[0]!='\0' and SL_Separator[0]!='\0') {
		char TimeStr[255];
		struct tm TM;
		if (0<strftime(TimeStr, 255, SL_TimeFormat, localtime_r(&MsgTime, &TM)))
			sprintf(OutMsg+strlen(OutMsg), "%s", TimeStr);
		// Separator
		sprintf(OutMsg+strlen(OutMsg), "%s", SL_Separator);
	}
	
	// Severity message. If severity mixes the levels, we print only the most important one
	#define SEVERITY (	\
				Level & SL_ERROR   ? SL_MSG_ERROR :  \
				Level & SL_WARNING ? SL_MSG_WARNING :\
				Level & SL_NOTICE  ? SL_MSG_NOTICE : \
				Level & SL_DEBUG   ? SL_MSG_DEBUG :	 \
				"   ")	// Invalid level
		
	if (SL_Separator[0]!='\0')
		sprintf(OutMsg+strlen(OutMsg), "%s%s", SEVERITY, SL_Separator);	// Fallback if File fails

	// (Almost always present) origin message
	if (Origin and Origin[0]!='\0' and SL_Separator[0]!='\0')
		sprintf(OutMsg+strlen(OutMsg), "%s%s", Origin, SL_Separator);	// Fallback if File fails

	// Optional repeat count portion
	if (Count>1)
		sprintf(OutMsg+strlen(OutMsg), "[*%d]%s", Count, SL_Separator);	// Fallback if File fails

	// Write message itself
	sprintf(OutMsg+strlen(OutMsg), "%s", Message);	// Fallback if File fails

	// And now output everything
	if (File==stderr or fputs(OutMsg, File)<0)
		fputs(OutMsg, stderr);	// Fallback to stderr if File fails
	
	if (File!=stdout and File!=stderr) fclose(File);

	// Optional callback to inform the user
	if (SL_Callback) SL_Callback(OutMsg, Level);
}

///////////////////////////////////////////////////////////////////////////////
/// HIFN	Display the message and its count. Remove it from the list
/// HIPAR	Index/Position in the list
///////////////////////////////////////////////////////////////////////////////
static void FlushMessage(const int Index) {
	if (Messages==NULL or 						// Not in use
		Messages[Index].Message==NULL) 
		return;	// Error
	if (Index<0 or Index>=SL_NoRepeatLastN)
		return;	// Should not happen
	
	if (Messages[Index].Count>1)				// The 1st one has already been printed
		DisplayMessage(	Messages[Index].TimeStampLast, 
						Messages[Index].Severity, 
						Messages[Index].Origin, 
						Messages[Index].Message, 
						Messages[Index].Count);
	
	// Clear it off the list
	Messages[Index].TimeStampFirst=
	Messages[Index].TimeStampLast =
	Messages[Index].Severity      =
	Messages[Index].Count         = 0;
	if (Messages[Index].Origin ) free(Messages[Index].Origin ); Messages[Index].Origin =NULL;
	if (Messages[Index].Message) free(Messages[Index].Message); Messages[Index].Message=NULL;
	
}
	
///////////////////////////////////////////////////////////////////////////////
/// HIFN	Find the message if it's already in the list. Update the list in any case
/// HIPAR	Message/Message to identify and update
/// HIRET	The index in the Messages[] array or -1 if not found
///////////////////////////////////////////////////////////////////////////////
static int FindMessage(char *Message) {
	int i;
	if (Messages==NULL or *Message=='\0') return -1;
	
	for (i=0; i<SL_NoRepeatLastN; i++)
		if (Messages[i].Message and 
			strcmp(Message, Messages[i].Message)==0) {
			Messages[i].TimeStampLast=time(NULL);	// Now
			if (( ++Messages[i].Count>=SL_RepeatMaxCount and 
				SL_RepeatMaxCount>0 ) or
				( SL_RepeatMaxSeconds>0 and 
				difftime(Messages[i].TimeStampLast, Messages[i].TimeStampFirst)>=SL_RepeatMaxSeconds )) {
				// We went above the count or time limit
				DisplayMessage(	Messages[i].TimeStampLast, 
								Messages[i].Severity, 
								Messages[i].Origin, 
								Messages[i].Message, 
								Messages[i].Count);
				Messages[i].Count=1;	// The current one has just been displayed
				Messages[i].TimeStampFirst=Messages[i].TimeStampLast;	// Now
			}
			return i;
		}
	return -1;	// Not found
}
	
///////////////////////////////////////////////////////////////////////////////
/// HIFN	Add the message to an empty spot, or flush out the oldest one
/// HIPAR	Origin/Contains filename, function name and/or line number. Must be a dynamically allocated string
/// HIPAR	Message/Message to add
/// HIRET	The index of the added message in the Messages[] array, -1 in case of error
///////////////////////////////////////////////////////////////////////////////
static int AddMessageToList(const int Level,  char *Origin, const char *Msg) {
	int i=-1, j;
	time_t Oldest, Now=time(NULL);

	if (Msg     ==NULL) { free(Origin); return -1; }
	if (Messages==NULL) goto Display;
	
	for (i=0; i<SL_NoRepeatLastN; i++)
		if (Messages[i].Message==NULL) 
			break;	// [i] is available

	if (i==SL_NoRepeatLastN) {					// All spots are taken
		Oldest=Now;
		i=0;
		for (j=0; j<SL_NoRepeatLastN; j++)		// Find the oldest
			if (Messages[j].TimeStampLast<Oldest) { 
				Oldest=Messages[j].TimeStampLast;
				i=j;
			}
		FlushMessage(i);	// Display and clear the oldest. [i] is now available
	}

	if (Messages[i].Message) {	// This shouldn't happen
		free(Messages[i].Message); Messages[i].Message=NULL;
		fprintf(stderr, "\nMessage[%d] not freed in %s line %d", i, __func__, __LINE__);
	}
	if (NULL==(Messages[i].Message=malloc(strlen(Msg)+1))) {
		fprintf(stderr, "\nOut of memory for message in %s line %d", __func__, __LINE__);
		free(Origin);
		return -1;
	}
	strcpy(Messages[i].Message, Msg);
	
	if (Messages[i].Origin) {	// This shouldn't happen
		free(Messages[i].Origin); Messages[i].Origin=NULL;
		fprintf(stderr, "\nOrigin[%d] not freed in %s line %d", i, __func__, __LINE__);
	}

	Messages[i].Origin=Origin;	// Will be freed or reused later
	Messages[i].TimeStampFirst=Messages[i].TimeStampLast=Now;
	Messages[i].Severity=Level;
	Messages[i].Count=1;
	
Display:
	DisplayMessage(Now, Level, Origin, Msg, 1);	// Print the first one
	if (Messages==NULL) free(Origin); 
	return i;	
}
	
///////////////////////////////////////////////////////////////////////////////
/// HIFN	Log a message at a given level
/// HIFN	If the file proves non-writable, stderr is used as a fallback
/// HIFN	EXAMPLE	(SL_DBG, "Current number %d", Nb)
/// HIPAR	Level/Log level to use. If it doesn't match the filter, it's discarded
/// HIPAR	OrigFunc/Optional part of the message that doesn't need to be different 
/// HIPAR	OrigFunc/Suggested value is __func__ to pass the name of the calling function
/// HIPAR	fmt/Usual printf syntax
/// HIPARV	Usual printf extra parameters
///////////////////////////////////////////////////////////////////////////////
void SimpleLog_Write(const int Level, 
					 const char *OrigFile, const char *OrigFunc, const int LineNb, 
					 const char *fmt, ... ) {
	int Nb;
	va_list str_args;
	char Message[SL_MSG_MAX]="\0", *P;
	if (!(Level & SL_FilterLevel)) return;	// Nothing to write
	
	LOCK;
	
	va_start( str_args, fmt );

#pragma GCC diagnostic ignored "-Wunknown-pragmas"
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wformat"
	Nb=vsnprintf(Message, SL_MSG_MAX-2, fmt, str_args);
#pragma clang diagnostic pop

	va_end( str_args );

	if (Nb<=0 or Message[0]=='\0')
		strcpy(Message, "Invalid SimpleLog message");		// Potential breakpoint here
	
	Message[SL_MSG_MAX-2]='\0';								// 1 char of margin

	for (P=Message; *P!='\0'; P++) if (iscntrl(*P)) *P=' ';	// Replace control chars with spaces
	
	int HasFile=(OrigFile and OrigFile[0]!='\0');
	int HasFunc=(OrigFunc and OrigFunc[0]!='\0');
	char *Origin=malloc((HasFile?strlen(OrigFile):0) + 
						(HasFunc?strlen(OrigFunc):0) + 10);
	sprintf(Origin, "%s%s%s%s%d",
			HasFile?OrigFile:"", HasFile or HasFunc  ? "-" : "", 
			HasFunc?OrigFunc:"", HasFunc or LineNb>0 ? "-" : "", 
			LineNb);
	// If the message is found, update its counter and timing
	if (FindMessage(Message)<0)
		// Otherwise add it (or replace older one) and display it 
		AddMessageToList(Level, Origin, Message);	
	else { free(Origin); Origin=NULL; }

	UNLOCK; return;
}

///////////////////////////////////////////////////////////////////////////////
/// HIFN	If repeated messages are in memory, write them
/// HIFN	Call this function before exiting or before you want to rotate the log.
///////////////////////////////////////////////////////////////////////////////
void SimpleLog_Flush(void) {
	int i, j;
	time_t Oldest;
	
	LOCK;
	
	do {
		Oldest=time(NULL);
		i=-1;
		for (j=0; j<SL_NoRepeatLastN; j++)		// Find the oldest and print it first
			if (Messages[j].TimeStampLast!=0 and Messages[j].TimeStampLast<=Oldest) { 
				Oldest=Messages[j].TimeStampLast;
				i=j;
			}
		if (i!=-1) FlushMessage(i);	// Display and clear the oldest. [i] is now available
		else break;	// List is now empty
	} while (1);

	UNLOCK; return;
}

///////////////////////////////////////////////////////////////////////////////
/// HIFN	Flush and free whatever pointers are left. Call this only if you need a clean valgrind output...
///////////////////////////////////////////////////////////////////////////////
void SimpleLog_Free(void) {
	SimpleLog_Flush();
	if (SL_PathName  ) free(SL_PathName  ); SL_PathName  =NULL;
	if (SL_TimeFormat) free(SL_TimeFormat); SL_TimeFormat=NULL;
	if (Messages) free(Messages);; Messages=NULL;
}
	
///////////////////////////////////////////////////////////////////////////////
/// HIFN	Register a callback function to be called whenever a new message is logged
/// HIFN	For instance if you want to display the messages in a user interface box
/// HIFN	in addition to the log file
/// HIPAR	cb / A callback funtion with a string parameter containing the logged entry and an int with the error level (1/2/4/8)
/// HIPAR	cb / Pass NULL to unregister the function
/// HIPAR	cb / Note that you can manipulate the passed message inside the function
///////////////////////////////////////////////////////////////////////////////
extern void SimpleLog_RegisterCallback(void (*cb)(char*, int)) {
	SL_Callback=cb;
}




///////////////////////////////////////////////////////////////////////////////
#ifdef SL_STANDALONE
// The above file is meant to be compiled with your progams, or compiled as a library,
// but we can also create a command line utility as a sort of 'super-uniq'
// Simply compile with: gcc -o SimpleLog SimpleLog.c -DSL_STANDALONE

int main(int argc, char **argv) {
	char buf[BUFSIZ];
	int i, LastN=0, Count=1, Sec=0;
	
	for (i=1; i<argc; i++) {
		if (0==strcmp("-h", argv[i])) {
			fprintf(stderr, "%s [-h]\n"\
							"\tAvoid repeating lines, even if not in sequence.\n"
							"\t-h\tThis help\n"
							"\t-LN\tWill avoid repeating the [L]ast N messages if they are identical\n"
							"\t-CN\tDisplay the message after it's been [C]ounted N times. 0 to keep them forever in memory, 1 to print them all as they come\n"
							"\t-SN\tKeep lines for a number of [S]econds before a flush, 0 for forever\n"
							"EXAMPLES:\n"
							"\t-L0 -C1 -S0\tprint all messages (default)\n"
							"\t-L1 -C0 -S0\tAvoid repeating the last message, similar to 'uniq'\n"
							"\t-L10 -C20 -S0\tAmong the last 10 different messages, avoid repeating them at most 20 times\n"
							"\t-L100 -C0 -S60\tAmong the last 100 different messages, avoid repeating them but print them at least once per minute\n"
							"TEST (in bash):\n"
							"\t$ for i in {1..1000}; do echo $((RANDOM/4096)); sleep 1; done | %s -L10 -C10 -S60\n"
							, argv[0], argv[0]);
			return 1; 
		}
		else if (0==strncmp("-L", argv[i], 2)) LastN=atoi(argv[i]+2);
		else if (0==strncmp("-C", argv[i], 2)) Count=atoi(argv[i]+2);
		else if (0==strncmp("-S", argv[i], 2)) Sec=atoi(argv[i]+2);
		else { fprintf(stderr, "Unknown option %s\n", argv[i]); return 2; }
	}
	
	SimpleLog_Setup(NULL, NULL, LastN, Count, Sec, NULL);
	dup2(1, 2);  //redirects stderr to stdout below this line.
	SimpleLog_FilterLevel(SL_ALL);
	
	while (fgets(buf, BUFSIZ, stdin))
		SLOG(SDBG, "%s", buf);

	SimpleLog_Flush();
	putchar('\n');
	return 0;
}
#endif
///////////////////////////////////////////////////////////////////////////////
