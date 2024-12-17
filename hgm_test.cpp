#include <iostream>
#include <fstream>
#include <sstream>
// N2 headers
#include <libconfig.h>
#include "SimpleLog.h"
#include "N2readData.h"

using namespace std;

std::string format_EDM_filename( int input_run, int input_cycle, std::string element );

int main( int argc, char ** argv ){
	
	////////////////////////////////////////////////////////////////////////
	// Read in input arguments
	if( argc != 5 ){
		cerr << "Incorrect number of arguments. Instead use:\n";
		cerr << "\t./counts_analysis [input run] [input cycle] [start time] [stop time]\n";
		return -1;
	}	
	int input_run = atoi(argv[1]);
	int input_cycle = atoi(argv[2]);
	double start_time = atof(argv[3]);
	double stop_time = atof(argv[4]);

	////////////////////////////////////////////////////////////////////////
	// Setup log for reader
	SimpleLog_Setup(NULL, NULL, 0, 0, 0, "\t");
	SimpleLog_FilterLevel(SL_ERROR|SL_WARNING);

	////////////////////////////////////////////////////////////////////////
	// Load Hgm file
	std::string filename_hgm = format_EDM_filename(input_run,input_cycle,"hgm");
	tN2data N2data_hgm = {0};
	N2_ReadFile(filename_hgm.c_str(), &N2data_hgm);
	if( !N2data_hgm.Data ){
		cerr << "Could not open file " << filename_hgm << "\n";
		exit(-1);
	}

	////////////////////////////////////////////////////////////////////////
	// Loop over data
	uint64_t file_start_time_hgm 	= (long long)N2data_hgm.FirstTimeStamp;
	if( N2data_hgm.Data && N2data_hgm.NbRow > 0 && N2data_hgm.ReservedSize > 0 ){
		// for each event in the hgm:
		for( int r=0; r<N2data_hgm.NbRow ; r++ ){

			// get timestamp for event since start time:
			uint64_t current_hgm_time = (((long long *)N2data_hgm.TimeStamp)[r]) - file_start_time_hgm;
			double HgM_Time = current_hgm_time/1E9; // in s
			// read ADC data
			double ADC1 = ((double **)N2data_hgm.Data)[r][1];
			double ADC2 = ((double **)N2data_hgm.Data)[r][2];
			double ADC3 = ((double **)N2data_hgm.Data)[r][3];
			double ADC4 = ((double **)N2data_hgm.Data)[r][4];
			double ADC5 = ((double **)N2data_hgm.Data)[r][5];
			double ADC6 = ((double **)N2data_hgm.Data)[r][6];
			double ADC7 = ((double **)N2data_hgm.Data)[r][7];
				
			// do stuff here with data
			cout << HgM_Time << " " << ADC1 << " " << ADC2 << " " << ADC3 << " " << ADC4 << " " << ADC5 << " " << ADC6 << " " << ADC7 << "\n";		
		}// row loop
	}//end if data


	return 1;
}



std::string format_EDM_filename( int input_run, int input_cycle, std::string element ){
	int prefix = input_run/1000;
	int prefix_cpy = prefix;
	int pad = 0;
	while( prefix_cpy/10 != 0 ){
		pad += 1;
		prefix_cpy /= 10;
	}
	std::string prefix_str = std::string(2-pad , '0').append(std::to_string(prefix));
	std::string prefix_two_str = std::to_string(input_run - prefix*1000);
	if( input_run-prefix*1000 <10) {
		prefix_two_str = std ::string(2,'0').append(prefix_two_str);
	}
	else if (input_run-prefix*1000 < 100){
		prefix_two_str = std::string(1,'0').append(prefix_two_str);
	}
	std::string prefix_three_str = std::string(2-pad , '0').append(std::to_string(input_run));

	int cyc_pad = 0;
	int cyc_cpy = input_cycle;
	while( cyc_cpy/10 != 0 ){
		cyc_pad += 1;
		cyc_cpy /= 10;
	}
	std::string prefix_four_str = std::string(5-cyc_pad, '0').append(std::to_string(input_cycle));
	std::string filename = "/xdata/n2edmdata/"+prefix_str+"/"+prefix_two_str+"/"+prefix_three_str+"_"+prefix_four_str+"_000_"+element+"_000.hd";
	return filename;
}

