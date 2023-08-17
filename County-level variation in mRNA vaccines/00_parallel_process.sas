
options mprint mtrace mlogic macrogen symbolgen;
options linesize = 180 pagesize = 50 nocenter validvarname = upcase msglevel=I;

/* DEFINE MACROS */
%let max_runs 	= 10;
%let saspath 	= FILE PATH REDACTED;
%let sas_code 	= FILE PATH REDACTED;
%let logpath 	= FILE PATH REDACTED;
%let run_date 	= 3_13_2023;
%let log_name 	= 02_kc_master_data;
%let max_partitions = 10;



/* RUN PRE-PROCESSING PROGRAM TO MAKE THE SAS SESSION TASKS */
 * iter keeps track of the iteration number;
%macro pp;

	%do g = 1 %to &max_runs.;
		systask command " ""&saspath.\sas.exe""  ""&sas_code.""
		-log ""&logpath.\&run_date._&log_name._&g..log""
		-nosplash -nologo -icon
		-bufno 1000 -bufsize 16k -threads -sgio
		-initstmt ""%nrquote(%)global iter; %nrquote(%)let iter=&g.;
					%nrquote(%)let run_date=&run_date.;
					%nrquote(%)let max_partitions=&max_partitions.;
					%nrquote(%)let partition_mode=0;
					%nrquote(%)let max_runs=&max_runs.; "" "
		taskname=task_&g. status=rc_&g.;
	%end; 

	waitfor _all_
		%do g = 1 %to &max_runs.;
			task_&g.
		%end;
	;

	%do g = 1 %to &max_runs.;
		%put The SAS return code for task run &g. is &&rc_&g..;
	%end;

%mend;

%pp;
