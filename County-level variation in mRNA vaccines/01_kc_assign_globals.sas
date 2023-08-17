



%global main_vars obs start_partition end_partition iter prop_vars map_vars bene_lvl_vars where_cond keep_vars;


		%let main_vars = bid: bene_age bene_class0 bene_class12 since: bene_race_cd bene_sex: censor: combined: enr: frail: hcc: full: f: race: state: year mont: WEEK_: ;

		%let map_vars= bid:  censor: state: year mont: WEEK_:	zip: fip:  INC_:  ;	
		%let obs=max;

		%let prop_vars = bid:  censor: state: year mont: WEEK_:	bene_class: zip: fip:  INC_0001A  INC_0011A  INC_0031A INC_0034A INC_CVS_Pfizer1  INC_CVS_Moderna1 INC_CVS_JO:  INC_WAG_Pfizer1  INC_WAG_Moderna1 INC_WAG_JO:;
		%let keep_vars =		INC_0001A      INC_CVS_Pfizer1      INC_WAG_Pfizer1 
					INC_0003A      INC_CVS_Pfizer3      INC_WAG_Pfizer3      INC_0004A      INC_CVS_Pfizer4      INC_WAG_Pfizer4
					INC_0011A      INC_CVS_Moderna1      INC_WAG_Moderna1 
					INC_0013A      INC_CVS_Moderna3      INC_WAG_Moderna3      INC_0064A      INC_0094A      INC_CVS_Moderna4      INC_WAG_Moderna4;
		%let bene_lvl_vars= 		max_f_week_id         
						max_f_week_st_dt      
						max_f_bene_age       
						max_f_bene_sex_cd     
						max_f_bene_rti_race_cd
						max_f_censor_flag    
						max_f_state_final    
						max_f_fips_final     
						max_f_full_dual      
						max_f_bene_class0     
					
						
						max_b_week_id       
						max_b_week_st_dt    
						max_b_bene_age       
						max_b_bene_sex_cd    
						max_b_bene_rti_race_cd
						max_b_censor_flag     
						max_b_state_final     
						max_b_fips_final    
						max_b_full_dual      
						max_b_bene_class0    ;


		%let where_cond = 					INC_0001A  =1 |  INC_CVS_Pfizer1  =1 |  INC_WAG_Pfizer1 =1|
					INC_0003A  =1 |  INC_CVS_Pfizer3  =1 |  INC_WAG_Pfizer3  =1 |  INC_0004A  =1 |  INC_CVS_Pfizer4  =1 |  INC_WAG_Pfizer4=1|
					INC_0011A  =1 |  INC_CVS_Moderna1  =1 |  INC_WAG_Moderna1 =1|
					INC_0013A  =1 |  INC_CVS_Moderna3  =1 |  INC_WAG_Moderna3  =1 |  INC_0064A  =1 |  INC_0094A  =1 |  INC_CVS_Moderna4  =1 |  INC_WAG_Moderna4=1;


	%if &iter. = 1 %then %do;
		%let start_partition =1;
		%let end_partition = 10;
	%end;

	%if &iter. = 2 %then %do;
		%let start_partition = 11;
		%let end_partition = 20;
	%end;

	%if &iter. = 3 %then %do;
		%let start_partition = 21;
		%let end_partition = 30;
	%end;

	%if &iter. = 4 %then %do;
		%let start_partition = 31;
		%let end_partition = 40;
	%end;

	%if &iter. = 5 %then %do;
		%let start_partition = 41;
		%let end_partition = 50;
	%end;

	%if &iter. = 6 %then %do;
		%let start_partition = 51;
		%let end_partition = 60;
	%end;

	%if &iter. = 7 %then %do;
		%let start_partition = 61;
		%let end_partition = 70;
	%end;

	%if &iter. = 8 %then %do;
		%let start_partition = 71;
		%let end_partition = 80;
	%end;

	%if &iter. = 9 %then %do;
		%let start_partition = 81;
		%let end_partition = 90;
	%end;

	%if &iter. = 10 %then %do;
		%let start_partition = 91;
		%let end_partition = 100;
	%end;

