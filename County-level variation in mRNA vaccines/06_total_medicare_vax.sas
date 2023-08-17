

	options mprint mtrace mlogic macrogen symbolgen;
	options linesize = 180 pagesize = 50 nocenter validvarname = upcase msglevel=I;

	libname stack    "FILE PATH REDACTED";
	libname cdc "FILE PATH REDACTED";
	libname rural "FILE PATH REDACTED";


					proc sql;
						create table cdc.county_lvl_total as  
						select 
						fipscd,
						sum (fips_persons) as total_bene
						from cdc.denom4_fipscd_2019
						group by fipscd;
					quit;



					data cdc.overall_w_tot_medi;
						if 0 then set cdc.county_lvl_total;
						if _n_=1 then do;
							dcl hash rr (dataset:"cdc.county_lvl_total");
							rr.definekey ("fipscd");
							rr.definedata (all:"y");
							rr.definedone();							
						end;
						call missing (of _all_);
						set rural._2_dose1_booster_1_100_rural (keep=bid: wk_id_dose_1 wk_st_dt_dose_1 age_dose_1 age_cat_dose_1 pfizer_f instudy_dose_1 state_dose_1 region_dose_1 type 
							enrollment_dose_1 dual_dose_1 county_dose_1 race_dose_1 sex_dose_1  code_f  where=(type="dose_1")) ;	
						rc=rr.find(key:county_dose_1);							
					run;




					

					data cdc.f_overall_w_tot_medi;
						if 0 then set cdc.pct_overall_w_tot_medi (keep= fips: total: stateabr code_2013 tot_dose_1);
						if _n_=1 then do;
							dcl hash rr (dataset:"cdc.pct_overall_w_tot_medi (keep= fips: total: stateabr code_2013 tot_dose_1)");
							rr.definekey ("fipscd");
							rr.definedata (all:"y");
							rr.definedone();							
						end;
						call missing (of _all_);
						set rural._2_dose1_booster_1_100_rural (keep=bid: wk_id_dose_1 wk_st_dt_dose_1 age_dose_1 age_cat_dose_1 pfizer_f instudy_dose_1 state_dose_1 region_dose_1 type 
							enrollment_dose_1 dual_dose_1 county_dose_1 race_dose_1 sex_dose_1  code_f  where=(type="dose_1")) ;	
						rc1=rr.find(key:county_dose_1);							
					run;

					data cdc.f_overall_w_tot_medi_1;
						set cdc.f_overall_w_tot_medi;
						pct_medi=tot_dose_1/total_bene;
					run;
/*
					proc univariate noprint data= cdc.f_overall_w_tot_medi_1;						
						var pct_medi;
						output out= cdc.quintiles pctlpts = 0  10  50 60   pctlpre=pct ;
					run;

					data _null_;
						set cdc.quintiles;
						call symput ('q1' , pct0);
						call symput ('q2' , pct10);
						call symput ('q3' , pct40);
						call symput ('q4' , pct50);
						call symput ('q5' , pct60);					
					run;

					data cdc.f_overall_w_tot_medi_1;
						set cdc.f_overall_w_tot_medi_1;
						if pct_medi=. then x_quint=.;
						else if pct_medi le &q1 then x_quint=1;
						else if pct_medi le &q2 then x_quint=2;
						else if pct_medi le &q3 then x_quint=3;
						else if pct_medi le &q4 then x_quint=4;
						else x_quint=5;
					run;

					proc means data=cdc.f_overall_w_tot_medi_1 missing;
						class x_quint;
						var pct_medi;
					run;
				
	*/

					data cdc.f_overall_w_tot_medi_1;
						set cdc.f_overall_w_tot_medi_1;
						if pct_medi=. then x_quint=.;
						else if pct_medi le .10 then x_quint=1;
						else if pct_medi le .20 then x_quint=2;
						else if pct_medi le .30 then x_quint=3;
						else if pct_medi lt .50 then x_quint=4;
						else if pct_medi ge .50 then x_quint=5;
						
					run;
	

					proc means data=cdc.f_overall_w_tot_medi_1 missing;
						class x_quint;
						var pct_medi;
					run;
				


					proc export data = cdc.f_overall_w_tot_medi_1
						outfile = "FILE PATH REDACTED"
						dbms = dta replace;						
					run; 

