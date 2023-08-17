

	options mprint mtrace mlogic macrogen symbolgen;
	options linesize = 180 pagesize = 50 nocenter validvarname = upcase msglevel=I;

	libname stack    "FILE PATH REDACTED";
	
	libname rural "FILE PATH REDACTED";
	libname cdc "FILE PATH REDACTED";



				proc import datafile ="FILE PATH REDACTED"
					out=cdc.cdc_vax
					dbms=xlsx replace;
				run;

	
	
				

				data cdc.county_lvl_total_missing;
					set cdc.cdc_vax (keep= date fips recip_state ADMINISTERED_DOSE1_RECIP_65PLUS series_complete_65: where=(missing(ADMINISTERED_DOSE1_RECIP_65PLUS)));
					yr=year(date);
				run;


				data cdc.county_lvl_total;
					set cdc.cdc_vax (keep= date fips recip_state ADMINISTERED_DOSE1_RECIP_65PLUS series_complete_65: where=(date="30sep2021"d));
					yr=year(date);
					fips_n=cats(repeat('0',5-length(fips)-1), fips);
					drop fips;
				run;
				
				data cdc._1_overall_w_tot_medi;
					if 0 then set cdc.county_lvl_total;
					if _n_=1 then do;
						dcl hash rr (dataset:"cdc.county_lvl_total");
						rr.definekey ("fips_n" , "recip_state");
						rr.definedata (all:"y");
						rr.definedone();							
					end;
					call missing (of _all_);
					set rural._2_dose1_booster_1_100_rural (keep=bid: wk_id_dose_1 wk_st_dt_dose_1 age_dose_1 age_cat_dose_1 pfizer_f instudy_dose_1 state_dose_1 region_dose_1 type 
						enrollment_dose_1 dual_dose_1 county_dose_1 race_dose_1 sex_dose_1 instudy_dose_1 code_f where=(instudy_dose_1 =1)) ;	
					rc=rr.find(key:county_dose_1, key: state_dose_1);							
				run;




				

					data cdc._2_pct_w_tot_medi;
						if 0 then set cdc.county_lvl_total;
						if _n_=1 then do;
							dcl hash rr (dataset:"cdc.county_lvl_total");
							rr.definekey ("fips_n" , "recip_state");
							rr.definedata (all:"y");
							rr.definedone();							
						end;
						call missing (of _all_);
						set cdc.pct_overall_w_rural(keep= fips:  stateabr code_2013 tot_dose_1) ;	
						rc1=rr.find(key:fips_final, key: stateabr);							
					run;

					data cdc._3_pct_w_tot_medi;
						set cdc._2_pct_w_tot_medi;
						pct_cdc=tot_dose_1/ADMINISTERED_DOSE1_RECIP_65PLUS;
					run;


					proc univariate noprint data= cdc._3_pct_w_tot_medi;						
						var pct_cdc;
						output out= cdc.quintiles pctlpts = 10  20  50    pctlpre=pct ;
					run;

					data _null_;
						set cdc.quintiles;
						call symput ('q1' , pct10);
						call symput ('q2' , pct20);
						call symput ('q3' , pct50);
										
					run;

					
					data cdc._4_pct_w_tot_medi;
						set cdc._3_pct_w_tot_medi;
						if pct_cdc=. then x_quint=.;
						else if pct_cdc le .20 then x_quint=1;
						else if pct_cdc gt .20 and pct_cdc le .50 then x_quint=2;
						else if pct_cdc gt .50 then x_quint=3;
					run;

					proc means data=cdc._4_pct_w_tot_medi missing;
						class x_quint;
						var pct_cdc;
					run;
				


				
				



				data cdc._5_overall_w_tot_medi;
					if 0 then set cdc._4_pct_w_tot_medi;
					if _n_=1 then do;
						dcl hash rr (dataset:"cdc._4_pct_w_tot_medi");
						rr.definekey ("fips_n" , "recip_state");
						rr.definedata (all:"y");
						rr.definedone();							
					end;
					call missing (of _all_);
					set cdc._1_overall_w_tot_medi ;	
					rc=rr.find(key:county_dose_1, key: state_dose_1);							
				run;


					proc export data = cdc._5_overall_w_tot_medi
						outfile = "FILE PATH REDACTED"
						dbms = dta replace;						
					run; 





					%macro finals (list);

					%local i;
					%let i=1;
					%do %while (%scan (&list, &i) ne);
						%let num=%scan (&list, &i);
						


						proc sql;
							create table cdc.tot_&num.     as
							select			
							count (distinct BID_CWB_5) as n_bene						
							from cdc._5_overall_w_tot_medi
							where  x_quint=&num  and age_dose_1>=65;
						quit;
						
						proc sql;
							create table cdc.county_&num.     as
							select			
							count (distinct(catt(county_dose_1, 'x', state_dose_1)))as n_bene						
							from cdc._5_overall_w_tot_medi
							where  x_quint=&num and age_dose_1>=65;
						quit;


						proc sql;
							create table cdc.urban_&num.     as
							select	
							code_2013,		
							count (distinct BID_CWB_5)						
							from cdc._5_overall_w_tot_medi
							where  x_quint=&num  and age_dose_1>=65
							group by code_2013;
						quit;


					

					%let i=%sysevalf(&i+1);
					%end;
					%mend finals;
					%finals (1  2   3);

					proc univariate data=cdc._5_overall_w_tot_medi (where=(not missing(x_quint) and age_dose_1>=65));
						class x_quint;
						var pct_cdc;
					run;


					%*overall;

					proc sql;
						create table cdc.tot_overall    as
						select			
						count (distinct BID_CWB_5) as n_bene						
						from cdc._5_overall_w_tot_medi
						where  not missing(x_quint) and age_dose_1>=65
						 ;
					quit;
					
					proc sql;
						create table cdc.county_overall     as
						select			
						count (distinct(catt(county_dose_1, 'x', state_dose_1)))as n_bene						
						from cdc._5_overall_w_tot_medi
						where   not missing(x_quint) and age_dose_1>=65;
					quit;


					proc sql;
						create table cdc.urban_overall     as
						select	
						code_2013,		
						count (distinct BID_CWB_5)						
						from cdc._5_overall_w_tot_medi
						where  not missing(x_quint) and age_dose_1>=65
						group by code_2013;
					quit;


					%**************cdc summaries**************;



				%macro finals (list);

						%local i;
						%let i=1;
					%do %while (%scan (&list, &i) ne);
						%let num=%scan (&list, &i);
						
	
						data cdc.summary_&num. ;
							length category summ $200.; 
							set cdc.tot_&num.      (in=in_tot     )
								cdc.county_&num.   (in=in_c )
								cdc.urban_&num.		 (in=in_u     )
								;

								if in_tot then category ="total";
								if in_c then category ="county";
								if in_u then do;
									if code_2013=1 then category="Large central metro";
									if code_2013=2 then category="Large fringe metro";
									if code_2013=3 then category="Medium metro";
									if code_2013=4 then category="Small metro";
									if code_2013=5 then category="Micropolitan";
									if code_2013=6 then category="Non-core";
								end;



						run;

						proc sql ;
							select n_bene into : total TRIMMED
							from cdc.summary_&num.
							where category="total";
						quit;

						%PUT n_bene is &total;

						data cdc.f_pct_&num. (rename=(n_bene=n_bene_&num. pct=pct_&num.));
							length category $20.   n_bene 8.; format n_bene comma12.;
							set cdc.summary_&num.;
							pct=n_bene/&total;

							n_pct_&num.= cat (strip(put(n_bene, comma12.)), " (", strip(put(pct, percent8.2)), ")");
							if not missing(summ) then n_pct_&num.=summ;
							drop summ;
						run;





						%let i=%sysevalf(&i+1);
					%end;
				%mend finals;
					%finals (1   2  3);



					proc univariate data=cdc._5_overall_w_tot_medi (where=(not missing(x_quint) and age_dose_1>=65));
						
						var pct_cdc;
					run;
