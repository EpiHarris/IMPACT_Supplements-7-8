*******************************************************************************************;
*this program stacks all the files created at partition level using 02a_kc_master_data.sas*;
*******************************************************************************************;

options mprint mtrace mlogic macrogen symbolgen;
options linesize = 180 pagesize = 50 nocenter validvarname = upcase msglevel=I;

libname stack    "FILE PATH REDACTED";

%macro libraries;

		%do p = 1 %to 100;
			libname part_&p.   "FILE PATH REDACTED";
		%end;

%mend;
%libraries;


			%macro stacking(stacks=, summs=, tab1=, final=, misc=);

				%if &stacks. %then %do;

					data stack.n_agg_dose1_booster_1_100;
						set
						%do p = 1 %to 100;	  	
							part_&p.._4_in_study_flags_&p. (in=in_&p.)  
						%end;
						;
						length file $10.;
						%do p = 1 %to 100;	  	
							if in_&p. then file="in_&p.";
						%end;
					run;
					

					data stack.agg_dose1_booster_1_100;
						set stack.n_agg_dose1_booster_1_100 ;
						if age_dose_1 = .  then age_cat_dose_1 = .;
						if age_dose_1 < 66 then age_cat_dose_1=0;
						if age_dose_1 >= 66 and age_dose_1 < 70 then age_cat_dose_1=1;
						if age_dose_1 >= 70 and age_dose_1 < 75 then age_cat_dose_1=2;
						if age_dose_1 >= 75 and age_dose_1 < 80 then age_cat_dose_1=3;
						if age_dose_1 >= 80 and age_dose_1 < 85 then age_cat_dose_1=4;
						if age_dose_1 >= 85 and age_dose_1 < 90 then age_cat_dose_1=5;
						if age_dose_1 >= 90 then age_cat_dose_1=6;	

						if age_booster = .  then age_cat_booster = .;
						if age_booster < 66 then age_cat_booster=0;
						if age_booster >= 66 and age_booster < 70 then age_cat_booster=1;
						if age_booster >= 70 and age_booster < 75 then age_cat_booster=2;
						if age_booster >= 75 and age_booster < 80 then age_cat_booster=3;
						if age_booster >= 80 and age_booster < 85 then age_cat_booster=4;
						if age_booster >= 85 and age_booster < 90 then age_cat_booster=5;
						if age_booster >= 90 then age_cat_booster=6;	
						if region_dose_1 =4 or region_booster=4 then delete;
					run;

					proc export data = stack.agg_dose1_booster_1_100
						outfile = "FILE PATH REDACTED"
						dbms = dta replace;
						fmtlib = formats;
					run; 





				%end;

				%if &summs. %then %do;

					proc sql;
						create table stack.percentages_dose_1 as 
						select
						a.county_dose_1,
						a.state_dose_1,
						n_pfi_dose_1,
						n_mod_dose_1,
						tot_dose_1,
						n_pfi_dose_1/tot_dose_1 as pct_pfi_dose_1,
						n_mod_dose_1/tot_dose_1 as pct_mod_dose_1
						from
						(select
							county_dose_1,state_dose_1,
							count(distinct bid_cwb_5) as tot_dose_1
							from stack.agg_dose1_booster_1_100
							where instudy_dose_1=1
							group by county_dose_1, state_dose_1) as a 

						left join 
						(select
							county_dose_1, state_dose_1,
							count(distinct bid_cwb_5) as n_pfi_dose_1
							from stack.agg_dose1_booster_1_100
							where dose_1="pfizer" and instudy_dose_1=1
							group by county_dose_1, state_dose_1) as  b 
							on a.county_dose_1=b.county_dose_1 and a.state_dose_1=b.state_dose_1
							left join
							(select
								county_dose_1, state_dose_1,
								count(distinct bid_cwb_5) as n_mod_dose_1
								from stack.agg_dose1_booster_1_100
								where dose_1="moderna" and instudy_dose_1=1
								group by county_dose_1, state_dose_1) as c   
							on a.county_dose_1=c.county_dose_1 and a.state_dose_1=c.state_dose_1;
						quit;

						

						proc sql;
							create table stack.percentages_booster as 
							select
							a.county_booster,
							a.state_booster,
							n_pfi_booster,
							n_mod_booster,
							tot_booster,
							n_pfi_booster/tot_booster as pct_pfi_booster,
							n_mod_booster/tot_booster as pct_mod_booster
							from
							(select
								county_booster,state_booster,
								count(distinct bid_cwb_5) as tot_booster
								from stack.agg_dose1_booster_1_100
								where instudy_booster=1
								group by county_booster, state_booster) as a 

							left join 
							(select
								county_booster, state_booster,
								count(distinct bid_cwb_5) as n_pfi_booster
								from stack.agg_dose1_booster_1_100
								where booster="pfizer" and instudy_booster=1
								group by county_booster, state_booster) as  b 
								on a.county_booster=b.county_booster and a.state_booster=b.state_booster
								left join
								(select
									county_booster, state_booster,
									count(distinct bid_cwb_5) as n_mod_booster
									from stack.agg_dose1_booster_1_100
									where booster="moderna" and instudy_booster=1
									group by county_booster, state_booster) as c   
								on a.county_booster=c.county_booster and a.state_booster=c.state_booster;
						quit;


						proc sql;
							create table stack.pct_overall as
							select 
							coalesce(x.county_dose_1, y.county_booster) as county,
							coalesce(x.state_dose_1, state_booster)     as state,
							pct_pfi_dose_1, 
							pct_pfi_booster, 
							pct_mod_dose_1, 
							pct_mod_booster,
							tot_dose_1, 
							tot_booster, 
							n_pfi_dose_1, 
							n_pfi_booster,
							n_mod_dose_1,
							n_mod_booster
							from stack.percentages_dose_1 as x full join stack.percentages_booster as y
							on x.county_dose_1=y.county_booster and x.state_dose_1=y.state_booster;
						quit;


			

				%end;*end of switch;

				%if &tab1. %then %do;
			
					%macro finals (list);

					%local i;
					%let i=1;
					%do %while (%scan (&list, &i) ne);
						%let num=%scan (&list, &i);
						

						proc sql;
							create table stack.tot_&num.     as
							select			
							count (distinct BID_CWB_5) as n_bene						
							from stack.agg_dose1_booster_1_100
							where instudy_&num.=1;
						quit;
						

						proc sql;
							create table stack.age_&num.     as				
							select
							avg(age_&num.) as mean_age,
							std(age_&num.) as std_age
							from stack.agg_dose1_booster_1_100
							where instudy_&num.=1;
						quit;
						

						proc sql;
							create table stack.agecat_&num.     as				
							select
							age_cat_&num.,
							count (distinct BID_CWB_5) as n_bene
							from stack.agg_dose1_booster_1_100
							where instudy_&num.=1
							group by age_cat_&num.;
						quit;


						proc sql;					
							create table stack.female_&num.     as	
							select
							count (distinct bid_cwb_5) as n_bene
							from stack.agg_dose1_booster_1_100
							where sex_&num.="2" and instudy_&num.=1;
						quit;

						
						proc sql;
							create table stack.race_&num.     as					
								select	
								"white" as race,
								count (distinct bid_cwb_5) as n_bene
								from stack.agg_dose1_booster_1_100
								where instudy_&num.=1  and race_&num.="1"

							union corr	
							

							select	
								"black" as race,
								count (distinct bid_cwb_5) as n_bene
								from stack.agg_dose1_booster_1_100
								where instudy_&num.=1 and race_&num.="2"

							union corr

							select	
								"other" as race,
								count (distinct bid_cwb_5) as n_bene
								from stack.agg_dose1_booster_1_100
								where instudy_&num.=1 and race_&num.="3"


								union corr

							select	
								"asian" as race,
								count (distinct bid_cwb_5) as n_bene
								from stack.agg_dose1_booster_1_100
								where instudy_&num.=1 and race_&num.="4"

									union corr

							
							select	
								"hispanic" as race,
								count (distinct bid_cwb_5) as n_bene
								from stack.agg_dose1_booster_1_100
								where instudy_&num.=1 and race_&num.="5"

									union corr

							select	
								"native" as race,
								count (distinct bid_cwb_5) as n_bene
								from stack.agg_dose1_booster_1_100
								where instudy_&num.=1 and race_&num.="6"
							
								union corr

							select	
								"missing" as race,
								count (distinct bid_cwb_5) as n_bene
								from stack.agg_dose1_booster_1_100
								where instudy_&num.=1 and ( race_&num.="0" or missing(race_&num.));
						quit;

						
						proc sql;
							create table stack.geog_&num.     as					
								select	
								region_&num,
								count (distinct bid_cwb_5) as n_bene
								from stack.agg_dose1_booster_1_100
								where instudy_&num.=1
								group by region_&num;
						quit;

						

						proc sql;
							create table stack.enr_&num.     as					
								select	
								enrollment_&num,		
								count (distinct bid_cwb_5) as n_bene
								from stack.agg_dose1_booster_1_100
								where instudy_&num.=1 
								group by enrollment_&num.;								
						quit;


						proc sql;
						create table stack.dual_&num.     as					
							select
							count (distinct bid_cwb_5) as n_bene
							from stack.agg_dose1_booster_1_100
							where instudy_&num.=1 and  dual_&num.=1;					

						quit;


					

					%let i=%sysevalf(&i+1);
					%end;
					%mend finals;
					%finals (dose_1 booster);

			%end; %*end of switch;

			%if &final %then %do;



				%macro finals (list);

						%local i;
						%let i=1;
					%do %while (%scan (&list, &i) ne);
						%let num=%scan (&list, &i);
						
	
						data stack.summary_&num. ;
							length category summ $200.; 
							set stack.tot_&num.      (in=in_tot     )								
								stack.age_&num.		(in=in_age     )
								stack.agecat_&num.   (in=in_agecat  )
								stack.female_&num.   (in=in_female  )
								stack.race_&num.     (in=in_race    )
								stack.geog_&num.     (in=in_geog    )
								stack.enr_&num.      (in=in_enr     )
								stack.dual_&num.     (in=in_dual    )
								;
								
								if in_tot then category ="tot";
								if in_age then category ="mean_age";
								if in_agecat then do;
									if age_cat_&num.= 0 then category= "<66"  ;
									if age_cat_&num.= 1 then category= "66-69";
									if age_cat_&num.= 2 then category= "70-74";
									if age_cat_&num.= 3 then category= "75-79";
									if age_cat_&num.= 4 then category= "80-84";
									if age_cat_&num.= 5 then category= "85-89";
									if age_cat_&num.= 6 then category= "90+"  ;
								end;

								if in_geog then do;
									if region_&num. =0 then category= "Northeast";
									if region_&num. =1 then category= "Midwest";
									if region_&num. =2 then category= "South";
									if region_&num. =3 then category= "West";
									if region_&num. =4 then category= "Other_terr";
								end;

								if in_enr then do;
									if enrollment_&num. = "FFS"   then  category="FFS";
									if enrollment_&num. = "AONLY" then  category="AONLY";
									if enrollment_&num. = "MA"    then  category="MA";
									if enrollment_&num. = "OTHER" then  category="OTHER";	
								end;
	
								

								if in_female then category="female";
								if in_race then category= race;
								if in_dual then category="Full dual";
								
								

								if category= "mean_age" then do;
									summ= cat(strip(put(mean_age, 8.2)), " (", strip(put(std_age, 8.2)), ")");
								end;
								if category= "mean age" then n_bene=summ;
								

								drop age_cat: race region: enrollment:  mean: std:;
						run;


						proc sql ;
							select n_bene into : total TRIMMED
							from stack.summary_&num.
							where category="tot";
						quit;

						%PUT n_bene is &total;

						data stack.f_pct_&num. (rename=(n_bene=n_bene_&num. pct=pct_&num.));
							length category $20.   n_bene 8.; format n_bene comma12.;
							set stack.summary_&num.;
							pct=n_bene/&total;

							n_pct_&num.= cat (strip(put(n_bene, comma12.)), " (", strip(put(pct, percent8.2)), ")");
							if not missing(summ) then n_pct_&num.=summ;
							drop summ;
						run;




						%let i=%sysevalf(&i+1);
					%end;
				%mend finals;
					%finals (dose_1 booster);

					data stack.final_summary;
						length iso 3. category $50. n_bene_dose_1   pct_dose_1 8. n_pct_dose_1 $50.;
						if 0 then set stack.f_pct_booster;
						if _N_=1 then do;
							
							dcl hash ff (dataset:"stack.f_pct_booster");
							ff.definekey ("category");
							ff.definedata (all:"y");
							ff.definedone();
						end;
						call missing (of _all_);
						set stack.f_pct_dose_1;
						iso=_n_;
						rc=ff.find();
					run;



			%end;
			%if &misc %then %do;

			%*find num of pfi and mod dose1 and boosters;
/*

				proc sql;
					select
					dose_1,
					count(distinct bid_cwb_5) as n_bene 
					from stack.agg_dose1_booster_1_100
					where instudy_dose_1=1
					group by dose_1;
				quit;

				proc sql;
					select
					booster,
					count(distinct bid_cwb_5) as n_bene 
					from stack.agg_dose1_booster_1_100
					where instudy_booster=1
					group by booster;
				quit;



				%*table 2;
				%* Number of counties and number of benes vaccinated  stratified by pfizer and moderna for dose 1 and boosters;


				proc sql;
					create table stack.table_2 as 
					select
					count (distinct case when instudy_dose_1 =1  then catt(county_dose_1,  'x', state_dose_1)  else "" end) as n_county_dose_1,
					count (distinct case when instudy_booster=1  then catt(county_booster, 'x', state_booster) else "" end) as n_county_booster,

					count (distinct case when instudy_dose_1 =1  and not missing (dose_1)  then bid_cwb_5 else "" end) as n_vax_dose_1,
					count (distinct case when instudy_booster=1  and not missing (booster) then bid_cwb_5 else "" end) as n_vax_booster,

					count (distinct case when instudy_dose_1 =1  and not missing (dose_1)  and dose_1 ="pfizer"  then bid_cwb_5 else "" end) as n_pfi_dose_1,
					count (distinct case when instudy_booster=1  and not missing (booster) and booster="pfizer"  then bid_cwb_5 else "" end) as n_pfi_booster,

					count (distinct case when instudy_dose_1 =1  and not missing (dose_1)  and dose_1 ="moderna"  then bid_cwb_5 else "" end) as n_mod_dose_1,
					count (distinct case when instudy_booster=1  and not missing (booster) and booster="moderna"  then bid_cwb_5 else "" end) as n_mod_booster
					from stack.agg_dose1_booster_1_100;
				quit;


				%*Number of counties and benes vaccinated stratified by region for both dose 1 and booster;

				proc sql;
					create table stack.table_2_region_dose_1 as 
					select
					region_dose_1,
					count (distinct(catt(county_dose_1,  'x', state_dose_1))) as n_county_dose_1,
					count (distinct bid_cwb_5) as n_bene_dose_1 
					from stack.agg_dose1_booster_1_100
					where instudy_dose_1=1
					group by region_dose_1;
				quit;

				%*for boosters;
				proc sql;
					create table stack.table_2_region_booster as 
					select
					region_booster,
					count (distinct(catt(county_booster,  'x', state_booster))) as n_county_booster,
					count (distinct bid_cwb_5) as n_bene_booster
					from stack.agg_dose1_booster_1_100
					where instudy_booster=1
					group by region_booster;
				quit;

					
				

				data stack.pct_overall_w_regions;
					set stack.pct_overall (where=(state not in ("PR", "TR", "EX")));
					if missing(state) then region= .;				
					if state in ("CT" , "ME" , "MA" , "NH" , "RI" , "VT" , "NJ" , "NY" , "PA")                                                           then region= 0; *Northeast;			
					if state in ("IN" , "IL" , "MI" , "OH" , "WI" , "IA" , "KS" , "MN" , "MO" , "NE" , "ND" , "SD")                                      then region= 1; *Midwest;				
					if state in ("DE" , "DC" , "FL" , "GA" , "MD" , "NC" , "SC" , "VA" , "WV" , "AL" , "KY" , "MS" , "TN" , "AR" , "LA" , "OK" , "TX")   then region= 2; *South;				
					if state in ("AZ" , "CO" , "ID" , "NM" , "MT" , "UT", "NV" , "WY", "AK","CA","HI" , "OR" , "WA")                                     then region= 3; *West;				
				run;

				


				proc univariate data=stack.pct_overall_w_regions (where=(not missing(tot_dose_1)));
					var tot_dose_1 ;
					
					output out=stack.wo_reg_dist_dose_1
					pctlpts = 25 50 75 pctlpre=p_;
				run;


				proc univariate data=stack.pct_overall_w_regions (where=(not missing(tot_booster)));
					var tot_booster;
					
					output out=stack.wo_reg_dist_booster
					pctlpts = 25 50 75 pctlpre=p_;
				run;


				proc univariate data=stack.pct_overall_w_regions (where=(not missing(tot_dose_1)));
					var pct_pfi_dose_1 ;
					
					output out=stack.wo_reg_dist_dose_1_pfi
					pctlpts = 25 50 75 pctlpre=p_;
				run;


				proc univariate data=stack.pct_overall_w_regions (where=(not missing(tot_booster)));
					var pct_pfi_booster;
					
					output out=stack.wo_reg_dist_booster_pfi
					pctlpts = 25 50 75 pctlpre=p_;
				run;













%*************************with region stratification**********************;

				

				proc univariate data=stack.pct_overall_w_regions (where=(not missing(tot_dose_1)));
					var tot_dose_1 ;
					class region;
					output out=stack.dist_dose_1
					pctlpts = 25 50 75 pctlpre=p_;
				run;


				proc univariate data=stack.pct_overall_w_regions (where=(not missing(tot_booster)));
					var tot_booster;
					class region;
					output out=stack.dist_booster
					pctlpts = 25 50 75 pctlpre=p_;
				run;


				proc univariate data=stack.pct_overall_w_regions (where=(not missing(tot_dose_1)));
					var pct_pfi_dose_1 ;
					class region;
					output out=stack.dist_dose_1_pfi
					pctlpts = 25 50 75 pctlpre=p_;
				run;


				proc univariate data=stack.pct_overall_w_regions (where=(not missing(tot_booster)));
					var pct_pfi_booster;
					class region;
					output out=stack.dist_booster_pfi
					pctlpts = 25 50 75 pctlpre=p_;
				run;


				

			%end;
	


			%mend;
			%stacking (stacks=1, summs=1, tab1=1, final=1, misc=1);


			data stack.match_0_1_100;
				set stack.agg_dose1_booster_1_100 ;
				if ("01jan2021"d <= wk_st_dt_dose_1  <= "31jul2021"d and match_dose_1=0 and state_dose_1="MA") or
				
			run;

			

			proc sql;
				select
				count (distinct bid_cwb_5) as n_bene
				from stack.agg_dose1_booster_1_100
				where match_dose_1=0 and state_final="MA";
			quit;
