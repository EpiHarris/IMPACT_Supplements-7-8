*********************************************************;
This program adds the NCHS rural urban classification t0*;
***************************county level data*************;


options mprint mtrace mlogic macrogen symbolgen;
options linesize = 180 pagesize = 50 nocenter validvarname = upcase msglevel=I;

libname rural    "FILE PATH REDACTED";
libname stack    "FILE PATH REDACTED";



		proc import out=rural.data_rural datafile="FILE PATH REDACTED";
		run;
		
		data rural.f_data_rural;
			set rural.data_rural  (keep=fips_final stateabr countyname code_2013
			rename=(fips_final=fips_f stateabr=state_f countyname=county_f code_2013=code_f));
		run;


		data rural.b_data_rural;
			set rural.data_rural  (keep=fips_final stateabr countyname code_2013
			rename=(fips_final=fips_b stateabr=state_b countyname=county_b code_2013=code_b));
		run;

		data rural.dose1_booster_1_100_rural;
			if 0 then set rural.f_data_rural rural.b_data_rural;
			if _n_=1 then do;
				dcl hash rr (dataset:"rural.f_data_rural");
				rr.definekey ("fips_f");
				rr.definedata (all:"y");
				rr.definedone();

				dcl hash uu (dataset:"rural.b_data_rural");
				uu.definekey ("fips_b");
				uu.definedata (all:"y");
				uu.definedone();
			end;
			call missing (of _all_);

			set stack.agg_dose1_booster_1_100 ;
			length county_dose_1 county_booster $5.; format county_dose_1 county_booster $5.;			
			 rc_f= rr.find( key:county_dose_1);
			 rc_b= uu.find( key:county_booster);
		run;
	
		proc sql;
			create table rural.dose_1_rural_summ     as				
			select
			code_f,
			count (distinct BID_CWB_5) as n_bene
			from rural.dose1_booster_1_100_rural
			where instudy_dose_1=1
			group by code_f;
		quit;
		
	
		proc sql;
			create table rural.booster_rural_summ     as				
			select
			code_b,
			count (distinct BID_CWB_5) as n_bene
			from rural.dose1_booster_1_100_rural
			where instudy_booster=1
			group by code_b;
		quit;

			data rural._2_dose1_booster_1_100_rural;
				set  rural.dose1_booster_1_100_rural;
				if instudy_dose_1=1 and dose_1="pfizer"  then pfizer_f=1; 
				if instudy_dose_1=1 and dose_1="moderna" then pfizer_f=0; 

				if instudy_booster=1 and booster="pfizer"  then pfizer_b=1; 
				if instudy_booster=1 and booster="moderna" then pfizer_b=0;
			run; 

			proc sql;
				create table rural.dose_1_rural_pfi_summ     as				
				select
				code_f,pfizer_f,
				count (distinct BID_CWB_5) as n_bene
				from rural._2_dose1_booster_1_100_rural
				where instudy_dose_1=1
				group by code_f,pfizer_f;
			quit;
			
		
			proc sql;
				create table rural.booster_rural_pfi_summ     as				
				select
				code_b,pfizer_b,
				count (distinct BID_CWB_5) as n_bene
				from rural._2_dose1_booster_1_100_rural
				where instudy_booster=1
				group by code_b, pfizer_b;
			quit;


				proc sql;
						create table rural.percentages_dose_1 as 
						select
						a.county_dose_1,
						a.state_dose_1,
						a.code_f,
						n_pfi_dose_1,
						n_mod_dose_1,
						tot_dose_1,
						n_pfi_dose_1/tot_dose_1 as pct_pfi_dose_1,
						n_mod_dose_1/tot_dose_1 as pct_mod_dose_1
						from
						(select
							county_dose_1,state_dose_1, code_f,
							count(distinct bid_cwb_5) as tot_dose_1
							from rural._2_dose1_booster_1_100_rural
							where instudy_dose_1=1
							group by wk_id_dose_1) as a 

						left join 
						(select
							wk_id_dose_1
							count(distinct bid_cwb_5) as n_pfi_dose_1
							from rural._2_dose1_booster_1_100_rural
							where dose_1="pfizer" and instudy_dose_1=1
							group by wk_id_dose_1) as  b 
							on a.wk_id_dose_1=b.wk_id_dose_1
							left join
							(select
								wk_id_dose_1
								count(distinct bid_cwb_5) as n_mod_dose_1
								from rural._2_dose1_booster_1_100_rural
								where dose_1="moderna" and instudy_dose_1=1
								group by county_dose_1, state_dose_1, code_f) as c   
							on a.county_dose_1=c.county_dose_1 and a.state_dose_1=c.state_dose_1;
						quit;

						

						proc sql;
							create table rural.percentages_booster as 
							select
							a.county_booster,
							a.state_booster,
							a.code_b,
							n_pfi_booster,
							n_mod_booster,
							tot_booster,
							n_pfi_booster/tot_booster as pct_pfi_booster,
							n_mod_booster/tot_booster as pct_mod_booster
							from
							(select
								county_booster,state_booster,code_b,
								count(distinct bid_cwb_5) as tot_booster
								from rural._2_dose1_booster_1_100_rural
								where instudy_booster=1
								group by county_booster, state_booster,code_b) as a 

							left join 
							(select
								county_booster, state_booster,code_b,
								count(distinct bid_cwb_5) as n_pfi_booster
								from rural._2_dose1_booster_1_100_rural
								where booster="pfizer" and instudy_booster=1
								group by county_booster, state_booster,code_b) as  b 
								on a.county_booster=b.county_booster and a.state_booster=b.state_booster
								left join
								(select
									county_booster, state_booster,code_b,
									count(distinct bid_cwb_5) as n_mod_booster
									from rural._2_dose1_booster_1_100_rural
									where booster="moderna" and instudy_booster=1
									group by county_booster, state_booster,code_b) as c   
								on a.county_booster=c.county_booster and a.state_booster=c.state_booster;
						quit;


						proc sql;
							create table rural.pct_overall as
							select 
							coalesce(x.county_dose_1, y.county_booster) as county,
							coalesce(x.state_dose_1, state_booster)     as state,
							coalesce(x.code_f, code_b)     as rural,
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
							from rural.percentages_dose_1 as x full join rural.percentages_booster as y
							on x.county_dose_1=y.county_booster and x.state_dose_1=y.state_booster;
						quit;

	


						data rural.pct_overall_w_rural;
							if 0 then set rural.data_rural (keep=fips_final stateabr code_2013);
							if _n_=1 then do;
								dcl hash rr (dataset:"rural.data_rural  (keep=fips_final stateabr code_2013)");
								rr.definekey ("fips_final", "stateabr");
								rr.definedata (all:"y");
								rr.definedone();							
							end;
							call missing (of _all_);
							set stack.pct_overall ;	
							rc=rr.find(key:county, key:state);							
						run;


						data rural._2_pct_overall_w_rural;
							set rural.pct_overall_w_rural;
							if code_2013=1 then class="Large central metro";
							if code_2013=2 then class="Large fringe metro";
							if code_2013=3 then class="Medium metro";
							if code_2013=4 then class="Small metro";
							if code_2013=5 then class="Micropolitan";
							if code_2013=6 then class="Non-core";
						run;





					proc export data = rural._2_dose1_booster_1_100_rural
						outfile = "FILE PATH REDACTED"
						dbms = dta replace;
						fmtlib = formats;
					run; 





					data rural.final_dose_1;
						set rural._2_dose1_booster_1_100_rural (where=(instudy_dose_1=1));
						mth_dose_1=month(wk_st_dt_dose_1);
					run;

					data rural.final_booster;
						set rural._2_dose1_booster_1_100_rural (where=(instudy_booster=1));
						mth_booster=month(wk_st_dt_booster);
					run;


					proc sql;
						create table rural.months_dose_1 as 
						select
						a.mth_dose_1,						
						code_f,
						n_dose_1,					
						tot_dose_1,
						n_dose_1/tot_dose_1 as pct_dose_1						
						from
						(select
							mth_dose_1,
							count(distinct bid_cwb_5) as tot_dose_1
							from rural.final_dose_1
							where instudy_dose_1=1
							group by mth_dose_1) as a 

						left join 
						(select
							mth_dose_1, code_f,
							count(distinct bid_cwb_5) as n_dose_1
							from rural.final_dose_1
							where instudy_dose_1=1
							group by mth_dose_1, code_f) as  b 
							on a.mth_dose_1=b.mth_dose_1;
						quit;

						
						proc sql;
							create table rural.months_booster as 
							select
							a.mth_booster,						
							code_b,
							n_booster,					
							tot_booster,
							n_booster/tot_booster as pct_booster						
							from
							(select
								mth_booster,
								count(distinct bid_cwb_5) as tot_booster
								from rural.final_booster
								where instudy_booster=1
								group by mth_booster) as a 

							left join 
							(select
								mth_booster, code_b,
								count(distinct bid_cwb_5) as n_booster
								from rural.final_booster
								where instudy_booster=1
								group by mth_booster, code_b) as  b 
								on a.mth_booster=b.mth_booster;
						quit;

						





					proc sql;
						create table rural.denom_dose_1 as 
						select					
							mth_dose_1,
							code_f,
							count(distinct bid_cwb_5) as tot_dose_1
							from rural.final_dose_1
							where instudy_dose_1=1
							group by mth_dose_1,code_f;
					quit;

					proc sql;
						create table rural.numerator_dose_1 as 
						select
							mth_dose_1, 
							code_f,
							pfizer_f,
							count(distinct bid_cwb_5) as n_dose_1
							from rural.final_dose_1
							where instudy_dose_1=1 
							group by mth_dose_1, code_f,pfizer_f;
					quit;

					

					proc sql;
						create table rural.denom_booster as 
						select					
							mth_booster,
							code_b,
							count(distinct bid_cwb_5) as tot_booster
							from rural.final_booster
							where instudy_booster=1
							group by mth_booster,code_b;
					quit;

					proc sql;
						create table rural.numerator_booster as 
						select
							mth_booster, 
							code_b,
							pfizer_b,
							count(distinct bid_cwb_5) as n_booster
							from rural.final_booster
							where instudy_booster=1 
							group by mth_booster, code_b,pfizer_b;
					quit;
