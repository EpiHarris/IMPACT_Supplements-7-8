
libname input    "FILE PATH REDACTED" access=readonly;


*options dlcreatedir;

%macro libraries;

		%do p = 1 %to 100;
			*libname folder    "FILE PATH REDACTED";
			
			libname part_&p.   "FILE PATH REDACTED";
		
			/*
			proc datasets library = part_&p. kill;
			run;
			quit;
*/
		%end;

%mend;
%libraries;

options  mprint mtrace mlogic macrogen symbolgen;
options linesize = 180 pagesize = 50 nocenter validvarname = upcase msglevel=I;

%include "FILE PATH REDACTED";

	

		%macro doses;
			%do p = &start_partition. %to &end_partition.;	
	
			
				data part_&p.._1a_flag_all_doses_&p.;				
					set input.bene_week_&p. (obs=&obs. keep = bid: week_id week_st_dt cen: cme: state: fips: bene_class0 &keep_vars.  bene_age bene_rti: bene_sex_cd full: where=(&where_cond.));
					format f_week_st_dt b_week_st_dt date9.;

					fips_final_n=cats(repeat('0',5-length(fips_final)-1),fips_final);
					fips_2=substr(fips_final_n,1,2);
					if fips_2="00" then fips_2="";

					pfi_1=sum (INC_0001A , INC_CVS_Pfizer1, INC_WAG_Pfizer1);
					pfi_4=sum (INC_0003A , INC_CVS_Pfizer3, INC_WAG_Pfizer3 , INC_0004A , INC_CVS_Pfizer4, INC_WAG_Pfizer4);
				

					mod_1=sum (INC_0011A , INC_CVS_Moderna1 , INC_WAG_Moderna1);
					mod_4=sum (INC_0013A , INC_CVS_Moderna3 , INC_WAG_Moderna3, INC_0064A , INC_0094A , INC_CVS_Moderna4, INC_WAG_Moderna4);
				
					if pfi_1>0  or mod_1>0 then do; 
						
						f_week_id         = week_id;
						f_week_st_dt      = week_st_dt;
						f_bene_age        = bene_age;
						f_bene_sex_cd     = bene_sex_cd;
						f_bene_rti_race_cd= bene_rti_race_cd;
						f_censor_flag     = censor_flag;
						f_state_final     = state_final;
						f_fips_final      = fips_final; 
						f_full_dual       = full_dual;
						f_bene_class0     = bene_class0;
					end;					
					if pfi_4>0  or mod_4>0 then do; 
						   
						b_week_id         = week_id;
						b_week_st_dt      = week_st_dt;
						b_bene_age        = bene_age;
						b_bene_sex_cd     = bene_sex_cd;
						b_bene_rti_race_cd= bene_rti_race_cd;
						b_censor_flag     = censor_flag;
						b_state_final     = state_final;
						b_fips_final      = fips_final;
						b_full_dual       = full_dual;
						b_bene_class0     = bene_class0; 
					end;
					
				
					drop inc_:;	
				run;


					proc import datafile= "FILE PATH REDACTED"
						out=part_&p..usps_fips_code
						dbms=xlsx replace;
					run;


					data part_&p..usps_fips_code ;
						set part_&p..usps_fips_code;
						fips_usps=put(fips,12.);
						state_usps=strip(statecode);
						drop fips state statecode;
					run;




					data part_&p.._1_flag_all_doses_&p.;	
						if 0 then set part_&p..usps_fips_code;
						if _N_=1 then do;							
							dcl hash ff (dataset:"part_&p..usps_fips_code");
							ff.definekey ("state_usps" , "fips_usps");
							ff.definedata ("state_usps","fips_usps");
							ff.definedone();
						end;
						call missing (of _all_);
						set part_&p.._1_flag_all_doses_&p.;
						match=0;
						if ff.find(key:state_final, key:fips_2)=0 then match=1;		
						if pfi_1>0  or mod_1>0 then f_match         = match;
						if pfi_4>0  or mod_4>0 then b_match         = match;
											
					run;

				
					data  part_&p..ck_dose_1s_&p. (keep=bid: fips: state: week: pfi_1 mod_1 f_:) part_&p..ck_boosters_&p. (keep=bid: fips: state: week: pfi_4 mod_4 b_:) ;
						set  part_&p.._1_flag_all_doses_&p. (obs=&obs.) ;
						if pfi_1>0 or mod_1>0 then output part_&p..ck_dose_1s_&p. ;
						if pfi_4>0 or mod_4>0 then output part_&p..ck_boosters_&p. ;
					run;

					proc sort data= part_&p..ck_dose_1s_&p. nodupkey out=part_&p..dedup_ck_dose_1s_&p.;
						by bid_cwb_5;
					run;

					proc sort data= part_&p..ck_dose_1s_&p. nodupkey out=part_&p..dedup_ck_dose_1s_&p.;
						by bid_cwb_5;
					run;

					proc sort data= part_&p..ck_boosters_&p. nodupkey out=part_&p..dedup_ck_boosters_&p.;
						by bid_cwb_5;
					run;

					proc sql;
						create table part_&p.._not_in_boost as  
						select distinct (bid_cwb_5) as n_bene
						from part_&p..dedup_ck_boosters_&p. 
						except
						select distinct (bid_cwb_5) as n_bene 
						from part_&p..dedup_ck_dose_1s_&p.;
					quit;



					data part_&p.._2_bene_lvl_flags_&p. ;
						merge part_&p..dedup_ck_dose_1s_&p. (in=in_dose_1) part_&p..dedup_ck_boosters_&p. (in=in_booster) ;
						length type $10.;
						if in_dose_1 then type="dose_1";
						if in_booster then type="booster";
						by bid_cwb_5;
					run;

					

					data part_&p.._3_del_w_both_mrna_&p.;	
						length BID_CWB_5 $50.  dose_1 booster $10.;
						set part_&p.._2_bene_lvl_flags_&p.;
						
						if missing(f_state_final) then region_dose_1 = .;				
						if f_state_final in ("CT" , "ME" , "MA" , "NH" , "RI" , "VT" , "NJ" , "NY" , "PA")                                                           then region_dose_1 = 0; *Northeast;			
						if f_state_final in ("IN" , "IL" , "MI" , "OH" , "WI" , "IA" , "KS" , "MN" , "MO" , "NE" , "ND" , "SD")                                      then region_dose_1 = 1; *Midwest;				
						if f_state_final in ("DE" , "DC" , "FL" , "GA" , "MD" , "NC" , "SC" , "VA" , "WV" , "AL" , "KY" , "MS" , "TN" , "AR" , "LA" , "OK" , "TX")   then region_dose_1 = 2; *South;				
						if f_state_final in ("AZ" , "CO" , "ID" , "NM" , "MT" , "UT", "NV" , "WY", "AK","CA","HI" , "OR" , "WA")                                     then region_dose_1 = 3; *West;				
						if f_state_final in ("PR" , "TR" , "EX")                                                                                                     then region_dose_1 = 4; *Other territories;

						if missing(b_state_final) then region_booster = .;				
						if b_state_final in ("CT" , "ME" , "MA" , "NH" , "RI" , "VT" , "NJ" , "NY" , "PA")                                                           then region_booster = 0; *Northeast;			
						if b_state_final in ("IN" , "IL" , "MI" , "OH" , "WI" , "IA" , "KS" , "MN" , "MO" , "NE" , "ND" , "SD")                                      then region_booster = 1; *Midwest;				
						if b_state_final in ("DE" , "DC" , "FL" , "GA" , "MD" , "NC" , "SC" , "VA" , "WV" , "AL" , "KY" , "MS" , "TN" , "AR" , "LA" , "OK" , "TX")   then region_booster = 2; *South;				
						if b_state_final in ("AZ" , "CO" , "ID" , "NM" , "MT" , "UT", "NV" , "WY", "AK","CA","HI" , "OR" , "WA")                                     then region_booster = 3; *West;				
						if b_state_final in ("PR" , "TR" , "EX")                                                                                                     then region_booster = 4; *Other territories;
 
						if pfi_1>0 and mod_1=0 then dose_1= "pfizer";
						if pfi_1=0 and mod_1>0 then dose_1= "moderna";

						if pfi_4>0 and mod_4=0 then booster= "pfizer";
						if pfi_4=0 and mod_4>0 then booster= "moderna";

						state_dose_1 =put(f_state_final, 8.);
						state_booster=put(b_state_final, 8.);

						rename f_week_id          = wk_id_dose_1;
						rename f_week_st_dt       = wk_st_dt_dose_1;
						rename f_bene_age         = age_dose_1;
						rename f_bene_sex_cd      = sex_dose_1;
						rename f_bene_rti_race_cd = race_dose_1;
						rename f_censor_flag      = censor_flag_dose_1;	

						rename f_fips_final       = county_dose_1;
						rename f_bene_class0      = enrollment_dose_1;
						rename f_full_dual        = dual_dose_1;
						rename f_match            = match_dose_1;
						

						rename b_week_id          = wk_id_booster;
						rename b_week_st_dt       = wk_st_dt_booster;
						rename b_bene_age         = age_booster;
						rename b_bene_sex_cd      = sex_booster;
						rename b_bene_rti_race_cd = race_booster;
						rename b_censor_flag      = censor_flag_booster;					
						rename b_fips_final       = county_booster;
						rename b_bene_class0      = enrollment_booster;
						rename b_full_dual        = dual_booster;
						rename b_match            = match_booster;

						drop pfi: mod: fips_usps state_usps fips_final_n fips_2;
					run;


					data part_&p.._4_in_study_flags_&p.;	
						set part_&p.._3_del_w_both_mrna_&p.;
						instudy_dose_1=0; instudy_booster=0;
						if not missing(dose_1) and  "01jan2021"d <= wk_st_dt_dose_1  <= "31jul2021"d and censor_flag_dose_1 =0 and not missing (county_dose_1) and match_dose_1=1 then instudy_dose_1 =1;
						if not missing(booster) and "01aug2021"d <= wk_st_dt_booster <= "30apr2022"d and censor_flag_booster=0 and not missing (county_booster) and match_booster=1 then instudy_booster=1;
					run;



					
REDACTED CODE DUE TO PRIVACY AND DATA COMPLIANCE	




			%end;
		%mend;
		%doses;
	
