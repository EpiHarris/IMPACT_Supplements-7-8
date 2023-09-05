**********************************************************************************;
*creats map for dose 1 distribution**********************************************;
**********************************************************************************;
options mprint mtrace mlogic macrogen symbolgen;
options linesize = 180 pagesize = 50 nocenter validvarname = upcase msglevel=I;

libname stack    "FILE PATH REDACTED";
libname map      "FILE PATH REDACTED";





				data map.m_vax_by_zip ;
					set stack.pct_overall (keep= county state pct_pfi_dose_1 pct_mod_dose_1 tot_dose_1 where=(not missing(tot_dose_1)));
					length id $15.;
					fips_final=county;
					id='US-'||trim(left(fips_final));
					length my_html $200.;
			
					if pct_mod_dose_1 ge .90 then pct_bucket=1;
					else if pct_mod_dose_1 ge .70 and pct_mod_dose_1 lt .90 then pct_bucket=2;
					else if pct_mod_dose_1 ge .50 and pct_mod_dose_1 lt .70 then pct_bucket=3;					
				
					

					else if pct_pfi_dose_1 gt .50 and pct_pfi_dose_1 lt .70 then pct_bucket=4;										
					else if pct_pfi_dose_1 ge .70 and pct_pfi_dose_1 lt .90 then pct_bucket=5;
					else if pct_pfi_dose_1 ge .90   then pct_bucket=6;

				
					my_html='title='||quote(trim(left(catt(state,'-',fips_final)))||'0d'x||
						"Moderna: "||put (pct_mod_dose_1,percentn7.1)|| '0d'x||
						"Pfizer: "||put (pct_pfi_dose_1,percentn7.1));
					
						
				run;






				data map.county;
					set mapsgfk.us_counties;
				run;

				proc sort data =map.county nodupkey out=map.dedup_county;
					by statecode id;
				run;

				data map.m_vax_by_zip_1 ;
					if 0 then set map.dedup_county (keep= statecode id);
					if _n_=1 then do;
						dcl hash mm (dataset:"map.dedup_county (keep= statecode id)");
						mm.definekey ("statecode" , "id");
						mm.definedata (all:"y");
						mm.definedone();
					end;
					call missing(of _all_);
					set map.m_vax_by_zip;
					rc= mm.find(key: state, key:id);
				run;



				%let name= dose_1_distribution;
				filename odsout '.';

				goptions reset=all cback=white border ;

				data contuall akuall hiuall ;
					set mapsgfk.us_counties (where=(density<=2) drop=resolution);
					if statecode='HI' then output hiuall;
					else if statecode='AK' then output akuall;					
					else output contuall;
				run;				

				
				proc gproject data=contuall out=contpall latlong eastlong degrees dupok;
					id state county  ;
				run;

				
				proc gproject data=akuall out=akpall latlong eastlong degrees dupok longmin=-168.4137104 nodateline;
					id state county  ;
				run;

				proc gproject data=hiuall out=hipall latlong eastlong degrees dupok longmin = -160.309348;
					id state county  ;
				run;

				%*scaling factors**************;

					proc sql noprint;
						select min(x) into: hixmin from hipall;
						select min(y) into: hiymin from hipall;
						select max(x) into: hixmax from hipall;
						select max(y) into: hiymax from hipall;

						select min(x) into: akxmin from akpall;
						select min(y) into: akymin from akpall;
						select max(x) into: akxmax from akpall;
						select max(y) into: akymax from akpall;

						select min(x) into: contxmin from contpall;
						select min(y) into: contymin from contpall;
						select max(x) into: contxmax from contpall;
						select max(y) into: contymax from contpall;
					quit;run;


					data akpall (drop = FACT1-FACT2 BX BY AX AY);
						set akpall;
						FACT1 = &contxmin + 0.200* (&contxmax - &contxmin);
						FACT2 = &contymin + 0.285* (&contymax - &contymin);

						AX = (FACT1  - &contxmin)  /  (&akxmax - &akxmin);
						AY = (FACT2  - &contymin)  /  (&akymax - &akymin);
						BX = &contxmin - AX* &akxmin;
						BY = &contymin - AY* &akymin;
						X  = X * AX + BX;
						Y  = Y * AY + BY;
					run;

					data hipall (drop = FACT1-FACT3 BX BY AX AY);
						set hipall;
						FACT1 = &contxmin + 0.200* (&contxmax - &contxmin);
						FACT2 = &contxmin + 0.300* (&contxmax - &contxmin);
						FACT3 = &contymin + 0.143* (&contymax - &contymin);

						AX = (FACT2  - FACT1)  /  (&hixmax - &hixmin);
						AY = (FACT3  - &contymin)  /  (&hiymax - &hiymin);
						
						BX = FACT1 - AX* &hixmin;
						BY = &contymin - AY* &hiymin;
						X  = X * AX + BX;
						Y  = Y * AY + BY;
					run;

					%*recombine;

					data uscounty;
						set contpall akpall hipall ;
					run;

					data uscounty;
						set uscounty;
						original_order= _n_;
					run;

					proc sort data=uscounty out=uscounty;
						by state county original_order;
					run;

					%*save;

					libname here '.';
					data here.uscounty (keep = statecode state county id segment x y density);
						set uscounty;
					run;
			
					proc gmap map=here.uscounty data=here.uscounty;
						id state county;
						choro segment / levels=1 nolegend coutline=black;
					run;


					proc gremove data=here.uscounty out=map.anno_outline;
						id  county;
						by state notsorted;
					run;


				

					proc gmap map =map.anno_outline data=map.anno_outline;
						id state;
						choro segment /levels=1 nolegend coutline=black;
					run;

				


				data map.anno_outline;
					set map.anno_outline;
					by state segment notsorted;					
					*hsys='3';					
					length function $8. color $8.;
					color='black' ; retain size 1;
					style='mempty';when ='a'; xsys='2'; ysys='2';
					if first.segment then function='Poly';
					else function ='Polycont';
				run;

				proc sort data= map.anno_outline nodupkey; by state id; run;

				goptions device=png;
				goptions xpixels =900 ypixels=600;
				goptions border;

				ODS listing close;
				ODS HTML path=odsout body="&name..htm" style=htmlblue;

				

				pattern1 v=s  c=dep; 
				pattern2 v=s  c=mop;
				pattern3 v=s  c=vpav; 
				 
				pattern4 v=s  c=liyg;
				pattern5 v=s  c=mog;
				pattern6 v=s  c=deg;		
				*pattern7 v=s  c=cxffffff;

			

				legend1 mode=share across=1 label=none  position = (bottom right )
				shape=bar (.11in, .11in)  
				value = ( justify=left
				t=1  ">=90% Moderna"	
				t=2  ">=70&<90%"			
				t=3  ">=50&<70%"
							
				t=4  ">50&<70% Pfizer"				
				t=5  ">=70&<90%"
				t=6  ">=90%"
				);


/*
				OPTIONS orientation=landscape;
				goptions
				reset=all ftext='HELVETICA/BO'
				htext=4
				gunit=pct
				rotate=landscape;

				ods listing close;
				ods pdf file="FILE PATH REDACTED" dpi=3000;
				options nodate; options nonumber;
*/
				*options nobyline;		
				proc gmap data=map.m_vax_by_zip_1  map=here.uscounty all;
					id statecode id;
					
					choro pct_bucket/ levels=1 discrete midpoints =  1  2  3  4  5  6  

					cdefault = black
					coutline=black  anno=map.anno_outline 
					legend=legend1
					html=my_html
					des= '' name="&name";
				run;
				quit;
				ODS html CLOSE;
				ODS LISTING;

%*******************************************************************************************;
%* explore counties that did not match;

/*
	
				data map.us_map_data_explr;
					set master.us_map_1_100;
					inc_pfiz     = sum(INC_0001A, INC_CVS_Pfizer1 , INC_WAG_Pfizer1);
					inc_mode     = sum(INC_0011A , INC_CVS_Moderna1 ,INC_WAG_Moderna1);	
					if inc_pfiz>0 and inc_mode>0 then pfi_mod=1;				
				run;



				proc sql;
					create table map.map_more_than_one_vax as
					select
					"pfi_mod" as category,
					count (distinct bid_cwb_5) as n_bene
					from map.us_map_data_explr
					where pfi_mod=1;
				quit;

	

	/*****************debug********************/
/*

				PROC SQL;
					select (catt(statecode, "x" , id)) as states 
					from map.dedup_county
					except
					select (catt(statecode, "x" , id)) as states 
					from map.m_vax_by_zip_1;
				quit;



				proc sort data=map.county  (where=(statecode ^= "PR")) nodupkey out=map.dedup_county_1;
					by statecode id;
				run;

				proc sort data=map.county (where=(statecode = "PR")) nodupkey out=map.dedup_county_2;
					by statecode id2;
				run;

				data map.dedup_county;
					set map.dedup_county_1 map.dedup_county_2;
					if statecode="PR" then	id=id2;	
					if statecode ^= "VI" then output;
				run;

*/
