
options mprint mtrace mlogic macrogen symbolgen;
options linesize = 180 pagesize = 50 nocenter validvarname = upcase msglevel=I;


libname map_b     "FILE PATH REDACTED";


libname stack    "FILE PATH REDACTED";
libname map      "FILE PATH REDACTED";




				data map_b.m_vax_by_zip ;
					set stack.pct_overall (keep= county state pct_pfi_booster pct_mod_booster tot_booster where=(not missing(tot_booster)));
					length id $15.;
					fips_final=county;
					id='US-'||trim(left(fips_final));
					length my_html $200.;
			
					if pct_mod_booster ge .90 then pct_bucket=1;
					else if pct_mod_booster ge .70 and pct_mod_booster lt .90 then pct_bucket=2;
					else if pct_mod_booster ge .50 and pct_mod_booster lt .70 then pct_bucket=3;					
				
					
					else if pct_pfi_booster gt .50 and pct_pfi_booster lt .70 then pct_bucket=4;										
					else if pct_pfi_booster ge .70 and pct_pfi_booster lt .90 then pct_bucket=5;
					else if pct_pfi_booster ge .90   then pct_bucket=6;

					
					my_html='title='||quote(trim(left(catt(state,'-',fips_final)))||'0d'x||
						"Moderna: "||put (pct_mod_booster,percentn7.1)|| '0d'x||
						"Pfizer: "||put (pct_pfi_booster,percentn7.1));
					
						
				run;






				data map_b.county;
					set mapsgfk.us_counties;
				run;

				proc sort data =map_b.county nodupkey out=map_b.dedup_county;
					by statecode id;
				run;

				data map_b.m_vax_by_zip_1 ;
					if 0 then set map_b.dedup_county (keep= statecode id);
					if _n_=1 then do;
						dcl hash mm (dataset:"map_b.dedup_county (keep= statecode id)");
						mm.definekey ("statecode" , "id");
						mm.definedata (all:"y");
						mm.definedone();
					end;
					call missing(of _all_);
					set map_b.m_vax_by_zip;
					rc= mm.find(key: state, key:id);
				run;

		



				%let name= booster_distribution;
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


					proc gremove data=here.uscounty out=map_b.anno_outline;
						id  county;
						by state notsorted;
					run;


				

					proc gmap map =map_b.anno_outline data=map_b.anno_outline;
						id state;
						choro segment /levels=1 nolegend coutline=black;
					run;

				


				data map_b.anno_outline;
					set map_b.anno_outline;
					by state segment notsorted;					
					*hsys='3';					
					length function $8. color $8.;
					color='black' ; retain size 1;
					style='mempty';when ='a'; xsys='2'; ysys='2';
					if first.segment then function='Poly';
					else function ='Polycont';
				run;

				proc sort data= map_b.anno_outline nodupkey; by state id; run;

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
				*pattern8 v=s  c=cxffffff;
			

/*
				legend1 mode=share across=1 label=none  position = (bottom right )
				shape=bar (.11in, .11in)  
				value = ( justify=left
				t=1  ">=90% Moderna"	
				t=2  ">=70&<90%"			
				t=3  ">50&<70%"
				t=4  "=50%"				
				t=5  ">50&<70% Pfizer"				
				t=6  ">=70&<90%"
				t=7  ">=90%"
				);*/

				ods pdf file="FILE PATH REDACTED" dpi=3000;
				options nodate; options nonumber;
				*options nobyline;		
				proc gmap data=map_b.m_vax_by_zip_1 map=here.uscounty all;
					id statecode id;
					
					choro pct_bucket/ levels=1 discrete midpoints =  1  2  3  4  5  6 

					cdefault = black
					coutline=black  anno=map_b.anno_outline
					nolegend
					html=my_html
					des= '' name="&name";
				run;
				quit;
				ODS pdf CLOSE;
				ODS LISTING;

