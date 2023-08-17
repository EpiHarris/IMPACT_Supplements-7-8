


				data want ;
					set have /*county_level_proprotions dataset*/ 
					length id $15.;
					fips_final=county;
					id='US-'||trim(left(fips_final));
					length my_html $200.;
			
					if pct_mod_dose_1 ge .90 then pct_bucket=1;
					else if pct_mod_dose_1 ge .70 and pct_mod_dose_1 lt .90 then pct_bucket=2;
					else if pct_mod_dose_1 gt .50 and pct_mod_dose_1 lt .70 then pct_bucket=3;					
				
					else if pct_mod_dose_1 eq .50 then pct_bucket=4;

					else if pct_pfi_dose_1 gt .50 and pct_pfi_dose_1 lt .70 then pct_bucket=5;										
					else if pct_pfi_dose_1 ge .70 and pct_pfi_dose_1 lt .90 then pct_bucket=6;
					else if pct_pfi_dose_1 ge .90   then pct_bucket=7;

					else if pct_bucket=. then pct_bucket=8;

					/*the following code will create interative graphs*/
					my_html='title='||quote(trim(left(catt(state,'-',fips_final)))||'0d'x||
						"Moderna: "||put (pct_mod_dose_1,percentn7.1)|| '0d'x||
						"Pfizer: "||put (pct_pfi_dose_1,percentn7.1));
					
						
				run;



				data county;
					set mapsgfk.us_counties; /*sas default county dataset*/
				run;

				proc sort data = county nodupkey out= dedup_county;
					by statecode id;
				run;

				data  want_1 ;
					if 0 then set  dedup_county (keep= statecode id);
					if _n_=1 then do;
						dcl hash mm (dataset:" dedup_county (keep= statecode id)");
						mm.definekey ("statecode" , "id");
						mm.definedata (all:"y");
						mm.definedone();
					end;
					call missing(of _all_);
					set  want;
					rc= mm.find(key: state, key:id);
				run;

		



				%let name= map_1; /*name you want to give your map file*/
				filename odsout '.';

				goptions reset=all cback=white border ;

				/*this is to create alaska and hawaii*/

				data contuall akuall hiuall ;
					set mapsgfk.us_counties (where=(density<=2) drop=resolution);
					if statecode='HI' then output hiuall;
					else if statecode='AK' then output akuall;					
					else output contuall;
				run;				

				/*this will project the maps , make sure to close the screen when the maps gets projected*/
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


					proc gremove data=here.uscounty out= anno_outline;
						id  county;
						by state notsorted;
					run;


				

					proc gmap map = anno_outline data= anno_outline;
						id state;
						choro segment /levels=1 nolegend coutline=black;
					run;

				


				data  anno_outline;
					set  anno_outline;
					by state segment notsorted;
					length function $8. color $8.;
					color='black' ; retain size 1;
					style='mempty';when ='a'; xsys='2'; ysys='2';
					if first.segment then function='Poly';
					else function ='Polycont';
				run;

				proc sort data=  anno_outline nodupkey; by state id; run;

				goptions device=png;
				goptions xpixels =900 ypixels=600;
				goptions border;

				ODS listing close;
				ODS HTML path=odsout body="&name..htm" style=htmlblue;

				
				/*this is for map colors, you can change the colors */
				pattern1 v=s  c=dep; 
				pattern2 v=s  c=mop;
				pattern3 v=s  c=vpav; 
				pattern4 v=s  c=ligr;  
				pattern5 v=s  c=liyg;
				pattern6 v=s  c=mog;
				pattern7 v=s  c=deg;		
				pattern8 v=s  c=cxffffff;
			


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
				);

				*options nobyline;		
				proc gmap data= want_1 map=here.uscounty all;
					id statecode id;
					
					choro pct_bucket/ levels=1 discrete midpoints =  1  2  3  4  5  6  7  

					cdefault = black
					coutline=black  anno= anno_outline
					 legend = legend1
					html=my_html
					des= '' name="&name";
				run;
				quit;
				ODS HTML CLOSE;
				ODS LISTING;

