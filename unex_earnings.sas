/*
	macro that computes unexpected earnings for fiscal years
	dsin: input dataset with gvkey fyear datadate ibes_ticker
*/

%macro unex_earn(dsin=, dsout=);

data ue_1 (keep = gvkey fyear datadate ibes_ticker );
set &dsin;
if ibes_ticker ne "";
run;

/* consensus forecast */
proc sql;	
	create table ue_2 as 
	select a.*, b.meanest, b.statpers
	from ue_1 a left join ibes.statsum_epsus b
	on a.ibes_ticker = b.ticker 
	and missing(b.meanest) ne 1
    and b.measure="EPS"
    and b.fiscalp="ANN"
    and b.fpi = "1"
    and a.datadate - 40 < b.STATPERS < a.datadate 
    and a.datadate -5 <= b.FPEDATS <= a.datadate +5 
	/* take most recent one in case of multiple matches */
	group by ibes_ticker, datadate
	having max(b.statpers) = b.statpers; 
quit;

/* get actual earnings */
proc sql;
  create table ue_3 as
  select a.*, b.PENDS, b.VALUE, b.ANNDATS, b.value - a.meanest as unex, abs( calculated unex) as absunex
  from ue_2 a left join ibes.act_epsus b
  on 
        a.ibes_ticker = b.ticker
	and missing(b.VALUE) ne 1
    and b.MEASURE="EPS"
    and b.PDICITY="ANN"
    and a.datadate -5 <= b.PENDS <= a.datadate +5;
quit;

/* force unique records - keep the one with largest surprise*/
proc sort data=ue_3; by gvkey datadate descending absunex;run;
proc sort data=ue_3 nodupkey; by gvkey datadate ;run;


proc sql;
	create table &dsout as 
	select a.*, b.unex from &dsin a left join ue_3 b on a.gvkey = b.gvkey and a.datadate = b.datadate;
quit;

%mend;
