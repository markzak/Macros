/* 
	Use this macro to create a 'starting' dataset based on Funda

	1. retrieves compustat Funda variables gvkey, fyear, datadate and &vars from year1 to year2
	2. gets &laggedvars for the previous years (typically lagged assets, sales, marketcap) (need to be in &vars)
	3. creates key (gvkey || fyear) and appends some common firm identifiers (permno, cusip, ibes_ticker)

	Add 1 more year if lagged data is needed (a self join is used to get lagged data)

	invoke as:
	%getFunda(dsout=a_funda1, vars=at sale ceq csho prcc_f, laggedvars=at, year1=1990, year2=2013);

	dependencies: uses Clay macros (%do_over)
*/

%macro getFunda(dsout=, vars=, laggedvars=, year1=2010, year2=2013);

/* Funda data */
data getf_1 (keep = key gvkey fyear datadate &vars);
set comp.funda;
if &year1 <= fyear <= &year2;
if indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C' ;
key = gvkey || fyear;
run;

/* 	Keep first record in case of multiple records; */
proc sort data =getf_1 nodupkey; by gvkey fyear;run;

/* Add lagged assets */
%if "&laggedvars" ne "" %then %do;
	/* add lagged vars */
	proc sql;
		create table getf_2 as
		select a.*, %do_over(values=&laggedvars, between=comma, phrase=b.? as ?_lag) 
		from  getf_1 a left join  getf_1 b
		on a.gvkey = b.gvkey and a.fyear -1 = b.fyear;
	quit;
%end;
%else %do;
	/* do not add lagged vars */
	data getf_2; set getf_1; run;
%end;

/* Permno as of datadate*/
proc sql; 
  create table getf_3 as 
  select a.*, b.lpermno as permno
  from getf_2 a left join crsp.ccmxpf_linktable b 
    on a.gvkey eq b.gvkey 
    and b.lpermno ne . 
    and b.linktype in ("LC" "LN" "LU" "LX" "LD" "LS") 
    and b.linkprim IN ("C", "P")  
    and ((a.datadate >= b.LINKDT) or b.LINKDT eq .B) and  
       ((a.datadate <= b.LINKENDDT) or b.LINKENDDT eq .E)   ; 
quit; 

/* retrieve historic cusip */
proc sql;
  create table getf_4 as
  select a.*, b.ncusip
  from getf_3 a, crsp.dsenames b
  where 
        a.permno = b.PERMNO
    and b.namedt <= a.datadate <= b.nameendt
    and b.ncusip ne "";
  quit;
 
/* force unique records */
proc sort data=getf_4 nodupkey; by key;run;
 
/* get ibes ticker */
proc sql;
  create table &dsout as
  select distinct a.*, b.ticker as ibes_ticker
  from getf_4 a left join ibes.idsum b
  on 
        a.NCUSIP = b.CUSIP
    and a.datadate > b.SDATES ;
quit;

/* force unique records */
proc sort data=&dsout nodupkey; by key;run;

/*cleanup */
proc datasets library=work; delete getf_1 - getf_4; quit;
%mend;
