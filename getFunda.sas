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

/*****************************************************************************/
/*
	macro that appends Q4 earnings annoucement date ('earn_ann_dt') to input dataset 
	Earnings announcement date can be taken from Fundq or IBES (depending on 'source' argument)
	dsin: dataset to get earnings announcement date from
		- holds: gvkey, fyear, datadate, ibes_ticker (if source is IBES)
	dsout: dataset to generate
	source: fundq (default) or ibes
		- fundq retrieves rdq from fundq
		- ibes retrieves date from ibes actu_epsus
	invoke as:
	%earn_ann_date(dsin=mydata, dsout=myresult, source=IBES);
*/

%macro earn_ann_date(dsin=, dsout=, varname=anndate, source=fundq);

%if &source eq fundq %then %do;

	proc sql;	
		create table &dsout as 
		select a.*, b.rdq as &varname format date9.
		from &dsin a left join comp.fundq b
		on a.gvkey eq b.gvkey 
		/* matching datadate will only be true for q4 */
		and a.datadate eq b.datadate;
	quit;

%end;

%else %do;
	proc sql;	
		create table &dsout as 
		select a.*, b.anndats as &varname format date9.
		from &dsin a left join ibes.actu_epsus b
		on a.ibes_ticker = b.ticker 
		and b.MEASURE="EPS"
    	and b.PDICITY="QTR"
		/* IBES period end date 'close to' end of fiscal year */		
		and a.datadate -5 <= b.PENDS <= a.datadate +5;
	quit;
%end;

/* force unique obs */
proc sort data = &dsout nodupkey; by gvkey fyear;run;
%mend;

/*****************************************************************************/

/* 
  macro to compute beta 
	dsin, required:
		key, permno, &estDate (e.g. datadate)
  nMonths: #months to use
  minMonths: minimum #months
  estDate: estimation date (nMonths measured before this date)
*/

%macro getBeta(dsin=, dsout=, nMonths=, minMonths=12, estDate=);

/* create return window dates: mStart - mEnd */
data getb_1 (keep = key permno mStart mEnd);
set &dsin; 
/* drop obs with missing estimation date */
if &estDate ne .;
mStart=INTNX('Month',&estDate, -&nMonths, 'E'); 
mEnd=INTNX('Month',&estDate, -1, 'E'); 
if permno ne .;  
format mStart mEnd date.;
run;
  
/* get stock and market return */
proc sql;
  create table getb_2
    (keep = key permno mStart mEnd date ret vwretd) as
  select a.*, b.date, b.ret, c.vwretd
  from   getb_1 a, crsp.msf b, crsp.msix c
  where a.mStart-5 <= b.date <= a.mEnd +5
  and a.permno = b.permno
  and missing(b.ret) ne 1
  and b.date = c.caldt;
quit;

/* force unique obs */  
proc sort data = getb_2 nodup;by key date;run;

/* estimate beta for each key 
	EDF adds R-squared (_RSQ_), #degrees freedom (_EDF_) to regression output
*/
proc reg outest=getb_3 data=getb_2;
   id key;
   model  ret = vwretd  / noprint EDF ;
   by key;
run;

/* drop if fewer than &minMonths used*/
%let edf_min = %eval(&minMonths - 2);
%put Minimum number of degrees of freedom: &edf_min;

/* create output dataset */
proc sql;
  create table &dsout as 
	select a.*, b.vwretd as beta 
	from &dsin a left join getb_3 b on a.key=b.key and b._EDF_ > &edf_min;
quit;

%mend;

/*****************************************************************************/


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

/*****************************************************************************/

/*
	dsin: input dataset with key permno beta &eventdate
	dsout: dataset to create
	eventdate: variable that holds eventdate (e.g. ann_date)
	start: window start relative to event date (e.g. 0 or -1)
	end: window end relative to event date (eq 1 or 2)
	varname: variable name for abnormal return to create 
		computed as: sum of abnormal returns, computed as firm return - beta x market return
	
	example invoke:
	%eventReturn(dsin=d_beta, dsout=e_ret, eventdate=ann_dt, start=-1, end=2, varname=abnret);
*/

%macro eventReturn(dsin=, dsout=, eventdate=, start=0, end=1, varname=car);

data er_1 (keep = key permno beta &eventdate);
set &dsin;
if &eventdate ne .;
if beta ne .;
run;

/* measure the window using trading days*/
proc sql; create table er_2 as select distinct date from crsp.dsf;quit;

/* create counter */
data er_2; set er_2; count = _N_;run;

/* window size in trading days */
%let windowSize = %eval(&end - &start); 

/* get the closest event window returns, using trading days */
proc sql;
	create table er_3 as 
	select a.*, b.date, b.count
	from er_1 a, er_2 b	
	/* enforce 'lower' end of window: trading day must be on/after event (not before)
		using +10 to give some slack at the upper end */
	where b.date >= (a.&eventdate + &start) and b.date <=(a.&eventdate + &end + 10)
	group by key
	/* enforce 'upper' end of window: minimum count + windowsize must equal maximum count */
	having min (b.count) + &windowSize <= b.count;
quit;


/* determine the start trading day of return window */
proc sql;
	create table er_3 as 
	select a.*, b.count as wS
	from er_1 a, er_2 b	
	where b.date >= (a.&eventdate + &start) 
	group by key
	having min (b.count) = b.count ;
quit;
/* pull in trading days for event window */
proc sql;
	create table er_4 as 
	select a.*, b.date
	from er_3 a, er_2 b	
	where a.ws <= b.count <= a.ws + &windowSize;
quit;
proc sort data=er_4; by key date;run;

/* append firm return and index return */
proc sql;
	create table er_5 as
	select a.*, b.ret, c.vwretd, a.beta * c.vwretd as expected_ret /* assuming alpha of zero */
	from er_4 a, crsp.dsf b, crsp.dsix c
	where a.permno = b.permno
	and a.date = b.date
	and b.date = c.caldt
	and missing(b.ret) ne 1; 
quit;

/* sum abnret - thanks Lin */
proc sql;
	create table er_6 as 
	select key, exp(sum(log(1+ret)))-exp(sum(log(1+expected_ret))) as abnret
	from er_5 group by key;
quit;

/* create output dataset */
proc sql;
	create table &dsout as
	select a.*, b.abnret as &varname
	from &dsin a left join er_6 b
	on a.key = b.key;
quit;
%mend;


/*****************************************************************************/

/*****************************************
Trim or winsorize macro
* byvar = none for no byvar;
* type  = delete/winsor (delete will trim, winsor will winsorize;
*dsetin = dataset to winsorize/trim;
*dsetout = dataset to output with winsorized/trimmed values;
*byvar = subsetting variables to winsorize/trim on;
****************************************/
 
%macro winsor(dsetin=, dsetout=, byvar=none, vars=, type=winsor, pctl=1 99);
 
%if &dsetout = %then %let dsetout = &dsetin;
    
%let varL=;
%let varH=;
%let xn=1;
 
%do %until ( %scan(&vars,&xn)= );
    %let token = %scan(&vars,&xn);
    %let varL = &varL &token.L;
    %let varH = &varH &token.H;
    %let xn=%EVAL(&xn + 1);
%end;
 
%let xn=%eval(&xn-1);
 
data xtemp;
    set &dsetin;
    run;
 
%if &byvar = none %then %do;
 
    data xtemp;
        set xtemp;
        xbyvar = 1;
        run;
 
    %let byvar = xbyvar;
 
%end;
 
proc sort data = xtemp;
    by &byvar;
    run;
 
proc univariate data = xtemp noprint;
    by &byvar;
    var &vars;
    output out = xtemp_pctl PCTLPTS = &pctl PCTLPRE = &vars PCTLNAME = L H;
    run;
 
data &dsetout;
    merge xtemp xtemp_pctl;
    by &byvar;
    array trimvars{&xn} &vars;
    array trimvarl{&xn} &varL;
    array trimvarh{&xn} &varH;
 
    do xi = 1 to dim(trimvars);
 
        %if &type = winsor %then %do;
            if not missing(trimvars{xi}) then do;
              if (trimvars{xi} < trimvarl{xi}) then trimvars{xi} = trimvarl{xi};
              if (trimvars{xi} > trimvarh{xi}) then trimvars{xi} = trimvarh{xi};
            end;
        %end;
 
        %else %do;
            if not missing(trimvars{xi}) then do;
              if (trimvars{xi} < trimvarl{xi}) then delete;
              if (trimvars{xi} > trimvarh{xi}) then delete;
            end;
        %end;
 
    end;
    drop &varL &varH xbyvar xi;
    run;
 
%mend winsor;




/*cleantable Macro by Mark*/


rsubmit;
%macro CleanTable(dsin=, dsout=, Vars=, ByVar=,ByCategories=);

/* descriptive statistics -- creates one row with all data*/
proc sort data=&dsin; by &ByVar;run;
proc means data=&dsin NOPRINT;
OUTPUT OUT=n_0 (drop= _type_ _freq_) mean= p10= p25= p50= p75= p90= N=/autoname;
var &Vars;
Class &ByVar;
run;

/* convert it to a table format */
proc transpose data=n_0 out=n_1;
run;
data n_2;
set n_1;
varname=scan(_name_,1,'_');
stat=scan(_name_,2,'_');
drop _name_;
run;
proc sort data=n_2;
by varname;
run;
proc transpose data=n_2 out=&dsout(drop=_name_);
by varname;
id stat;
var col1-col&ByCategories;
run;
/*cleanup*/
proc datasets library=work; delete n_0 - n_2; quit;

%mend;



