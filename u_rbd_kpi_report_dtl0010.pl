/opt/etl/prd/etl/APP/RBD/Z_RBD_KPI_REPORT_DTL_UAT/bin> cat u_rbd_kpi_report_dtl0010.pl
######################################################
#   $Header: /CVSROOT/smartone/Code/ETL/Template/b_JobName0010.pl,v 1.2 2005/11/16 08:35:06 MichaelNg Exp $
#   Purpose:
#
#
######################################################


use Date::Manip;
use DBI;
use Spreadsheet::WriteExcel;
use Math::BigFloat;

my $ETLVAR = $ENV{"AUTO_ETLVAR"};
my $ETLVAR = "/opt/etl/prd/etl/APP/RBD/Z_RBD_KPI_REPORT_DTL_UAT/bin/master_dev.pl";
require $ETLVAR;


my $MASTER_TABLE = ""; #Please input the final target ADW table name here

my $TDSVR,$TDUSR,$TDPWD,$TDDB,$TDTABLE,$ENV,$OUTPUT_FILE_PATH;
my $FILE_PREFIX, $FILE_DATE;
my $REPORT_DATE, $ABNORMAL_FOUND, $ABNORMAL_RATE;

my $dbh, $datastmt, $ret;


##################################################################################################################################
##################################################################################################################################
##################################################################################################################################



sub runSQLPLUS{
  my $SQLCMD_FILE="${etlvar::AUTO_GEN_TEMP_PATH}${etlvar::ETLJOBNAME}_sqlcmd.sql";
  open SQLCMD, ">" . $SQLCMD_FILE || die "Cannot open file" ;
  print SQLCMD<<ENDOFINPUT;
        ${etlvar::LOGON_TD}
        ${etlvar::SET_MAXERR}
        ${etlvar::SET_ERRLVL_1}
        ${etlvar::SET_ERRLVL_2}
        set define off;

--Please type your SQL statement here

---------------------------------------------------------------------------------------------------------------------------------------------

TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP;
TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001A_TMP;
TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_002A1_TMP;
TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_002A2_TMP;
TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_BOLT_TMP;
TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_003A1_TMP;
TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_003A2_TMP;
TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_003A1_TMP2;
TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004C1_TMP;
TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004C2_TMP;
TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004D1_TMP;
TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004D2_TMP;
TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004A1_TMP;
TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004A2_TMP;
TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_PEND_TMP;
TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_ADD_VAS_TMP;
TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_END_VAS_TMP;
TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_CD_TMP;
TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_BOLT_ON_TMP;


--Store code
INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP
(
  Store_Cd
  ,Daily_sales_sms
)
select distinct store_cd
--,coalesce(Daily_sales_sms,'N') 
,CASE WHEN Store_Cd = 'MCP' THEN 'Y' ELSE coalesce(Daily_sales_sms,'N') END --temp solution add closed store 'MCP' in Report (modified date: 2019-09-30) 
from (
SELECT
          Unit_Cd as Store_Cd
          ,Daily_sales_sms
FROM
        ${etlvar::ADWDB}.RBD_UNIT
where 
        (Store_Close_Date = TO_DATE('${etlvar::MINDATE}','YYYY-MM-DD')
        OR  Store_Close_Date > trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH'))
        union all (
                SELECT distinct pos_shop_cd as Store_Cd,
                c.DAILY_SALES_SMS
                from  ${etlvar::ADWDB}.pos_inv_header b
                left join ${etlvar::ADWDB}.rbd_unit c 
                on b.pos_shop_cd = c.unit_cd
                where
                b.inv_date >= trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')
                and b.inv_num not in (
                        select inv_num from ${etlvar::ADWDB}.pos_return_header where trx_date >= trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') 
                )
)
union all
select distinct pos_shop_cd, c.daily_sales_sms 
from 
${etlvar::ADWDB}.fes_usr_info b
left join ${etlvar::ADWDB}.rbd_unit c 
on b.pos_shop_cd = c.unit_cd
)
;

INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001A_TMP
(
  Store_Cd
  ,Rate_Plan_Grp
  ,Order_Cnt
)
select
        a.store_cd
        ,'01. True unlimited data plans'  Rate_Plan_Grp
        ,0
from
 ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP a
UNION
select
        a.store_cd
        ,'02. Unlimited data plans'
        ,0
from
 ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP a
UNION
select
        a.store_cd
        ,'03. High tier plans (5GB or above) with add-on pack'
        ,0
from
 ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP a
UNION
select
        a.store_cd
        ,'04. High tier plans (5GB or above) without add-on pack'
        ,0
from
 ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP a
UNION
select
        a.store_cd
        ,'05. Family Plan (Main SIM)'
        ,0
from
 ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP a
UNION
select
        a.store_cd
        ,'06. Family plans (Secondary SIM)'
        ,0
from
 ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP a
UNION
select
        a.store_cd
        ,'07. Mid tier plans (1.5GB - 4.9GB)'
        ,0
from
 ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP a
UNION
select
        a.store_cd
        ,'08. Upper low tier plans (500MB - 1.4GB)'
        ,0
from
 ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP a
UNION
select
        a.store_cd
        ,'09. Low tier plans (below 500MB)'
        ,0
from
 ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP a
UNION
select
        a.store_cd
        ,'10. ExtraCare plans'
        ,0
from
 ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP a
UNION
select
        a.store_cd
        ,'11. Speed capped unlimited data plans'
        ,0
from
 ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP a
UNION
select
        a.store_cd
        ,'12. Other plans'
        ,0
from
 ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP a
UNION
select
        a.store_cd
        ,'13. Fiber Broadband:100MB'
        ,0
from
 ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP a
UNION
select
        a.store_cd
        ,'14. Fiber Broadband:500MB'
        ,0
from
 ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP a
UNION
select
        a.store_cd
        ,'15. Fiber Broadband:1000MB'
        ,0
from
 ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP a
UNION
select
        a.store_cd
        ,'16. HomeTel'
        ,0
from
 ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP a
UNION
select
        a.store_cd
        ,'17. HPP'
        ,0
from
 ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP a
;

-------------------------------------------------------------------------------------------------------------------------------------

-- Activation by free_data_entitle
INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_002A1_TMP
(
  Cust_Num
  ,Subr_Num
  ,Store_Cd
  ,Rate_Plan_Cd
  ,Order_Num
  ,subr_sw_on_date
  ,subr_sw_off_date
)
SELECT
 a.Cust_Num
, a.Subr_Num
, b.dealer_name as Store_Cd
, a.rate_plan_cd as Rate_Plan_Cd
, a.Subr_Num as Order_Num
, c.subr_sw_on_date
, c.subr_sw_off_date
from (
select aa.*
,     row_number() over ( partition by cust_num, subr_num order by start_date, subr_sw_on_date, subr_sw_off_date)  rn
          from ${etlvar::ADWDB}.subr_info_hist aa
          ) a
, ${etlvar::ADWDB}.dealer_ref b
, ${etlvar::ADWDB}.subr_info_hist c
where a.subr_stat_cd in ('OK', 'PE')
and a.line_cat in ('L', 'B', 'F', 'H', 'M', 'R', 'X')
and substr(a.rate_plan_cd, 1, 4) <> 'ADON'
and substr(a.rate_plan_cd, 1, 3) <> 'AON'
and a.dealer_cd = b.dealer_cd
and substr(a.subr_num, 1, 3) not in ('448', '443')
and a.start_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
and (a.subr_sw_on_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
or   a.subr_sw_on_date = TO_DATE('${etlvar::MAXDATE}','YYYY-MM-DD'))
and a.cust_num = c.cust_num
and a.subr_num = c.subr_num
and TO_DATE('${etlvar::TXDATE}','YYYY-MM-DD') between c.start_date and c.end_date
and (TO_DATE('${etlvar::TXDATE}','YYYY-MM-DD') between c.subr_sw_on_date and c.subr_sw_off_date
or   c.subr_sw_on_date  = TO_DATE('${etlvar::MAXDATE}','YYYY-MM-DD'))
and c.subr_stat_cd in ('OK', 'PE')
and b.dealer_name in (select store_cd from  ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP)
and a.rn = 1
;

--PR list
INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_002A2_TMP
(
  Cust_Num
  ,Subr_Num
  ,Program_ID
  ,Status
)
SELECT
 a.Cust_Num
, a.Subr_Num
, a.Program_ID
, a.Status
from
(
select Cust_Num,Subr_Num,Program_ID,Status
from ${etlvar::ADWDB}.PRO_CASE_HIST
WHERE (substr(Program_ID,1,3) in ('PRR','PRS','PR0')
AND Status in ('AO', 'FF','CO'))
UNION
select Cust_Num,Subr_Num,Program_ID,Status
from ${etlvar::ADWDB}.PRO_CASE_QUEUE
WHERE (substr(Program_ID,1,3) in ('PRR','PRS','PR0')
AND Status in ('AO', 'FF','CO'))
)a
,${etlvar::ADWDB}.PRO_PROGRAM_INFO b
where a.Program_ID= b.Program_ID
AND b.end_date >= trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')
AND b.Start_date < to_date('${etlvar::TXDATE}','YYYY-MM-DD')
;


---------------------------------------------------------------------------------------------------------------------------------------------

--Invoice
INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_003A1_TMP
(
  Cust_Num
  ,Subr_Num
  ,Store_Cd
  ,Rate_Plan_Cd
  ,Order_Num
  ,inv_date             
  ,mkt_cd
  ,ref
  ,online_inv
  ,LD_revenue
  ,subr_stat_cd
  ,ld_cd
  ,eff_date
)
select 
  distinct
 Cust_Num
, Subr_Num
, x.Store_Cd
, Rate_Plan_Cd
, Order_Num
, inv_date
, mkt_cd
, 'Invoice'
, online_inv_flg
, ld_revenue
, subr_stat_cd
, ld_cd
, eff_date
from
(
        SELECT
          distinct
         b.Cust_Num
        , b.Subr_Num
        , b.pos_shop_cd as Store_Cd
        , d.rate_plan_cd as Rate_Plan_Cd
        , b.inv_num as Order_Num
        , b.inv_date
        , b.mkt_cd
        , 'Invoice'
        , b.online_inv_flg
        , d.subr_stat_cd 
        , c.ld_revenue
        , b.ld_cd
        , inv_date as eff_date
        from  ${etlvar::ADWDB}.pos_inv_header b
        , ${etlvar::ADWDB}.mkt_ref_vw c
        , ${etlvar::ADWDB}.subr_info_hist d
        where
                b.cust_num = d.cust_num
        and b.subr_num = d.subr_num
        and b.inv_date between d.start_date and d.end_date
        and b.mkt_cd = c.mkt_cd
        and d.subr_stat_cd in ('OK', 'PE')
        and c.ld_revenue in('P','V')
        and b.inv_num like 'I%'
        and b.inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
        and b.inv_num not in (select inv_num from ${etlvar::ADWDB}.pos_return_header where trx_date >= trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH'))
        and b.pos_shop_cd in (select store_cd from  ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP )
)x 
left join ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP y
on x.store_cd = y.store_cd
;

commit;

INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_003A1_TMP2
(
        CUST_NUM
        ,SUBR_NUM
)
select cust_num, subr_num from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_003A1_TMP
;

INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_003A1_TMP
(
  Cust_Num
  ,Subr_Num
  ,Store_Cd
  ,Rate_Plan_Cd
  ,Order_Num
  ,inv_date             
  ,mkt_cd
  ,ref
  ,online_inv
  ,LD_revenue
  ,subr_stat_cd
  ,ld_cd
  ,eff_date
)
select 
  distinct
 Cust_Num
, Subr_Num
, x.Store_Cd
, Rate_Plan_Cd
, Order_Num
, inv_date
, mkt_cd
, 'Invoice'
, online_inv_flg
, ld_revenue
, subr_stat_cd
, ld_cd
, eff_date
from
(
        SELECT
          distinct
         b.Cust_Num
        , b.Subr_Num
        , b.pos_shop_cd as Store_Cd
        , d.rate_plan_cd as Rate_Plan_Cd
        , b.inv_num as Order_Num
        , b.inv_date
        , b.mkt_cd
        , 'Invoice'
        , b.online_inv_flg
        , d.subr_stat_cd 
        , c.ld_revenue
        , b.ld_cd
        , inv_date as eff_date
        from  ${etlvar::ADWDB}.pos_inv_header b
        , ${etlvar::ADWDB}.mkt_ref_vw c
        , ${etlvar::ADWDB}.subr_info_hist d
        where
                b.cust_num = d.cust_num
        and b.subr_num = d.subr_num
        and b.inv_date between d.start_date and d.end_date
        and b.mkt_cd = c.mkt_cd
        and d.subr_stat_cd in ('OK', 'PE')
        and c.ld_revenue in('P','V')
        and b.inv_num like 'F%'
        and b.inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
        and b.inv_num not in (select inv_num from ${etlvar::ADWDB}.pos_return_header where trx_date >= trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH'))
        and b.pos_shop_cd in (select store_cd from  ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP )
        and (b.cust_num, b.subr_num) in (
        select cust_num, subr_num from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_002A1_TMP
        )
        and (b.cust_num, b.subr_num) not in (select cust_num, subr_num from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_003A1_TMP2)
)x 
left join ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP y
on x.store_cd = y.store_cd
;

commit;

TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_003A1_TMP2;
INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_003A1_TMP2
(
        CUST_NUM
        ,SUBR_NUM
)
select cust_num, subr_num from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_003A1_TMP
;

INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_003A1_TMP
(
  Cust_Num
  ,Subr_Num
  ,Store_Cd
  ,Rate_Plan_Cd
  ,Order_Num
  ,inv_date             
  ,mkt_cd
  ,ref
  ,online_inv
  ,LD_revenue
  ,subr_stat_cd
  ,ld_cd
  ,eff_date
)
select /*+ parallel(32) */
  distinct
 Cust_Num
, Subr_Num
, x.Store_Cd
, Rate_Plan_Cd
, Order_Num
, inv_date
, mkt_cd
, 'Invoice'
, online_inv_flg
, ld_revenue
, subr_stat_cd
, ld_cd
, eff_date
from
(
        SELECT  /*+ parallel(32) */
          distinct
         b.Cust_Num
        , b.Subr_Num
        , b.pos_shop_cd as Store_Cd
        , d.rate_plan_cd as Rate_Plan_Cd
        , b.inv_num as Order_Num
        , b.inv_date
        , b.mkt_cd
        , 'Invoice'
        , b.online_inv_flg
        , d.subr_stat_cd 
        , c.ld_revenue
        , b.ld_cd
        , inv_date as eff_date
        from  ${etlvar::ADWDB}.pos_inv_header b
        , ${etlvar::ADWDB}.mkt_ref_vw c
        , ${etlvar::ADWDB}.subr_info_hist d
        , ${etlvar::ADWDB}.prepd_postpaid_subr e
        where
                b.cust_num = d.cust_num
        and b.subr_num = d.subr_num
        and b.inv_date between d.start_date and d.end_date
        and b.mkt_cd = c.mkt_cd
        and d.subr_stat_cd in ('OK', 'PE')
        and c.ld_revenue not in('P','V')
        and e.line_cat ='L'
        and b.cust_num = e.cust_num
        and b.subr_num = e.subr_num
        and b.inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
        and b.inv_num not in (select inv_num from ${etlvar::ADWDB}.pos_return_header where trx_date >= trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH'))
        and b.pos_shop_cd in (select store_cd from  ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP )
        and (b.cust_num, b.subr_num) not in (select cust_num, subr_num from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_003A1_TMP2)
)x 
left join ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP y
on x.store_cd = y.store_cd
;

commit;

INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_003A2_TMP
(
  Cust_Num
  ,Subr_Num
  ,Store_Cd
  ,Rate_Plan_Cd
  ,Order_Num
  ,Orig_ld_cd
  ,New_ld_cd
  ,Orig_ld_exp_date
  ,New_ld_exp_date
  ,Orig_rate_plan_cd
  ,New_rate_plan_cd
  ,Trx_Type
  ,inv_date
  ,mkt_cd
  ,ref
  ,online_inv
  ,orig_ld_revenue
  ,new_ld_revenue
  ,subr_stat_cd
  ,eff_date
)
with A_pos_return_header as
(
select /*+MATERIALIZE */
           a.inv_num
from ${etlvar::ADWDB}.pos_return_header a
),
B_subr_info_hist as
(
select /*+MATERIALIZE */
*
from ${etlvar::ADWDB}.subr_info_hist h
),
C_subr_ld_hist as
(
select /*+MATERIALIZE */
*
from ${etlvar::ADWDB}.subr_ld_hist h1
left join ${etlvar::ADWDB}.mkt_ref_vw m
on h1.mkt_cd = m.mkt_cd
WHERE ld_revenue in('P','V')
and h1.inv_Num not in (select inv_num from A_pos_return_header)
)
select
/*+ parallel(32) */
  b.Cust_Num
  ,b.Subr_Num
  ,b.Store_Cd
  ,b.Rate_Plan_Cd
  ,b.Order_Num
  ,coalesce(c.Orig_ld_cd,'NA')
  ,coalesce(d.New_ld_cd,'NA')
  ,coalesce(c.Ld_Expired_Date,date'2999-12-31') as Orig_ld_exp_date
  ,coalesce(d.Ld_Expiry_Date,date'1900-01-01') as New_ld_exp_date
  ,coalesce(b.Orig_rate_plan_cd,'NA')
  ,coalesce(d.New_rate_plan_cd,'NA')
  ,'NA'
  ,b.inv_date
  ,d.mkt_cd
  ,d.ref
  ,d.online_inv
  ,coalesce(c.ld_revenue,' ')
  ,coalesce(b.ld_revenue,' ')
  ,b.subr_stat_cd
  ,d.eff_date
from (
                select t.Cust_Num,t.Subr_Num,t.Store_Cd,t.Rate_Plan_Cd,t.Order_Num,t.inv_date,
                                h.Rate_Plan_Cd as Orig_rate_plan_cd,t.Rate_Plan_Cd as New_rate_plan_cd,t.mkt_cd,t.ref, t.subr_stat_cd, t.ld_revenue
                from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_003A1_TMP t
                left outer join
                B_subr_info_hist h
                on t.Subr_Num=h.Subr_Num
                and t.cust_Num=h.cust_Num
                and t.inv_date -1 between h.start_date and h.end_date
                and h.subr_stat_cd in ('OK','PE')
         ) b
left outer join (
                select * from (
                select t1.Cust_Num,t1.Subr_Num,t1.Order_Num,h1.Ld_Cd as Orig_ld_cd,h1.Ld_Expired_Date,t1.mkt_cd,t1.ref, h1.ld_revenue,
                row_number() over(partition by h1.cust_num,h1.subr_num,t1.order_num order by h1.Ld_Expired_Date desc) as rn
                from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_003A1_TMP t1
                left outer join
                C_subr_ld_hist h1
                on t1.Subr_Num=h1.Subr_Num
                and t1.Cust_Num=h1.Cust_Num
                and h1.void_flg <> 'Y'
                and h1.waived_flg <> 'Y'
                and t1.inv_date -1 between h1.start_date and h1.end_date
                )
                where rn = 1
) c
on b.Cust_Num=c.Cust_Num
and b.Subr_Num=c.Subr_Num
and b.Order_Num=c.Order_Num
and b.ld_revenue = c.ld_revenue
left outer join (
                select * from (
                        select * from ( 
                                select h2.Cust_Num,h2.Subr_Num,h2.inv_Num as Order_Num,
                                case when h2.ld_cd <> t2.ld_cd and t2.ld_cd <> ' '
                                then t2.ld_cd
                                else h2.Ld_Cd 
                                end as New_ld_cd,
                                h2.Ld_Expiry_Date,h2.rate_plan_cd as New_rate_plan_cd,t2.mkt_cd,t2.ref,t2.online_inv
                                ,coalesce(t2.eff_date,h2.inv_date) as eff_date
                                ,row_number() over(partition by h2.cust_num,h2.subr_num,t2.order_num order by h2.Ld_Expiry_Date,h2.Ld_Cd desc ) as rn
                                from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_003A1_TMP t2
                                left outer join
                                ${etlvar::ADWDB}.pos_inv_header h2
                                on t2.Subr_Num=h2.Subr_Num
                                and t2.Cust_Num=h2.Cust_Num
                                and t2.order_Num=h2.inv_Num
                        ) x
                ) z where z.rn = 1
) d
on b.Cust_Num=d.Cust_Num
and b.Subr_Num=d.Subr_Num
and b.Order_Num=d.Order_Num
;

--Temp table for grouping om orders
INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_PEND_TMP
(
  Cust_num
  ,Subr_num
  ,Create_Date
  ,Create_By
  ,Rate_Plan_Cd
  ,Order_num
  ,New_ld_cd
  ,mkt_cd
  ,add_vas
  ,eff_date
)
select Cust_Num,Subr_Num,create_date, Create_By,coalesce(new_plan_cd,' ') as new_plan_cd,Order_No,Ld_Cd,
mkt_cd, coalesce(add_vas,' '),eff_date
from (
SELECT  Cust_Num,Subr_Num,create_date, Create_By,new_plan_cd,Order_No,Ld_Cd,mkt_cd, add_vas,eff_date
,row_number() over(partition by cust_num,subr_num,order_no,ld_cd order by b_create_date desc, b_create_time desc) as rn
FROM (
        select a.Cust_Num,a.Subr_Num, a.mkt_cd,
        a.create_date, 
        a.Create_By
        ,
        case 
    when a.create_date > b.create_date
        then a.new_plan_cd
    when a.create_date = b.create_date
        then 
            case when a.create_time > b.create_time
            then 
                case when a.new_plan_cd <> b.new_plan_cd
                and (b.new_plan_cd is not null and b.new_plan_cd <> ' ')
                then b.new_plan_cd
                when a.new_plan_cd <> b.new_plan_cd
                and (a.new_plan_cd is not null and a.new_plan_cd <> ' ')
                then a.new_plan_cd
                else a.new_plan_cd
                end      
            when a.create_time = b.create_time
                then 
                    case when a.new_plan_cd <> b.new_plan_cd
                    and (b.new_plan_cd is not null and b.new_plan_cd <> ' ')
                    then b.new_plan_cd
                    when a.new_plan_cd <> b.new_plan_cd
                    and (a.new_plan_cd is not null and a.new_plan_cd <> ' ')
                    then a.new_plan_cd
                    else a.new_plan_cd
                    end                            
            else b.new_plan_cd
            end
    else b.new_plan_cd
    end as new_plan_cd
        ,a.Order_No,
        a.Ld_Cd,
        case 
    when a.create_date > b.create_date
        then a.add_vas
    when a.create_date = b.create_date
        then 
            case when a.create_time > b.create_time
            then 
                case when a.add_vas <> b.add_vas
                and (b.add_vas is not null and b.add_vas <> ' ')
                then b.add_vas
                when a.add_vas <> b.add_vas
                and (a.add_vas is not null and a.add_vas <> ' ')
                then a.add_vas
                else a.add_vas
                end      
            when a.create_time = b.create_time
                then 
                    case when a.add_vas <> b.add_vas
                    and (b.add_vas is not null and b.add_vas <> ' ')
                    then b.add_vas
                    when a.add_vas <> b.add_vas
                    and (a.add_vas is not null and a.add_vas <> ' ')
                    then a.add_vas
                    else a.add_vas
                    end                            
            else b.add_vas
            end
    else b.add_vas
    end as add_vas
        ,a.create_time 
        ,b.create_date as b_create_date
        ,b.create_time as b_create_time
        ,case when b.eff_date is null
        then a.eff_date
        when a.eff_date > b.eff_date
        then a.eff_date
        else b.eff_date
        end as eff_date
        from (
        --Select add-ld order
        select Cust_Num,Subr_Num , Create_By,Mkt_Cd,Case_Id as Order_No,create_date,new_plan_cd,Ld_Cd,create_time,add_vas,eff_date
        FROM ${etlvar::ADWDB}.OM_PENDING_CHG_PLAN a
        where ld_cd <> ' '
        and create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
        union  all
        select Cust_Num,Subr_Num,Create_By,Mkt_Cd,Case_Id as Order_No,create_date,new_plan_cd,Ld_Cd,create_time,add_vas,eff_date
        FROM
        ${etlvar::ADWDB}.OM_COMPLETE_CHG_PLAN b
        where ld_cd <> ' '
        and create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
        ) a 
        left join 
        (
        --Self join to get latest new plan cd
        select Cust_Num,Subr_Num , Create_By,Mkt_Cd,Case_Id as Order_No,create_date,new_plan_cd,Ld_Cd,create_time,add_vas,eff_date
        FROM ${etlvar::ADWDB}.OM_PENDING_CHG_PLAN a
        where new_plan_cd <> ' '
        and create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
        union  all
        select Cust_Num,Subr_Num,Create_By,Mkt_Cd,Case_Id as Order_No,create_date,new_plan_cd,Ld_Cd,create_time,add_vas,eff_date
        FROM
        ${etlvar::ADWDB}.OM_COMPLETE_CHG_PLAN b
        where new_plan_cd <> ' '
        and create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
        ) b
        on a.cust_num = b.cust_num
        and a.subr_num = b.subr_num
        and a.create_date = b.create_date
)
)where 
rn = 1 and 
ld_cd <> ' ';



INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004C1_TMP
(
  Cust_Num
  ,Subr_Num
  ,Store_Cd
  ,Rate_Plan_Cd
  ,order_num
  ,Create_date
  ,Ld_Cd                  
  ,mkt_cd
  ,ref
  ,subr_stat_cd
  ,ld_revenue
  ,add_vas
  ,eff_date
)
select  
  z.Cust_Num, z.Subr_Num, z.Store_Cd  as Store_Cd, z.rate_plan_cd, z.Order_Num, z.Create_date, 
  case when z.Ld_Cd is null
  then ' '
  else z.Ld_Cd
  end Ld_cd, z.mkt_cd, 'Add ld - OM Order'
  ,subr_stat_cd
  ,ld_revenue
  ,add_vas
  ,eff_date
  from (
SELECT
  a.Cust_Num
, a.Subr_Num
, c.pos_shop_cd  as Store_Cd
, CASE WHEN nvl(a.Rate_Plan_Cd,' ') <> ' ' THEN a.Rate_Plan_Cd
  ELSE b.rate_plan_cd
  END Rate_Plan_Cd
, a.order_no as Order_Num
, a.Create_date
, a.Ld_Cd
, a.mkt_cd
, b.subr_stat_cd
, d.ld_revenue
, a.add_vas
, a.eff_date
, row_number() over(partition by a.cust_num,a.subr_num,a.order_no order by a.ld_cd desc ) as rn
from
(
select a.Cust_Num as Cust_Num
                ,a.Subr_Num as Subr_Num
                ,a.Create_By as Create_By
                ,a.mkt_cd
                ,coalesce(t.Order_Num,a.case_id) as order_no
                ,a.create_date as create_date
                ,case when (t.Rate_Plan_Cd is not null and t.Rate_Plan_Cd <> ' ')
                then t.Rate_Plan_Cd
                else a.new_plan_cd
                end     as Rate_Plan_Cd
                ,case when (t.Rate_Plan_Cd is not null and t.Rate_Plan_Cd <> ' ')
                then t.New_ld_cd
                else a.ld_cd
                end as Ld_Cd
                ,case when (t.add_vas is not null and t.add_vas <> ' ')
                then t.add_vas
                else a.add_vas
                end as add_vas
                ,coalesce(t.eff_date,a.eff_date) as eff_date
FROM
${etlvar::ADWDB}.OM_PENDING_CHG_PLAN a
left outer join ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_PEND_TMP t
on a.Cust_num=t.Cust_num
and a.Subr_num=t.Subr_num
and a.Create_Date=t.Create_Date
and a.Create_By=t.Create_By
UNION
select b.Cust_Num as Cust_Num
                ,b.Subr_Num as Subr_Num
                ,b.Create_By as Create_By
                ,b.mkt_cd
                ,coalesce(t.Order_Num,b.case_id) as order_no
                ,b.create_date as create_date
                ,case when (t.Rate_Plan_Cd is not null and t.Rate_Plan_Cd <> ' ')
                then t.Rate_Plan_Cd
                else b.new_plan_cd
                end     as Rate_Plan_Cd
                ,case when (t.New_ld_cd is not null and t.New_ld_cd <> ' ')
                then t.New_ld_cd
                else b.ld_cd
                end as Ld_Cd
                ,case when (t.add_vas is not null and t.add_vas <> ' ')
                then t.add_vas
                else b.add_vas
                end as add_vas
                ,coalesce(t.eff_date,b.eff_date) as eff_date
FROM
${etlvar::ADWDB}.OM_COMPLETE_CHG_PLAN b
left outer join ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_PEND_TMP t
on b.Cust_num=t.Cust_num
and b.Subr_num=t.Subr_num
and b.Create_Date=t.Create_Date
and b.Create_By=t.Create_By
) a
, ${etlvar::ADWDB}.subr_info_hist b
, ${etlvar::ADWDB}.fes_usr_info c
, ${etlvar::ADWDB}.mkt_ref_vw d
where a.cust_num = b.cust_num
and a.subr_num = b.subr_num
and a.create_by = c.usr_name
and a.mkt_cd = d.mkt_cd
and b.subr_stat_cd in ('OK', 'PE')
and a.create_date between b.start_date and b.end_date
and a.create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
and (d.ld_revenue = 'P' or d.ld_revenue = 'V')
and c.pos_shop_cd in
(
select store_cd
from  ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP
)
and (a.cust_num, a.subr_num) not in (select cust_num, subr_num from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_003A1_TMP)
) z 
where rn = 1
and (cust_num, subr_num) not in (
        select /* PARALLEL(32) */ cust_num, subr_num from ${etlvar::ADWDB}.subr_info_hist
        where subr_stat_cd in ('SU','TX')
        and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD') between start_Date and end_Date
)
;
commit;

INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004C2_TMP
(
  Cust_Num
  ,Subr_Num
  ,Store_Cd
  ,Rate_Plan_Cd
  ,order_num
  ,Orig_ld_cd
  ,New_ld_cd
  ,Orig_ld_exp_date
  ,New_ld_exp_date
  ,Orig_rate_plan_cd
  ,New_rate_plan_cd
  ,Trx_Type
  ,Create_date
  ,Ld_Cd
  ,mkt_cd
  ,ref
  ,subr_stat_cd
  ,orig_ld_revenue
  ,new_ld_revenue
  ,add_vas
  ,eff_date
)
with
 A_pos_return_header as
(
select /*+MATERIALIZE */
           a.inv_num
from ${etlvar::ADWDB}.pos_return_header a
),
 B_subr_info_hist as
(
select /*+MATERIALIZE */
*
from ${etlvar::ADWDB}.subr_info_hist h
),
 C_subr_ld_hist as
(
select /*+MATERIALIZE */
*
from ${etlvar::ADWDB}.subr_ld_hist h1
left join ${etlvar::ADWDB}.mkt_ref_vw m
on h1.mkt_cd = m.mkt_cd
WHERE ld_revenue in('P','V')
and h1.inv_Num not in (select inv_num from A_pos_return_header)
)
select
/*+ parallel(32) */
  b.Cust_Num
  ,b.Subr_Num
  ,b.Store_Cd
  ,b.Rate_Plan_Cd
  ,b.order_num
  ,coalesce(c.Orig_ld_cd,'NA')
  ,coalesce(b.New_ld_cd,'NA')
  ,coalesce(c.Orig_ld_exp_date,date'2999-12-31')
  ,date'1900-01-01'
  ,coalesce(b.Orig_rate_plan_cd,'NA')
  ,coalesce(b.New_rate_plan_cd,'NA')
  ,'NA'
  ,b.Create_date
  ,b.New_ld_cd
  ,b.mkt_cd
  ,b.ref
  ,b.subr_stat_cd
  ,coalesce(c.ld_revenue,' ')
  ,coalesce(b.ld_revenue,' ')
  ,add_vas
  ,b.eff_date
from (
                select t.Cust_Num,t.Subr_Num,t.Store_Cd,t.Rate_Plan_Cd,t.order_num,t.Ld_Cd as New_ld_cd,t.Create_date, t.add_vas,
                                h.Rate_Plan_Cd as Orig_rate_plan_cd,t.Rate_Plan_Cd as New_rate_plan_cd,t.mkt_cd,t.ref, t.subr_stat_cd, t.ld_revenue
                                ,t.eff_date
                from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004C1_TMP t
                left outer join
                B_subr_info_hist h
                on t.Subr_Num=h.Subr_Num
                and t.Cust_Num=h.Cust_Num
                and t.Create_date -1 between h.start_date and h.end_date
                and h.subr_stat_cd in ('OK','PE')
         ) b
left outer join (
                select * from (
                select t1.Cust_Num,t1.Subr_Num,t1.Order_Num,h1.Ld_Cd as Orig_ld_cd,h1.Ld_Expired_Date as Orig_ld_exp_date,t1.mkt_cd,t1.ref,h1.ld_revenue,
                row_number() over(partition by h1.cust_num,h1.subr_num,t1.order_num order by h1.Ld_Expired_Date desc ) as rn
                from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004C1_TMP t1
                left outer join
                C_subr_ld_hist h1
                on t1.Subr_Num=h1.Subr_Num
                and t1.Cust_Num=h1.Cust_Num
                and t1.create_date -1 between h1.start_date and h1.end_date
                and h1.waived_flg <> 'Y'
                and h1.void_flg <> 'Y'
                )
                where rn = 1
) c
on b.Cust_Num=c.Cust_Num
and b.Subr_Num=c.Subr_Num
and b.Order_Num=c.Order_Num
and b.ld_revenue = c.ld_revenue
left outer join (
                select * from (
                        select * from ( 
                                select h2.Cust_Num,h2.Subr_Num,h2.inv_Num as Order_Num,h2.Ld_Cd as New_ld_cd,
                                h2.Ld_Expiry_Date,h2.rate_plan_cd as New_rate_plan_cd,
                                row_number() over(partition by h2.cust_num,h2.subr_num,t2.order_num order by h2.Ld_Expiry_Date,h2.Ld_Cd desc ) as rn
                                from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_PEND_TMP t2
                                left outer join
                                ${etlvar::ADWDB}.pos_inv_header h2
                                on t2.Subr_Num=h2.Subr_Num
                                and t2.Cust_Num=h2.Cust_Num
                        ) x
                ) z where z.rn = 1
) d
on b.Cust_Num=d.Cust_Num
and b.Subr_Num=d.Subr_Num
and b.Order_Num=d.Order_Num
;
commit;


-- FL Order
INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004D1_TMP
(
  Cust_Num
  ,Subr_Num
  ,Store_Cd
  ,Rate_Plan_Cd
  ,order_num
  ,Create_date                  
  ,Ld_Cd                        
  ,mkt_cd
  ,ref
  ,subr_stat_cd
  ,ld_revenue
  ,eff_date
)
SELECT
  a.Cust_Num
, a.Subr_Num
, c.pos_shop_cd  as Store_Cd
, b.rate_plan_cd as Rate_Plan_Cd
, a.order_no as Order_Num
, a.Create_date                 
, a.Ld_Cd                       
, a.mkt_cd
, 'Add ld - FL Order'
, b.subr_stat_cd
, d.ld_revenue
, a.eff_date
from
(
select Cust_Num,Subr_Num,Create_By,Mkt_Cd,Case_Id as Order_No,create_date,Ld_Cd,eff_date
FROM
${etlvar::ADWDB}.FL_OM_PENDING_CHG_PLAN a
UNION
select Cust_Num,Subr_Num,Create_By,Mkt_Cd,Case_Id as Order_No,create_date,Ld_Cd,eff_date
FROM
${etlvar::ADWDB}.FL_OM_COMPLETE_CHG_PLAN b
) a
, ${etlvar::ADWDB}.subr_info_hist b
, ${etlvar::ADWDB}.fes_usr_info c
, ${etlvar::ADWDB}.mkt_ref_vw d
where a.cust_num = b.cust_num
and a.subr_num = b.subr_num
and a.create_by = c.usr_name
and a.mkt_cd = d.mkt_cd
and b.subr_stat_cd in ('OK', 'PE')
and a.create_date between b.start_date and b.end_date
and a.create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
and (d.ld_revenue = 'P' or d.ld_revenue = 'V')
and c.pos_shop_cd in (select store_cd from  ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP)
and (a.cust_num, a.subr_num) not in (select cust_num, subr_num from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_003A1_TMP )
and (a.cust_num, a.subr_num) not in (select cust_num, subr_num from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004C1_TMP )
and (a.cust_num, a.subr_num) not in (
        select /* PARALLEL(32) */ cust_num, subr_num from ${etlvar::ADWDB}.subr_info_hist
        where subr_stat_cd in ('SU','TX')
        and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD') between start_Date and end_Date
)
;


INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004D2_TMP
(
  Cust_Num
  ,Subr_Num
  ,Store_Cd
  ,Rate_Plan_Cd
  ,order_num
  ,Orig_ld_cd
  ,New_ld_cd
  ,Orig_ld_exp_date
  ,New_ld_exp_date
  ,Orig_rate_plan_cd
  ,New_rate_plan_cd
  ,Trx_Type
  ,Create_date
  ,Ld_Cd
  ,mkt_cd
  ,ref
  ,subr_stat_cd
  ,orig_ld_revenue
  ,new_ld_revenue
  ,eff_date
)
with A_pos_return_header as
(
select /*+MATERIALIZE */
           a.inv_num
from ${etlvar::ADWDB}.pos_return_header a
),
B_subr_info_hist as
(
select /*+MATERIALIZE */
*
from ${etlvar::ADWDB}.subr_info_hist h
),
C_subr_ld_hist as
(
select /*+MATERIALIZE */
*
from ${etlvar::ADWDB}.subr_ld_hist h1
left join ${etlvar::ADWDB}.mkt_ref_vw m
on h1.mkt_cd = m.mkt_cd
WHERE ld_revenue in('P','V')
and h1.inv_Num not in (select inv_num from A_pos_return_header)
)
select
  /*+ parallel(32) */
  b.Cust_Num
  ,b.Subr_Num
  ,b.Store_Cd
  ,b.Rate_Plan_Cd
  ,b.order_num
  ,coalesce(c.Orig_ld_cd,'NA')
  ,coalesce(b.New_ld_cd,'NA')
  ,coalesce(c.Orig_ld_exp_date,date'2999-12-31')
  ,date'1900-01-01'
  ,coalesce(b.Orig_rate_plan_cd,'NA')
  ,coalesce(b.New_rate_plan_cd,'NA')
  ,'NA'
  ,b.Create_date
  ,b.New_ld_cd
  ,b.mkt_cd
  ,b.ref
  ,b.subr_stat_cd
  ,coalesce(b.ld_revenue,' ')
  ,coalesce(c.ld_revenue,' ')
  ,b.eff_date
from (
                select t.Cust_Num,t.Subr_Num,t.Store_Cd,t.Rate_Plan_Cd,t.order_num,t.Ld_Cd as New_ld_cd,t.Create_date,
                                h.Rate_Plan_Cd as Orig_rate_plan_cd,t.Rate_Plan_Cd as New_rate_plan_cd,t.mkt_cd,t.ref, t.subr_stat_cd, t.ld_revenue
                                ,t.eff_date
                from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004D1_TMP t
                left outer join
                B_subr_info_hist h
                on t.Subr_Num=h.Subr_Num
                and t.Cust_Num=h.Cust_Num
                and t.Create_date -1 between h.start_date and h.end_date
                and h.subr_stat_cd in ('OK','PE')
         ) b
left outer join (
                select * from (
                select t1.Cust_Num,t1.Subr_Num,t1.Order_Num,h1.Ld_Cd as Orig_ld_cd,h1.Ld_Expired_Date as Orig_ld_exp_date,t1.mkt_cd,t1.ref,h1.ld_revenue,
                row_number() over(partition by h1.cust_num,h1.subr_num,t1.order_num order by h1.Ld_Expired_Date desc ) as rn
                from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004D1_TMP t1
                left outer join
                C_subr_ld_hist h1
                on t1.Subr_Num=h1.Subr_Num
                and t1.Cust_Num=h1.Cust_Num
                and h1.void_flg <> 'Y'
                and h1.waived_flg <> 'Y'
                and t1.create_date -1 between h1.start_date and h1.end_date
                )
                where rn = 1
) c
on b.Cust_Num=c.Cust_Num
and b.Subr_Num=c.Subr_Num
and b.Order_Num=c.Order_Num
and b.ld_revenue = c.ld_revenue
;

commit;

INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004A1_TMP
(
  Cust_Num
  ,Subr_Num
  ,Store_Cd
  ,Rate_Plan_Cd
  ,Order_Num
  ,Create_date                  
  ,ld_cd                        
  ,new_plan_cd                  
  ,old_plan_cd                  
  ,mkt_cd                       
  ,ref
  ,ld_revenue
  ,subr_stat_cd
  ,add_vas
  ,eff_date
)
select distinct 
CUST_NUM, SUBR_NUM, STORE_CD, rate_plan_cd, 
ORDER_NUM, CREATE_DATE, LD_CD, NEW_PLAN_CD, old_plan_cd, mkt_cd, 'Chg plan only'
,' ' as ld_revenue, subr_stat_cd,add_vas,eff_date
from (
select
distinct
x.cust_num,x.subr_num, c.pos_shop_cd as store_cd, 
x.rate_plan_cd as new_plan_cd,
x.Order_num,
x.create_date,
x.ld_cd,
x.rate_plan_cd as rate_plan_cd,
z.rate_plan_cd as old_plan_cd,
x.mkt_cd,
z.subr_stat_cd,
add_vas,eff_date
,row_number() over(partition by x.cust_num,x.subr_num,x.order_num order by b_create_date desc, b_create_time desc) as rn
from ( 
        SELECT distinct
          a.Cust_Num
        , a.Subr_Num
        ,
        case 
    when a.create_date > b.create_date
        then a.new_plan_cd
    when a.create_date = b.create_date
        then 
            case when a.create_time > b.create_time
            then 
                case when a.new_plan_cd <> b.new_plan_cd
                and (b.new_plan_cd is not null and b.new_plan_cd <> ' ')
                then b.new_plan_cd
                when a.new_plan_cd <> b.new_plan_cd
                and (a.new_plan_cd is not null and a.new_plan_cd <> ' ')
                then a.new_plan_cd
                else a.new_plan_cd
                end      
            when a.create_time = b.create_time
                then 
                    case when a.new_plan_cd <> b.new_plan_cd
                    and (b.new_plan_cd is not null and b.new_plan_cd <> ' ')
                    then b.new_plan_cd
                    when a.new_plan_cd <> b.new_plan_cd
                    and (a.new_plan_cd is not null and a.new_plan_cd <> ' ')
                    then a.new_plan_cd
                    else a.new_plan_cd
                    end                            
            else b.new_plan_cd
            end
    else b.new_plan_cd
    end as rate_plan_cd,
        case 
    when a.create_date > b.create_date
        then a.add_vas
    when a.create_date = b.create_date
        then 
            case when a.create_time > b.create_time
            then 
                case when a.add_vas <> b.add_vas
                and (b.add_vas is not null and b.add_vas <> ' ')
                then b.add_vas
                when a.add_vas <> b.add_vas
                and (a.add_vas is not null and a.add_vas <> ' ')
                then a.add_vas
                else a.add_vas
                end      
            when a.create_time = b.create_time
                then 
                    case when a.add_vas <> b.add_vas
                    and (b.add_vas is not null and b.add_vas <> ' ')
                    then b.add_vas
                    when a.add_vas <> b.add_vas
                    and (a.add_vas is not null and a.add_vas <> ' ')
                    then a.add_vas
                    else a.add_vas
                    end                            
            else b.add_vas
            end
    else b.add_vas
    end as add_vas 
        , a.order_no as Order_Num            
        , a.Ld_Cd       
        , a.new_plan_cd
        , a.mkt_cd
        , 'Chg plan only'
        , a.create_date
        ,b.create_date as b_create_date
        ,a.create_time 
        ,a.create_by
        ,b.create_time as b_create_time
        ,case when a.eff_date > b.eff_date
        then a.eff_date
        else b.eff_date
        end as eff_date
        from (
        select a.Cust_Num,a.Subr_Num,a.Create_By,a.Mkt_Cd,a.Case_Id as Order_No,a.create_date,a.new_plan_cd,a.Ld_Cd,a.create_time,add_vas,eff_date
        FROM
        ${etlvar::ADWDB}.OM_PENDING_CHG_PLAN a
        where create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
        AND new_plan_cd <> ' '
        UNION
        select b.Cust_Num,b.Subr_Num,b.Create_By,b.Mkt_Cd,b.Case_Id as Order_No,b.create_date,b.new_plan_cd,b.Ld_Cd,b.create_time,add_vas,eff_date
        FROM
        ${etlvar::ADWDB}.OM_COMPLETE_CHG_PLAN b
        where create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
        AND new_plan_cd <> ' '
        ) a
        left join (
        select a.Cust_Num,a.Subr_Num,a.Create_By,a.Mkt_Cd,a.Case_Id as Order_No,a.create_date,a.new_plan_cd,a.Ld_Cd,a.create_time,add_vas,eff_date
        FROM
        ${etlvar::ADWDB}.OM_PENDING_CHG_PLAN a
        where create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
        AND new_plan_cd <> ' '
        UNION
        select b.Cust_Num,b.Subr_Num,b.Create_By,b.Mkt_Cd,b.Case_Id as Order_No,b.create_date,b.new_plan_cd,b.Ld_Cd,b.create_time,add_vas,eff_date
        FROM
        ${etlvar::ADWDB}.OM_COMPLETE_CHG_PLAN b
        where create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
        AND new_plan_cd <> ' '
        ) b
        on a.cust_num = b.cust_num
        and a.subr_num = b.subr_num
        and a.create_date = b.create_date
        ) x
        , ${etlvar::ADWDB}.fes_usr_info c
        , ${etlvar::ADWDB}.subr_info_hist z
        where x.cust_num = z.cust_num
        and x.subr_num = z.subr_num
        and x.create_date - 1 between z.start_date and z.end_date
        and x.new_plan_cd <> z.rate_plan_cd
        and z.subr_stat_cd = 'OK'
        and z.subr_sw_on_date <> TO_DATE('${etlvar::MAXDATE}','YYYY-MM-DD')
        and x.create_by = c.usr_name
        and c.pos_shop_cd in (select store_cd from  ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP)
        and (x.cust_num, x.subr_num) not in (select cust_num, subr_num from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_002A1_TMP )
        and (x.cust_num, x.subr_num) not in (select cust_num, subr_num from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_003A1_TMP )
        and (x.cust_num, x.subr_num) not in (select cust_num, subr_num from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004C1_TMP )
        and (x.cust_num, x.subr_num) not in (select cust_num, subr_num from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004D1_TMP )
) 
where rn = 1
and (cust_num, subr_num) not in (
        select /* PARALLEL(32) */ cust_num, subr_num from ${etlvar::ADWDB}.subr_info_hist
        where subr_stat_cd in ('SU','TX')
        and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD') between start_Date and end_Date
)
;
commit;


INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004A2_TMP
(
        Cust_Num
        ,Subr_Num
        ,Store_Cd
        ,Rate_Plan_Cd
        ,Order_Num
        ,Orig_ld_cd
        ,New_ld_cd
        ,Orig_ld_exp_date
        ,New_ld_exp_date
        ,Orig_rate_plan_cd
        ,New_rate_plan_cd
        ,Trx_Type
        ,create_date
        ,mkt_cd
        ,ref
        ,ld_revenue
        ,subr_stat_cd
        ,add_vas
        ,eff_date
)
with A_pos_return_header as
(
select /*+MATERIALIZE */
           a.inv_num
from ${etlvar::ADWDB}.pos_return_header a
),
B_subr_ld_hist as
(
select /*+MATERIALIZE */
*
from ${etlvar::ADWDB}.subr_ld_hist h
where h.mkt_cd in (select mkt_cd from ${etlvar::ADWDB}.mkt_ref_vw where ld_revenue in ('P','V'))
and h.inv_num not in (select inv_num from ${etlvar::ADWDB}.pos_return_header)
)
select
/*+ parallel(32) */
                a.Cust_Num
                ,a.Subr_Num
                ,a.Store_Cd
                ,a.Rate_Plan_Cd
                ,a.Order_Num
                ,coalesce(Orig_ld_cd,'NA')
                ,coalesce(New_ld_cd,'NA')
                ,coalesce(Orig_ld_exp_date,date'2999-12-31')
                ,coalesce(New_ld_exp_date,date'1900-01-01')
                ,a.old_plan_cd
                ,a.Rate_Plan_Cd
                ,coalesce(b.Trx_Type,'NA')
                ,a.create_date
                ,a.mkt_cd
                ,a.ref
                ,a.ld_revenue
                ,a.subr_stat_cd
                ,a.add_vas
                ,a.eff_date
from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004A1_TMP a
left outer join (
select
                t.Cust_Num
                ,t.Subr_Num
                ,t.Store_Cd
                ,t.Rate_Plan_Cd
                ,t.Order_Num
                ,coalesce(h.Ld_Cd,'NA') as Orig_ld_cd
                ,coalesce(t.ld_cd,'NA') as New_ld_cd
                ,coalesce(h.Ld_Expired_Date,date'2999-12-31') as Orig_ld_exp_date
                ,date'1900-01-01' as New_ld_exp_date
                ,t.old_plan_cd
                ,t.new_plan_cd
                ,'NA' as Trx_Type
                ,row_number() over(partition by h.cust_num,h.subr_num,t.order_num order by h.Ld_Expired_Date desc,t.ld_cd desc ) rn
                ,t.mkt_cd
                ,t.ref
                ,t.ld_revenue
                ,t.subr_stat_cd
from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004A1_TMP t
left outer join (
    select b.rate_plan_cd, a.* 
    from B_subr_ld_hist a
    left join ${etlvar::ADWDB}.pos_inv_header b
    on a.inv_num = b.inv_num
) h
on t.Subr_Num=h.Subr_Num
and t.cust_num=h.cust_num
where t.Create_date-1 between h.start_date and h.end_date
and h.void_flg <> 'Y'
and h.waived_flg <> 'Y'
) b
on a.Subr_Num=b.Subr_Num
and a.cust_Num=b.cust_Num
and a.Store_Cd=b.Store_Cd
where rn = 1
;
commit;

INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_ADD_VAS_TMP
(
CUST_NUM 
,SUBR_NUM 
,STORE_CD 
,ORDER_NUM
,ADD_VAS  
)
select /*+ parallel(32) */ distinct
cust_num, subr_num, store_cd, order_num, 
replace(COALESCE(listagg(ADD_VAS,',') within group (order by cust_num, subr_num, store_cd, order_num),' '),' ,','') as ADD_VAS
from (
        select /*+ parallel(32) */ distinct
        CUST_NUM, SUBR_NUM, STORE_CD,ORDER_NUM, ADD_VAS
        from (
                select /*+ parallel(32) */ distinct
                x.*
                from (
                        select /*+ parallel(32) */ distinct
                        a.*, b.add_vas, b.end_vas
                        from (
                                select /*+ parallel(32) */ 
                                cust_num, subr_num, store_cd, rate_plan_cd, order_num, inv_date, mkt_cd, ref 
                                from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_003A1_TMP
                                union
                                select /*+ parallel(32) */ 
                                cust_num, subr_num, store_cd, rate_plan_cd, order_num, create_date, mkt_cd, ref 
                                from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004C1_TMP
                                union
                                select /*+ parallel(32) */ 
                                cust_num, subr_num, store_cd, rate_plan_cd, order_num, create_date, mkt_cd, ref 
                                from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004D1_TMP
                                union
                                select /*+ parallel(32) */ 
                                cust_num, subr_num, store_cd, rate_plan_cd, order_num, create_date, mkt_cd, ref 
                                from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004A1_TMP
                        ) a
                        left join (
                                select /*+ parallel(32) */ 
                                cust_num, subr_num, case_id as order_num, add_vas, end_vas, create_date
                                from ${etlvar::ADWDB}.om_complete_chg_plan
                                where create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') 
                                and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                union all
                                select /*+ parallel(32) */ 
                                cust_num, subr_num, case_id as order_num, add_vas, end_vas, create_date
                                from ${etlvar::ADWDB}.om_pending_chg_plan
                                where create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') 
                                and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                union all
                                select /*+ parallel(32) */ 
                                cust_num, subr_num, case_id as order_num, ' '  as add_vas, ' ' as end_vas, create_date
                                from ${etlvar::ADWDB}.fl_om_complete_chg_plan
                                where create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') 
                                and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                union all
                                select /*+ parallel(32) */ 
                                cust_num, subr_num, case_id as order_num, ' '  as add_vas, ' ' as end_vas, create_date
                                from ${etlvar::ADWDB}.fl_om_pending_chg_plan
                                where create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') 
                                and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                union all
                                select /*+ parallel(32) */ 
                                cust_num, subr_num, inv_num as order_num, ' '  as add_vas, ' ' as end_vas, inv_date
                                from ${etlvar::ADWDB}.pos_inv_header
                                where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') 
                                and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                        ) b
                        on a.cust_num = b.cust_num
                        and a.subr_num = b.subr_num
                        and a.inv_date = b.create_date
                ) x,
                table(cast(multiset(select level from dual connect by  level <= length (regexp_replace(x.add_vas, '[^,]+'))  + 1) as sys.OdciNumberList)) levels
        ) y,
        table(cast(multiset(select level from dual connect by  level <= length (regexp_replace(y.end_vas, '[^,]+'))  + 1) as sys.OdciNumberList)) levels
) a 
left join ${etlvar::ADWDB}.free_data_entitle_n_ref b
on a.add_vas = b.bill_serv_cd
group by 
CUST_NUM 
,SUBR_NUM 
,STORE_CD 
,ORDER_NUM
;


INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_END_VAS_TMP
(
CUST_NUM 
,SUBR_NUM 
,STORE_CD 
,ORDER_NUM
,END_VAS  
)
select /*+ parallel(32) */ 
distinct
cust_num, subr_num, store_cd, order_num, 
replace(COALESCE(listagg(END_VAS,',') within group (order by cust_num, subr_num, store_cd, order_num),' '),' ,','') as END_VAS
from (
        select /*+ parallel(32) */ 
        distinct
        CUST_NUM, SUBR_NUM, STORE_CD,ORDER_NUM, END_VAS
        from (
                select /*+ parallel(32) */ 
                distinct
                x.*
                from (
                        select /*+ parallel(32) */ 
                        distinct
                        a.*, b.add_vas, b.end_vas
                        from (
                                select /*+ parallel(32) */ 
                                cust_num, subr_num, store_cd, rate_plan_cd, order_num, inv_date, mkt_cd, ref 
                                from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_003A1_TMP
                                union
                                select /*+ parallel(32) */ 
                                cust_num, subr_num, store_cd, rate_plan_cd, order_num, create_date, mkt_cd, ref 
                                from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004C1_TMP
                                union
                                select /*+ parallel(32) */ 
                                cust_num, subr_num, store_cd, rate_plan_cd, order_num, create_date, mkt_cd, ref 
                                from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004D1_TMP
                                union
                                select /*+ parallel(32) */ 
                                cust_num, subr_num, store_cd, rate_plan_cd, order_num, create_date, mkt_cd, ref 
                                from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004A1_TMP
                        ) a
                        left join (
                                select /*+ parallel(32) */ 
                                cust_num, subr_num, case_id as order_num, add_vas, end_vas, create_date
                                from ${etlvar::ADWDB}.om_complete_chg_plan
                                where create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') 
                                and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                union all
                                select /*+ parallel(32) */ 
                                cust_num, subr_num, case_id as order_num, add_vas, end_vas, create_date
                                from ${etlvar::ADWDB}.om_pending_chg_plan
                                where create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') 
                                and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                union all
                                select /*+ parallel(32) */ 
                                cust_num, subr_num, case_id as order_num, ' '  as add_vas, ' ' as end_vas, create_date
                                from ${etlvar::ADWDB}.fl_om_complete_chg_plan
                                where create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') 
                                and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                union all
                                select /*+ parallel(32) */ 
                                cust_num, subr_num, case_id as order_num, ' '  as add_vas, ' ' as end_vas, create_date
                                from ${etlvar::ADWDB}.fl_om_pending_chg_plan
                                where create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') 
                                and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                union all
                                select /*+ parallel(32) */ 
                                cust_num, subr_num, inv_num as order_num, ' '  as add_vas, ' ' as end_vas, inv_date
                                from ${etlvar::ADWDB}.pos_inv_header
                                where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') 
                                and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                        ) b
                        on a.cust_num = b.cust_num
                        and a.subr_num = b.subr_num
                        and a.inv_date = b.create_date
                ) x,
                table(cast(multiset(select level from dual connect by  level <= length (regexp_replace(x.add_vas, '[^,]+'))  + 1) as sys.OdciNumberList)) levels
        ) y,
        table(cast(multiset(select level from dual connect by  level <= length (regexp_replace(y.end_vas, '[^,]+'))  + 1) as sys.OdciNumberList)) levels
) a 
left join ${etlvar::ADWDB}.free_data_entitle_n_ref b
on a.end_vas = b.bill_serv_cd
group by 
CUST_NUM 
,SUBR_NUM 
,STORE_CD 
,ORDER_NUM
;


INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_CD_TMP
(
CUST_NUM 
,SUBR_NUM 
,STORE_CD 
,ORDER_NUM
,BOLT_ON_VAS  
)
select /*+ parallel(32) */ distinct
CUST_NUM, SUBR_NUM, STORE_CD,ORDER_NUM, 
replace(COALESCE(listagg(ACTIVE_BOLT_ON_VAS,',') within group (order by cust_num, subr_num, store_cd, order_num),' '),' ,','') as ACTIVE_BOLT_ON_VAS
from (
                select /*+ parallel(32) */ distinct
                a.*, b.bill_serv_cd as ACTIVE_BOLT_ON_VAS
                from (
                        select /*+ parallel(32) */ 
                        cust_num, subr_num, store_cd, rate_plan_cd, order_num, inv_date, mkt_cd, ref 
                        from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_003A1_TMP
                        union
                        select /*+ parallel(32) */ 
                        cust_num, subr_num, store_cd, rate_plan_cd, order_num, create_date, mkt_cd, ref 
                        from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004C1_TMP
                        union
                        select /*+ parallel(32) */ 
                        cust_num, subr_num, store_cd, rate_plan_cd, order_num, create_date, mkt_cd, ref 
                        from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004D1_TMP
                        union
                        select /*+ parallel(32) */ 
                        cust_num, subr_num, store_cd, rate_plan_cd, order_num, create_date, mkt_cd, ref 
                        from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004A1_TMP
                ) a
                left join (
                        select /*+ parallel(32) */ 
                        cust_num, subr_num, bill_start_date, bill_end_date, bill_serv_cd
                        from ${etlvar::ADWDB}.bill_servs
                        where bill_serv_cd in (select bill_serv_cd from ${etlvar::ADWDB}.free_data_entitle_n_ref)
                        union all
                        select /*+ parallel(32) */ 
                        cust_num, subr_num, bill_start_date, bill_end_date, bill_serv_cd
                        from ${etlvar::ADWDB}.bill_servs_pend_case
                        where bill_serv_cd in (select bill_serv_cd from ${etlvar::ADWDB}.free_data_entitle_n_ref)
                ) b
                on a.cust_num = b.cust_num
                and a.subr_num = b.subr_num 
                and a.inv_date < b.bill_end_date
) a 
left join ${etlvar::ADWDB}.free_data_entitle_n_ref b
on a.ACTIVE_BOLT_ON_VAS = b.bill_serv_cd
group by 
CUST_NUM 
,SUBR_NUM 
,STORE_CD 
,ORDER_NUM
;


INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_BOLT_ON_TMP
(
CUST_NUM 
,SUBR_NUM 
,STORE_CD 
,ORDER_NUM
,BOLT_ON_FLG  
)
select /*+ parallel(32) */ 
distinct
CUST_NUM ,SUBR_NUM ,STORE_CD ,ORDER_NUM,
case when count(distinct b.bill_serv_cd)+count(a.ACTIVE_BOLT_ON_VAS) > count(distinct c.bill_serv_cd)
and rate_plan_cd in (
        select rate_plan_cd
        from ${etlvar::ADWDB}.rate_plan_ref
        where free_data_entitle in 
        (
        select free_data_entitle 
        from ${etlvar::ADWDB}.shk_rate_plan_grp_ref
        where shk_plan_subgrp in 
        (
        'High tier (5GB or above) plans with add-on pack'
        ,'High tier (5GB or above) plans without add-on pack'
        )
        )
)
then 'Y'
else 'N'
end as BOLT_ON_FLG
from (
        select /*+ parallel(32) */ 
        distinct
        CUST_NUM, SUBR_NUM, STORE_CD, ORDER_NUM, ADD_VAS,rate_plan_cd,
        trim(regexp_substr(y.end_Vas, '[^,]+', 1, levels.column_value)) as END_VAS, ACTIVE_BOLT_ON_VAS
        from (
                select /*+ parallel(32) */ 
                distinct
                CUST_NUM, SUBR_NUM, STORE_CD,ORDER_NUM,rate_plan_cd,
                trim(regexp_substr(x.add_Vas, '[^,]+', 1, levels.column_value)) as ADD_VAS, END_VAS, ACTIVE_BOLT_ON_VAS
                from (
                                        select /*+ parallel(32) */ 
                                        distinct
                                        a.*, b.bill_serv_cd as ACTIVE_BOLT_ON_VAS, c.add_vas, c.end_vas
                                        from (
                                                select /*+ parallel(32) */ 
                                                cust_num, subr_num, store_cd, rate_plan_cd, order_num, inv_date, mkt_cd, ref 
                                                from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_003A1_TMP
                                                union
                                                select /*+ parallel(32) */ 
                                                cust_num, subr_num, store_cd, rate_plan_cd, order_num, create_date, mkt_cd, ref 
                                                from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004C1_TMP
                                                union
                                                select /*+ parallel(32) */ 
                                                cust_num, subr_num, store_cd, rate_plan_cd, order_num, create_date, mkt_cd, ref 
                                                from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004D1_TMP
                                                union
                                                select /*+ parallel(32) */ 
                                                cust_num, subr_num, store_cd, rate_plan_cd, order_num, create_date, mkt_cd, ref 
                                                from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004A1_TMP
                                        ) a
                                        left join (
                                                select /*+ parallel(32) */ 
                                                cust_num, subr_num, bill_start_date, bill_end_date, bill_serv_cd
                                                from ${etlvar::ADWDB}.bill_servs
                                                where bill_serv_cd in (select bill_serv_cd from ${etlvar::ADWDB}.free_data_entitle_n_ref)
                                                union all
                                                select /*+ parallel(32) */ 
                                                cust_num, subr_num, bill_start_date, bill_end_date, bill_serv_cd
                                                from ${etlvar::ADWDB}.bill_servs_pend_case
                                                where bill_serv_cd in (select bill_serv_cd from ${etlvar::ADWDB}.free_data_entitle_n_ref)
                                        ) b
                                        on a.cust_num = b.cust_num
                                        and a.subr_num = b.subr_num 
                                        and a.inv_date < b.bill_end_date
                                        left join (
                        select /*+ parallel(32) */ 
                                                cust_num, subr_num, case_id as order_num, add_vas, end_vas, create_date
                        from ${etlvar::ADWDB}.om_complete_chg_plan
                        where create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') 
                        and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                        union all
                        select /*+ parallel(32) */ 
                                                cust_num, subr_num, case_id as order_num, add_vas, end_vas, create_date
                        from ${etlvar::ADWDB}.om_pending_chg_plan
                        where create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') 
                        and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                        union all
                        select /*+ parallel(32) */ 
                                                cust_num, subr_num, case_id as order_num, ' '  as add_vas, ' ' as end_vas, create_date
                        from ${etlvar::ADWDB}.fl_om_complete_chg_plan
                        where create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') 
                        and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                        union all
                        select /*+ parallel(32) */ 
                                                cust_num, subr_num, case_id as order_num, ' '  as add_vas, ' ' as end_vas, create_date
                        from ${etlvar::ADWDB}.fl_om_pending_chg_plan
                        where create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') 
                        and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                        union all
                        select /*+ parallel(32) */ 
                                                cust_num, subr_num, inv_num as order_num, ' '  as add_vas, ' ' as end_vas, inv_date
                        from ${etlvar::ADWDB}.pos_inv_header
                        where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') 
                        and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                    ) c
                    on a.cust_num = c.cust_num
                    and a.subr_num = c.subr_num
                    and a.inv_date = c.create_date
                ) x,
                table(cast(multiset(select level from dual connect by  level <= length (regexp_replace(x.add_vas, '[^,]+'))  + 1) as sys.OdciNumberList)) levels
        ) y,
        table(cast(multiset(select level from dual connect by  level <= length (regexp_replace(y.end_vas, '[^,]+'))  + 1) as sys.OdciNumberList)) levels
) a 
left join ${etlvar::ADWDB}.free_data_entitle_n_ref b
on a.add_vas = b.bill_serv_cd
left join ${etlvar::ADWDB}.free_data_entitle_n_ref c
on a.end_vas = c.bill_Serv_cd
group by 
CUST_NUM 
,SUBR_NUM 
,STORE_CD 
,ORDER_NUM
,rate_plan_cd
;

---------------------------------------------------------------------------------------------------------------------------------------------



EXIT;

ENDOFINPUT
  close(SQLCMD);
  print("sqlplus /\@${etlvar::TDDSN} \@$SQLCMD_FILE");
  my $ret = system("sqlplus /\@${etlvar::TDDSN} \@$SQLCMD_FILE");
  if ($ret != 0)
  {
    return (1);
  }
  return 0;

}


##################################################################################################################################
##################################################################################################################################
##################################################################################################################################



sub initParam{

    print("\n\n\n#####################################\n");
    print("#  Init Parameters\n");
    print("#####################################\n\n");

    my $err = "";

    # ------------------------------------------------------------------#
    #  Please define the parameters for this job below.                 #
    # ------------------------------------------------------------------#

    #my $rundate = DateCalc("${etlvar::TXDATE}", "- 1 days", \$err);
    #my $From_rundate = DateCalc("${etlvar::TXDATE}", "- 0 days", \$err);
    #my $To_rundate = DateCalc("${etlvar::TXDATE}", "- 0 days", \$err);
    #$REPORT_DATE = &UnixDate($rundate, "%Y-%m-%d");
    #$FROM_REPORT_DATE = &UnixDate($From_rundate, "%Y-%m-%d");
    #$TO_REPORT_DATE = &UnixDate($To_rundate, "%Y-%m-%d");
    #$FILE_DATE = &UnixDate($rundate, "%Y%m%d");


    my $rundate = DateCalc("${etlvar::TXDATE}", "- 0 days", \$err);
    my $From_rundate =&UnixDate(DateCalc("${etlvar::TXDATE}", "- 1 days", \$err), "%Y%m");
    my $To_rundate = DateCalc("${etlvar::TXDATE}", "- 1 days", \$err);
    $REPORT_DATE = &UnixDate($rundate, "%Y-%m-%d");
    $FROM_REPORT_DATE = &UnixDate("${From_rundate}01", "%Y-%m-%d");
    $SUBJECT_REPORT_DATE = &UnixDate($From_rundate, "%Y%m");
    $TO_REPORT_DATE = &UnixDate($To_rundate, "%Y-%m-%d");
    $FILE_DATE = &UnixDate($rundate, "%Y%m");



    if ($ENV eq "DEV"){

        ##  DEVELOPMENT  ##
        $TDUSR = "${etlvar::TDUSR}";
        $TDPWD = "${etlvar::TDPWD}";
        $TDDSN = $ENV{"AUTO_DSN"};

        $OUTPUT_FILE_PATH = "${etlvar::ETL_OUTPUT_DIR}/${etlvar::ETLSYS}/U_NRBD_COMP_RETENT_RPT_TEST";
        $FILE_PREFIX = "Hitrate";

    }
    else
    {
        ##  PRODUCTION  ##
        $TDUSR = "${etlvar::TDUSR}";
        $TDPWD = "${etlvar::TDPWD}";
        $TDDSN = $ENV{"AUTO_DSN"};

        $OUTPUT_FILE_PATH = "${etlvar::ETL_OUTPUT_DIR}/${etlvar::ETLSYS}/U_NRBD_COMP_RETENT_RPT_TEST";
        $FILE_PREFIX = "Hitrate";

    }

    $ABNORMAL_FOUND=0;
    $ABNORMAL_RATE=0.8;
}


##################################################################################################################################
##################################################################################################################################
##################################################################################################################################




#We need to have variable input for the program to start
if ($#ARGV < 0){
    print("Syntax : perl <Script Name> <System Name>_<Job Name>_<TXDATE>.dir>\n");
    print("Example: perl b_cust_info0010.pl adw_b_cust_info_20051010.dir\n");
    exit(1);
}


#Call the function we want to run
open(STDERR, ">&STDOUT");

my $pre = etlvar::preProcess($ARGV[0]);
my $rc = etlvar::getTXDate($MASTER_TABLE);

# disable print buffer
$| = 1;

#init param
initParam();

$ret = runSQLPLUS();









if($ret == 0)
{

}



my $post = etlvar::postProcess();

exit($ret);














