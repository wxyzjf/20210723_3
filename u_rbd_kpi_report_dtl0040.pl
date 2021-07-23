/opt/etl/prd/etl/APP/RBD/Z_RBD_KPI_REPORT_DTL_UAT/bin> cat u_rbd_kpi_report_dtl0040.pl
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

###my $ETLVAR = $ENV{"AUTO_ETLVAR"};
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

--Refresh image
DELETE FROM ${etlvar::MIGADWDB}.RBD_KPI_REPORT_CNT_SUMM
where txdate-1 between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1;

DELETE FROM ${etlvar::MIGADWDB}.RBD_KPI_REPORT_REVENUE_SUMM   
where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1;

--Distinct invoice count
--Service
INSERT INTO ${etlvar::MIGADWDB}.RBD_KPI_REPORT_CNT_SUMM
(
store_cd,
staff_name,
SUPER_CARE_PLAN,
OTHER_SERVICE_PLAN,
SERVICE_PLAN_TOTAL,
NON_SERVICE_PLAN,
DISTINCT_INV_NUM_CNT,
ITEM,
TXDATE
)
select /*+ parallel(32) */
store_cd, 
coalesce(staff_name,' '), coalesce(super_care_plans_revenue,0), 
coalesce(other_service_plans_revenue,0), 
coalesce(Total_service_revenue,0), 
coalesce(Non_service_plan,0), 
coalesce(NO_OF_INV,0), 
'Service' as item, 
TXDATE
from (
        select /*+ parallel(32) */ 
        store_cd,
        staff_name,
        sum(coalesce(super_care_plans_revenue,0)) as super_care_plans_revenue,
        sum(coalesce(other_service_plans_revenue,0)) as other_service_plans_revenue,
        sum(coalesce(vas_fee,0)) as Non_service_plan,
        sum(coalesce(Total_service_revenue,0)) as Total_service_revenue,
        count(distinct order_num) as no_of_inv,
        'Service'
        ,inv_date+1 as txdate
        from ${etlvar::MIGADWDB}.RBD_KPI_REPORT_SERV_REV_DTL x
        where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
    group by store_cd, inv_date, staff_name
)
where store_cd in (select store_cd from ${etlvar::MIGDB}.u_rbd_kpi_report_tx_001_tmp where daily_sales_sms='Y')
;


--Device sales
INSERT INTO ${etlvar::MIGADWDB}.RBD_KPI_REPORT_CNT_SUMM
(
store_cd,
staff_name,
HANDSET,
DISTINCT_INV_NUM_CNT,
ITEM,
TXDATE
)
select /*+ parallel(32) */
store_cd, 
coalesce(staff_name,' '), coalesce(REVENUE,0), coalesce(NO_OF_INV,0), 'Device sales' as item, TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD') as TXDATE
from (
        select /*+ parallel(32) */ 
        STORE_CD, staff_name, 
        sum(pos_amt)+sum(TOTAL_TRADE_IN_AMT) as revenue, 
        --+sum(COUPON_DISCOUNT_AMT)
        --+sum(TOTAL_HS_REBATE_AMT), already included in pos_amt 
        count(distinct inv_num) as no_of_inv, 'Device sales'
        ,inv_date+1 as txdate
        from ${etlvar::MIGADWDB}.RBD_KPI_REPORT_HS_DTL
        where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
    group by store_cd, inv_date, staff_name
)
where store_cd in (select store_cd from ${etlvar::MIGDB}.u_rbd_kpi_report_tx_001_tmp where daily_sales_sms='Y')
;

--Prepaid SIM
INSERT INTO ${etlvar::MIGADWDB}.RBD_KPI_REPORT_CNT_SUMM
(
store_cd,
staff_name,
PREPAID_SIM,
DISTINCT_INV_NUM_CNT,
ITEM,
TXDATE
)
select /*+ parallel(32) */
store_cd, 
coalesce(staff_NAME,' '), coalesce(REVENUE,0), coalesce(NO_OF_INV,0), 'Prepaid SIM' as item, 
TXDATE
from (
        select /*+ parallel(32) */ 
        STORE_CD, staff_NAME, sum(pos_amt) as revenue, count(distinct inv_num) as no_of_inv, 'Prepaid SIM'
        ,inv_date+1 as txdate
        from ${etlvar::ADWDB}.RBD_KPI_REPORT_SIM_DTL
        where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
    group by store_cd, inv_date, staff_name
)
where store_cd in (select store_cd from ${etlvar::MIGDB}.u_rbd_kpi_report_tx_001_tmp where daily_sales_sms='Y')
;

--Accessory
INSERT INTO ${etlvar::MIGADWDB}.RBD_KPI_REPORT_CNT_SUMM
(
store_cd,
staff_name,
ACCESSORIES,
DISTINCT_INV_NUM_CNT,
ITEM,
TXDATE
)
select /*+ parallel(32) */
store_cd, 
coalesce(staff_NAME,' '), coalesce(REVENUE,0), coalesce(NO_OF_INV,0), 'Accessories' as item, TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD') as TXDATE
from (
        select /*+ parallel(32) */ 
        a.STORE_CD
        , staff_NAME
        ---MOD_SR0002530---
        ---Invole redeem amt in accessory ---
        --, sum(pos_amt) as revenue --+sum(COUPON_DISCOUNT_AMT)
        , sum(pos_amt) + sum(COUPON_DISCOUNT_AMT) as revenue
        ,count(distinct inv_num) as no_of_inv, 'Accessories'
        ,inv_date+1 as txdate
        from ${etlvar::MIGADWDB}.RBD_KPI_REPORT_ACCESORY_DTL  a
        left join ${etlvar::ADWDB}.fes_usr_info d
        on a.salesman_cd = d.usr_name
        where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
    group by store_cd, inv_date, staff_name
) 
where store_cd in (select store_cd from ${etlvar::MIGDB}.u_rbd_kpi_report_tx_001_tmp where daily_sales_sms='Y')
;

--Prepaid Vouchers
INSERT INTO ${etlvar::MIGADWDB}.RBD_KPI_REPORT_CNT_SUMM
(
store_cd,
staff_name,
PREPAID_VOUCHER,
DISTINCT_INV_NUM_CNT,
ITEM,
TXDATE
)
select /*+ parallel(32) */
store_cd, 
coalesce(staff_name,' '), coalesce(REVENUE,0), coalesce(NO_OF_INV,0), 'Prepaid vocher' as item, TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD') as TXDATE
from (
        select /*+ parallel(32) */ 
        STORE_CD, staff_name, sum(pos_amt) as revenue , count(distinct inv_num) as no_of_inv, 'Prepaid vocher' as item
        ,inv_date+1 as txdate
        from (
                select distinct *
                from ${etlvar::MIGADWDB}.RBD_KPI_REPORT_PREPA_VOU_DTL a
                left join ${etlvar::ADWDB}.fes_usr_info d
                on a.salesman_cd = d.usr_name
        ) 
        where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
    group by store_cd, inv_date, staff_name
)
where store_cd in (select store_cd from ${etlvar::MIGDB}.u_rbd_kpi_report_tx_001_tmp where daily_sales_sms='Y')
;

--Others
INSERT INTO ${etlvar::MIGADWDB}.RBD_KPI_REPORT_CNT_SUMM
(
store_cd,
staff_name,
OTHER,
DISTINCT_INV_NUM_CNT,
ITEM,
TXDATE
)
select /*+ parallel(32) */
store_cd, 
coalesce(staff_NAME,' '), coalesce(REVENUE,0), coalesce(NO_OF_INV,0), 'Other' as item, TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD') as TXDATE
from (
        select /*+ parallel(32) */ 
        STORE_CD, staff_NAME, sum(pos_amt) as revenue, count(distinct inv_num) as no_of_inv, 'Other'
        ,inv_date+1 as txdate
        from (
                select distinct *
                from ${etlvar::MIGADWDB}.RBD_KPI_REPORT_OTHER_DTL a
                left join ${etlvar::ADWDB}.fes_usr_info d
                on a.salesman_cd = d.usr_name
        ) 
        where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
    group by store_cd, inv_date, staff_name
) 
where store_cd in (select store_cd from ${etlvar::MIGDB}.u_rbd_kpi_report_tx_001_tmp where daily_sales_sms='Y')
;

--Revenue
INSERT INTO ${etlvar::MIGADWDB}.RBD_KPI_REPORT_REVENUE_SUMM
(
  STORE_CD          
  ,STAFF_NAME        
  ,SUPER_CARE_PLAN   
  ,OTHER_SERVICE_PLAN
  ,NON_SERVICE_PLAN  
  ,HANDSET           
  ,ACCESSORIES       
  ,PREPAID_SIM       
  ,OTHER             
  ,PREPAID_VOUCHER   
  ,TOTAL             
  ,DISTINCT_INV_NUM_CNT
  ,TXDATE
  ,SUMM_FIG
  ,INV_DATE
)
SELECT /*+ parallel(32) */
STORE_CD 
,STAFF_NAME
,SUM(SUPER_CARE_PLAN)     
,SUM(OTHER_SERVICE_PLAN) 
,SUM(NON_SERVICE_PLAN) 
,SUM(HANDSET)  
,SUM(ACCESSORIES)
,SUM(PREPAID_SIM)         
,SUM(OTHER)               
,SUM(PREPAID_VOUCHER)       
,SUM(SUPER_CARE_PLAN)+SUM(OTHER_SERVICE_PLAN)+SUM(NON_SERVICE_PLAN)+SUM(PREPAID_SIM)+SUM(PREPAID_VOUCHER)+SUM(OTHER)+SUM(HANDSET)+SUM(ACCESSORIES) 
,SUM(DISTINCT_INV_NUM_CNT)
,txdate
,'N' as SUMM_FIG
,txdate-1 as inv_date
FROM 
${etlvar::MIGADWDB}.RBD_KPI_REPORT_CNT_SUMM
where txdate-1 between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
and store_cd in (select store_cd from ${etlvar::MIGDB}.u_rbd_kpi_report_tx_001_tmp where daily_sales_sms='Y')
GROUP BY STORE_CD, STAFF_NAME, txdate
;

--Total and Average
INSERT INTO ${etlvar::MIGADWDB}.RBD_KPI_REPORT_REVENUE_SUMM
(
  STORE_CD          
  ,STAFF_NAME        
  ,SUPER_CARE_PLAN   
  ,OTHER_SERVICE_PLAN
  ,NON_SERVICE_PLAN  
  ,HANDSET           
  ,ACCESSORIES       
  ,PREPAID_SIM       
  ,OTHER             
  ,PREPAID_VOUCHER   
  ,TOTAL             
  ,DISTINCT_INV_NUM_CNT
  ,TXDATE
  ,SUMM_FIG
  ,INV_DATE
)
        select /*+ parallel(32) */
          STORE_CD          
          ,STAFF_NAME        
          ,SUPER_CARE_PLAN   
          ,OTHER_SERVICE_PLAN
          ,NON_SERVICE_PLAN  
          ,HANDSET           
          ,ACCESSORIES       
          ,PREPAID_SIM       
          ,OTHER             
          ,PREPAID_VOUCHER   
          ,TOTAL             
          ,DISTINCT_INV_NUM_CNT
          ,txdate
          ,'Y'
          ,txdate-1
        from (
                select /*+ parallel(32) */ 
                store_cd,
                Store_cd || ' Total' as staff_name     
                ,sum(SUPER_CARE_PLAN) as SUPER_CARE_PLAN
                ,sum(OTHER_SERVICE_PLAN) as OTHER_SERVICE_PLAN
                ,sum(NON_SERVICE_PLAN) as NON_SERVICE_PLAN
                ,sum(HANDSET) as HANDSET
                ,sum(ACCESSORIES) as ACCESSORIES
                ,sum(PREPAID_SIM) as PREPAID_SIM
                ,sum(OTHER) as OTHER      
                ,sum(PREPAID_VOUCHER) as  PREPAID_VOUCHER
                ,sum(TOTAL) as TOTAL
                ,sum(DISTINCT_INV_NUM_CNT)as DISTINCT_INV_NUM_CNT
                ,inv_date+1 as txdate
                from 
                ${etlvar::MIGADWDB}.RBD_KPI_REPORT_REVENUE_SUMM
                where SUMM_FIG <> 'Y'
                and inv_date between trunc(TO_DATE('${etlvar::TXDATE}','YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                group by inv_date, store_cd
                union all
                select
                store_cd,
                Store_cd || ' Average' as staff_name
                ,round(SUM(SUPER_CARE_PLAN)/count(distinct staff_name),3) as SUPER_CARE_PLAN
                ,round(SUM(OTHER_SERVICE_PLAN)/count(distinct staff_name),3) 
                ,round(SUM(NON_SERVICE_PLAN)/count(distinct staff_name),3)   
                ,round(SUM(HANDSET)/count(distinct staff_name),3)            
                ,round(SUM(ACCESSORIES)/count(distinct staff_name),3)       
                ,round(SUM(PREPAID_SIM)/count(distinct staff_name),3)        
                ,round(SUM(OTHER)/count(distinct staff_name),3)             
                ,round(SUM(PREPAID_VOUCHER)/count(distinct staff_name),3)    
                ,round(SUM(TOTAL)/count(distinct staff_name),3)              
                ,round(SUM(DISTINCT_INV_NUM_CNT)/count(distinct staff_name),3)
                ,inv_date+1 as txdate
                from 
                ${etlvar::MIGADWDB}.RBD_KPI_REPORT_REVENUE_SUMM
                where SUMM_FIG <> 'Y'
                and inv_date between trunc(TO_DATE('${etlvar::TXDATE}','YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                group by inv_date, store_cd
                union all
                select
                'All RBD'  as store_cd
                ,'RBD Average'  as staff_name
                ,ROUND(SUM(SUPER_CARE_PLAN)     /count(distinct store_cd),3)
                ,ROUND(SUM(OTHER_SERVICE_PLAN)  /count(distinct store_cd),3)
                ,ROUND(SUM(NON_SERVICE_PLAN)    /count(distinct store_cd),3)
                ,ROUND(SUM(HANDSET)                             /count(distinct store_cd),3)
                ,ROUND(SUM(ACCESSORIES)                 /count(distinct store_cd),3)
                ,ROUND(SUM(PREPAID_SIM)                 /count(distinct store_cd),3)
                ,ROUND(SUM(OTHER)                               /count(distinct store_cd),3)
                ,ROUND(SUM(PREPAID_VOUCHER)     /count(distinct store_cd),3)
                ,ROUND(SUM(TOTAL)                       /count(distinct store_cd),3)
                ,ROUND(SUM(DISTINCT_INV_NUM_CNT)/count(distinct store_cd),3)
                ,inv_date+1 as txdate
                from 
                ${etlvar::MIGADWDB}.RBD_KPI_REPORT_REVENUE_SUMM
                where SUMM_FIG <> 'Y'
                and inv_date between trunc(TO_DATE('${etlvar::TXDATE}','YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                group by inv_date
                union all
                select
                'All RBD'  as store_cd,
                'RBD Total'  as staff_name
                ,SUM(SUPER_CARE_PLAN)   
                ,SUM(OTHER_SERVICE_PLAN)
                ,SUM(NON_SERVICE_PLAN)  
                ,SUM(HANDSET)                     
                ,SUM(ACCESSORIES)                 
                ,SUM(PREPAID_SIM)                 
                ,SUM(OTHER)                       
                ,SUM(PREPAID_VOUCHER)   
                ,SUM(TOTAL)                   
                ,SUM(DISTINCT_INV_NUM_CNT)
                ,inv_date+1 as txdate
                from 
                ${etlvar::MIGADWDB}.RBD_KPI_REPORT_REVENUE_SUMM
                where SUMM_FIG <> 'Y'
                and inv_date between trunc(TO_DATE('${etlvar::TXDATE}','YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                group by inv_date
        )
        ;
        COMMIT;

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














