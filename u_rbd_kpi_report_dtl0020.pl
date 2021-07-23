/opt/etl/prd/etl/APP/RBD/Z_RBD_KPI_REPORT_DTL_UAT/bin> cat u_rbd_kpi_report_dtl0020.pl
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
use Time::Piece;

##my $ETLVAR = $ENV{"AUTO_ETLVAR"};
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

TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TRX_TMP001;
TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_MAP_INV_ORDER;

--Map Invoice and OM Orders
INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_MAP_INV_ORDER (
        INV_NUM           
        ,CUST_NUM          
        ,SUBR_NUM          
        ,INV_DATE        
        ,POS_SHOP_CD
        ,SALESMAN_CD       
        ,LD_CD             
        ,MKT_CD            
        ,RATE_PLAN_CD      
        ,EFF_DATE          
        ,LD_EXP_DATE
)
select 
        INV_NUM           
        ,CUST_NUM          
        ,SUBR_NUM          
        ,INV_DATE        
        ,store_cd
        ,SALESMAN_CD       
        ,LD_CD             
        ,MKT_CD            
        ,RATE_PLAN_CD      
        ,EFF_DATE 
        ,LD_EXPIRY_DATE
from 
(
        select /*+ parallel(32) */ 
        distinct inv_num, a.cust_num, a.subr_num, 
        a.inv_date,a.store_cd,d.salesman_cd, 
        case when a.ld_cd <> ' '
        then a.ld_cd
        else c.ld_cd
        end as ld_cd, a.mkt_cd, --rate_plan_cd,
        c.new_plan_cd as RATE_PLAN_CD, c.eff_date
        ,d.ld_expiry_date
        ,row_number() over (partition by a.cust_num, a.subr_num, a.order_num order by c.eff_date desc , c.eff_time desc) as rn
        from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_003A1_TMP a
        left join ${etlvar::ADWDB}.mkt_ref_vw b
        on a.mkt_cd = b.mkt_cd
        left join (
        select * from ${etlvar::ADWDB}.om_complete_chg_plan
        where create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
        union all
        select * from ${etlvar::ADWDB}.om_pending_chg_plan
        where create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
        ) c
        on a.cust_num = c.cust_num
        and a.subr_num = c.subr_num
        and a.inv_date = c.create_date
        left join (
        select * from ${etlvar::ADWDB}.pos_inv_header 
        where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
        ) d
        on a.cust_num = d.cust_num
        and a.subr_num = d.subr_num
        and a.order_num = d.inv_num
        where 
        c.create_date >= c.eff_date
        and c.new_plan_cd <> ' '
        and a.order_num like 'I%'
) where rn = 1
;

INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_MAP_INV_ORDER (
        INV_NUM           
        ,CUST_NUM          
        ,SUBR_NUM          
        ,INV_DATE        
        ,POS_SHOP_CD
        ,SALESMAN_CD       
        ,LD_CD             
        ,MKT_CD            
        ,RATE_PLAN_CD      
        ,EFF_DATE     
        ,LD_EXP_DATE
)
select 
        INV_NUM           
        ,CUST_NUM          
        ,SUBR_NUM          
        ,INV_DATE        
        ,store_cd
        ,SALESMAN_CD       
        ,LD_CD             
        ,MKT_CD            
        ,RATE_PLAN_CD      
        ,EFF_DATE 
        ,LD_EXPIRY_DATE
from 
(
        select /*+ parallel(32) */ 
        distinct a.order_num as inv_num, a.cust_num, a.subr_num, 
        a.inv_date,a.store_cd,d.salesman_cd, 
        case when a.ld_cd <> ' '
        then a.ld_cd
        else d.ld_cd
        end as ld_cd, a.mkt_cd, --rate_plan_cd,
        d.rate_plan_cd as RATE_PLAN_CD, d.inv_date as eff_date
        ,d.ld_expiry_date
        ,row_number() over (partition by a.cust_num, a.subr_num, a.order_num order by a.inv_date desc) as rn
        from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_003A1_TMP a
        left join ${etlvar::ADWDB}.mkt_ref_vw b
        on a.mkt_cd = b.mkt_cd
        left join (
        select * from ${etlvar::ADWDB}.om_complete_chg_plan
        where create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
        union all
        select * from ${etlvar::ADWDB}.om_pending_chg_plan
        where create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
        ) c
        on a.cust_num = c.cust_num
        and a.subr_num = c.subr_num
        and a.inv_date = c.create_date
        left join (
        select * from ${etlvar::ADWDB}.pos_inv_header 
        where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
        ) d
        on a.cust_num = d.cust_num
        and a.subr_num = d.subr_num
        and a.inv_date = d.inv_date
        where 
        c.case_id is null and d.inv_num like 'F%'
        and ((a.rate_plan_cd = ' ' and d.rate_plan_cd <> ' ')
        or (a.ld_cd = ' ' and d.ld_cd <> ' '))
) where rn = 1
;
commit;


--I invoice
INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TRX_TMP001 
(
        CUST_NUM 
        ,SUBR_NUM 
        ,STORE_CD 
        ,RATE_PLAN_CD 
        ,ORDER_NUM 
        ,ORIG_LD_CD 
        ,NEW_LD_CD 
        ,ORIG_LD_EXP_DATE 
        ,NEW_LD_EXP_DATE 
        ,ORIG_RATE_PLAN_CD 
        ,NEW_RATE_PLAN_CD 
        ,INV_DATE 
        ,MKT_CD 
        ,REF 
        ,RATE_PLAN_GRP 
        ,TRX_TYPE 
        ,FREE_DATA_ENTITLE 
        ,CONTACTED_BY_CALL_CENTER 
        ,ONLINE_INV 
        ,REPORT_DATE 
        ,DAILY_SALES_SMS 
        ,EFF_DATE 
        ,CREATE_BY 
        ,ORIG_LD_REVENUE 
        ,NEW_LD_REVENUE 
        ,SUBR_STAT_CD 
        ,FA_INVOICE_FLG 
        ,ADD_VAS 
        ,ORIG_CREATE_BY 
        ,ORIG_HANDLE_BY 
        ,ADD_VAS_FROM_OM
        ,END_VAS
        ,BOLT_ON_FLG 
        ,ACTIVE_BOLT_ON_VAS 
)
select 
distinct /*+ parallel(32) */ 
        CUST_NUM, SUBR_NUM, STORE_CD, RATE_PLAN_CD, ORDER_NUM, 
        ORIG_LD_CD, NEW_LD_CD, ORIG_LD_EXP_DATE, NEW_LD_EXP_DATE, ORIG_RATE_PLAN_CD, NEW_RATE_PLAN_CD, INV_DATE,
        MKT_CD,         REF,    RATE_PLAN_GRP, TRX_TYPE, FREE_DATA_ENTITLE, CONTACTED_BY_CALL_CENTER, ONLINE_INV 
        ,inv_date+1 as report_date, daily_sales_sms, eff_date, create_by, orig_ld_revenue,new_ld_revenue, subr_stat_cd
        ,FA_INVOICE_FLG, add_vas, ORIG_CREATE_BY, ORIG_HANDLE_BY
        ,ADD_VAS_FROM_OM ,END_VAS , BOLT_ON_FLG, ACTIVE_BOLT_ON_VAS
from (
                select  /*+ parallel(32) */  a.*
                ,case
                when a.free_data_entitle = 'Mass Mobile True unlimited data plans' then '01. True unlimited data plans'
                when a.free_data_entitle = 'Mass Mobile 5GB FUP unlimited data plans' then '02. Unlimited data plans'
                when a.free_data_entitle = 'Mass Mobile 5GB or above plans WITH add-on unlimited data feature' then '03. High tier (5GB or above) plans with add-on pack'
                when a.free_data_entitle = 'Mass Mobile 5GB or above drop dead plans WITHOUT add-on unlimited data feature (exclude Family plans)' then '04. High tier (5GB or above) plans without add-on pack'
                when a.free_data_entitle = 'Mass Mobile Family plans (Main SIM only)' then '05. Family Plan (Main SIM)'
                when a.free_data_entitle = 'Mass Mobile Family plans (Secondary SIM only)' then '06. Family plans (Secondary SIM)' 
                when a.free_data_entitle = 'Mass Mobile 1.5GB-4.9GB drop dead plans' then '07. Mid tier plans (1.5GB - 4.9GB)'
                when a.free_data_entitle = 'Mass Mobile 500MB-1.4GB drop dead plans' then '08. Upper low tier plans (500MB - 1.4GB)'
                when a.free_data_entitle = 'Mass Mobile below 500MB drop dead plans' then '09. Low tier plans (below 500MB)'
                when a.free_data_entitle = 'Mass Mobile 1GB (up to 42Mbps) + unlimited (up to 128kbps) plans' then '10. ExtraCare plans'
                when a.free_data_entitle = 'Mass Mobile 1GB + unlimited plans' then '10. ExtraCare plans'
                when a.free_data_entitle = 'Mass Mobile ExtraCare plans' then '10. ExtraCare plans'
                when a.free_data_entitle = 'Mass Mobile Speed capped unlimited data (up to 2Mbps) plans' then '11. Speed capped unlimited data plans'
                when a.free_data_entitle = 'Mass Mobile Speed capped unlimited data (up to 384kbps) plans' then '11. Speed capped unlimited data plans'
                when a.free_data_entitle = 'Mass Mobile Speed capped unlimited data (up to 768kbps) plans' then '11. Speed capped unlimited data plans'
                when x.rate_plan_grp = 'Residential Fibre Broadband 100' then '13. Fiber Broadband:100MB'
                when x.rate_plan_grp = 'Residential Fibre Broadband 500' then '14. Fiber Broadband:500MB'
                when x.rate_plan_grp = 'Residential Fibre Broadband 1000' then '15. Fiber Broadband:1000MB'
                when substr(x.rate_plan_grp, 1, 11) = 'FBB HomeTel' then '16. HomeTel'
                when (substr(x.rate_plan_grp, 1, 9) = 'HomePhone' or x.rate_plan_grp = 'Jupiter') then '17. HPP'
                when a.free_data_entitle = 'Mass Mobile 5GB or above drop dead plans WITHOUT add-on unlimited data feature' 
                then '04. High tier (5GB or above) plans without add-on pack'
                when a.free_data_entitle = 'Mass Mobile 5GB or above plans WITHOUT add-on pack (exclude Family plans)' 
                then '04. High tier (5GB or above) plans without add-on pack'
                when a.free_data_entitle = 'Mass Mobile 5GB or above plans WITH add-on pack (exclude Family plans)' 
                then '03. High tier (5GB or above) plans with add-on pack'
                when a.free_data_entitle = 'Mass Mobile 5GB or above plans WITH add-on pack' 
                then '03. High tier (5GB or above) plans with add-on pack'
                when a.free_data_entitle = 'Mass Mobile 5GB or above plans (exclude Family plans)' 
                then '04. High tier (5GB or above) plans without add-on pack'
                else '12. Other plans'
                end  Rate_Plan_Grp
                --Change plan only
                ,case when ref ='Chg plan only'
                then 'UPGRADE'
                --No ld_cd or no expire_date
                when (a.orig_ld_cd ='NA' or a.orig_ld_cd = ' ') or 
                (a.orig_ld_exp_date = date '1900-01-01' or a.orig_ld_exp_date = date '2999-12-31')
                then 'RETENTION'
                --To be expired within 3 months or already expired
                when months_between(trunc(a.orig_ld_exp_date,'MONTH'), trunc(a.inv_date,'MONTH')) <= 3
                then 'RETENTION'
                --To be expired more than 3 months
                when months_between(trunc(a.orig_ld_exp_date,'MONTH'), trunc(a.inv_date,'MONTH')) > 3
                then 'UPGRADE'
                else 'RETENTION'
                end as trx_type
                ,case when (a.cust_num, a.subr_num) in (select cust_num, subr_num from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_002A2_TMP)
                then 'Y'
                else 'N'
                end as CONTACTED_BY_CALL_CENTER
                ,y.daily_sales_sms
                ,case when zz.salesman_flg <> 'Y'
                then z.usr_id
                else z.salesman_cd 
                end as create_by
                ,case when (
                case when zz.salesman_flg <> 'Y'
                then z.usr_id
                else z.salesman_cd 
                end
                ) like 'FA%'
                then 'Y'
                else 'N'
                end as FA_INVOICE_FLG
                ,' ' as add_vas
                ,z.salesman_cd as ORIG_CREATE_BY
                ,z.usr_id as ORIG_HANDLE_BY
                ,' ' as END_VAS, ' ' as ACTIVE_BOLT_ON_VAS, ' ' as BOLT_ON_FLG ,' ' as ADD_VAS_FROM_OM
                from (
                                select /*+ parallel(32) */ 
                                distinct a.CUST_NUM, a.SUBR_NUM, a.STORE_CD, 
                                a.ORDER_NUM, 
                                case when (xx.rate_plan_cd <> a.RATE_PLAN_CD )
                                and (xx.rate_plan_cd <> ' ' and xx.rate_plan_cd is not null)
                                then xx.rate_plan_cd
                                else a.rate_plan_cd
                                end as rate_plan_cd,  a.ORIG_LD_CD, 
                                case when (xx.ld_cd is null or xx.ld_cd = ' ')
                                then a.NEW_LD_CD
                                else xx.ld_cd
                                end as new_ld_cd,
                                a.ORIG_LD_EXP_DATE, 
                                case when (xx.ld_exp_date is null or xx.ld_exp_date = date '1900-01-01' or a.NEW_LD_EXP_DATE = date '2999-12-31')
                                then a.new_ld_exp_date
                                else xx.ld_exp_date
                                end as new_ld_exp_date,
                                a.INV_DATE, 
                                a.ORIG_RATE_PLAN_CD, 
                                case when (xx.rate_plan_cd <> a.RATE_PLAN_CD )
                                and (xx.rate_plan_cd <> ' ' and xx.rate_plan_cd is not null)
                                then xx.rate_plan_cd
                                else 
                                        case when a.new_rate_plan_cd = ' ' and a.rate_plan_cd <> ' '
                                        then a.rate_plan_cd 
                                        else a.new_rate_plan_cd
                                        end
                                end as new_rate_plan_cd, 
                                b.FREE_DATA_ENTITLE, a.MKT_CD, a.REF, a.online_inv, 
                                a.orig_ld_revenue,a.new_ld_revenue, a.subr_stat_cd, 
                                case when (xx.eff_date = date '1900-01-01' or xx.eff_date = date '2999-12-31' or xx.eff_date is null)
                                then a.eff_date
                                else xx.eff_date
                                end as eff_date
                                from
                                ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_003A2_TMP a
                                left join ${etlvar::MIGDB}.U_RBD_KPI_REPORT_MAP_INV_ORDER xx
                                on 
                                a.cust_num = xx.cust_num
                                and a.subr_num = xx.subr_num
                                and a.order_num = xx.inv_num
                                left join
                                ${etlvar::ADWDB}.rate_plan_ref b
                                on      (
                                case when (xx.rate_plan_cd <> a.RATE_PLAN_CD )
                                and (xx.rate_plan_cd <> ' ' and xx.rate_plan_cd is not null)
                                then xx.rate_plan_cd
                                else a.rate_plan_cd
                                end
                                ) = b.rate_plan_cd
                        )  a
                left join
                ${etlvar::ADWDB}.rate_plan_ref x
                on a.rate_plan_cd = x.rate_plan_cd
                left join 
                ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP y
                on a.store_cd = y.store_cd
                left join (
                select * from ${etlvar::ADWDB}.pos_inv_header 
                where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                ) z
                on a.order_num = z.inv_num
                left join ${etlvar::ADWDB}.fes_usr_info zz
                on z.salesman_cd = zz.usr_name
)
;

--Add ld
INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TRX_TMP001 
(
        CUST_NUM 
        ,SUBR_NUM 
        ,STORE_CD 
        ,RATE_PLAN_CD 
        ,ORDER_NUM 
        ,ORIG_LD_CD 
        ,NEW_LD_CD 
        ,ORIG_LD_EXP_DATE 
        ,NEW_LD_EXP_DATE 
        ,ORIG_RATE_PLAN_CD 
        ,NEW_RATE_PLAN_CD 
        ,INV_DATE 
        ,MKT_CD 
        ,REF 
        ,RATE_PLAN_GRP 
        ,TRX_TYPE 
        ,FREE_DATA_ENTITLE 
        ,CONTACTED_BY_CALL_CENTER 
        ,ONLINE_INV 
        ,REPORT_DATE 
        ,DAILY_SALES_SMS 
        ,EFF_DATE 
        ,CREATE_BY 
        ,ORIG_LD_REVENUE 
        ,NEW_LD_REVENUE 
        ,SUBR_STAT_CD 
        ,FA_INVOICE_FLG 
        ,ADD_VAS 
        ,ORIG_CREATE_BY 
        ,ORIG_HANDLE_BY 
        ,END_VAS
        ,ACTIVE_BOLT_ON_VAS 
        ,BOLT_ON_FLG 
        ,ADD_VAS_FROM_OM
)
select /*+ parallel(32) */ 
distinct
        CUST_NUM,       SUBR_NUM 
        ,store_cd
        ,RATE_PLAN_CD, ORDER_NUM, ORIG_LD_CD, NEW_LD_CD ,ORIG_LD_EXP_DATE, NEW_LD_EXP_DATE
        ,ORIG_RATE_PLAN_CD, NEW_RATE_PLAN_CD, CREATE_DATE, MKT_CD, REF
        ,RATE_PLAN_GRP, TRX_TYPE, FREE_DATA_ENTITLE, CONTACTED_BY_CALL_CENTER
        ,case when upper(create_by) like '%ONLINE%'
        then 'Y'
        else 'N'
        end as ONLINE_INV
        ,create_date+1 as report_date
        ,daily_sales_sms,  eff_date, create_by, orig_ld_revenue,new_ld_revenue, subr_stat_cd,
        case when create_by like 'FA%'
        then 'Y'
        else 'N'
        end as FA_INVOICE_FLG
        ,add_vas, ORIG_CREATE_BY, ORIG_HANDLE_BY
        ,' ' as END_VAS, ' ' as BOLT_ON_VAS, ' ' as BOLT_ON_FLG, ' ' as ADD_VAS
from (
                select /*+ parallel(32) */ 
                a.*
                , case
                when a.free_data_entitle = 'Mass Mobile True unlimited data plans' then '01. True unlimited data plans'
                when a.free_data_entitle = 'Mass Mobile 5GB FUP unlimited data plans' then '02. Unlimited data plans'
                when a.free_data_entitle = 'Mass Mobile 5GB or above plans WITH add-on unlimited data feature' then '03. High tier (5GB or above) plans with add-on pack'
                when a.free_data_entitle = 'Mass Mobile 5GB or above drop dead plans WITHOUT add-on unlimited data feature (exclude Family plans)' then '04. High tier (5GB or above) plans without add-on pack'
                when a.free_data_entitle = 'Mass Mobile Family plans (Main SIM only)' then '05. Family Plan (Main SIM)'
                when a.free_data_entitle = 'Mass Mobile Family plans (Secondary SIM only)' then '06. Family plans (Secondary SIM)' 
                when a.free_data_entitle = 'Mass Mobile 1.5GB-4.9GB drop dead plans' then '07. Mid tier plans (1.5GB - 4.9GB)'
                when a.free_data_entitle = 'Mass Mobile 500MB-1.4GB drop dead plans' then '08. Upper low tier plans (500MB - 1.4GB)'
                when a.free_data_entitle = 'Mass Mobile below 500MB drop dead plans' then '09. Low tier plans (below 500MB)'
                when a.free_data_entitle = 'Mass Mobile 1GB (up to 42Mbps) + unlimited (up to 128kbps) plans' then '10. ExtraCare plans'
                when a.free_data_entitle = 'Mass Mobile 1GB + unlimited plans' then '10. ExtraCare plans'
                when a.free_data_entitle = 'Mass Mobile ExtraCare plans' then '10. ExtraCare plans'
                when a.free_data_entitle = 'Mass Mobile Speed capped unlimited data (up to 2Mbps) plans' then '11. Speed capped unlimited data plans'
                when a.free_data_entitle = 'Mass Mobile Speed capped unlimited data (up to 384kbps) plans' then '11. Speed capped unlimited data plans'
                when a.free_data_entitle = 'Mass Mobile Speed capped unlimited data (up to 768kbps) plans' then '11. Speed capped unlimited data plans'
                when x.rate_plan_grp = 'Residential Fibre Broadband 100' then '13. Fiber Broadband:100MB'
                when x.rate_plan_grp = 'Residential Fibre Broadband 500' then '14. Fiber Broadband:500MB'
                when x.rate_plan_grp = 'Residential Fibre Broadband 1000' then '15. Fiber Broadband:1000MB'
                when substr(x.rate_plan_grp, 1, 11) = 'FBB HomeTel' then '16. HomeTel'
                when (substr(x.rate_plan_grp, 1, 9) = 'HomePhone' or x.rate_plan_grp = 'Jupiter') then '17. HPP'
                when a.free_data_entitle = 'Mass Mobile 5GB or above drop dead plans WITHOUT add-on unlimited data feature' 
                then '04. High tier (5GB or above) plans without add-on pack'
                when a.free_data_entitle = 'Mass Mobile 5GB or above plans WITHOUT add-on pack (exclude Family plans)' 
                then '04. High tier (5GB or above) plans without add-on pack'
                when a.free_data_entitle = 'Mass Mobile 5GB or above plans WITH add-on pack (exclude Family plans)' 
                then '03. High tier (5GB or above) plans with add-on pack'
                when a.free_data_entitle = 'Mass Mobile 5GB or above plans WITH add-on pack' 
                then '03. High tier (5GB or above) plans with add-on pack'
                when a.free_data_entitle = 'Mass Mobile 5GB or above plans (exclude Family plans)' 
                then '04. High tier (5GB or above) plans without add-on pack'
                else '12. Other plans'
                end  Rate_Plan_Grp
                --Change plan only
                ,case when ref ='Chg plan only'
                then 'UPGRADE'
                when (a.orig_ld_cd ='NA' or a.orig_ld_cd = ' ') or 
                (a.orig_ld_exp_date = date '1900-01-01' or a.orig_ld_exp_date = date '2999-12-31')
                then 'RETENTION'
                --To be expired within 3 months or already expired
                when months_between(trunc(a.orig_ld_exp_date,'MONTH'), trunc(a.create_date,'MONTH')) <= 3
                then 'RETENTION'
                --To be expired more than 3 months
                when months_between(trunc(a.orig_ld_exp_date,'MONTH'), trunc(a.create_date,'MONTH')) > 3
                then 'UPGRADE'
                --No ld_cd or no expire_date
                else 'RETENTION'
                end as trx_type
                ,case when (a.cust_num, a.subr_num) in (select cust_num, subr_num from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_002A2_TMP)
                then 'Y'
                else 'N'
                end as CONTACTED_BY_CALL_CENTER
                ,y.daily_sales_sms
                ,z.eff_date
                ,case when xx.salesman_flg = 'Y'
                 then z.create_by
                 else z.handle_by
                 end as create_by
                ,z.create_by as orig_create_by
                ,z.handle_by as orig_handle_by
                from (
                                select distinct a.cust_num,a.subr_num,a.store_cd,a.order_num, a.add_vas,
                                a.rate_plan_cd,
                                a.NEW_LD_CD,a.ORIG_LD_CD,a.Orig_ld_exp_date,
                                a.create_date,
                                a.ORIG_RATE_PLAN_CD,
                                a.NEW_RATE_PLAN_CD,
                                a.orig_ld_revenue,
                                a.new_ld_revenue,
                                a.subr_stat_cd,
                                b.free_data_entitle
                                ,'N' as online_inv
                                ,NEW_LD_EXP_DATE
                                ,MKT_CD
                                ,REF
                                from
                                ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004C2_TMP a
                                left join
                                ${etlvar::ADWDB}.rate_plan_ref b
                                on a.rate_plan_cd = b.rate_plan_cd
                )  a
                left join
                ${etlvar::ADWDB}.rate_plan_ref x
                on a.rate_plan_cd = x.rate_plan_cd
                left join 
                ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP y
                on a.store_cd = y.store_cd
                left join (
                select * from ${etlvar::ADWDB}.om_complete_chg_plan
                where create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and 
                TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                union all
                select * from ${etlvar::ADWDB}.om_pending_chg_plan
                where create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and 
                TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                ) z
                on a.order_num = z.case_id
                left join ${etlvar::ADWDB}.fes_usr_info xx
                on z.create_by = xx.usr_name
                left join ${etlvar::ADWDB}.fes_usr_info yy
                on z.handle_by = yy.usr_name
)
;

--FL Order
INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TRX_TMP001 
(
        CUST_NUM 
        ,SUBR_NUM 
        ,STORE_CD 
        ,RATE_PLAN_CD 
        ,ORDER_NUM 
        ,ORIG_LD_CD 
        ,NEW_LD_CD 
        ,ORIG_LD_EXP_DATE 
        ,NEW_LD_EXP_DATE 
        ,ORIG_RATE_PLAN_CD 
        ,NEW_RATE_PLAN_CD 
        ,INV_DATE 
        ,MKT_CD 
        ,REF 
        ,RATE_PLAN_GRP 
        ,TRX_TYPE 
        ,FREE_DATA_ENTITLE 
        ,CONTACTED_BY_CALL_CENTER 
        ,ONLINE_INV 
        ,REPORT_DATE 
        ,DAILY_SALES_SMS 
        ,EFF_DATE 
        ,CREATE_BY 
        ,ORIG_LD_REVENUE 
        ,NEW_LD_REVENUE 
        ,SUBR_STAT_CD 
        ,FA_INVOICE_FLG 
        ,ADD_VAS 
        ,ORIG_CREATE_BY 
        ,ORIG_HANDLE_BY 
        ,END_VAS
        ,ACTIVE_BOLT_ON_VAS 
        ,BOLT_ON_FLG 
        ,ADD_VAS_FROM_OM

)
select /*+ parallel(32) */ 
distinct
        CUST_NUM,       SUBR_NUM 
        ,store_cd
        ,RATE_PLAN_CD, ORDER_NUM, ORIG_LD_CD, NEW_LD_CD ,ORIG_LD_EXP_DATE, NEW_LD_EXP_DATE
        ,ORIG_RATE_PLAN_CD, NEW_RATE_PLAN_CD, CREATE_DATE, MKT_CD, REF
        ,RATE_PLAN_GRP, TRX_TYPE, FREE_DATA_ENTITLE, CONTACTED_BY_CALL_CENTER
        ,case when upper(create_by) like '%ONLINE%'
        then 'Y'
        else 'N'
        end as ONLINE_INV
        ,create_date+1 as report_date
        ,daily_sales_sms,  eff_date, create_by, orig_ld_revenue,new_ld_revenue, subr_stat_cd,
        case when create_by like 'FA%'
        then 'Y'
        else 'N'
        end as FA_INVOICE_FLG
        ,add_vas, ORIG_CREATE_BY, ORIG_HANDLE_BY
        ,' ' as END_VAS, ' ' as ACTIVE_BOLT_ON_VAS, ' ' as BOLT_ON_FLG, ' ' as ADD_VAS_FROM_OM
from (
                select a.*
                , case
                when a.free_data_entitle = 'Mass Mobile True unlimited data plans' then '01. True unlimited data plans'
                when a.free_data_entitle = 'Mass Mobile 5GB FUP unlimited data plans' then '02. Unlimited data plans'
                when a.free_data_entitle = 'Mass Mobile 5GB or above plans WITH add-on unlimited data feature' then '03. High tier (5GB or above) plans with add-on pack'
                when a.free_data_entitle = 'Mass Mobile 5GB or above drop dead plans WITHOUT add-on unlimited data feature (exclude Family plans)' then '04. High tier (5GB or above) plans without add-on pack'
                when a.free_data_entitle = 'Mass Mobile Family plans (Main SIM only)' then '05. Family Plan (Main SIM)'
                when a.free_data_entitle = 'Mass Mobile Family plans (Secondary SIM only)' then '06. Family plans (Secondary SIM)' 
                when a.free_data_entitle = 'Mass Mobile 1.5GB-4.9GB drop dead plans' then '07. Mid tier plans (1.5GB - 4.9GB)'
                when a.free_data_entitle = 'Mass Mobile 500MB-1.4GB drop dead plans' then '08. Upper low tier plans (500MB - 1.4GB)'
                when a.free_data_entitle = 'Mass Mobile below 500MB drop dead plans' then '09. Low tier plans (below 500MB)'
                when a.free_data_entitle = 'Mass Mobile 1GB (up to 42Mbps) + unlimited (up to 128kbps) plans' then '10. ExtraCare plans'
                when a.free_data_entitle = 'Mass Mobile 1GB + unlimited plans' then '10. ExtraCare plans'
                when a.free_data_entitle = 'Mass Mobile ExtraCare plans' then '10. ExtraCare plans'
                when a.free_data_entitle = 'Mass Mobile Speed capped unlimited data (up to 2Mbps) plans' then '11. Speed capped unlimited data plans'
                when a.free_data_entitle = 'Mass Mobile Speed capped unlimited data (up to 384kbps) plans' then '11. Speed capped unlimited data plans'
                when a.free_data_entitle = 'Mass Mobile Speed capped unlimited data (up to 768kbps) plans' then '11. Speed capped unlimited data plans'
                when x.rate_plan_grp = 'Residential Fibre Broadband 100' then '13. Fiber Broadband:100MB'
                when x.rate_plan_grp = 'Residential Fibre Broadband 500' then '14. Fiber Broadband:500MB'
                when x.rate_plan_grp = 'Residential Fibre Broadband 1000' then '15. Fiber Broadband:1000MB'
                when substr(x.rate_plan_grp, 1, 11) = 'FBB HomeTel' then '16. HomeTel'
                when (substr(x.rate_plan_grp, 1, 9) = 'HomePhone' or x.rate_plan_grp = 'Jupiter') then '17. HPP'
                when a.free_data_entitle = 'Mass Mobile 5GB or above drop dead plans WITHOUT add-on unlimited data feature' 
                then '04. High tier (5GB or above) plans without add-on pack'
                when a.free_data_entitle = 'Mass Mobile 5GB or above plans WITHOUT add-on pack (exclude Family plans)' 
                then '04. High tier (5GB or above) plans without add-on pack'
                when a.free_data_entitle = 'Mass Mobile 5GB or above plans WITH add-on pack (exclude Family plans)' 
                then '03. High tier (5GB or above) plans with add-on pack'
                when a.free_data_entitle = 'Mass Mobile 5GB or above plans WITH add-on pack' 
                then '03. High tier (5GB or above) plans with add-on pack'
                when a.free_data_entitle = 'Mass Mobile 5GB or above plans (exclude Family plans)' 
                then '04. High tier (5GB or above) plans without add-on pack'
                else '12. Other plans'
                end  Rate_Plan_Grp
                ,case when ref ='Chg plan only'
                then 'UPGRADE'
                when (a.orig_ld_cd ='NA' or a.orig_ld_cd = ' ') or 
                (a.orig_ld_exp_date = date '1900-01-01' or a.orig_ld_exp_date = date '2999-12-31')
                then 'RETENTION'
                --To be expired within 3 months or already expired
                when months_between(trunc(a.orig_ld_exp_date,'MONTH'), trunc(a.create_date,'MONTH')) <= 3
                then 'RETENTION'
                --To be expired more than 3 months
                when months_between(trunc(a.orig_ld_exp_date,'MONTH'), trunc(a.create_date,'MONTH')) > 3
                then 'UPGRADE'
                --No ld_cd or no expire_date
                else 'RETENTION'
                end as trx_type
                ,case when (a.cust_num, a.subr_num) in (select cust_num, subr_num from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_002A2_TMP)
                then 'Y'
                else 'N'
                end as CONTACTED_BY_CALL_CENTER    
                ,y.daily_sales_sms
                ,z.eff_date
                ,case when xx.salesman_flg = 'Y'
                 then z.create_by
                 else z.handle_by
                 end as create_by
                ,z.create_by as orig_create_by
                ,z.handle_by as orig_handle_by                            
                from (
                        select /*+ parallel(32) */ 
                        distinct a.cust_num,a.subr_num,a.store_cd,a.order_num as order_num,
                        a.rate_plan_cd,
                           a.NEW_LD_CD,a.ORIG_LD_CD,a.Orig_ld_exp_date,
                        a.create_date,
                        a.ORIG_RATE_PLAN_CD,
                        a.NEW_RATE_PLAN_CD,
                        a.orig_ld_revenue,
                        a.new_ld_revenue,
                        a.subr_stat_cd,
                           b.free_data_entitle
                        , NEW_LD_EXP_DATE
                        , 'N' as online_inv
                        , REF
                        , MKT_CD
                        , ' ' as add_vas
                        from
                        ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004D2_TMP a
                        left join
                        ${etlvar::ADWDB}.rate_plan_ref b
                        on a.rate_plan_cd = b.rate_plan_cd
                )  a
                left join
                ${etlvar::ADWDB}.rate_plan_ref x
                on a.rate_plan_cd = x.rate_plan_cd
                left join 
                ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP y
                on a.store_cd = y.store_cd
                left join (
                        select * from ${etlvar::ADWDB}.fl_om_complete_chg_plan
                        where create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and 
                        TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                        union all
                        select * from ${etlvar::ADWDB}.fl_om_pending_chg_plan
                        where create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and 
                        TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                ) z
                on a.order_num = z.case_id
                left join ${etlvar::ADWDB}.fes_usr_info xx
                on z.create_by = xx.usr_name
                left join ${etlvar::ADWDB}.fes_usr_info yy
                on z.handle_by = yy.usr_name
)
;

--Change plan only
INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TRX_TMP001 
(
        CUST_NUM 
        ,SUBR_NUM 
        ,STORE_CD 
        ,RATE_PLAN_CD 
        ,ORDER_NUM 
        ,ORIG_LD_CD 
        ,NEW_LD_CD 
        ,ORIG_LD_EXP_DATE 
        ,NEW_LD_EXP_DATE 
        ,ORIG_RATE_PLAN_CD 
        ,NEW_RATE_PLAN_CD 
        ,INV_DATE 
        ,MKT_CD 
        ,REF 
        ,RATE_PLAN_GRP 
        ,TRX_TYPE 
        ,FREE_DATA_ENTITLE 
        ,CONTACTED_BY_CALL_CENTER 
        ,ONLINE_INV 
        ,REPORT_DATE 
        ,DAILY_SALES_SMS 
        ,EFF_DATE 
        ,CREATE_BY 
        ,ORIG_LD_REVENUE 
        ,NEW_LD_REVENUE 
        ,SUBR_STAT_CD 
        ,FA_INVOICE_FLG 
        ,ADD_VAS 
        ,ORIG_CREATE_BY 
        ,ORIG_HANDLE_BY 
        ,END_VAS
        ,ACTIVE_BOLT_ON_VAS 
        ,BOLT_ON_FLG 
        ,ADD_VAS_FROM_OM
)
select /*+ parallel(32) */ 
distinct
CUST_NUM,       SUBR_NUM 
,store_cd
,RATE_PLAN_CD, ORDER_NUM, ORIG_LD_CD, NEW_LD_CD ,ORIG_LD_EXP_DATE, NEW_LD_EXP_DATE
,ORIG_RATE_PLAN_CD, NEW_RATE_PLAN_CD, CREATE_DATE, MKT_CD, REF
,RATE_PLAN_GRP, TRX_TYPE, FREE_DATA_ENTITLE, CONTACTED_BY_CALL_CENTER
,case when upper(create_by) like '%ONLINE%'
then 'Y'
else 'N'
end as ONLINE_INV
,create_date+1 as report_date
,daily_sales_sms,  eff_date, create_by, orig_ld_revenue,new_ld_revenue, subr_stat_cd,
case when create_by like 'FA%'
then 'Y'
else 'N'
end as FA_INVOICE_FLG
,add_vas, ORIG_CREATE_BY, ORIG_HANDLE_BY
,' ' as END_VAS, ' ' as ACTIVE_BOLT_ON_VAS, ' ' as BOLT_ON_FLG, ' ' as ADD_VAS_FROM_OM
from (
        select a.*
        ,case
        when a.free_data_entitle = 'Mass Mobile True unlimited data plans' then '01. True unlimited data plans'
        when a.free_data_entitle = 'Mass Mobile 5GB FUP unlimited data plans' then '02. Unlimited data plans'
        when a.free_data_entitle = 'Mass Mobile 5GB or above plans WITH add-on unlimited data feature' then '03. High tier (5GB or above) plans with add-on pack'
        when a.free_data_entitle = 'Mass Mobile 5GB or above drop dead plans WITHOUT add-on unlimited data feature (exclude Family plans)' then '04. High tier (5GB or above) plans without add-on pack'
        when a.free_data_entitle = 'Mass Mobile Family plans (Main SIM only)' then '05. Family Plan (Main SIM)'
        when a.free_data_entitle = 'Mass Mobile Family plans (Secondary SIM only)' then '06. Family plans (Secondary SIM)' 
        when a.free_data_entitle = 'Mass Mobile 1.5GB-4.9GB drop dead plans' then '07. Mid tier plans (1.5GB - 4.9GB)'
        when a.free_data_entitle = 'Mass Mobile 500MB-1.4GB drop dead plans' then '08. Upper low tier plans (500MB - 1.4GB)'
        when a.free_data_entitle = 'Mass Mobile below 500MB drop dead plans' then '09. Low tier plans (below 500MB)'
        when a.free_data_entitle = 'Mass Mobile 1GB (up to 42Mbps) + unlimited (up to 128kbps) plans' then '10. ExtraCare plans'
        when a.free_data_entitle = 'Mass Mobile 1GB + unlimited plans' then '10. ExtraCare plans'
        when a.free_data_entitle = 'Mass Mobile ExtraCare plans' then '10. ExtraCare plans'
        when a.free_data_entitle = 'Mass Mobile Speed capped unlimited data (up to 2Mbps) plans' then '11. Speed capped unlimited data plans'
        when a.free_data_entitle = 'Mass Mobile Speed capped unlimited data (up to 384kbps) plans' then '11. Speed capped unlimited data plans'
        when a.free_data_entitle = 'Mass Mobile Speed capped unlimited data (up to 768kbps) plans' then '11. Speed capped unlimited data plans'
        when x.rate_plan_grp = 'Residential Fibre Broadband 100' then '13. Fiber Broadband:100MB'
        when x.rate_plan_grp = 'Residential Fibre Broadband 500' then '14. Fiber Broadband:500MB'
        when x.rate_plan_grp = 'Residential Fibre Broadband 1000' then '15. Fiber Broadband:1000MB'
        when substr(x.rate_plan_grp, 1, 11) = 'FBB HomeTel' then '16. HomeTel'
        when (substr(x.rate_plan_grp, 1, 9) = 'HomePhone' or x.rate_plan_grp = 'Jupiter') then '17. HPP'
        when a.free_data_entitle = 'Mass Mobile 5GB or above drop dead plans WITHOUT add-on unlimited data feature' 
        then '04. High tier (5GB or above) plans without add-on pack'
        when a.free_data_entitle = 'Mass Mobile 5GB or above plans WITHOUT add-on pack (exclude Family plans)' 
        then '04. High tier (5GB or above) plans without add-on pack'
        when a.free_data_entitle = 'Mass Mobile 5GB or above plans WITH add-on pack (exclude Family plans)' 
        then '03. High tier (5GB or above) plans with add-on pack'
        when a.free_data_entitle = 'Mass Mobile 5GB or above plans WITH add-on pack' 
        then '03. High tier (5GB or above) plans with add-on pack'
        when a.free_data_entitle = 'Mass Mobile 5GB or above plans (exclude Family plans)' 
        then '04. High tier (5GB or above) plans without add-on pack'
        else '12. Other plans'
        end  Rate_Plan_Grp
        ,'UPGRADE' Trx_Type
        ,case when (a.cust_num, a.subr_num) in (select cust_num, subr_num from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_002A2_TMP)
        then 'Y'
        else 'N'
        end as CONTACTED_BY_CALL_CENTER                 
        ,y.daily_sales_sms
        ,z.eff_date
        ,case when xx.salesman_flg = 'Y'
         then z.create_by
         else z.handle_by
         end as create_by
        ,z.create_by as orig_create_by
        ,z.handle_by as orig_handle_by
        from (
                select /*+ parallel(32) */ 
                distinct a.cust_num,a.subr_num,a.store_cd,a.order_num, a.add_vas,
                a.rate_plan_cd,
                a.NEW_LD_CD,a.ORIG_LD_CD,a.Orig_ld_exp_date,
                a.create_date,
                a.ORIG_RATE_PLAN_CD,
                a.NEW_RATE_PLAN_CD,
                a.ld_revenue as orig_ld_revenue,
                a.ld_revenue as new_ld_revenue,
                a.subr_stat_cd,
                b.free_data_entitle
                , NEW_LD_EXP_DATE
                , 'N' as online_inv
                , REF
                , MKT_CD
                from
                ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004A2_TMP a
                left join
                ${etlvar::ADWDB}.rate_plan_ref b
                on a.rate_plan_cd = b.rate_plan_cd
        )  a
        left join
        ${etlvar::ADWDB}.rate_plan_ref x
        on a.rate_plan_cd = x.rate_plan_cd
        left join 
        ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP y
        on a.store_cd = y.store_cd
        left join (
                select * from ${etlvar::ADWDB}.om_complete_chg_plan
                where create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and 
                TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                union all
                select * from ${etlvar::ADWDB}.om_pending_chg_plan
                where create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and 
                TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
        ) z
        on a.order_num = z.case_id
        left join ${etlvar::ADWDB}.fes_usr_info xx
        on z.create_by = xx.usr_name
        left join ${etlvar::ADWDB}.fes_usr_info yy
        on z.handle_by = yy.usr_name
)
;

update ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TRX_TMP001
set orig_ld_exp_date = date '1900-01-01'
where orig_ld_exp_date = date '2999-12-31'
;

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



sub runSQLPLUS2{
  my $SQLCMD_FILE="${etlvar::AUTO_GEN_TEMP_PATH}${etlvar::ETLJOBNAME}_sqlcmd.sql";
  open SQLCMD, ">" . $SQLCMD_FILE || die "Cannot open file" ;
  print SQLCMD<<ENDOFINPUT;
        ${etlvar::LOGON_TD}
        ${etlvar::SET_MAXERR}
        ${etlvar::SET_ERRLVL_1}
        ${etlvar::SET_ERRLVL_2}
        set define off;

--Please type your SQL statement here

TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TRX_TMP001;
TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_MAP_INV_ORDER;

--Map Invoice and OM Orders
INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_MAP_INV_ORDER (
        INV_NUM           
        ,CUST_NUM          
        ,SUBR_NUM          
        ,INV_DATE        
        ,POS_SHOP_CD
        ,SALESMAN_CD       
        ,LD_CD             
        ,MKT_CD            
        ,RATE_PLAN_CD      
        ,EFF_DATE          
        ,LD_EXP_DATE
)
select 
        INV_NUM           
        ,CUST_NUM          
        ,SUBR_NUM          
        ,INV_DATE        
        ,store_cd
        ,SALESMAN_CD       
        ,LD_CD             
        ,MKT_CD            
        ,RATE_PLAN_CD      
        ,EFF_DATE 
        ,LD_EXPIRY_DATE
from 
(
        select /*+ parallel(32) */ 
        distinct inv_num, a.cust_num, a.subr_num, 
        a.inv_date,a.store_cd,d.salesman_cd, 
        case when a.ld_cd <> ' '
        then a.ld_cd
        else c.ld_cd
        end as ld_cd, a.mkt_cd, --rate_plan_cd,
        c.new_plan_cd as RATE_PLAN_CD, c.eff_date
        ,d.ld_expiry_date
        ,row_number() over (partition by a.cust_num, a.subr_num, a.order_num order by c.eff_date desc , c.eff_time desc) as rn
        from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_003A1_TMP a
        left join ${etlvar::ADWDB}.mkt_ref_vw b
        on a.mkt_cd = b.mkt_cd
        left join (
        select * from ${etlvar::ADWDB}.om_complete_chg_plan
        where create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
        union all
        select * from ${etlvar::ADWDB}.om_pending_chg_plan
        where create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
        ) c
        on a.cust_num = c.cust_num
        and a.subr_num = c.subr_num
        and a.inv_date = c.create_date
        left join (
        select * from ${etlvar::ADWDB}.pos_inv_header 
        where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
        ) d
        on a.cust_num = d.cust_num
        and a.subr_num = d.subr_num
        and a.order_num = d.inv_num
        where 
        c.create_date >= c.eff_date
        and c.new_plan_cd <> ' '
        and a.order_num like 'I%'
) where rn = 1
;

INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_MAP_INV_ORDER (
        INV_NUM           
        ,CUST_NUM          
        ,SUBR_NUM          
        ,INV_DATE        
        ,POS_SHOP_CD
        ,SALESMAN_CD       
        ,LD_CD             
        ,MKT_CD            
        ,RATE_PLAN_CD      
        ,EFF_DATE     
        ,LD_EXP_DATE
)
select 
        INV_NUM           
        ,CUST_NUM          
        ,SUBR_NUM          
        ,INV_DATE        
        ,store_cd
        ,SALESMAN_CD       
        ,LD_CD             
        ,MKT_CD            
        ,RATE_PLAN_CD      
        ,EFF_DATE 
        ,LD_EXPIRY_DATE
from 
(
        select /*+ parallel(32) */ 
        distinct a.order_num as inv_num, a.cust_num, a.subr_num, 
        a.inv_date,a.store_cd,d.salesman_cd, 
        case when a.ld_cd <> ' '
        then a.ld_cd
        else d.ld_cd
        end as ld_cd, a.mkt_cd, --rate_plan_cd,
        d.rate_plan_cd as RATE_PLAN_CD, d.inv_date as eff_date
        ,d.ld_expiry_date
        ,row_number() over (partition by a.cust_num, a.subr_num, a.order_num order by a.inv_date desc) as rn
        from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_003A1_TMP a
        left join ${etlvar::ADWDB}.mkt_ref_vw b
        on a.mkt_cd = b.mkt_cd
        left join (
        select * from ${etlvar::ADWDB}.om_complete_chg_plan
        where create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
        union all
        select * from ${etlvar::ADWDB}.om_pending_chg_plan
        where create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
        ) c
        on a.cust_num = c.cust_num
        and a.subr_num = c.subr_num
        and a.inv_date = c.create_date
        left join (
        select * from ${etlvar::ADWDB}.pos_inv_header 
        where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
        ) d
        on a.cust_num = d.cust_num
        and a.subr_num = d.subr_num
        and a.inv_date = d.inv_date
        where 
        c.case_id is null and d.inv_num like 'F%'
        and ((a.rate_plan_cd = ' ' and d.rate_plan_cd <> ' ')
        or (a.ld_cd = ' ' and d.ld_cd <> ' '))
) where rn = 1
;
commit;

--I invoice
        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TRX_TMP001 
        (
                CUST_NUM 
                ,SUBR_NUM 
                ,STORE_CD 
                ,RATE_PLAN_CD 
                ,ORDER_NUM 
                ,ORIG_LD_CD 
                ,NEW_LD_CD 
                ,ORIG_LD_EXP_DATE 
                ,NEW_LD_EXP_DATE 
                ,ORIG_RATE_PLAN_CD 
                ,NEW_RATE_PLAN_CD 
                ,INV_DATE 
                ,MKT_CD 
                ,REF 
                ,RATE_PLAN_GRP 
                ,TRX_TYPE 
                ,FREE_DATA_ENTITLE 
                ,CONTACTED_BY_CALL_CENTER 
                ,ONLINE_INV 
                ,REPORT_DATE 
                ,DAILY_SALES_SMS 
                ,EFF_DATE 
                ,CREATE_BY 
                ,ORIG_LD_REVENUE 
                ,NEW_LD_REVENUE 
                ,SUBR_STAT_CD 
                ,FA_INVOICE_FLG 
                ,ADD_VAS 
                ,ORIG_CREATE_BY 
                ,ORIG_HANDLE_BY 
                ,ADD_VAS_FROM_OM
                ,END_VAS
                ,BOLT_ON_FLG 
                ,ACTIVE_BOLT_ON_VAS 
        )
        select 
        distinct /*+ parallel(32) */ 
                CUST_NUM, SUBR_NUM, STORE_CD, RATE_PLAN_CD, ORDER_NUM, 
                ORIG_LD_CD, NEW_LD_CD, ORIG_LD_EXP_DATE, NEW_LD_EXP_DATE, ORIG_RATE_PLAN_CD, NEW_RATE_PLAN_CD, INV_DATE,
                MKT_CD,         REF,    RATE_PLAN_GRP, TRX_TYPE, FREE_DATA_ENTITLE, CONTACTED_BY_CALL_CENTER, ONLINE_INV 
                ,inv_date+1 as report_date, daily_sales_sms, eff_date, create_by, orig_ld_revenue,new_ld_revenue, subr_stat_cd
                ,FA_INVOICE_FLG, add_vas, ORIG_CREATE_BY, ORIG_HANDLE_BY
                ,ADD_VAS_FROM_OM ,END_VAS , BOLT_ON_FLG, BOLT_ON_VAS
        from (
                        select  /*+ parallel(32) */  a.*
                        , case
                        when a.free_data_entitle = 'Mass Mobile True unlimited data plans' then '01. True unlimited data plans'
                        when a.free_data_entitle = 'Mass Mobile 5GB FUP unlimited data plans' then '02. Unlimited data plans'
                        when a.free_data_entitle = 'Mass Mobile 5GB or above plans WITH add-on unlimited data feature' then '03. High tier (5GB or above) plans with add-on pack'
                        when a.free_data_entitle = 'Mass Mobile 5GB or above drop dead plans WITHOUT add-on unlimited data feature (exclude Family plans)' then '04. High tier (5GB or above) plans without add-on pack'
                        when a.free_data_entitle = 'Mass Mobile Family plans (Main SIM only)' then '05. Family Plan (Main SIM)'
                        when a.free_data_entitle = 'Mass Mobile Family plans (Secondary SIM only)' then '06. Family plans (Secondary SIM)' 
                        when a.free_data_entitle = 'Mass Mobile 1.5GB-4.9GB drop dead plans' then '07. Mid tier plans (1.5GB - 4.9GB)'
                        when a.free_data_entitle = 'Mass Mobile 500MB-1.4GB drop dead plans' then '08. Upper low tier plans (500MB - 1.4GB)'
                        when a.free_data_entitle = 'Mass Mobile below 500MB drop dead plans' then '09. Low tier plans (below 500MB)'
                        when a.free_data_entitle = 'Mass Mobile 1GB (up to 42Mbps) + unlimited (up to 128kbps) plans' then '10. ExtraCare plans'
                        when a.free_data_entitle = 'Mass Mobile 1GB + unlimited plans' then '10. ExtraCare plans'
                        when a.free_data_entitle = 'Mass Mobile ExtraCare plans' then '10. ExtraCare plans'
                        when a.free_data_entitle = 'Mass Mobile Speed capped unlimited data (up to 2Mbps) plans' then '11. Speed capped unlimited data plans'
                        when a.free_data_entitle = 'Mass Mobile Speed capped unlimited data (up to 384kbps) plans' then '11. Speed capped unlimited data plans'
                        when a.free_data_entitle = 'Mass Mobile Speed capped unlimited data (up to 768kbps) plans' then '11. Speed capped unlimited data plans'
                        when x.rate_plan_grp = 'Residential Fibre Broadband 100' then '13. Fiber Broadband:100MB'
                        when x.rate_plan_grp = 'Residential Fibre Broadband 500' then '14. Fiber Broadband:500MB'
                        when x.rate_plan_grp = 'Residential Fibre Broadband 1000' then '15. Fiber Broadband:1000MB'
                        when substr(x.rate_plan_grp, 1, 11) = 'FBB HomeTel' then '16. HomeTel'
                        when (substr(x.rate_plan_grp, 1, 9) = 'HomePhone' or x.rate_plan_grp = 'Jupiter') then '17. HPP'
                        when a.free_data_entitle = 'Mass Mobile 5GB or above drop dead plans WITHOUT add-on unlimited data feature' 
                        then '04. High tier (5GB or above) plans without add-on pack'
                        when a.free_data_entitle = 'Mass Mobile 5GB or above plans WITHOUT add-on pack (exclude Family plans)' 
                        then '04. High tier (5GB or above) plans without add-on pack'
                        when a.free_data_entitle = 'Mass Mobile 5GB or above plans WITH add-on pack (exclude Family plans)' 
                        then '03. High tier (5GB or above) plans with add-on pack'
                        when a.free_data_entitle = 'Mass Mobile 5GB or above plans WITH add-on pack' 
                        then '03. High tier (5GB or above) plans with add-on pack'
                        when a.free_data_entitle = 'Mass Mobile 5GB or above plans (exclude Family plans)' 
                        then '04. High tier (5GB or above) plans without add-on pack'
                        else '12. Other plans'
                        end  Rate_Plan_Grp
                        --Change plan only
                        ,case when ref ='Chg plan only'
                        then 'UPGRADE'
                        --No ld_cd or no expire_date
                        when (a.orig_ld_cd ='NA' or a.orig_ld_cd = ' ') or 
                        (a.orig_ld_exp_date = date '1900-01-01' or a.orig_ld_exp_date = date '2999-12-31')
                        then 'RETENTION'
                        --To be expired within 3 months or already expired
                        when months_between(trunc(a.orig_ld_exp_date,'MONTH'), trunc(a.inv_date,'MONTH')) <= 3
                        then 'RETENTION'
                        --To be expired more than 3 months
                        when months_between(trunc(a.orig_ld_exp_date,'MONTH'), trunc(a.inv_date,'MONTH')) > 3
                        then 'UPGRADE'
                        else 'RETENTION'
                        end as trx_type
                        ,case when (a.cust_num, a.subr_num) in (select cust_num, subr_num from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_002A2_TMP)
                        then 'Y'
                        else 'N'
                        end as CONTACTED_BY_CALL_CENTER
                        ,y.daily_sales_sms
                        ,case when zz.salesman_flg <> 'Y'
                        then z.usr_id
                        when z.salesman_cd like 'FA%'
                        then z.usr_id
                        else z.salesman_cd 
                        end as create_by,
                        case when (
                        case when zz.salesman_flg <> 'Y'
                        then z.usr_id
                        else z.salesman_cd 
                        end
                        ) like 'FA%'
                        then 'Y'
                        else 'N'
                        end as FA_INVOICE_FLG,
                        ' ' as add_vas,
                        z.salesman_cd as ORIG_CREATE_BY, 
                        z.usr_id as ORIG_HANDLE_BY
                        from (
                                        select /*+ parallel(32) */ 
                                        distinct a.CUST_NUM, a.SUBR_NUM, a.STORE_CD, 
                                        a.ORDER_NUM, 
                                        case when (xx.rate_plan_cd <> a.RATE_PLAN_CD )
                                        and (xx.rate_plan_cd <> ' ' and xx.rate_plan_cd is not null)
                                        then xx.rate_plan_cd
                                        else a.rate_plan_cd
                                        end as rate_plan_cd,  a.ORIG_LD_CD, 
                                        case when (xx.ld_cd is null or xx.ld_cd = ' ')
                                        then a.NEW_LD_CD
                                        else xx.ld_cd
                                        end as new_ld_cd,
                                        a.ORIG_LD_EXP_DATE, 
                                        case when (xx.ld_exp_date is null or xx.ld_exp_date = date '1900-01-01' or a.NEW_LD_EXP_DATE = date '2999-12-31')
                                        then a.new_ld_exp_date
                                        else xx.ld_exp_date
                                        end as new_ld_exp_date,
                                        a.INV_DATE, 
                                        a.ORIG_RATE_PLAN_CD, 
                                        case when (xx.rate_plan_cd <> a.RATE_PLAN_CD )
                                        and (xx.rate_plan_cd <> ' ' and xx.rate_plan_cd is not null)
                                        then xx.rate_plan_cd
                                        else 
                                                case when a.new_rate_plan_cd = ' ' and a.rate_plan_cd <> ' '
                                                then a.rate_plan_cd 
                                                else a.new_rate_plan_cd
                                                end
                                        end as new_rate_plan_cd, 
                                        x.FREE_DATA_ENTITLE, a.MKT_CD, a.REF, a.online_inv, 
                                        a.orig_ld_revenue,a.new_ld_revenue, a.subr_stat_cd, 
                                        case when (xx.eff_date = date '1900-01-01' or xx.eff_date = date '2999-12-31' or xx.eff_date is null)
                                        then a.eff_date
                                        else xx.eff_date
                                        end as eff_date
                                        ,coalesce(b.ADD_VAS,' ') as ADD_VAS_FROM_OM
                                        ,coalesce(END_VAS,' ') as END_VAS
                                        ,coalesce(BOLT_ON_VAS,' ') as BOLT_ON_VAS
                                        ,coalesce(BOLT_ON_FLG,'N') as BOLT_ON_FLG
                                        from
                                        ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_003A2_TMP a
                                        left join 
                                        ${etlvar::MIGDB}.U_RBD_KPI_REPORT_ADD_VAS_TMP b
                                        on a.cust_num = b.cust_num
                                        and a.subr_num = b.subr_num
                                        and a.order_num = b.order_num
                                        and a.store_cd = b.store_cd
                                        left join ${etlvar::MIGDB}.U_RBD_KPI_REPORT_END_VAS_TMP c
                                        on a.cust_num = c.cust_num
                                        and a.subr_num = c.subr_num
                                        and a.order_num = c.order_num
                                        and a.store_cd = c.store_cd
                                        left join ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_CD_TMP d
                                        on a.cust_num = d.cust_num
                                        and a.subr_num = d.subr_num
                                        and a.order_num = d.order_num
                                        and a.store_cd = d.store_cd
                                        left join ${etlvar::MIGDB}.U_RBD_KPI_REPORT_BOLT_ON_TMP e
                                        on a.cust_num = e.cust_num
                                        and a.subr_num = e.subr_num
                                        and a.order_num = e.order_num
                                        and a.store_cd = e.store_cd
                                        left join ${etlvar::ADWDB}.subr_data_entitle_hist x
                                        on a.cust_num = x.cust_num
                                        and a.subr_num = x.subr_num
                                        and a.inv_date between x.start_date and x.end_date
                                        left join ${etlvar::MIGDB}.U_RBD_KPI_REPORT_MAP_INV_ORDER xx
                                        on 
                                        a.cust_num = xx.cust_num
                                        and a.subr_num = xx.subr_num
                                        and a.order_num = xx.inv_num
                                )  a
                        left join
                        ${etlvar::ADWDB}.rate_plan_ref x
                        on a.rate_plan_cd = x.rate_plan_cd
                        left join 
                        ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP y
                        on a.store_cd = y.store_cd
                        left join (
                        select * from ${etlvar::ADWDB}.pos_inv_header 
                        where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                        ) z
                        on a.order_num = z.inv_num
                        left join ${etlvar::ADWDB}.fes_usr_info zz
                        on z.salesman_cd = zz.usr_name
        );

--Add ld
        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TRX_TMP001 
        (
                CUST_NUM 
                ,SUBR_NUM 
                ,STORE_CD 
                ,RATE_PLAN_CD 
                ,ORDER_NUM 
                ,ORIG_LD_CD 
                ,NEW_LD_CD 
                ,ORIG_LD_EXP_DATE 
                ,NEW_LD_EXP_DATE 
                ,ORIG_RATE_PLAN_CD 
                ,NEW_RATE_PLAN_CD 
                ,INV_DATE 
                ,MKT_CD 
                ,REF 
                ,RATE_PLAN_GRP 
                ,TRX_TYPE 
                ,FREE_DATA_ENTITLE 
                ,CONTACTED_BY_CALL_CENTER 
                ,ONLINE_INV 
                ,REPORT_DATE 
                ,DAILY_SALES_SMS 
                ,EFF_DATE 
                ,CREATE_BY 
                ,ORIG_LD_REVENUE 
                ,NEW_LD_REVENUE 
                ,SUBR_STAT_CD 
                ,FA_INVOICE_FLG 
                ,ADD_VAS 
                ,ORIG_CREATE_BY 
                ,ORIG_HANDLE_BY 
                ,ADD_VAS_FROM_OM
                ,END_VAS
                ,BOLT_ON_FLG 
                ,ACTIVE_BOLT_ON_VAS 

        )
        select /*+ parallel(32) */ 
        distinct
                CUST_NUM,       SUBR_NUM, 
                store_cd
                ,RATE_PLAN_CD, ORDER_NUM, ORIG_LD_CD,   NEW_LD_CD,  
                ORIG_LD_EXP_DATE,       NEW_LD_EXP_DATE, ORIG_RATE_PLAN_CD,     NEW_RATE_PLAN_CD, CREATE_DATE
                ,MKT_CD,        REF,    RATE_PLAN_GRP, TRX_TYPE, FREE_DATA_ENTITLE
                ,CONTACTED_BY_CALL_CENTER
                ,case when upper(create_by) like '%ONLINE%'
                then 'Y'
                else 'N'
                end as ONLINE_INV
                ,create_date+1 as report_date, daily_sales_sms, eff_date
                ,create_by, orig_ld_revenue, new_ld_revenue, subr_stat_cd
                ,case when create_by like 'FA%'
                then 'Y'
                else 'N'
                end as FA_INVOICE_FLG
                ,add_vas, ORIG_CREATE_BY, ORIG_HANDLE_BY
                ,ADD_VAS_FROM_OM ,END_VAS , BOLT_ON_FLG, BOLT_ON_VAS
                from (
                                select /*+ parallel(32) */ a.*
                                , case
                                when a.free_data_entitle = 'Mass Mobile True unlimited data plans' then '01. True unlimited data plans'
                                when a.free_data_entitle = 'Mass Mobile 5GB FUP unlimited data plans' then '02. Unlimited data plans'
                                when a.free_data_entitle = 'Mass Mobile 5GB or above plans WITH add-on unlimited data feature' then '03. High tier (5GB or above) plans with add-on pack'
                                when a.free_data_entitle = 'Mass Mobile 5GB or above drop dead plans WITHOUT add-on unlimited data feature (exclude Family plans)' then '04. High tier (5GB or above) plans without add-on pack'
                                when a.free_data_entitle = 'Mass Mobile Family plans (Main SIM only)' then '05. Family Plan (Main SIM)'
                                when a.free_data_entitle = 'Mass Mobile Family plans (Secondary SIM only)' then '06. Family plans (Secondary SIM)' 
                                when a.free_data_entitle = 'Mass Mobile 1.5GB-4.9GB drop dead plans' then '07. Mid tier plans (1.5GB - 4.9GB)'
                                when a.free_data_entitle = 'Mass Mobile 500MB-1.4GB drop dead plans' then '08. Upper low tier plans (500MB - 1.4GB)'
                                when a.free_data_entitle = 'Mass Mobile below 500MB drop dead plans' then '09. Low tier plans (below 500MB)'
                                when a.free_data_entitle = 'Mass Mobile 1GB (up to 42Mbps) + unlimited (up to 128kbps) plans' then '10. ExtraCare plans'
                                when a.free_data_entitle = 'Mass Mobile 1GB + unlimited plans' then '10. ExtraCare plans'
                                when a.free_data_entitle = 'Mass Mobile ExtraCare plans' then '10. ExtraCare plans'
                                when a.free_data_entitle = 'Mass Mobile Speed capped unlimited data (up to 2Mbps) plans' then '11. Speed capped unlimited data plans'
                                when a.free_data_entitle = 'Mass Mobile Speed capped unlimited data (up to 384kbps) plans' then '11. Speed capped unlimited data plans'
                                when a.free_data_entitle = 'Mass Mobile Speed capped unlimited data (up to 768kbps) plans' then '11. Speed capped unlimited data plans'
                                when x.rate_plan_grp = 'Residential Fibre Broadband 100' then '13. Fiber Broadband:100MB'
                                when x.rate_plan_grp = 'Residential Fibre Broadband 500' then '14. Fiber Broadband:500MB'
                                when x.rate_plan_grp = 'Residential Fibre Broadband 1000' then '15. Fiber Broadband:1000MB'
                                when substr(x.rate_plan_grp, 1, 11) = 'FBB HomeTel' then '16. HomeTel'
                                when (substr(x.rate_plan_grp, 1, 9) = 'HomePhone' or x.rate_plan_grp = 'Jupiter') then '17. HPP'
                                when a.free_data_entitle = 'Mass Mobile 5GB or above drop dead plans WITHOUT add-on unlimited data feature' 
                                then '04. High tier (5GB or above) plans without add-on pack'
                                when a.free_data_entitle = 'Mass Mobile 5GB or above plans WITHOUT add-on pack (exclude Family plans)' 
                                then '04. High tier (5GB or above) plans without add-on pack'
                                when a.free_data_entitle = 'Mass Mobile 5GB or above plans WITH add-on pack (exclude Family plans)' 
                                then '03. High tier (5GB or above) plans with add-on pack'
                                when a.free_data_entitle = 'Mass Mobile 5GB or above plans WITH add-on pack' 
                                then '03. High tier (5GB or above) plans with add-on pack'
                                when a.free_data_entitle = 'Mass Mobile 5GB or above plans (exclude Family plans)' 
                                then '04. High tier (5GB or above) plans without add-on pack'
                                else '12. Other plans'
                                end  Rate_Plan_Grp
                                --Change plan only
                                ,case when ref ='Chg plan only'
                                then 'UPGRADE'
                                --No ld_cd or no expire_date
                                when (a.orig_ld_cd ='NA' or a.orig_ld_cd = ' ') or 
                                (a.orig_ld_exp_date = date '1900-01-01' or a.orig_ld_exp_date = date '2999-12-31')
                                then 'RETENTION'
                                --To be expired within 3 months or already expired
                                when months_between(trunc(a.orig_ld_exp_date,'MONTH'), trunc(a.create_date,'MONTH')) <= 3
                                then 'RETENTION'
                                --To be expired more than 3 months
                                when months_between(trunc(a.orig_ld_exp_date,'MONTH'), trunc(a.create_date,'MONTH')) > 3
                                then 'UPGRADE'
                                else 'RETENTION'
                                end as trx_type
                                ,case when (a.cust_num, a.subr_num) in (select cust_num, subr_num from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_002A2_TMP)
                                then 'Y'
                                else 'N'
                                end as CONTACTED_BY_CALL_CENTER
                                ,y.daily_sales_sms
                                ,case when xx.salesman_flg = 'Y'
                                 then z.create_by
                                 else z.handle_by
                                 end as create_by
                                ,z.create_by as orig_create_by
                                ,z.handle_by as orig_handle_by
                                ,z.add_vas
                                from (
                                        select distinct a.cust_num,a.subr_num,a.store_cd,a.order_num,a.eff_date,
                                        a.rate_plan_cd, a.NEW_LD_CD,a.ORIG_LD_CD,a.Orig_ld_exp_date,
                                        a.create_date, a.orig_rate_plan_cd, a.new_rate_plan_cd,
                                        a.subr_stat_cd, a.orig_ld_revenue, a.new_ld_revenue,
                                        coalesce(
                                        case 
                                        when BOLT_ON_FLG ='Y'
                                        then (select distinct free_data_entitle from ${etlvar::ADWDB}.free_data_entitle_n_ref)
                                        when x.free_data_entitle is not null
                                        then x.free_data_entitle
                                        else z.free_data_entitle
                                        end,' '
                                        ) as free_data_entitle
                                        ,'N' as online_inv
                                        ,NEW_LD_EXP_DATE
                                        ,MKT_CD
                                        ,REF
                                        ,coalesce(b.ADD_VAS,' ') as ADD_VAS_FROM_OM
                                        ,coalesce(END_VAS,' ') as END_VAS
                                        ,coalesce(BOLT_ON_VAS,' ') as BOLT_ON_VAS
                                        ,coalesce(BOLT_ON_FLG,'N') as BOLT_ON_FLG
                                        from
                                        ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004C2_TMP a
                                        left join 
                                        ${etlvar::TMPDB}.U_RBD_ALL_CHANNEL_ADD_VAS_TMP b
                                        on a.cust_num = b.cust_num
                                        and a.subr_num = b.subr_num
                                        and a.order_num = b.order_num
                                        and a.store_cd = b.store_cd
                                        left join ${etlvar::TMPDB}.U_RBD_ALL_CHANNEL_END_VAS_TMP c
                                        on a.cust_num = c.cust_num
                                        and a.subr_num = c.subr_num
                                        and a.order_num = c.order_num
                                        and a.store_cd = c.store_cd
                                        left join ${etlvar::TMPDB}.U_RBD_ALL_CHANNEL_VAS_CD_TMP d
                                        on a.cust_num = d.cust_num
                                        and a.subr_num = d.subr_num
                                        and a.order_num = d.order_num
                                        and a.store_cd = d.store_cd
                                        left join ${etlvar::TMPDB}.U_RBD_ALL_CHANNEL_BOLT_ON_TMP e
                                        on a.cust_num = e.cust_num
                                        and a.subr_num = e.subr_num
                                        and a.order_num = e.order_num
                                        and a.store_cd = e.store_cd
                                        left join
                                        ${etlvar::ADWDB}.subr_data_entitle_hist x
                                        on a.cust_num = x.cust_num
                                        and a.subr_num = x.subr_num
                                        and a.eff_date between x.start_date and x.end_date
                                        and x.start_date >= a.eff_date
                                        left join
                                        ${etlvar::ADWDB}.rate_plan_ref z
                                        on a.rate_plan_cd = z.rate_plan_cd
                                )  a
                                left join
                                ${etlvar::ADWDB}.rate_plan_ref x
                                on a.rate_plan_cd = x.rate_plan_cd
                                left join 
                                ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP y
                                on a.store_cd = y.store_cd
                                left join (
                                        select * from ${etlvar::ADWDB}.om_complete_chg_plan
                                        where create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and 
                                        TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                        union all
                                        select * from ${etlvar::ADWDB}.om_pending_chg_plan
                                        where create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and 
                                        TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                ) z
                                on a.order_num = z.case_id
                                left join ${etlvar::ADWDB}.fes_usr_info xx
                                on z.create_by = xx.usr_name
                                left join ${etlvar::ADWDB}.fes_usr_info yy
                                on z.handle_by = yy.usr_name
        ) a
        ;

--FL Order
        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TRX_TMP001 
        (
                CUST_NUM 
                ,SUBR_NUM 
                ,STORE_CD 
                ,RATE_PLAN_CD 
                ,ORDER_NUM 
                ,ORIG_LD_CD 
                ,NEW_LD_CD 
                ,ORIG_LD_EXP_DATE 
                ,NEW_LD_EXP_DATE 
                ,ORIG_RATE_PLAN_CD 
                ,NEW_RATE_PLAN_CD 
                ,INV_DATE 
                ,MKT_CD 
                ,REF 
                ,RATE_PLAN_GRP 
                ,TRX_TYPE 
                ,FREE_DATA_ENTITLE 
                ,CONTACTED_BY_CALL_CENTER 
                ,ONLINE_INV 
                ,REPORT_DATE 
                ,DAILY_SALES_SMS 
                ,EFF_DATE 
                ,CREATE_BY 
                ,ORIG_LD_REVENUE 
                ,NEW_LD_REVENUE 
                ,SUBR_STAT_CD 
                ,FA_INVOICE_FLG 
                ,ADD_VAS 
                ,ORIG_CREATE_BY 
                ,ORIG_HANDLE_BY 
                ,ADD_VAS_FROM_OM
                ,END_VAS
                ,BOLT_ON_FLG 
                ,ACTIVE_BOLT_ON_VAS 
        )
        select /*+ parallel(32) */ 
        distinct
                CUST_NUM,       SUBR_NUM, 
                store_cd
                ,RATE_PLAN_CD, ORDER_NUM, ORIG_LD_CD,   NEW_LD_CD,  
                ORIG_LD_EXP_DATE,       NEW_LD_EXP_DATE, ORIG_RATE_PLAN_CD,     NEW_RATE_PLAN_CD, CREATE_DATE
                ,MKT_CD,        REF,    RATE_PLAN_GRP, TRX_TYPE, FREE_DATA_ENTITLE
                ,CONTACTED_BY_CALL_CENTER
                ,case when upper(create_by) like '%ONLINE%'
                then 'Y'
                else 'N'
                end as ONLINE_INV
                ,create_date+1 as report_date, daily_sales_sms, eff_date
                ,create_by, orig_ld_revenue, new_ld_revenue, subr_stat_cd
                ,case when create_by like 'FA%'
                then 'Y'
                else 'N'
                end as FA_INVOICE_FLG
                ,add_vas, ORIG_CREATE_BY, ORIG_HANDLE_BY
                ,ADD_VAS_FROM_OM ,END_VAS , BOLT_ON_FLG, BOLT_ON_VAS
        from (
                                select /*+ parallel(32) */ a.*
                                , case
                                when a.free_data_entitle = 'Mass Mobile True unlimited data plans' then '01. True unlimited data plans'
                                when a.free_data_entitle = 'Mass Mobile 5GB FUP unlimited data plans' then '02. Unlimited data plans'
                                when a.free_data_entitle = 'Mass Mobile 5GB or above plans WITH add-on unlimited data feature' then '03. High tier (5GB or above) plans with add-on pack'
                                when a.free_data_entitle = 'Mass Mobile 5GB or above drop dead plans WITHOUT add-on unlimited data feature (exclude Family plans)' then '04. High tier (5GB or above) plans without add-on pack'
                                when a.free_data_entitle = 'Mass Mobile Family plans (Main SIM only)' then '05. Family Plan (Main SIM)'
                                when a.free_data_entitle = 'Mass Mobile Family plans (Secondary SIM only)' then '06. Family plans (Secondary SIM)' 
                                when a.free_data_entitle = 'Mass Mobile 1.5GB-4.9GB drop dead plans' then '07. Mid tier plans (1.5GB - 4.9GB)'
                                when a.free_data_entitle = 'Mass Mobile 500MB-1.4GB drop dead plans' then '08. Upper low tier plans (500MB - 1.4GB)'
                                when a.free_data_entitle = 'Mass Mobile below 500MB drop dead plans' then '09. Low tier plans (below 500MB)'
                                when a.free_data_entitle = 'Mass Mobile 1GB (up to 42Mbps) + unlimited (up to 128kbps) plans' then '10. ExtraCare plans'
                                when a.free_data_entitle = 'Mass Mobile 1GB + unlimited plans' then '10. ExtraCare plans'
                                when a.free_data_entitle = 'Mass Mobile ExtraCare plans' then '10. ExtraCare plans'
                                when a.free_data_entitle = 'Mass Mobile Speed capped unlimited data (up to 2Mbps) plans' then '11. Speed capped unlimited data plans'
                                when a.free_data_entitle = 'Mass Mobile Speed capped unlimited data (up to 384kbps) plans' then '11. Speed capped unlimited data plans'
                                when a.free_data_entitle = 'Mass Mobile Speed capped unlimited data (up to 768kbps) plans' then '11. Speed capped unlimited data plans'
                                when x.rate_plan_grp = 'Residential Fibre Broadband 100' then '13. Fiber Broadband:100MB'
                                when x.rate_plan_grp = 'Residential Fibre Broadband 500' then '14. Fiber Broadband:500MB'
                                when x.rate_plan_grp = 'Residential Fibre Broadband 1000' then '15. Fiber Broadband:1000MB'
                                when substr(x.rate_plan_grp, 1, 11) = 'FBB HomeTel' then '16. HomeTel'
                                when (substr(x.rate_plan_grp, 1, 9) = 'HomePhone' or x.rate_plan_grp = 'Jupiter') then '17. HPP'
                                when a.free_data_entitle = 'Mass Mobile 5GB or above drop dead plans WITHOUT add-on unlimited data feature' 
                                then '04. High tier (5GB or above) plans without add-on pack'
                                when a.free_data_entitle = 'Mass Mobile 5GB or above plans WITHOUT add-on pack (exclude Family plans)' 
                                then '04. High tier (5GB or above) plans without add-on pack'
                                when a.free_data_entitle = 'Mass Mobile 5GB or above plans WITH add-on pack (exclude Family plans)' 
                                then '03. High tier (5GB or above) plans with add-on pack'
                                when a.free_data_entitle = 'Mass Mobile 5GB or above plans WITH add-on pack' 
                                then '03. High tier (5GB or above) plans with add-on pack'
                                when a.free_data_entitle = 'Mass Mobile 5GB or above plans (exclude Family plans)' 
                                then '04. High tier (5GB or above) plans without add-on pack'
                                else '12. Other plans'
                                end  Rate_Plan_Grp
                                --Change plan only
                                ,case when ref ='Chg plan only'
                                then 'UPGRADE'
                                --No ld_cd or no expire_date
                                when (a.orig_ld_cd ='NA' or a.orig_ld_cd = ' ') or 
                                (a.orig_ld_exp_date = date '1900-01-01' or a.orig_ld_exp_date = date '2999-12-31')
                                then 'RETENTION'
                                --To be expired within 3 months or already expired
                                when months_between(trunc(a.orig_ld_exp_date,'MONTH'), trunc(a.create_date,'MONTH')) <= 3
                                then 'RETENTION'
                                --To be expired more than 3 months
                                when months_between(trunc(a.orig_ld_exp_date,'MONTH'), trunc(a.create_date,'MONTH')) > 3
                                then 'UPGRADE'
                                else 'RETENTION'
                                end as trx_type
                                ,case when (a.cust_num, a.subr_num) in (select cust_num, subr_num from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_002A2_TMP)
                                then 'Y'
                                else 'N'
                                end as CONTACTED_BY_CALL_CENTER
                                ,y.daily_sales_sms
                                ,case when xx.salesman_flg = 'Y'
                                 then z.create_by
                                 else z.handle_by
                                 end as create_by
                                ,z.create_by as orig_create_by
                                ,z.handle_by as orig_handle_by
                                ,' ' as add_vas                   
                                from (
                                                select distinct a.cust_num,a.subr_num,a.store_cd,a.order_num,a.eff_date,
                                                a.rate_plan_cd, a.NEW_LD_CD,a.ORIG_LD_CD,a.Orig_ld_exp_date,
                                                a.create_date, a.orig_rate_plan_cd, a.new_rate_plan_cd,
                                                a.subr_stat_cd, a.orig_ld_revenue, a.new_ld_revenue,
                                                coalesce(
                                                case 
                                                when BOLT_ON_FLG ='Y'
                                                then (select distinct free_data_entitle from ${etlvar::ADWDB}.free_data_entitle_n_ref)
                                                when x.free_data_entitle is not null
                                                then x.free_data_entitle
                                                else z.free_data_entitle
                                                end,' '
                                                ) as free_data_entitle
                                                ,'N' as online_inv
                                                ,NEW_LD_EXP_DATE
                                                ,MKT_CD
                                                ,REF
                                                ,coalesce(b.ADD_VAS,' ') as ADD_VAS_FROM_OM
                                                ,coalesce(END_VAS,' ') as END_VAS
                                                ,coalesce(BOLT_ON_VAS,' ') as BOLT_ON_VAS
                                                ,coalesce(BOLT_ON_FLG,'N') as BOLT_ON_FLG
                                                from
                                                ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004D2_TMP a
                                                left join 
                                                ${etlvar::TMPDB}.U_RBD_ALL_CHANNEL_ADD_VAS_TMP b
                                                on a.cust_num = b.cust_num
                                                and a.subr_num = b.subr_num
                                                and a.order_num = b.order_num
                                                and a.store_cd = b.store_cd
                                                left join ${etlvar::TMPDB}.U_RBD_ALL_CHANNEL_END_VAS_TMP c
                                                on a.cust_num = c.cust_num
                                                and a.subr_num = c.subr_num
                                                and a.order_num = c.order_num
                                                and a.store_cd = c.store_cd
                                                left join ${etlvar::TMPDB}.U_RBD_ALL_CHANNEL_VAS_CD_TMP d
                                                on a.cust_num = d.cust_num
                                                and a.subr_num = d.subr_num
                                                and a.order_num = d.order_num
                                                and a.store_cd = d.store_cd
                                                left join ${etlvar::TMPDB}.U_RBD_ALL_CHANNEL_BOLT_ON_TMP e
                                                on a.cust_num = e.cust_num
                                                and a.subr_num = e.subr_num
                                                and a.order_num = e.order_num
                                                and a.store_cd = e.store_cd
                                                left join
                                                ${etlvar::ADWDB}.subr_data_entitle_hist x
                                                on a.cust_num = x.cust_num
                                                and a.subr_num = x.subr_num
                                                and a.eff_date between x.start_date and x.end_date
                                                and x.start_date >= a.eff_date
                                                left join
                                                ${etlvar::ADWDB}.rate_plan_ref z
                                                on a.rate_plan_cd = z.rate_plan_cd
                                        )  a
                                        left join
                                        ${etlvar::ADWDB}.rate_plan_ref x
                                        on a.rate_plan_cd = x.rate_plan_cd
                                        left join 
                                        ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP y
                                        on a.store_cd = y.store_cd
                                        left join (
                                                select * from ${etlvar::ADWDB}.fl_om_complete_chg_plan
                                                where create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and 
                                                TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                                union all
                                                select * from ${etlvar::ADWDB}.fl_om_pending_chg_plan
                                                where create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and 
                                                TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                        ) z
                                        on a.order_num = z.case_id
                                        left join ${etlvar::ADWDB}.fes_usr_info xx
                                        on z.create_by = xx.usr_name
                                        left join ${etlvar::ADWDB}.fes_usr_info yy
                                        on z.handle_by = yy.usr_name
        ) a
        ;

--Change plan only
        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TRX_TMP001 
        (
                CUST_NUM 
                ,SUBR_NUM 
                ,STORE_CD 
                ,RATE_PLAN_CD 
                ,ORDER_NUM 
                ,ORIG_LD_CD 
                ,NEW_LD_CD 
                ,ORIG_LD_EXP_DATE 
                ,NEW_LD_EXP_DATE 
                ,ORIG_RATE_PLAN_CD 
                ,NEW_RATE_PLAN_CD 
                ,INV_DATE 
                ,MKT_CD 
                ,REF 
                ,RATE_PLAN_GRP 
                ,TRX_TYPE 
                ,FREE_DATA_ENTITLE 
                ,CONTACTED_BY_CALL_CENTER 
                ,ONLINE_INV 
                ,REPORT_DATE 
                ,DAILY_SALES_SMS 
                ,EFF_DATE 
                ,CREATE_BY 
                ,ORIG_LD_REVENUE 
                ,NEW_LD_REVENUE 
                ,SUBR_STAT_CD 
                ,FA_INVOICE_FLG 
                ,ADD_VAS 
                ,ORIG_CREATE_BY 
                ,ORIG_HANDLE_BY 
                ,ADD_VAS_FROM_OM
                ,END_VAS
                ,BOLT_ON_FLG 
                ,ACTIVE_BOLT_ON_VAS 
        )
        select /*+ parallel(32) */ 
        distinct
                CUST_NUM,       SUBR_NUM, 
                store_cd
                ,RATE_PLAN_CD, ORDER_NUM, ORIG_LD_CD,   NEW_LD_CD,  
                ORIG_LD_EXP_DATE,       NEW_LD_EXP_DATE, ORIG_RATE_PLAN_CD,     NEW_RATE_PLAN_CD, CREATE_DATE
                ,MKT_CD,        REF,    RATE_PLAN_GRP, TRX_TYPE, FREE_DATA_ENTITLE
                ,CONTACTED_BY_CALL_CENTER
                ,case when upper(create_by) like '%ONLINE%'
                then 'Y'
                else 'N'
                end as ONLINE_INV
                ,create_date+1 as report_date, daily_sales_sms, eff_date
                ,create_by, ' ' as orig_ld_revenue, ' ' as new_ld_revenue, subr_stat_cd
                ,case when create_by like 'FA%'
                then 'Y'
                else 'N'
                end as FA_INVOICE_FLG
                ,add_vas, ORIG_CREATE_BY, ORIG_HANDLE_BY
                ,ADD_VAS_FROM_OM ,END_VAS , BOLT_ON_FLG, BOLT_ON_VAS
        from (
                                select /*+ parallel(32) */ a.*
                                , case
                                when a.free_data_entitle = 'Mass Mobile True unlimited data plans' then '01. True unlimited data plans'
                                when a.free_data_entitle = 'Mass Mobile 5GB FUP unlimited data plans' then '02. Unlimited data plans'
                                when a.free_data_entitle = 'Mass Mobile 5GB or above plans WITH add-on unlimited data feature' then '03. High tier (5GB or above) plans with add-on pack'
                                when a.free_data_entitle = 'Mass Mobile 5GB or above drop dead plans WITHOUT add-on unlimited data feature (exclude Family plans)' then '04. High tier (5GB or above) plans without add-on pack'
                                when a.free_data_entitle = 'Mass Mobile Family plans (Main SIM only)' then '05. Family Plan (Main SIM)'
                                when a.free_data_entitle = 'Mass Mobile Family plans (Secondary SIM only)' then '06. Family plans (Secondary SIM)' 
                                when a.free_data_entitle = 'Mass Mobile 1.5GB-4.9GB drop dead plans' then '07. Mid tier plans (1.5GB - 4.9GB)'
                                when a.free_data_entitle = 'Mass Mobile 500MB-1.4GB drop dead plans' then '08. Upper low tier plans (500MB - 1.4GB)'
                                when a.free_data_entitle = 'Mass Mobile below 500MB drop dead plans' then '09. Low tier plans (below 500MB)'
                                when a.free_data_entitle = 'Mass Mobile 1GB (up to 42Mbps) + unlimited (up to 128kbps) plans' then '10. ExtraCare plans'
                                when a.free_data_entitle = 'Mass Mobile 1GB + unlimited plans' then '10. ExtraCare plans'
                                when a.free_data_entitle = 'Mass Mobile ExtraCare plans' then '10. ExtraCare plans'
                                when a.free_data_entitle = 'Mass Mobile Speed capped unlimited data (up to 2Mbps) plans' then '11. Speed capped unlimited data plans'
                                when a.free_data_entitle = 'Mass Mobile Speed capped unlimited data (up to 384kbps) plans' then '11. Speed capped unlimited data plans'
                                when a.free_data_entitle = 'Mass Mobile Speed capped unlimited data (up to 768kbps) plans' then '11. Speed capped unlimited data plans'
                                when x.rate_plan_grp = 'Residential Fibre Broadband 100' then '13. Fiber Broadband:100MB'
                                when x.rate_plan_grp = 'Residential Fibre Broadband 500' then '14. Fiber Broadband:500MB'
                                when x.rate_plan_grp = 'Residential Fibre Broadband 1000' then '15. Fiber Broadband:1000MB'
                                when substr(x.rate_plan_grp, 1, 11) = 'FBB HomeTel' then '16. HomeTel'
                                when (substr(x.rate_plan_grp, 1, 9) = 'HomePhone' or x.rate_plan_grp = 'Jupiter') then '17. HPP'
                                when a.free_data_entitle = 'Mass Mobile 5GB or above drop dead plans WITHOUT add-on unlimited data feature' 
                                then '04. High tier (5GB or above) plans without add-on pack'
                                when a.free_data_entitle = 'Mass Mobile 5GB or above plans WITHOUT add-on pack (exclude Family plans)' 
                                then '04. High tier (5GB or above) plans without add-on pack'
                                when a.free_data_entitle = 'Mass Mobile 5GB or above plans WITH add-on pack (exclude Family plans)' 
                                then '03. High tier (5GB or above) plans with add-on pack'
                                when a.free_data_entitle = 'Mass Mobile 5GB or above plans WITH add-on pack' 
                                then '03. High tier (5GB or above) plans with add-on pack'
                                when a.free_data_entitle = 'Mass Mobile 5GB or above plans (exclude Family plans)' 
                                then '04. High tier (5GB or above) plans without add-on pack'
                                else '12. Other plans'
                                end  Rate_Plan_Grp
                                --Change plan only
                                ,'UPGRADE' as trx_type
                                ,case when (a.cust_num, a.subr_num) in (select cust_num, subr_num from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_002A2_TMP)
                                then 'Y'
                                else 'N'
                                end as CONTACTED_BY_CALL_CENTER
                                ,y.daily_sales_sms
                                ,case when xx.salesman_flg = 'Y'
                                 then z.create_by
                                 else z.handle_by
                                 end as create_by
                                ,z.create_by as orig_create_by
                                ,z.handle_by as orig_handle_by
                                ,z.add_vas
                                from (
                                                select distinct a.cust_num,a.subr_num,a.store_cd,a.order_num,a.eff_date,
                                                a.rate_plan_cd, a.NEW_LD_CD,a.ORIG_LD_CD,a.Orig_ld_exp_date,
                                                a.create_date, a.orig_rate_plan_cd, a.new_rate_plan_cd,
                                                a.subr_stat_cd,
                                                coalesce(
                                                case 
                                                when BOLT_ON_FLG ='Y'
                                                then (select distinct free_data_entitle from ${etlvar::ADWDB}.free_data_entitle_n_ref)
                                                when x.free_data_entitle is not null
                                                then x.free_data_entitle
                                                else z.free_data_entitle
                                                end,' '
                                                ) as free_data_entitle
                                                ,'N' as online_inv
                                                ,NEW_LD_EXP_DATE
                                                ,MKT_CD
                                                ,REF
                                                ,coalesce(b.ADD_VAS,' ') as ADD_VAS_FROM_OM
                                                ,coalesce(END_VAS,' ') as END_VAS
                                                ,coalesce(BOLT_ON_VAS,' ') as BOLT_ON_VAS
                                                ,coalesce(BOLT_ON_FLG,'N') as BOLT_ON_FLG
                        from
                        ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_004A2_TMP a
                                                left join 
                                                ${etlvar::TMPDB}.U_RBD_ALL_CHANNEL_ADD_VAS_TMP b
                                                on a.cust_num = b.cust_num
                                                and a.subr_num = b.subr_num
                                                and a.order_num = b.order_num
                                                and a.store_cd = b.store_cd
                                                left join ${etlvar::TMPDB}.U_RBD_ALL_CHANNEL_END_VAS_TMP c
                                                on a.cust_num = c.cust_num
                                                and a.subr_num = c.subr_num
                                                and a.order_num = c.order_num
                                                and a.store_cd = c.store_cd
                                                left join ${etlvar::TMPDB}.U_RBD_ALL_CHANNEL_VAS_CD_TMP d
                                                on a.cust_num = d.cust_num
                                                and a.subr_num = d.subr_num
                                                and a.order_num = d.order_num
                                                and a.store_cd = d.store_cd
                                                left join ${etlvar::TMPDB}.U_RBD_ALL_CHANNEL_BOLT_ON_TMP e
                                                on a.cust_num = e.cust_num
                                                and a.subr_num = e.subr_num
                                                and a.order_num = e.order_num
                                                and a.store_cd = e.store_cd
                                                left join
                                                ${etlvar::ADWDB}.subr_data_entitle_hist x
                                                on a.cust_num = x.cust_num
                                                and a.subr_num = x.subr_num
                                                and a.eff_date between x.start_date and x.end_date
                                                and x.start_date >= a.eff_date
                        left join
                        ${etlvar::ADWDB}.rate_plan_ref z
                        on a.rate_plan_cd = z.rate_plan_cd
                                        ) a
                                        left join
                                        ${etlvar::ADWDB}.rate_plan_ref x
                                        on a.rate_plan_cd = x.rate_plan_cd
                                        left join 
                                        ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP y
                                        on a.store_cd = y.store_cd
                                        left join (
                                                select * from ${etlvar::ADWDB}.om_complete_chg_plan
                                                where create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and 
                                                TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                                union all
                                                select * from ${etlvar::ADWDB}.om_pending_chg_plan
                                                where create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and 
                                                TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                        ) z
                                        on a.order_num = z.case_id
                                        left join ${etlvar::ADWDB}.fes_usr_info xx
                                        on z.create_by = xx.usr_name
                                        left join ${etlvar::ADWDB}.fes_usr_info yy
                                        on z.handle_by = yy.usr_name
        ) a
;

update ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TRX_TMP001
set orig_ld_exp_date = date '1900-01-01'
where orig_ld_exp_date = date '2999-12-31'
;


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

my $dateformat = "%Y-%m-%d";
my $date = "2016-10-31";
$boundaryDate = Time::Piece->strptime($date, $dateformat);
my $txDate = Time::Piece->strptime("${etlvar::TXDATE}", $dateformat);

print "boundaryDate = $boundaryDate\n";
print "txDate = $txDate\n";

if($txDate <= $boundaryDate){
        print "TX Date is before 2016-Nov, not using data entitle hist table\n";
        $ret = runSQLPLUS()
}
else {
        print "TX Date is at least 2016-Nov, using data entitle hist table\n";
        $ret = runSQLPLUS2();
}


#$ret = runSQLPLUS();









if($ret == 0)
{

}



my $post = etlvar::postProcess();

exit($ret);














