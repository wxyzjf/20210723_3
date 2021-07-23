/opt/etl/prd/etl/APP/RBD/Z_RBD_KPI_REPORT_DTL_UAT/bin> cat u_rbd_kpi_report_dtl0025.pl
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

my $ETLVAR = $ENV{"AUTO_ETLVAR"};
##require $ETLVAR;
my $ETLVAR = "/opt/etl/prd/etl/APP/RBD/Z_RBD_KPI_REPORT_DTL_UAT/bin/master_dev.pl";

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

TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TRX_TMP002;
TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_STAFF_NAME;
TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_REMAP_RBD_TMP;

INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_STAFF_NAME
(
  USR_NAME    
  ,STAFF_ID    
  ,POS_SHOP_CD 
  ,FULL_NAME   
  ,GRP_NAME    
  ,SUB_GRP_NAME
  ,CALL_CENTER_TEAM
)
select 
a.usr_name,a.staff_id, a.pos_shop_cd, b.full_name, a.grp_name, a.sub_grp_name, a.call_centre_team 
from (
        select * from ${etlvar::ADWDB}.fes_usr_info
        where upper(full_name) not in ('DEMO','NULL','BMDM')
        and upper(full_name) not like '%TEST%'
) a
left join (
        select * from (
                select /*+ parallel(32) */ 
                staff_id, full_name
                ,row_number() over (partition by staff_id order by pos_shop_cd, create_ts) as rn
                from ${etlvar::ADWDB}.fes_usr_info
                where upper(full_name) not in ('DEMO','NULL','BMDM')
                and upper(full_name) not like '%TEST%'
        )
        where rn = 1
) b
on a.staff_id = b.staff_id
;

INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TRX_TMP002
(
        CUST_NUM 
        ,SUBR_NUM 
        ,STORE_CD 
        ,RATE_PLAN_CD 
        ,ORDER_NUM 
        ,ORIG_LD_CD 
        ,NEW_LD_CD 
        ,ORIG_LD_EXP_DATE 
        ,ORIG_LD_START_DATE 
        ,NEW_LD_EXP_DATE 
        ,NEW_LD_START_DATE 
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
        ,ORIG_FREE_DATA_ENTITLE
        ,PROGRAM_ID
        ,PR_STATUS
        ,CREATE_BY
        ,COMMISSION_BY
        ,LST_MOD_DATE
        ,PROGRAM_START_DATE
        ,PROGRAM_END_DATE
        ,REDEMPTION_DATE
        ,USER_GROUP_OF_COMMISSION_BY
        ,USER_GROUP_OF_CREATE_BY
        ,CHANNEL_EFFORT
        ,EFF_DATE
        ,NEW_ACTIVATION_FLG
        ,orig_ld_revenue
        ,new_ld_revenue
        ,subr_stat_cd
        ,TXDATE
        ,FA_INVOICE_FLG
        ,REPLACE_OLD_LD
        ,add_vas
        ,ORIG_CREATE_BY
        ,ORIG_HANDLE_BY
        ,CREATE_BY_CALL_CENTER_TEAM
        ,COMM_BY_CALL_CENTER_TEAM
        ,END_VAS
        ,ACTIVE_BOLT_ON_VAS
        ,BOLT_ON_FLG
)
select /*+ parallel(32) */
distinct
a.CUST_NUM, a.SUBR_NUM, a.STORE_CD, a.RATE_PLAN_CD, a.ORDER_NUM, a.ORIG_LD_CD, a.NEW_LD_CD, a.ORIG_LD_EXP_DATE, 
coalesce(orig_ld_start_date,date '1900-01-01'),
a.NEW_LD_EXP_DATE, 
case when a.order_num like 'ORDER%' and ref <> 'Chg plan only'
and (new_ld_start_date = date '1900-01-01' or new_ld_start_date = date '2999-12-31' or new_ld_start_date is null)
then eff_date
else coalesce(new_ld_start_date,date '1900-01-01')
end as new_ld_start_date,
a.ORIG_RATE_PLAN_CD, a.NEW_RATE_PLAN_CD, a.INV_DATE, a.MKT_CD, a.REF, a.RATE_PLAN_GRP, a.TRX_TYPE, 
a.FREE_DATA_ENTITLE as NEW_FREE_DATA_ENTITLE,
a.CONTACTED_BY_CALL_CENTER, a.ONLINE_INV, a.REPORT_DATE, a.DAILY_SALES_SMS, coalesce(b.orig_free_data_entitle,' ')
, coalesce(c.program_id,' ') as program_id
, coalesce(c.pr_status, ' ') as status
, a.CREATE_BY
, coalesce(c.comm_by,' ') as commission_by
, coalesce(c.lst_mod_date,date '1900-01-01') as last_modified_date
, coalesce(c.start_date,date '1900-01-01') as program_start_date
, coalesce(c.end_date,date '1900-01-01') as program_end_date
, coalesce(d.redem_end_date,date '1900-01-01') as redemption_date
, case when c.comm_by like 'CA%'
  then 'BM'
  else coalesce(e.grp_name,' ') 
  end as user_group_of_commission_by
, case when a.create_by like 'CA%'
  then 'BM'
  else coalesce(f.grp_name,' ') 
  end as user_group_of_create_by
,case when (c.comm_by is null or upper(comm_by) like '%ADMIN%' or comm_by = ' ') or (a.inv_date > d.redem_end_date)
then
        case when f.call_center_team ='TS'
        then 'Cold call'        --i.e. Telesales
        when f.call_center_team in ('PR')
        then 'Call centre'      --i.e. Outbound
        when f.call_center_team in ('CS Fixed', 'CS Mobile GZ', 'CS Mobile HK')
        then 'Call centre'      --i.e. Inbound
        when f.call_center_team in ('CS Live Chat')
        then 'Call centre'      --i.e. Live Chat
        when a.create_by like 'CA%'
        then 'BM'
        when (upper(a.create_by) like '%ONLINE%' or a.online_inv in ('Y','B'))
        then 'Online'
        when a.create_by like 'FA%'
        then 'Street fighter'   --i.e. DS
        when a.store_cd in (select store_cd from ${etlvar::MIGDB}.U_ALL_CHANNEL_001_TMP where daily_sales_sms ='Y')
        then 'RBD'
        else 'Others'
        end
else
        case when e.call_center_team ='TS' and 
        (
                pr_status in ('AO','Accept Offer')
                or (
                        pr_status not in ('AO','Accept Offer') and (pr_status <> ' ' and pr_status is not null)
                        and comm_by <> ' '
                )
        )
        then 'Cold call'        --i.e. Telesales
        when e.call_center_team in ('PR') and 
        (
                pr_status in ('AO','Accept Offer')
                or (
                        pr_status not in ('AO','Accept Offer') and (pr_status <> ' ' and pr_status is not null)
                        and comm_by <> ' '
                )
        )
        then 'Call centre'      --i.e. Outbound
        when e.call_center_team in ('CS Fixed', 'CS Mobile GZ', 'CS Mobile HK') and 
        (
                pr_status in ('AO','Accept Offer')
                or (
                        pr_status not in ('AO','Accept Offer') and (pr_status <> ' ' and pr_status is not null)
                        and comm_by <> ' '
                )
        )
        then 'Call centre'      --i.e. Inbound
        when e.call_center_team in ('CS Live Chat') and 
        (
                pr_status in ('AO','Accept Offer')
                or (
                        pr_status not in ('AO','Accept Offer') and (pr_status <> ' ' and pr_status is not null)
                        and comm_by <> ' '
                )
        )
        then 'Call centre'      --i.e. Live Chat
        when c.comm_by like 'CA%'
        then 'BM'
        when (upper(C.comm_by) like '%ONLINE%' or a.online_inv in ('Y','B'))
        then 'Online'
        when C.comm_by like 'FA%'
        then 'Street fighter'   --i.e. DS
        when a.store_cd in (select store_cd from ${etlvar::MIGDB}.U_ALL_CHANNEL_001_TMP where daily_sales_sms ='Y')
        then 'RBD'
        else 'Others'
        end
end as channel_effort
  ,a.EFF_DATE
  ,' '
  ,a.orig_ld_revenue
  ,a.new_ld_revenue
  ,a.subr_stat_cd
  ,a.report_date as txdate
  ,FA_INVOICE_FLG
  ,' ' as REPLACE_OLD_LD
  ,a.add_vas
  ,a.ORIG_CREATE_BY
  ,a.ORIG_HANDLE_BY
  ,coalesce(e.call_center_team,' ') as comm_by_call_center_team
  ,coalesce(f.call_center_team,' ') as create_by_call_center_team
  ,a.END_VAS, a.ACTIVE_BOLT_ON_VAS, a.BOLT_ON_FLG
from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TRX_TMP001 a
left join (
        select /*+ parallel(32) */ 
        a.cust_num,a.subr_num,y.free_data_entitle as orig_free_data_entitle, a.store_cd, a.order_num
        from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TRX_TMP001 a
         left join
         ${etlvar::ADWDB}.rate_plan_ref y
         on a.orig_rate_plan_cd = y.rate_plan_cd
)  b
on a.cust_num = b.cust_num
and a.subr_num = b.subr_num
and a.store_cd = b.store_cd
and a.order_num = b.order_num
left join (
        select cust_num, subr_num, program_id, comm_by, pr_status, lst_mod_date, start_date, end_date
        from (
                select cust_num,subr_num,program_id,comm_by, pr_status,lst_mod_date,start_date,end_date
                ,row_number() over (partition by cust_num,subr_num order by
                create_date desc, lst_mod_date desc, end_date desc, last_contact_Date desc) as rn
                from (
                        select y.*
                        ,row_number() over (partition by cust_num, subr_num, order_num, store_cd order by lst_mod_date desc, priority desc) as rn
                        from (
                                select x.*
                                from (
                                                select /*+ parallel(32) */ 
                                                a.cust_num,a.subr_num,program_id,comm_by, pr_status, lst_mod_date,start_date,end_date,a.store_cd,a.order_num
                                                ,c.create_date, c.last_contact_Date
                                                ,case when (upper(comm_by) like '%ADMIN%' or comm_by = ' ' or comm_by is null)
                                                then 0
                                                else 1
                                                end as priority
                                                from (
                                                        select * from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TRX_TMP001 
                                                        where (cust_num, subr_num) not in (
                                                        select cust_num, subr_num from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_002A1_TMP
                                                        )
                                                ) a
                                                left join ${etlvar::ADWDB}.pr_status c
                                                on a.cust_num = c.cust_num
                                                and a.subr_num = c.subr_num
                                                where (program_id like 'PRR%' or program_id like 'HP%' or program_id like 'FB%' or program_id like 'PR00%')
                                                and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1 between c.start_date and c.end_date
                                ) x 
                        )  y
                ) where rn = 1
        )
        where rn = 1
        union all
        select cust_num, subr_num, program_id, comm_by, pr_status, last_modify_date, program_start_date, program_end_date
        from (
                select 
                cust_num,subr_num,program_id,comm_by, pr_status,last_modify_date,program_start_date,program_end_date
                ,row_number() over (partition by STORE_CD, CUST_NUM, SUBR_NUM, ORDER_NUM, NEW_LD_CD order by program_id, last_modify_date desc, program_end_date) as rn 
                from (
                        select 
                        a.cust_num, a.subr_num, a.store_cd, a.order_num, a.new_ld_cd,
                        b.program_id, b.comm_by,
                        b.program_start_date, b.program_end_date, 
                        b.status as pr_status, b.last_modify_date, b.user_id
                        from (
                                select * from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TRX_TMP001 
                                where (cust_num, subr_num) in (
                                select cust_num, subr_num from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_002A1_TMP
                                )
                        ) a
                        left join (
                                select b.usr_name as comm_by, a.* 
                                from ${etlvar::ADWDB}.pro_cold_call a
                                left join ${etlvar::ADWDB}.fes_usr_info b
                                on a.user_id = b.fes_usr_id
                                where (
                                program_id like 'COLD%'
                                or program_id like 'WARM%'
                                or program_id like 'REF%'
                                or program_id like 'RMP%'
                                or program_id like 'RPE%'
                                or program_id like 'RNN%'
                                or program_id like 'RMG%'
                                ) 
                                and status ='AO'
                        ) b
                        on      a.cust_num = b.cust_num
                        and a.subr_num = b.subr_num
                        and trunc(inv_date ,'MONTH') = trunc(last_modify_date,'MONTH')
                ) a
        )
        where rn = 1
) c
on a.subr_num = c.subr_num
and a.cust_num = c.cust_num
left join (select program_id,start_date, end_date,redem_end_date from ${etlvar::ADWDB}.pro_program_info) d
on c.program_id = d.program_id
left join ${etlvar::MIGDB}.U_RBD_ALL_CHANNEL_STAFF_NAME e
on replace(c.comm_by,' ','') = e.usr_name
left join ${etlvar::MIGDB}.U_RBD_ALL_CHANNEL_STAFF_NAME f
on a.create_by = f.usr_name
left join (
        select  /*+ parallel(32) */ 
        CUST_NUM, SUBR_NUM, LD_CD, ld_start_date as orig_ld_start_date
        ,INV_DATE, LD_EXPIRED_DATE as orig_ld_exp_date, WAIVED_FLG, VOID_FLG, LD_REVENUE, INV_NUM
        from (
                select /*+ parallel(32) */
                a.cust_num, a.subr_num, a.ld_Cd, 
                case when (a.ld_start_date = date '2999-12-31' or a.ld_start_date = date '1900-01-01' or a.ld_start_date is null)
                then add_months(ld_expired_date,-cast(substr(a.ld_cd,4,2) as decimal))+1
                else a.ld_start_date
                end as ld_start_date,
                a.ld_expired_date,inv_date,
                a.waived_flg, a.void_flg, b.ld_revenue,
                a.inv_num
                ,row_number() over (partition by a.cust_num, a.subr_num order by inv_date desc, a.ld_expired_date desc) as rn
                from ${etlvar::ADWDB}.subr_ld_hist a
                left join ${etlvar::ADWDB}.mkt_ref_vw b
                on a.mkt_cd = b.mkt_cd
                left join ${etlvar::ADWDB}.pos_inv_header c
                on a.inv_num = c.inv_num
                and a.cust_num = c.cust_num
                and a.subr_num = c.subr_num
                and a.ld_expired_date = c.ld_expiry_date
                where b.ld_revenue in ('P','V')
                and end_date = date '2999-12-31'
                and void_flg <> 'Y'
                and waived_flg <> 'Y'
        ) where rn = 2
) g
on a.cust_num = g.cust_num
and a.subr_num = g.subr_num
and a.orig_ld_revenue = g.ld_revenue
left join (
        select  /*+ parallel(32) */
        CUST_NUM, SUBR_NUM, LD_CD,ld_start_date as new_ld_start_date
        ,INV_DATE, LD_EXPIRED_DATE, WAIVED_FLG, VOID_FLG, LD_REVENUE, INV_NUM
        from (
                select /*+ parallel(32) */ 
                a.cust_num, a.subr_num, a.ld_Cd, 
                case when (a.ld_start_date = date '2999-12-31' or a.ld_start_date = date '1900-01-01' or a.ld_start_date is null)
                then add_months(ld_expired_date,-cast(substr(a.ld_cd,4,2) as decimal))+1
                else a.ld_start_date
                end as ld_start_date,
                a.ld_expired_date,inv_date,
                a.waived_flg, a.void_flg, b.ld_revenue,
                a.inv_num
                ,row_number() over (partition by a.cust_num, a.subr_num order by inv_date desc, a.ld_expired_date desc) as rn
                from ${etlvar::ADWDB}.subr_ld_hist a
                left join ${etlvar::ADWDB}.mkt_ref_vw b
                on a.mkt_cd = b.mkt_cd
                left join ${etlvar::ADWDB}.pos_inv_header c
                on a.inv_num = c.inv_num
                and a.cust_num = c.cust_num
                and a.subr_num = c.subr_num
                and a.ld_expired_date = c.ld_expiry_date
                where b.ld_revenue in ('P','V')
                and end_date = date '2999-12-31'
                and void_flg <> 'Y'
                and waived_flg <> 'Y'
        ) where rn = 1
) h
on a.cust_num = h.cust_num
and a.subr_num = h.subr_num
and a.new_ld_revenue = h.ld_revenue
;
commit;

update ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TRX_TMP002 set new_ld_start_date = 
add_months(new_ld_exp_date,-cast(substr(new_ld_cd,4,2) as decimal))+1
where (new_ld_start_date = date '1900-01-01' or new_ld_start_date = date '2999-12-31' or new_ld_start_date is null)
and (new_ld_exp_date <> date '1900-01-01' and new_ld_exp_date <> date '2999-12-31' and new_ld_exp_date is not null) 
and (new_ld_Cd <> ' ' and new_ld_cd is not null and new_ld_cd <> 'NA')
;
commit;

update ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TRX_TMP002 set orig_ld_start_date = 
add_months(orig_ld_exp_date,-cast(substr(orig_ld_cd,4,2) as decimal))+1
where (orig_ld_start_date = date '1900-01-01' or orig_ld_start_date = date '2999-12-31' or orig_ld_start_date is null)
and (orig_ld_exp_date <> date '1900-01-01' and orig_ld_exp_date <> date '2999-12-31' and orig_ld_exp_date is not null) 
and (orig_ld_Cd <> ' ' and orig_ld_cd is not null and orig_ld_cd <> 'NA')
;
commit;

update ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TRX_TMP002 set new_ld_exp_date = 
add_months(NEW_LD_START_DATE,cast(substr(new_ld_cd,4,2) as decimal))-1
where (NEW_LD_EXP_DATE = date '1900-01-01' or NEW_LD_EXP_DATE = date '2999-12-31' or NEW_LD_EXP_DATE is null)
and (NEW_LD_START_DATE <> date '1900-01-01' and NEW_LD_START_DATE <> date '2999-12-31' and NEW_LD_START_DATE is not null) 
and (new_ld_Cd <> ' ' and new_ld_cd is not null and new_ld_cd <> 'NA')
;
commit;

update ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TRX_TMP002 set orig_ld_exp_date = 
add_months(orig_LD_START_DATE,cast(substr(orig_ld_cd,4,2) as decimal))-1
where (orig_ld_exp_date = date '1900-01-01' or orig_ld_exp_date = date '2999-12-31' or orig_ld_exp_date is null)
and (orig_LD_START_DATE <> date '1900-01-01' and orig_LD_START_DATE <> date '2999-12-31' and orig_LD_START_DATE is not null) 
and (orig_ld_Cd <> ' ' and orig_ld_Cd is not null and orig_ld_Cd <> 'NA')
;
commit;

update ${etlvar::MIGADWDB}.RBD_KPI_REPORT_TRX_DTL set new_ld_start_date = eff_date
where order_num like 'ORDER%' and ref <> 'Chg plan only'
;

update ${etlvar::MIGADWDB}.RBD_KPI_REPORT_TRX_DTL set new_ld_exp_date = 
add_months(NEW_LD_START_DATE,cast(substr(new_ld_cd,4,2) as decimal))-1
where order_num like 'ORDER%' and ref <> 'Chg plan only'
;

update ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TRX_TMP002
set new_activation_flg = 
(
        case when (cust_num, subr_num) in (
        select cust_num, subr_num from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_002A1_TMP
        ) then 'Y'
        else 'N'
        end
)
;
commit;

update ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TRX_TMP002
set trx_type = 'NEW ACTIVATION', ref = 'NEW ACTIVATION'
where new_activation_flg = 'Y'
;
commit;

update ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TRX_TMP002
set REPLACE_OLD_LD = (
        case when (NEW_LD_START_DATE = date '2999-12-31' or NEW_LD_START_DATE = date '1900-01-01'
        or ORIG_LD_EXP_DATE = date '2999-12-31' or ORIG_LD_EXP_DATE = date '1900-01-01')
        then 'N' 
        when (trunc(NEW_LD_START_DATE,'MONTH') = trunc(ORIG_LD_START_DATE,'MONTH')) and 
        (trunc(NEW_LD_EXP_DATE,'MONTH') = trunc(ORIG_LD_EXP_DATE,'MONTH'))
        then 'N' 
        when trunc(NEW_LD_START_DATE,'MONTH')<=trunc(ORIG_LD_EXP_DATE,'MONTH')
        then 'Y'
        else 'N'
        end
)
;
commit;

delete from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TRX_TMP002
where create_by like 'CA%'
;
commit;

INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_REMAP_RBD_TMP
(
        SUBMISSION_DATE
        , SUBR_NUM
        , CUST_NUM
        , RENEWED_PLAN_CD
        , STORE_CD
        , SALESMAN_CD
        , GRP_NAME
)
SELECT
        a.SUBMISSION_DATE
        ,a.SUBR_NUM
        ,a.CUST_NUM
        ,a.STORE_CD
        ,a.SALESMAN_CD
        ,a.RENEWED_PLAN_CD
        ,coalesce(b.GRP_NAME,' ')
FROM ${etlvar::ADWDB}.ONLINE_RETENT_PORTAL_TRANS a
left join prd_Adw.fes_usr_info b
on a.salesman_Cd = b.usr_name
where trunc(submission_date ,'MONTH') = trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')
;

update ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TRX_TMP002 x
set channel_effort ='RBD',
(store_cd, create_by, orig_create_by, orig_handle_by
, user_group_of_create_by, commission_by, user_group_of_commission_by) =(
    select store_cd, salesman_cd, salesman_cd, salesman_cd, grp_name, ' ', ' '
        from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_REMAP_RBD_TMP y 
        where
        x.CUST_NUM = y.CUST_NUM
        and x.cust_num = y.cust_num
        and x.subr_Num = y.subr_num
        and x.rate_plan_cd = y.renewed_plan_cd
        and x.inv_date = y.submission_date
)
where exists(
        select store_cd
        from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_REMAP_RBD_TMP y
        where
        x.CUST_NUM = y.CUST_NUM
        and x.cust_num = y.cust_num
        and x.subr_Num = y.subr_num
        and x.rate_plan_cd = y.renewed_plan_cd
        and x.inv_date = y.submission_date
)
and channel_effort ='Online'
;



DELETE FROM ${etlvar::MIGADWDB}.RBD_KPI_REPORT_TRX_DTL
where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
;
commit;

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














