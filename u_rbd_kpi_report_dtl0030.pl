/opt/etl/prd/etl/APP/RBD/Z_RBD_KPI_REPORT_DTL_UAT/bin> cat u_rbd_kpi_report_dtl0030.pl
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

--Retrieve details

TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_FL_FEE_TMP;
TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_LD_REBATE_AMT;
TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_FEE_TMP1;
TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_FEE_TMP2;
TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_FEE_TMP3;
TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_FEE_TMP4;
TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_TMP_SUMM;

TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_SERV_REV_T001;
TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_SIM_T001;
TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_ACCESORY_T001;
TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_PREP_VOU_T001;
TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_OTHER_T001;
TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_T001;
TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_HS_T001;

INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_LD_REBATE_AMT
(
MKT_CD,
VAS_CD,
REBATE_AMT
)
select b.mkt_cd ,coalesce(b.VAS_CD,' ') , (b.cb_AMT + coalesce(d.rebate_amt,0)) rebate_amt from 
        ( 
select a.mkt_cd
,a.VAS_CD
,a.cb_AMT
from 
( select mkt_cd , VAS_CD ,sum( PERIOD_CNT*cb_AMT) cb_AMT 
from 
( 
 select 
 a.mkg_cd as Mkt_Cd
,a.cr_cd
,a.ctr_type
,CASE WHEN a.ctr_type='PERIODIC' and a.Pattern <> ' ' THEN regexp_count(a.Pattern,'Y',1,'i')
       ELSE a.PERIOD_CNT 
 END PERIOD_CNT
,coalesce(b.cb_AMT,0) cb_AMT
,coalesce(b.VAS_CD,'') VAS_CD
from ${etlvar::ADWDB}.BILL_CRBK_OFFER_REF a 
left outer join 
(
with tbl_max_vas_cnt as(
select rownum as vas_num
from dual
connect by  rownum<( select max(regexp_count(vas_cd,',',1,'i')+1) from  ${etlvar::ADWDB}.bill_crbk_check_ref)
)
select regexp_substr(r.vas_cd,'[[:alnum:]]+',1,m.vas_num) as Vas_cd 
,m.vas_num
,r.Cr_Cd
,r.check_type
,r.cb_amt
from  ${etlvar::ADWDB}.bill_crbk_check_ref r
     ,tbl_max_vas_cnt m
where regexp_count(vas_cd,',',1,'i')+1 >= m.vas_num 
) b 
on a.cr_cd=b.cr_cd 
) b 
group by mkt_cd, VAS_CD
) a 
) b 
left outer join
(select distinct mkt_cd , rebate_amt from 
${etlvar::ADWDB}.MKT_REBATE_REF 
) d
on b.mkt_cd=d.mkt_cd
;
 COMMIT;

INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_FL_FEE_TMP
(
        CUST_NUM,
        SUBR_NUM,
        orig_fl_rate_plan_mthly_fee,
        new_fl_rate_plan_mthly_fee 
)
SELECT /*+ parallel(32) */
a.cust_num, a.subr_num, 
coalesce(c.monthly_fee,0) as orig_fl_rate_plan_monthly_fee,
coalesce(b.monthly_fee,0) as new_fl_rate_plan_monthly_fee 
FROM ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TRX_TMP002 a
left join (
    select /*+ parallel(32) */ 
        x.*,coalesce(y.monthly_fee,z.monthly_fee) as monthly_fee
        from (
        select * from ${etlvar::ADWDB}.FL_SUBR_INFO_HIST
        where subr_stat_cd in ('OK','PE')
        and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD') - 1 between start_date and end_date
        ) x
    left join ${etlvar::ADWDB}.nsp_bill_serv_ref y
    on x.rate_plan_cd = y.BILL_SERV_CD
    left join ${etlvar::ADWDB}.fl_rate_plan_ref z
    on x.rate_plan_cd = z.rate_plan_cd
) b
on a.cust_num = b.cust_num
and a.subr_num = b.subr_num
left join (
    select /*+ parallel(32) */ 
        x.*,coalesce(y.monthly_fee,z.monthly_fee) as monthly_fee
        from (
        select * from ${etlvar::ADWDB}.FL_SUBR_INFO_HIST
        where subr_stat_cd in ('OK','PE')
        and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD') - 2 between start_date and end_date
        )   x
    left join ${etlvar::ADWDB}.nsp_bill_serv_ref y
    on x.rate_plan_cd = y.BILL_SERV_CD
    left join ${etlvar::ADWDB}.fl_rate_plan_ref z
    on x.rate_plan_cd = z.rate_plan_cd
) c
on a.cust_num = c.cust_num
and a.subr_num = c.subr_num
where (new_rate_plan_Cd = 'JUP' or orig_rate_plan_Cd = 'JUP')
;
COMMIT;

INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_SERV_REV_T001
(
        STAFF_NAME
        ,STORE_CD 
        ,ORDER_NUM 
        ,INV_DATE 
        ,CUST_NUM 
        ,SUBR_NUM 
        ,CREATE_BY 
        ,COMMISSION_BY 
        ,CREATE_BY_FULL_NAME 
        ,COMMISSION_BY_FULL_NAME 
        ,CHANNEL_EFFORT 
        ,TRX_TYPE 
        ,ORIG_LD_EXP_DATE
        ,ORIG_LD_START_DATE
        ,NEW_LD_EXP_DATE
        ,NEW_LD_START_DATE   
        ,ORIG_LD_CD 
        ,NEW_LD_CD
        ,MKT_CD
        ,NEW_FREE_DATA_ENTITLE 
        ,ORIG_FREE_DATA_ENTITLE 
        ,ORIG_RATE_PLAN_CD 
        ,NEW_RATE_PLAN_CD 
        ,RATE_PLAN_GRP 
        ,OLD_LD_PERIOD 
        ,NEW_LD_PERIOD 
        ,TOTAL_REVENUE_OF_NEW_LD 
        ,VAS_FEE 
        ,OLD_PLAN_ADMIN_FEE_RATE 
        ,NEW_PLAN_ADMIN_FEE_RATE 
        ,TOTAL_REBATE_OF_FREE_MONTH 
        ,NUM_OF_MONTH_ORIG_LD_LEFT 
        ,NEW_RATE_PLAN_CHRG_RATE 
        ,ORIG_RATE_PLAN_CHRG_RATE
        ,ORIG_LD_REVENUE 
        ,NEW_LD_REVENUE 
        ,HANDSET_REBATE_AMT
        ,MONTHLY_PREPAY_AMT
        ,REF
        ,TXDATE
        ,REPLACE_OLD_LD
        ,add_vas
        ,eff_date
        ,ORIG_CREATE_BY
        ,ORIG_HANDLE_BY
        ,CREATE_BY_STAFF_ID
        ,COMM_BY_STAFF_ID
        ,CREATE_BY_CALL_CENTER_TEAM
        ,COMM_BY_CALL_CENTER_TEAM
        ---MOD_SR0002530---
        ,SMT_COUPON_AMT
        ,THIRDPARTY_PREM_AMT
)
select /*+ parallel(32) */
        create_by_full_name as staff_name
    ,STORE_CD 
    ,ORDER_NUM 
    ,INV_DATE 
    ,CUST_NUM 
    ,SUBR_NUM 
    ,CREATE_BY 
    ,COMMISSION_BY 
    ,CREATE_BY_FULL_NAME 
    ,COMMISSION_BY_FULL_NAME 
    ,CHANNEL_EFFORT 
    ,TRX_TYPE 
        ,ORIG_LD_EXP_DATE
        ,coalesce(ORIG_LD_START_DATE,date '1900-01-01')
        ,NEW_LD_EXP_DATE
        ,coalesce(NEW_LD_START_DATE,date '1900-01-01')
    ,ORIG_LD_CD 
    ,NEW_LD_CD
        ,MKT_CD
    ,FREE_DATA_ENTITLE 
    ,ORIG_FREE_DATA_ENTITLE 
    ,ORIG_RATE_PLAN_CD 
    ,NEW_RATE_PLAN_CD 
    ,RATE_PLAN_GRP 
    ,coalesce(OLD_LD_PERIOD,0)
    ,coalesce(NEW_LD_PERIOD,0)
    ,coalesce(TOTAL_REVENUE_OF_NEW_LD,0)
    ,coalesce(VAS_FEE,0)
    ,coalesce(OLD_PLAN_ADMIN_FEE_RATE,0)
    ,coalesce(NEW_PLAN_ADMIN_FEE_RATE,0)
    ,coalesce(TOTAL_REBATE_OF_FREE_MONTH,0)
    ,coalesce(NUM_OF_MONTH_ORIG_LD_LEFT,0)
    ,coalesce(NEW_RATE_PLAN_CHRG_RATE,0)
    ,coalesce(ORIG_RATE_PLAN_CHRG_RATE,0)
    ,coalesce(ORIG_LD_REVENUE,' ')
    ,coalesce(NEW_LD_REVENUE,' ')
    ,coalesce(HANDSET_REBATE_AMT,0)
    ,coalesce(MONTHLY_PREPAY_AMT,0)
        ,REF
    ,TXDATE
        ,REPLACE_OLD_LD
        ,add_vas
        ,eff_date
        ,ORIG_CREATE_BY
        ,ORIG_HANDLE_BY
        ,coalesce(y.STAFF_ID,' ')
        ,coalesce(z.STAFF_ID,' ')
        ,CREATE_BY_CALL_CENTER_TEAM
        ,COMM_BY_CALL_CENTER_TEAM
        ---MOD_SR0002530---
        ,coalesce(smt_coupon_amt,0)
        ,coalesce(thirdparty_prem_amt,0)
from (
    select /*+ parallel(32) */
    distinct
        a.store_cd, a.order_num,a.inv_date, a.cust_num, a.subr_num, a.create_by, a.commission_by, 
                a.create_by_call_center_team, a.comm_by_call_center_team,
                coalesce(i.full_name,' ') as create_by_full_name,
                coalesce(j.full_name,' ') as commission_by_full_name,
        a.channel_effort,
        trx_type, 
        orig_ld_exp_date, 
        orig_ld_start_date, 
        new_ld_exp_date, 
        new_ld_start_date, 
        orig_ld_cd, 
        new_ld_cd,
                a.mkt_cd,
        free_data_entitle, 
        orig_free_data_entitle,
        orig_rate_plan_cd, 
        new_rate_plan_cd, 
        rate_plan_grp,
                --Ld preiod
                case when (orig_ld_cd <> ' ' and orig_ld_cd <> 'NA')
                then coalesce(cast(substr(orig_ld_cd,4,2) as decimal),0)
                else 0
                end as OLD_LD_PERIOD,
                case when (NEW_LD_CD <> ' ' and NEW_LD_CD <> 'NA')
                and ref<>'Chg plan only'
                then coalesce(cast(substr(new_ld_cd,4,2) as decimal),1)
                else 1
                end as NEW_LD_PERIOD,
        --total contracted revenue
                0 as total_revenue_of_new_ld,
        --vas
        0 as vas_fee,
        --admin fee
        coalesce(g.bill_rate,0) old_plan_admin_fee_rate,
        coalesce(d.bill_rate,0) new_plan_admin_fee_rate,
        --total rebate amount of free months 
        coalesce(e.rebate_amt,0) as total_rebate_of_free_month,
        --remaining month of replaced original ld
        case when (orig_ld_exp_date <> date '2999-12-31' and orig_ld_exp_date <> date '1900-01-01')
        and 
        (
            extract(year from orig_ld_exp_date) > extract (year from inv_date)
            or (
                extract(year from orig_ld_exp_date) = extract(year from inv_date) and
                extract(month from orig_ld_exp_date) > extract(month from inv_date)
            )
        )
        and (orig_ld_cd <> 'NA' and orig_ld_cd <> ' ')
        then 
                        case when extract(year from orig_ld_exp_date) > extract (year from inv_date)
                        then 
                        (extract(year from orig_ld_exp_date) - extract(year from inv_date))*12 + 
                        extract(month from orig_ld_exp_date) - extract(month from inv_date)
                        when extract(year from orig_ld_exp_date) = extract (year from inv_date)
                        then extract(month from orig_ld_exp_date) - extract(month from inv_date)
                        else 0
                        end
        else 0
        end as NUM_OF_MONTH_ORIG_LD_LEFT,
        --rate plan traiff of new rate plan 
        case when new_rate_plan_cd = 'JUP'
        then k.new_fl_rate_plan_mthly_fee
        else coalesce(b.recur_bill_rate,0)
        end as new_rate_plan_chrg_rate,
        --rate plan traiff of original rate plan 
        case when orig_rate_plan_cd = 'JUP'
        then k.orig_fl_rate_plan_mthly_fee
        else coalesce(f.bill_rate,0)
        end as orig_rate_plan_chrg_rate,
        --orig & new ld reveneu flag
        a.orig_ld_revenue,
        a.new_ld_revenue,
        coalesce(h.net_amt,0) as handset_rebate_amt,
        coalesce(l.net_amt,0) as monthly_prepay_amt,
                REF,
        a.inv_date+1 as TXDATE,
                a.REPLACE_OLD_LD,
                add_vas,
                a.eff_date
                ,a.ORIG_CREATE_BY
                ,a.ORIG_HANDLE_BY
                --,i.STAFF_ID as CREATE_BY_STAFF_ID
        ---MOD_SR0002530---
        ,coalesce(m.coupon_value,0) as smt_coupon_amt
        ,coalesce(n.ssp_amt,0) as thirdparty_prem_amt
        from 
        ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TRX_TMP002 a
        left join (
        select /*+ parallel(32) */ * from ${etlvar::ADWDB}.bill_serv_ref
                where eff_end_date = date '2999-12-31'
                ) b
        on a.new_rate_plan_cd = b.bill_serv_cd
        left join (
            select /*+ parallel(32) */ a.rate_plan_cd,a.suppl_serv_cd, b.bill_rate
            from ${etlvar::ADWDB}.rate_plan_suppl_serv_map a
            left join ${etlvar::ADWDB}.bill_serv_ref b
            on a.suppl_serv_cd = b.bill_serv_cd
            where suppl_serv_cd in ('ADM','ADM2')
        ) d
        on a.new_rate_plan_cd = d.rate_plan_cd
        left join ${etlvar::MIGDB}.u_rbd_kpi_report_ld_rebate_amt e
        on a.rate_plan_cd = e.vas_cd
        and a.mkt_cd = e.mkt_cd
        left join (
        select * from ${etlvar::ADWDB}.bill_serv_ref
        where eff_end_date = date '2999-12-31'
                ) f
                on a.orig_rate_plan_cd = f.bill_serv_cd
        left join (
            select /*+ parallel(32) */ a.rate_plan_cd,a.suppl_serv_cd, b.bill_rate
            from ${etlvar::ADWDB}.rate_plan_suppl_serv_map a
            left join ${etlvar::ADWDB}.bill_serv_ref b
            on a.suppl_serv_cd = b.bill_serv_cd
            where suppl_serv_cd in ('ADM','ADM2')
        ) g
        on a.orig_rate_plan_cd = g.rate_plan_cd
        ---MOD_SR0002530---
        ---- Change to get rebate from HS_SUBSIDY
        left join (
                   Select a.order_num, a.cust_num, a.subr_num,sum(b.net_subsidy_amt) as net_amt 
                    from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TRX_TMP002 a
                        ,${etlvar::ADWDB}.HS_SUBSIDY b
                   where a.order_num = b.inv_num
                group by  a.order_num, a.cust_num, a.subr_num
        ) h 
        on a.order_num = h.order_num
        --(
--
--                              select /*+ parallel(32) */ 
--                              distinct a.inv_num, a.cust_num, a.subr_num, a.salesman_cd, a.mkt_cd, sum(b.net_amt) as net_amt
--                              from ${etlvar::ADWDB}.pos_inv_header a
--                              left join ${etlvar::ADWDB}.pos_inv_detail b
--                              on a.cust_num = b.cust_num
--                              and a.subr_num = b.subr_num
--                              and a.inv_num = b.inv_num
--                              where b.common_desc like '%HANDSET%REBATE%'
--                              and a.inv_num like 'I%'
--                              and a.inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
--                              and a.inv_num not in (select inv_num from ${etlvar::ADWDB}.pos_return_header where trx_date >=  trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH'))
--                              group by a.inv_num, a.cust_num, a.subr_num, a.salesman_cd, a.mkt_cd
--                              union all
--                              select /*+ parallel(32) */ distinct a.inv_num, a.cust_num, a.subr_num, a.salesman_cd, a.mkt_cd, sum(b.net_amt) as net_amt
--                              from ${etlvar::ADWDB}.pos_inv_header a
--                              left join ${etlvar::ADWDB}.pos_inv_detail b
--                              on a.cust_num = b.cust_num
--                              and a.subr_num = b.subr_num
--                              and a.inv_num = b.inv_num
--                              where b.common_desc like '%HANDSET%REBATE%'
--                              and a.inv_num like 'F%'
--                              and a.inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
--                              and a.inv_num not in (select inv_num from ${etlvar::ADWDB}.pos_return_header where trx_date >=  trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH'))
--                              group by a.inv_num, a.cust_num, a.subr_num, a.salesman_cd, a.mkt_cd
 --       ) h
  --      on a.order_num = h.inv_num
--              and a.cust_num = h.cust_num
--              and a.subr_num = h.subr_num
        left join ${etlvar::MIGDB}.U_RBD_KPI_REPORT_STAFF_NAME i
                on a.create_by = i.usr_name
        left join ${etlvar::MIGDB}.U_RBD_KPI_REPORT_STAFF_NAME j
                on a.commission_by = j.usr_name
        left join ${etlvar::MIGDB}.u_rbd_kpi_report_fl_fee_tmp k
        on a.cust_num = k.cust_num
        and a.subr_num = k.subr_num
        left join (
                                select /*+ parallel(32) */
                                distinct a.inv_num, a.cust_num, a.subr_num, a.salesman_cd, a.mkt_cd, sum(b.net_amt) as net_amt
                                from ${etlvar::ADWDB}.pos_inv_header a
                                left join ${etlvar::ADWDB}.pos_inv_detail b
                                on a.cust_num = b.cust_num
                                and a.subr_num = b.subr_num
                                and a.inv_num = b.inv_num
                                where b.common_desc like '%MTHLY%FEE%PREPAYMENT%'
                                and a.inv_num like 'I%'
                                and a.inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                and a.inv_num not in (select inv_num from ${etlvar::ADWDB}.pos_return_header where trx_date >=  trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH'))
                                group by a.inv_num, a.cust_num, a.subr_num, a.salesman_cd, a.mkt_cd
                                union all
                                select /*+ parallel(32) */ a.inv_num, a.cust_num, a.subr_num, a.salesman_cd, a.mkt_cd, sum(b.net_amt) as net_amt
                                from ${etlvar::ADWDB}.pos_inv_header a
                                left join ${etlvar::ADWDB}.pos_inv_detail b
                                on a.cust_num = b.cust_num
                                and a.subr_num = b.subr_num
                                and a.inv_num = b.inv_num
                                where b.common_desc like '%MTHLY%FEE%PREPAYMENT%'
                                and a.inv_num like 'F%'
                                and a.inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                and a.inv_num not in (select inv_num from ${etlvar::ADWDB}.pos_return_header where trx_date >=  trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH'))
                                group by a.inv_num, a.cust_num, a.subr_num, a.salesman_cd, a.mkt_cd
                ) l
                on a.cust_num = l.cust_num
                and a.subr_num = l.subr_num
        ---MOD_SR0002530---
        ---- Prepare the smt coupon
left join
    (Select t.order_num,sum(r.coupon_value) as  coupon_value
        from     ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TRX_TMP002 t
                ,${etlvar::ADWDB}.pos_inv_header p
                ,${etlvar::ADWDB}.pos_prem_entitle_coup_ref pc
                ,${etlvar::ADWDB}.pos_prem_coup_value_ref r
  where t.subr_num = p.subr_num
    and t.cust_num = p.cust_num
    and t.inv_date = p.inv_date    
    and p.mkt_cd = pc.entitle_mkt_cd
    and p.inv_date between pc.start_date and pc.end_date    
    and p.inv_num not in (
        select pr.inv_num from prd_adw.pos_return_header pr
        --where  pr.trx_date between  add_months(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD'),-12) and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
    ) 
    --and p.inv_num <> t.order_num
    and pc.coupon_code = r.coupon_code
    and t.inv_date between r.start_date and r.end_date
    and p.inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
    group by t.order_num) m
        on a.order_num  = m.order_num
        ---MOD_SR0002530---
        ---- Prepare the 3rd party
left join (
        select t.order_num,max(to_number(wr.ssp_amt)) ssp_amt
        from mig_adw.U_RBD_KPI_REPORT_TRX_TMP002 t
            ,prd_adw.pos_inv_header h
            ,mig_adw.B_RPT_SIMO_PREM_MKT_REF wr
        where   t.subr_num=h.subr_num
          and t.cust_num = h.cust_num
          and t.inv_date = h.inv_date
          and h.mkt_cd = wr.mkt_cd
          and wr.prem_type in ('SSP_THIRDPARTY_VAS')
          and h.inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
          and h.inv_date between wr.eff_s_date and wr.eff_e_date
        group by t.order_num
    )n
        on a.order_num = n.order_num
) x 
left join ${etlvar::MIGDB}.U_RBD_KPI_REPORT_STAFF_NAME y
on x.create_by = y.usr_name
left join ${etlvar::MIGDB}.U_RBD_KPI_REPORT_STAFF_NAME z
on x.commission_by = z.usr_name
;
COMMIT;

--Calculate revenue

UPDATE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_SERV_REV_T001
SET NUM_OF_MONTH_ORIG_LD_LEFT = 0
WHERE REPLACE_OLD_LD ='N'
and inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
;
COMMIT;

UPDATE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_SERV_REV_T001
SET NUM_OF_MONTH_ORIG_LD_LEFT = 0, ORIGINAL_LD_NET_CHARGE = 0
WHERE REPLACE_OLD_LD ='Y'
and trunc(orig_ld_start_date,'MONTH') > trunc(inv_date,'MONTH')
and inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
;
COMMIT;

UPDATE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_SERV_REV_T001
SET MONTHLY_PREPAY_AMT = 0
WHERE NEW_LD_REVENUE ='V'
and inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
;
COMMIT;

UPDATE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_SERV_REV_T001
SET ORIGINAL_LD_NET_CHARGE = 0
where NUM_OF_MONTH_ORIG_LD_LEFT = 0
and inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
;
COMMIT;

UPDATE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_SERV_REV_T001
SET NEW_PLAN_ADMIN_FEE_RATE = 0, OLD_PLAN_ADMIN_FEE_RATE =0
where new_ld_revenue = 'V'
and inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
;
COMMIT;

--Total contracted revenue
UPDATE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_SERV_REV_T001
SET total_revenue_of_new_ld = NEW_LD_PERIOD * (NEW_RATE_PLAN_CHRG_RATE+NEW_PLAN_ADMIN_FEE_RATE)
where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
;
COMMIT;

--Admin fee of new rate plan
UPDATE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_SERV_REV_T001
SET NEW_RATE_PLAN_ADMIN_FEE = NEW_LD_PERIOD * NEW_PLAN_ADMIN_FEE_RATE
where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
;
COMMIT;

--Admin fee left of original rate plan
UPDATE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_SERV_REV_T001
SET OLD_RATE_PLAN_ADMIN_FEE = NUM_OF_MONTH_ORIG_LD_LEFT * OLD_PLAN_ADMIN_FEE_RATE
WHERE NUM_OF_MONTH_ORIG_LD_LEFT > 0
and inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
;
COMMIT;

--Net revenue of original ld
UPDATE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_SERV_REV_T001
SET ORIGINAL_LD_NET_CHARGE = OLD_RATE_PLAN_ADMIN_FEE + NUM_OF_MONTH_ORIG_LD_LEFT * ORIG_RATE_PLAN_CHRG_RATE
WHERE NUM_OF_MONTH_ORIG_LD_LEFT > 0
and inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
;
COMMIT;

--Super care plans revenue
--Non-change plan only
UPDATE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_SERV_REV_T001
SET SUPER_CARE_PLANS_REVENUE = 
    total_revenue_of_new_ld - total_rebate_of_free_month
    - original_ld_net_charge - handset_rebate_amt
WHERE new_free_data_entitle in (
    select distinct a.free_data_entitle
    from ${etlvar::ADWDB}.rate_plan_ref a,${etlvar::ADWDB}.shk_rate_plan_grp_ref b
    where a.free_data_entitle = b.free_data_entitle
    and b.shk_plan_grp = 'SuperCare Mobile Mass plans')
and new_rate_plan_chrg_rate > 0
and inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
and ref <> 'Chg plan only'
;
COMMIT;

--Non-Super care plan reveune
--Non-change plan only
UPDATE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_SERV_REV_T001
SET OTHER_SERVICE_PLANS_REVENUE =
    total_revenue_of_new_ld - total_rebate_of_free_month
    - original_ld_net_charge - handset_rebate_amt
WHERE new_free_data_entitle not in (
    select distinct a.free_data_entitle
    from ${etlvar::ADWDB}.rate_plan_ref a,${etlvar::ADWDB}.shk_rate_plan_grp_ref b
    where a.free_data_entitle = b.free_data_entitle
    and b.shk_plan_grp = 'SuperCare Mobile Mass plans')
and new_rate_plan_chrg_rate > 0
and inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
and ref <> 'Chg plan only'
;
COMMIT;

--Super care plans revenue
--Change plan only
UPDATE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_SERV_REV_T001
SET SUPER_CARE_PLANS_REVENUE = 
(NEW_RATE_PLAN_CHRG_RATE - ORIG_RATE_PLAN_CHRG_RATE + NEW_PLAN_ADMIN_FEE_RATE - OLD_PLAN_ADMIN_FEE_RATE)* abs((OLD_LD_PERIOD - NEW_LD_PERIOD))
WHERE new_free_data_entitle in (
    select distinct a.free_data_entitle
    from ${etlvar::ADWDB}.rate_plan_ref a,${etlvar::ADWDB}.shk_rate_plan_grp_ref b
    where a.free_data_entitle = b.free_data_entitle
    and b.shk_plan_grp = 'SuperCare Mobile Mass plans')
and new_rate_plan_chrg_rate > 0
and inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
and ref = 'Chg plan only'
;
COMMIT;

--Non-Super care plan reveune
--Change plan only
UPDATE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_SERV_REV_T001
SET OTHER_SERVICE_PLANS_REVENUE = 
(NEW_RATE_PLAN_CHRG_RATE - ORIG_RATE_PLAN_CHRG_RATE + NEW_PLAN_ADMIN_FEE_RATE - OLD_PLAN_ADMIN_FEE_RATE)* abs((OLD_LD_PERIOD - NEW_LD_PERIOD))
WHERE new_free_data_entitle not in (
    select distinct a.free_data_entitle
    from ${etlvar::ADWDB}.rate_plan_ref a,${etlvar::ADWDB}.shk_rate_plan_grp_ref b
    where a.free_data_entitle = b.free_data_entitle
    and b.shk_plan_grp = 'SuperCare Mobile Mass plans')
and new_rate_plan_chrg_rate > 0
and inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
and ref = 'Chg plan only'
;
COMMIT;

--VAS revenue

--Ld revenue ='V'
INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_FEE_TMP1
(
        STORE_CD     
        ,ORDER_NUM    
        ,ACTUAL_ORDER_NUM
        ,CUST_NUM     
        ,SUBR_NUM     
        ,CREATE_BY    
        ,MKT_CD       
        ,LD_CD        
        ,LD_REVENUE   
        ,LD_START_DATE
        ,LD_EXP_DATE  
        ,VAS_CD  
        ,LD_PERIOD
        ,BILL_RATE          
        ,REBATE_AMT   
        ,TXDATE       
        ,INV_DATE
)
select /*+ parallel(32) */ distinct
STORE_CD, order_num, a.inv_num, CUST_NUM, SUBR_NUM, CREATE_BY, a.MKT_CD, LD_CD, a.LD_REVENUE,
LD_START_DATE, LD_EXP_DATE, a.VAS_CD, 
case when (ld_start_date <> date '1900-01-01' and ld_start_date <> date '2999-12-31' and ld_start_date is not null) 
and (ld_exp_date <> date '1900-01-01' and ld_exp_date <> date '2999-12-31' and ld_exp_date is not null)
then abs(months_between(trunc(ld_start_date,'MONTH'),trunc(ld_exp_date+1,'MONTH')))
when (ld_cd='NA' or ld_cd=' ')
then 1
else coalesce(cast(substr(ld_cd,4,2) as decimal),1)
end as LD_PERIOD,
d.bill_rate, coalesce(b.rebate_amt,0) as rebate_amt,txdate, inv_date
from (
        select /*+ parallel(32) */ 
        distinct a.store_cd, a.order_num, b.inv_num, a.cust_num, a.subr_num, a.create_by, start_date, end_date, e.ld_revenue,
        inv_date, txdate,
        case when a.mkt_cd <> b.mkt_cd
        then b.mkt_cd
        else a.mkt_cd
        end as mkt_cd,
        case when (c.bill_serv_cd <> ' ' and c.bill_serv_cd is not null)
        then c.bill_serv_Cd
        else e.ld_bill_cd
        end as vas_Cd,
        b.ld_cd,
        coalesce(bill_start_date, eff_date) as ld_start_date,
        b.ld_expired_date as ld_exp_date
        from (
                select /*+ parallel(32) */ * from ${etlvar::MIGADWDB}.RBD_KPI_REPORT_TRX_DTL
                where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                and new_ld_revenue = 'V'
                and eff_date <= sysdate
                ) a
        left join ${etlvar::ADWDB}.subr_ld_hist b
        on a.cust_num = b.cust_num
        and a.subr_num = b.subr_num
        left join (
        select /*+ parallel(32) */ cust_num, subr_num, bill_serv_cd, bill_start_date, bill_end_date from ${etlvar::ADWDB}.bill_servs
        where bill_start_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                and bill_end_date = date '2999-12-31'
        union
        select /*+ parallel(32) */ cust_num, subr_num, bill_serv_cd, bill_start_date, bill_end_date from ${etlvar::ADWDB}.bill_servs_pend_case
        where bill_start_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                and bill_end_date = date '2999-12-31'
        ) c
        on a.cust_num = c.cust_num
        and a.subr_num = c.subr_num
                and a.inv_date <= c.bill_start_date
        left join ${etlvar::ADWDB}.mkt_ref_vw e
        on (
        case when a.mkt_cd <> b.mkt_cd
        then b.mkt_cd
        else a.mkt_cd
        end
        ) = e.mkt_cd
        and e.ld_revenue <> 'P' 
) a
left join ${etlvar::MIGDB}.U_RBD_KPI_REPORT_LD_REBATE_AMT b
on a.mkt_Cd = b.mkt_Cd
and a.vas_cd = b.vas_Cd
left join ${etlvar::ADWDB}.mkt_ref_vw c
on a.mkt_cd = c.mkt_cd
left join (
select /*+ parallel(32) */ * from ${etlvar::ADWDB}.bill_serv_ref 
where eff_end_date = date '2999-12-31'
) d
on a.vas_cd = d.bill_serv_cd
left join (
select /*+ parallel(32) */ inv_num from ${etlvar::ADWDB}.pos_inv_header
where inv_date >= trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')
) e
on a.inv_num = e.inv_num
where ld_start_date >= start_date
and trunc(ld_exp_date,'MONTH') > trunc(inv_date,'MONTH')
and (ld_bill_Cd = a.vas_cd or (ld_bill_cd =' ' and c.ld_revenue ='V'))
and ld_start_date between start_date and end_date
and a.vas_cd not in ('ADM','ADM2')    
and a.vas_cd not in (select rate_plan_cd from ${etlvar::ADWDB}.rate_plan_ref)
and d.bill_rate <> 0
and a.ld_revenue is not null
and e.inv_num is not null
order by inv_num
;
COMMIT;

--New ld revenue ='P'
INSERT /*+ APPEND */ INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_FEE_TMP2
(
        STORE_CD     
        ,ORDER_NUM    
        ,ACTUAL_ORDER_NUM
        ,CUST_NUM     
        ,SUBR_NUM     
        ,CREATE_BY    
        ,MKT_CD       
        ,LD_CD        
        ,LD_REVENUE   
        ,LD_START_DATE
        ,LD_EXP_DATE  
        ,VAS_CD  
        ,LD_PERIOD
        ,BILL_RATE          
        ,REBATE_AMT   
        ,TXDATE       
        ,INV_DATE
)
select /*+ parallel(32) */ distinct
STORE_CD, order_num, a.inv_num, CUST_NUM, SUBR_NUM, CREATE_BY, a.MKT_CD, LD_CD, a.LD_REVENUE,
LD_START_DATE, LD_EXP_DATE, a.VAS_CD, 
case when (ld_start_date <> date '1900-01-01' and ld_start_date <> date '2999-12-31' and ld_start_date is not null) 
and (ld_exp_date <> date '1900-01-01' and ld_exp_date <> date '2999-12-31' and ld_exp_date is not null)
then abs(months_between(trunc(ld_start_date,'MONTH'),trunc(ld_exp_date+1,'MONTH')))
when (ld_cd='NA' or ld_cd=' ')
then 1
else coalesce(cast(substr(ld_cd,4,2) as decimal),1)
end as LD_PERIOD,
d.bill_rate, coalesce(b.rebate_amt,0) as rebate_amt,txdate, inv_date
from (
        select /*+ parallel(32) */ 
        distinct a.store_cd, a.order_num, b.inv_num, a.cust_num, a.subr_num, a.create_by, start_date, end_date, e.ld_revenue
        ,inv_date, txdate,
        case when a.mkt_cd <> b.mkt_cd
        then b.mkt_cd
        else a.mkt_cd
        end as mkt_cd,
        case when (c.bill_serv_cd <> ' ' and c.bill_serv_cd is not null)
        then c.bill_serv_Cd
        else e.ld_bill_cd
        end as vas_Cd,
        b.ld_cd,
        coalesce(bill_start_date, eff_date) as ld_start_date,
        b.ld_expired_date as ld_exp_date
        from (
                select /*+ parallel(32) */ * from ${etlvar::MIGADWDB}.RBD_KPI_REPORT_TRX_DTL
                where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                and new_ld_revenue = 'P'
                and eff_date <= sysdate
                and (cust_num, subr_num) not in (select cust_num, subr_num from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_FEE_TMP1)
                ) a
        left join ${etlvar::ADWDB}.subr_ld_hist b
        on a.cust_num = b.cust_num
        and a.subr_num = b.subr_num
        left join (
        select /*+ parallel(32) */ cust_num, subr_num, bill_serv_cd, bill_start_date, bill_end_date from ${etlvar::ADWDB}.bill_servs
        where bill_start_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                and bill_end_date = date '2999-12-31'
        union
        select /*+ parallel(32) */ cust_num, subr_num, bill_serv_cd, bill_start_date, bill_end_date from ${etlvar::ADWDB}.bill_servs_pend_case
        where bill_start_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                and bill_end_date = date '2999-12-31'
        ) c
        on a.cust_num = c.cust_num
        and a.subr_num = c.subr_num
                and a.inv_date <= c.bill_start_date
        left join ${etlvar::ADWDB}.mkt_ref_vw e
        on (
        case when a.mkt_cd <> b.mkt_cd
        then b.mkt_cd
        else a.mkt_cd
        end
        ) = e.mkt_cd
        where e.ld_revenue = 'P'
) a
left join ${etlvar::MIGDB}.U_RBD_KPI_REPORT_LD_REBATE_AMT b
on a.mkt_Cd = b.mkt_Cd
and a.vas_cd = b.vas_Cd
left join ${etlvar::ADWDB}.mkt_ref_vw c
on a.mkt_cd = c.mkt_cd
left join (
select /*+ parallel(32) */ * from ${etlvar::ADWDB}.bill_serv_ref 
where eff_end_date = date '2999-12-31'
) d
on a.vas_cd = d.bill_serv_cd
left join (
select /*+ parallel(32) */ inv_num from ${etlvar::ADWDB}.pos_inv_header
where inv_date >= trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')
) e
on a.inv_num = e.inv_num
where ld_start_date >= start_date
and trunc(ld_exp_date,'MONTH') > trunc(inv_date,'MONTH')
and ld_start_date between start_date and end_date
and a.vas_cd not in ('ADM','ADM2')    
and a.vas_cd not in (select rate_plan_cd from ${etlvar::ADWDB}.rate_plan_ref)
and d.bill_rate <> 0
and a.ld_revenue is not null
and e.inv_num is not null
order by inv_num
;
COMMIT;

--Extending vas
INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_FEE_TMP3
(
        STORE_CD     
        ,ORDER_NUM    
        ,ACTUAL_ORDER_NUM
        ,CUST_NUM     
        ,SUBR_NUM     
        ,CREATE_BY    
        ,MKT_CD       
        ,LD_CD        
        ,LD_REVENUE   
        ,LD_START_DATE
        ,LD_EXP_DATE  
        ,VAS_CD  
        ,LD_PERIOD
        ,BILL_RATE          
        ,REBATE_AMT   
        ,TXDATE       
        ,INV_DATE
)
select distinct
STORE_CD, order_num, a.inv_num, CUST_NUM, SUBR_NUM, CREATE_BY, a.MKT_CD, LD_CD, a.LD_REVENUE,
LD_START_DATE, LD_EXP_DATE, a.VAS_CD, 
case when (ld_start_date <> date '1900-01-01' and ld_start_date <> date '2999-12-31' and ld_start_date is not null) 
and (ld_exp_date <> date '1900-01-01' and ld_exp_date <> date '2999-12-31' and ld_exp_date is not null)
then abs(months_between(trunc(ld_start_date,'MONTH'),trunc(ld_exp_date+1,'MONTH')))
when (ld_cd='NA' or ld_cd=' ')
then 1
else coalesce(cast(substr(ld_cd,4,2) as decimal),1)
end as LD_PERIOD,
d.bill_rate, coalesce(b.rebate_amt,0) as rebate_amt,txdate, inv_date
from (
        select /*+ parallel(32) */ 
        distinct a.store_cd, a.order_num, b.inv_num, a.cust_num, a.subr_num, a.create_by, start_date, end_date, e.ld_revenue
        ,inv_date, txdate,
        case when a.mkt_cd <> b.mkt_cd
        then b.mkt_cd
        else a.mkt_cd
        end as mkt_cd,
        case when (c.bill_serv_cd <> ' ' and c.bill_serv_cd is not null)
        then c.bill_serv_Cd
        else e.ld_bill_cd
        end as vas_Cd,
        b.ld_cd,
        coalesce(bill_start_date, eff_date) as ld_start_date,
        b.ld_expired_date as ld_exp_date
        from (
                select /*+ parallel(32) */ * from ${etlvar::MIGADWDB}.RBD_KPI_REPORT_TRX_DTL
                where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                and eff_date <= sysdate
                and (cust_num, subr_num) not in (select cust_num, subr_num from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_FEE_TMP1)
                and (cust_num, subr_num) not in (select cust_num, subr_num from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_FEE_TMP2)
                ) a
        left join ${etlvar::ADWDB}.subr_ld_hist b
        on a.cust_num = b.cust_num
        and a.subr_num = b.subr_num
        left join (
        select /*+ parallel(32) */ cust_num, subr_num, bill_serv_cd, bill_start_date, bill_end_date from ${etlvar::ADWDB}.bill_servs
        where bill_end_date = date '2999-12-31'
        union
        select /*+ parallel(32) */ cust_num, subr_num, bill_serv_cd, bill_start_date, bill_end_date from ${etlvar::ADWDB}.bill_servs_pend_case
        where bill_end_date = date '2999-12-31'
        ) c
        on a.cust_num = c.cust_num
        and a.subr_num = c.subr_num
                and a.inv_date > c.bill_start_date
        left join ${etlvar::ADWDB}.mkt_ref_vw e
        on (
        case when a.mkt_cd <> b.mkt_cd
        then b.mkt_cd
        else a.mkt_cd
        end
        ) = e.mkt_cd
                where e.ld_revenue <> 'P'
) a
left join ${etlvar::MIGDB}.U_RBD_KPI_REPORT_LD_REBATE_AMT b
on a.mkt_Cd = b.mkt_Cd
and a.vas_cd = b.vas_Cd
left join ${etlvar::ADWDB}.mkt_ref_vw c
on a.mkt_cd = c.mkt_cd
left join (
select /*+ parallel(32) */ * from ${etlvar::ADWDB}.bill_serv_ref 
where eff_end_date = date '2999-12-31'
) d
on a.vas_cd = d.bill_serv_cd
left join (
select /*+ parallel(32) */ inv_num from ${etlvar::ADWDB}.pos_inv_header
where inv_date >= trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')
) e
on a.inv_num = e.inv_num
where ld_start_date >= start_date
and trunc(ld_exp_date,'MONTH') > trunc(inv_date,'MONTH')
and (ld_bill_Cd = a.vas_cd or (ld_bill_cd =' ' and c.ld_revenue ='V'))
and ld_start_date between start_date and end_date
and a.vas_cd not in ('ADM','ADM2')    
and a.vas_cd not in (select rate_plan_cd from ${etlvar::ADWDB}.rate_plan_ref)
and d.bill_rate <> 0
and a.ld_revenue is not null
and e.inv_num is not null
order by inv_num
;
COMMIT;

INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_FEE_TMP4
(
        STORE_CD     
        ,ORDER_NUM    
        ,ACTUAL_ORDER_NUM
        ,CUST_NUM     
        ,SUBR_NUM     
        ,CREATE_BY    
        ,MKT_CD       
        ,LD_CD        
        ,LD_REVENUE   
        ,LD_START_DATE
        ,LD_EXP_DATE  
        ,VAS_CD  
        ,LD_PERIOD
        ,BILL_RATE          
        ,REBATE_AMT   
        ,TXDATE       
        ,INV_DATE
)
select
STORE_CD, ORDER_NUM, ORDER_NUM as actual_order_num, CUST_NUM, SUBR_NUM, CREATE_BY, a.MKT_CD, NEW_LD_CD,
LD_REVENUE,NEW_LD_START_DATE, NEW_LD_EXP_DATE, a.VAS_CD, coalesce(LD_PERIOD,1), coalesce(b.bill_rate,0), coalesce(c.rebate_amt,0) as rebate_amt, txdate, inv_date
 from (
select 
distinct 
STORE_CD, ORDER_NUM, CUST_NUM, SUBR_NUM, CREATE_BY, MKT_CD, NEW_LD_CD, NEW_LD_START_DATE, 
NEW_LD_EXP_DATE, LD_REVENUE, TXDATE, INV_DATE, LD_PERIOD, 
trim(regexp_substr(x.vas_cd, '[^,]+', 1, levels.column_value)) as VAS_CD
from (
    select /*+ parallel(32) */
     distinct a.store_cd,  a.order_num, a.cust_num, a.subr_num, b.create_by, b.mkt_cd, b.new_ld_cd,
    b.NEW_LD_START_DATE, b.NEW_LD_EXP_DATE, c.ld_revenue, b.txdate, b.inv_date,
    case when (NEW_LD_START_DATE <> date '1900-01-01' and NEW_LD_START_DATE <> date '2999-12-31' and NEW_LD_START_DATE is not null)
    and (NEW_LD_EXP_DATE <> date '1900-01-01' and NEW_LD_EXP_DATE <> date '2999-12-31' and NEW_LD_EXP_DATE is not null)
    then abs(months_between(trunc(NEW_LD_START_DATE,'MONTH'),trunc(NEW_LD_EXP_DATE+1,'MONTH')))
    when (new_ld_cd='NA' or new_ld_cd=' ')
     then 1
     else coalesce(cast(substr(new_ld_cd,4,2) as decimal),1)
    end as LD_PERIOD,
    case when (a.add_vas =' ' or a.add_vas is null) and (c.ld_bill_cd <> ' ' and c.ld_bill_cd is not null)
    then c.ld_bill_cd
    else a.add_vas
    end as vas_cd
    from (
     select /*+ parallel(32) */ a.*
     from (
         select /*+ parallel(32) */
         a.cust_num, a.subr_num, a.store_cd, a.order_num,
         case when a.mkt_cd <> b.mkt_cd
         then b.mkt_cd
         else a.mkt_Cd
         end as mkt_Cd,
         case when (a.add_vas <> ' ' and b.add_vas not like '%,%') or (b.add_vas is null or b.add_vas = ' ')
         then a.add_vas
         else b.add_vas
         end as add_vas
         from (
         SELECT /*+ parallel(32) */ *
         FROM ${etlvar::MIGADWDB}.RBD_KPI_REPORT_TRX_DTL
         WHERE (eff_date > sysdate or ref ='Invoice')
                 and inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and    TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
         and (cust_num, subr_num) not in (select cust_num, subr_num from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_FEE_TMP1)
         and (cust_num, subr_num) not in (select cust_num, subr_num from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_FEE_TMP2)
         and (cust_num, subr_num) not in (select cust_num, subr_num from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_FEE_TMP3)
         and (cust_num, subr_num) in (
         select cust_num, subr_num
         from ${etlvar::ADWDB}.om_pending_chg_plan
         where add_vas <> ' '
         and eff_date > sysdate
         )
         ) a
         left join (
         select /*+ parallel(32) */ *
         from ${etlvar::ADWDB}.om_pending_chg_plan
         where add_vas <> ' '
         and eff_date > sysdate
         and create_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
         ) b
         on a.cust_num = b.cust_num
         and a.subr_num = b.subr_num
     ) a
     group by cust_num, subr_num, order_num, store_cd, add_vas, mkt_cd
    ) a
    left join ${etlvar::ADWDB}.RBD_KPI_REPORT_TRX_DTL b
    on a.cust_num = b.cust_num
    and a.subr_num = b.subr_num
    and a.store_cd = b.store_cd
    and a.order_num = b.order_num
    left join ${etlvar::ADWDB}.mkt_ref_vw c
    on b.mkt_cd = c.mkt_cd
) x ,
table(cast(multiset(select level from dual connect by  level <= length (regexp_replace(x.vas_cd, '[^,]+'))  + 1) as sys.OdciNumberList)) levels
) a
left join (
select /*+ parallel(32) */ * from ${etlvar::ADWDB}.bill_serv_ref
where eff_end_date = date '2999-12-31'
) b
on a.vas_cd = b.bill_serv_cd
left join ${etlvar::MIGDB}.U_RBD_KPI_REPORT_LD_REBATE_AMT c
on a.mkt_Cd = c.mkt_Cd
and a.vas_cd = c.vas_Cd
where (a.vas_cd is not null and a.vas_cd <> ' ' and a.ld_revenue is not null)
;
commit;

INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_TMP_SUMM
(
        CUST_NUM         
        ,SUBR_NUM         
        ,STORE_CD         
        ,ORDER_NUM     
        ,VAS_CD_LIST
        ,TOTAL_VAS_FEE
)
select distinct
    CUST_NUM, SUBR_NUM, STORE_CD, ORDER_NUM, listagg(vas_cd,', ') within group (order by cust_num, subr_num) as vas_cd_list 
        ,sum(ld_period*bill_rate-rebate_amt) as total_vas_fee
from (
    SELECT /*+ parallel(32) */ distinct
    CUST_NUM, SUBR_NUM, STORE_CD, ORDER_NUM
    ,vas_cd
    ,ld_period,bill_rate,rebate_amt
    ,row_number() over (
    partition by cust_num, subr_num, store_cd, vas_cd order by rebate_amt desc) as rn
    from 
    ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_FEE_TMP1    
) where rn = 1
group by CUST_NUM, SUBR_NUM, STORE_CD, ORDER_NUM
;
COMMIT;

INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_TMP_SUMM
(
        CUST_NUM         
        ,SUBR_NUM         
        ,STORE_CD         
        ,ORDER_NUM     
        ,VAS_CD_LIST
        ,TOTAL_VAS_FEE
)
select distinct
    CUST_NUM, SUBR_NUM, STORE_CD, ORDER_NUM, listagg(vas_cd,', ') within group (order by cust_num, subr_num) as vas_cd_list 
        ,sum(ld_period*bill_rate-rebate_amt) as total_vas_fee
from (
    SELECT /*+ parallel(32) */ distinct
    CUST_NUM, SUBR_NUM, STORE_CD, ORDER_NUM
    ,vas_cd
    ,ld_period,bill_rate,rebate_amt
    ,row_number() over (
    partition by cust_num, subr_num, store_cd, vas_cd order by rebate_amt desc) as rn
    from 
    ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_FEE_TMP2   
) where rn = 1
group by CUST_NUM, SUBR_NUM, STORE_CD, ORDER_NUM
;
COMMIT;

INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_TMP_SUMM
(
        CUST_NUM         
        ,SUBR_NUM         
        ,STORE_CD         
        ,ORDER_NUM     
        ,VAS_CD_LIST
        ,TOTAL_VAS_FEE
)
select distinct
    CUST_NUM, SUBR_NUM, STORE_CD, ORDER_NUM, listagg(vas_cd,', ') within group (order by cust_num, subr_num) as vas_cd_list 
        ,sum(ld_period*bill_rate-rebate_amt) as total_vas_fee
from (
    SELECT /*+ parallel(32) */ distinct
    CUST_NUM, SUBR_NUM, STORE_CD, ORDER_NUM
    ,vas_cd
    ,ld_period,bill_rate,rebate_amt
    ,row_number() over (
    partition by cust_num, subr_num, store_cd, vas_cd order by rebate_amt desc) as rn
    from 
    ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_FEE_TMP3    
) where rn = 1
group by CUST_NUM, SUBR_NUM, STORE_CD, ORDER_NUM
;
COMMIT;

INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_TMP_SUMM
(
        CUST_NUM         
        ,SUBR_NUM         
        ,STORE_CD         
        ,ORDER_NUM     
        ,VAS_CD_LIST
        ,TOTAL_VAS_FEE
)
select distinct
    CUST_NUM, SUBR_NUM, STORE_CD, ORDER_NUM, listagg(vas_cd,', ') within group (order by cust_num, subr_num) as vas_cd_list 
        ,sum(ld_period*bill_rate-rebate_amt) as total_vas_fee
from (
    SELECT /*+ parallel(32) */ distinct
    CUST_NUM, SUBR_NUM, STORE_CD, ORDER_NUM
    ,vas_cd
    ,ld_period,bill_rate,rebate_amt
    ,row_number() over (
    partition by cust_num, subr_num, store_cd, vas_cd order by rebate_amt desc) as rn
    from 
    ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_FEE_TMP4    
) where rn = 1
group by CUST_NUM, SUBR_NUM, STORE_CD, ORDER_NUM
;
COMMIT;

update ${etlvar::MIGDB}.U_RBD_KPI_REPORT_SERV_REV_T001 x set (add_vas,vas_fee) =
(
        select distinct vas_cd_list,coalesce(total_vas_fee,0) 
        from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_TMP_SUMM y
        where x.cust_num = y.cust_num
        and x.subr_num = y.subr_num
        and x.order_num = y.order_num
        and x.store_cd = y.STORE_CD
)
where exists (
        SELECT y.cust_num, y.subr_num
        FROM ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_TMP_SUMM y
        where x.cust_num = y.cust_num
        and x.subr_num = y.subr_num
        and x.order_num = y.order_num
        and x.store_cd = y.store_cd
)
and inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
;
COMMIT;

--Update the record details
update ${etlvar::MIGADWDB}.RBD_KPI_REPORT_TRX_DTL x set add_vas =
(
        select distinct vas_cd_list 
        from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_TMP_SUMM y
        where x.cust_num = y.cust_num
        and x.subr_num = y.subr_num
        and x.order_num = y.order_num
        and x.store_cd = y.STORE_CD
)
where exists (
        SELECT y.cust_num, y.subr_num
        FROM ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_TMP_SUMM y
        where x.cust_num = y.cust_num
        and x.subr_num = y.subr_num
        and x.order_num = y.order_num
        and x.store_cd = y.store_cd
)
and inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
;
COMMIT;

--Keep vas record details
INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_T001
(
        STORE_CD
        , ACTUAL_INV_NUM
        , CUST_NUM
        , SUBR_NUM
        , CREATE_BY
        , MKT_CD
        , LD_CD
        , LD_REVENUE
        , LD_START_DATE
        , LD_EXP_DATE
        , VAS_CD
        , BILL_RATE
        , LD_PERIOD
        , REBATE_AMT
        , TXDATE
        , INV_DATE
)
select /*+ parallel(32) */
        distinct
        STORE_CD
        , ACTUAL_ORDER_NUM
        , CUST_NUM
        , SUBR_NUM
        , CREATE_BY
        , MKT_CD
        , LD_CD
        , LD_REVENUE
        , LD_START_DATE
        , LD_EXP_DATE
        , VAS_CD
        , BILL_RATE
        , LD_PERIOD
        , REBATE_AMT
        , TXDATE
        , INV_DATE
from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_FEE_TMP1
;
COMMIT;

INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_T001
(
        STORE_CD
        , ACTUAL_INV_NUM
        , CUST_NUM
        , SUBR_NUM
        , CREATE_BY
        , MKT_CD
        , LD_CD
        , LD_REVENUE
        , LD_START_DATE
        , LD_EXP_DATE
        , VAS_CD
        , BILL_RATE
        , LD_PERIOD
        , REBATE_AMT
        , TXDATE
        , INV_DATE
)
select /*+ parallel(32) */
        distinct
        STORE_CD
        , ACTUAL_ORDER_NUM
        , CUST_NUM
        , SUBR_NUM
        , CREATE_BY
        , MKT_CD
        , LD_CD
        , LD_REVENUE
        , LD_START_DATE
        , LD_EXP_DATE
        , VAS_CD
        , BILL_RATE
        , LD_PERIOD
        , REBATE_AMT
        , TXDATE
        , INV_DATE
from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_FEE_TMP2
;
COMMIT;

INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_T001
(
        STORE_CD
        , ACTUAL_INV_NUM
        , CUST_NUM
        , SUBR_NUM
        , CREATE_BY
        , MKT_CD
        , LD_CD
        , LD_REVENUE
        , LD_START_DATE
        , LD_EXP_DATE
        , VAS_CD
        , BILL_RATE
        , LD_PERIOD
        , REBATE_AMT
        , TXDATE
        , INV_DATE
)
select /*+ parallel(32) */
        distinct
        STORE_CD
        , ACTUAL_ORDER_NUM
        , CUST_NUM
        , SUBR_NUM
        , CREATE_BY
        , MKT_CD
        , LD_CD
        , LD_REVENUE
        , LD_START_DATE
        , LD_EXP_DATE
        , VAS_CD
        , BILL_RATE
        , LD_PERIOD
        , REBATE_AMT
        , TXDATE
        , INV_DATE
from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_FEE_TMP3
;
COMMIT;

INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_T001
(
        STORE_CD
        , ACTUAL_INV_NUM
        , CUST_NUM
        , SUBR_NUM
        , CREATE_BY
        , MKT_CD
        , LD_CD
        , LD_REVENUE
        , LD_START_DATE
        , LD_EXP_DATE
        , VAS_CD
        , BILL_RATE
        , LD_PERIOD
        , REBATE_AMT
        , TXDATE
        , INV_DATE
)
select /*+ parallel(32) */
        distinct
        STORE_CD
        , ACTUAL_ORDER_NUM
        , CUST_NUM
        , SUBR_NUM
        , CREATE_BY
        , MKT_CD
        , LD_CD
        , LD_REVENUE
        , LD_START_DATE
        , LD_EXP_DATE
        , VAS_CD
        , BILL_RATE
        , LD_PERIOD
        , REBATE_AMT
        , TXDATE
        , INV_DATE
from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_VAS_FEE_TMP4
;
COMMIT;

UPDATE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_SERV_REV_T001
SET SUPER_CARE_PLANS_REVENUE = 0, OTHER_SERVICE_PLANS_REVENUE = 0, TOTAL_REVENUE_OF_NEW_LD = 0
WHERE new_ld_revenue='V'
and inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
;
COMMIT;

UPDATE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_SERV_REV_T001
SET VAS_FEE = 0
WHERE new_ld_revenue='P'
and (cust_num, subr_num) in (
select cust_num, subr_num from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_SERV_REV_T001
where new_ld_revenue<>'P'
)
and inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
;
COMMIT;

--Total service plan revenue
UPDATE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_SERV_REV_T001
SET TOTAL_SERVICE_REVENUE = SUPER_CARE_PLANS_REVENUE + OTHER_SERVICE_PLANS_REVENUE
where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
;
COMMIT;


----Handset sales
INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_HS_T001
(
        STAFF_NAME 
        ,STORE_CD 
        ,CUST_NUM 
        ,SUBR_NUM 
        ,INV_NUM 
        ,INV_DATE 
        ,SALESMAN_CD 
        ,MKT_CD 
        ,POS_AMT 
        ,TOTAL_TRADE_IN_AMT 
        ,WAREHOUSE 
        ,POS_PROD_CD 
        ,ITEM_CAT 
        ,RECPT_NUM 
        ,TOTAL_HS_REBATE_AMT 
        ,COUPON_DISCOUNT_AMT
        ,TXDATE
        ,ONLINE_INV_FLG
        ,QTY
        ,ORIG_SALESMAN_CD
        ,ORIG_USR_ID
        ,FA_INV_FLG
        ---MOD_SR0002530---
        ---Device sales change to hs subsisdy ---
        ,TOTAL_HS_SUBSIDY_AMT
)
select /*+ parallel(32) */ 
DISTINCT
    a.STAFF_NAME
    ,a.STORE_CD
    ,a.CUST_NUM
    ,a.SUBR_NUM
    ,a.INV_NUM
    ,a.INV_DATE
    ,a.SALESMAN_CD
    ,a.MKT_CD
        ---MOD_SR0002530---
        ---Device sales change to hs subsisdy ---
    --,a.POS_AMT-coalesce(b.TOTAL_HS_REBATE_AMT,0) as pos_amt
    ,a.POS_AMT-coalesce(d.net_subsidy_amt,0) - coalesce(c.COUPON_DISCOUNT_AMT,0) as pos_amt
    ,a.TOTAL_TRADE_IN_AMT
    ,a.WAREHOUSE
    ,a.POS_PROD_CD
    ,a.ITEM_CAT
    ,coalesce(c.RECPT_NUM,' ' )
    ,coalesce(b.TOTAL_HS_REBATE_AMT,0)
    ,coalesce(c.COUPON_DISCOUNT_AMT,0)
    ,a.TXDATE
        ,a.ONLINE_INV_FLG
        ,a.QTY
        ,d.salesman_cd as ORIG_SALESMAN_CD
        ,d.usr_id as ORIG_USR_ID
        ,case when (d.salesman_cd like 'FA%' or d.usr_id like 'FA%')
        then 'Y'
        else 'N'
        end as fa_flg
        ---MOD_SR0002530---
        ---Device sales change to hs subsisdy ---
    ,coalesce(d.net_subsidy_amt,0)
    from (
        select /*+ parallel(32) */ 
                coalesce(d.full_name, ' ') as staff_name, a.store_cd, a.cust_num, a.subr_num, a.inv_num, a.inv_date, a.salesman_cd, a.mkt_cd,pos_amt,
        a.warehouse, a.pos_prod_cd, a.item_cat
        ,a.inv_date+1 as TXDATE,total_trade_in_amt, a.online_inv_flg, a.qty
        from
        (
            select /*+ parallel(32) */ 
                        a.pos_shop_cd as store_cd, a.cust_num, a.subr_num, a.inv_num, a.inv_date, 
                        case when d.salesman_flg = 'Y'
                        then a.salesman_cd
                        else a.usr_id
                        end as salesman_cd, a.mkt_cd,sum(net_amt) as pos_amt, 
            listagg(b.warehouse,', ') within group (order by a.inv_num) as warehouse,
            listagg(b.pos_prod_cd,', ') within group (order by a.inv_num) as pos_prod_cd,
            listagg(c.item_cat,', ') within group (order by a.inv_num) as item_cat,
            sum(b.trade_in_amt) as total_trade_in_amt, a.online_inv_flg,
                        sum(
                        case when b.pos_prod_cd = 'CRPP'
                        then 0
                        else b.qty
                        end) as qty
            from 
            ${etlvar::ADWDB}.pos_inv_header a
            left join ${etlvar::ADWDB}.pos_inv_detail b
            on a.inv_num = b.inv_num
            left join ${etlvar::ADWDB}.pos_prod_ref c
            on  b.POS_PROD_CD = c.POS_PROD_CD 
                        left join ${etlvar::ADWDB}.fes_usr_info d
                        on a.salesman_cd = d.usr_name
                        left join ${etlvar::ADWDB}.fes_usr_info e
                        on a.usr_id = e.usr_name
            where 
            a.inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
            and (b.warehouse ='AH' or (b.warehouse='AA' and b.pos_prod_cd ='CRPP'))
            and b.pos_prod_cd not in ('CRRT','JDC','CRDP','CRCP','SIMTEENTRANSITH3','MONTHLYPLANREBATE','LDFD')
                        and a.inv_num like 'I%'
            and a.salesman_cd not like 'CA%'
            and a.pos_shop_cd in (select store_cd from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP)
            group by 
            a.pos_shop_cd, a.cust_num, a.subr_num, a.inv_num, a.inv_date, 
                        (
                        case when d.salesman_flg = 'Y'
                        then a.salesman_cd
                        else a.usr_id
                        end
                        ), a.mkt_cd, a.online_inv_flg
        ) a 
                left join ${etlvar::MIGDB}.U_RBD_KPI_REPORT_STAFF_NAME d
                on a.salesman_cd = d.usr_name
    ) a
    left join (
                select /*+ parallel(32) */ a.inv_num, a.cust_num, a.subr_num, a.salesman_cd, a.mkt_cd, sum(b.net_amt) as total_hs_rebate_amt
                from ${etlvar::ADWDB}.pos_inv_header a
                left join ${etlvar::ADWDB}.pos_inv_detail b
                on a.inv_num = b.inv_num
                where b.common_desc like '%HANDSET%REBATE%'
                and a.inv_Date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                group by 
                a.cust_num, a.subr_num, a.inv_num, a.inv_date, a.salesman_cd, a.mkt_cd
    ) b
    on a.inv_num = b.inv_num
    and a.cust_num = b.cust_num
    and a.subr_num = b.subr_num
    and a.salesman_cd = b.salesman_cd
    and a.mkt_cd = b.mkt_cd
    left join 
    (
            select /*+ parallel(32) */ x.recpt_num, X.inv_num, coupon_discount_amt
            from ${etlvar::ADWDB}.pos_recpt_detail x
            inner join (
            select recpt_num, sum(amt)  as coupon_discount_amt
            from ${etlvar::ADWDB}.pos_recpt_payment
            where PAYMENT_CD like '%COUP%'
            and recpt_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
            group by recpt_num
            ) y
            on x.recpt_num = y.recpt_num
            where x.recpt_Date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
    ) c
    on a.inv_num = c.inv_num
    left join ${etlvar::ADWDB}.pos_inv_header d
    on a.inv_num = d.inv_num
    and a.cust_num = d.cust_num
    and a.subr_num = d.subr_num
    ---MOD_SR0002530---
    ---Device sales change to hs subsisdy ---
    left join (
                Select b.inv_num,sum(b.net_subsidy_amt) as net_subsidy_amt
                    from ${etlvar::ADWDB}.HS_SUBSIDY b
                where
                        b.inv_date between add_months(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD'),-2) and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                group by  b.inv_num
        ) d on a.inv_num = d.inv_num
where a.inv_num not in 
(select inv_num from ${etlvar::ADWDB}.pos_return_header where trx_date >=  trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH'))

;
COMMIT;

----Prepaid SIM
INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_SIM_T001
(
        STAFF_NAME 
        ,STORE_CD 
        ,CUST_NUM 
        ,SUBR_NUM 
        ,INV_NUM 
        ,INV_DATE 
        ,SALESMAN_CD 
        ,MKT_CD 
        ,POS_AMT 
        ,WAREHOUSE 
        ,POS_PROD_CD 
        ,ITEM_CAT
        ,TXDATE
        ,ONLINE_INV_FLG
        ,QTY
        ,ORIG_SALESMAN_CD
        ,ORIG_USR_ID
        ,FA_INV_FLG
)
select /*+ parallel(32) */ 
DISTINCT
     a.STAFF_NAME
    ,a.STORE_CD
    ,a.CUST_NUM
    ,a.SUBR_NUM
    ,a.INV_NUM
    ,a.INV_DATE
    ,a.SALESMAN_CD
    ,a.MKT_CD
    ,a.POS_AMT
    ,a.WAREHOUSE
    ,a.POS_PROD_CD
    ,a.ITEM_CAT
    ,a.TXDATE
        ,a.ONLINE_INV_FLG
        ,a.QTY
        ,b.salesman_cd as ORIG_SALESMAN_CD
        ,b.usr_id as ORIG_USR_ID
        ,case when (b.salesman_cd like 'FA%' or b.usr_id like 'FA%')
        then 'Y'
        else 'N'
        end as fa_flg
from (
        select /*+ parallel(32) */ 
            x.STAFF_NAME, 
            x.STORE_CD, 
            x.CUST_NUM, 
            x.SUBR_NUM, 
            x.INV_NUM, 
            x.INV_DATE, 
            x.SALESMAN_CD, 
            x.MKT_CD, 
            x.POS_AMT, 
            x.WAREHOUSE, 
            x.POS_PROD_CD, 
            x.ITEM_CAT,
            TXDATE,
                        x.online_inv_flg,
                        x.QTY
            from 
        (
            select /*+ parallel(32) */
                        coalesce(d.full_name, ' ') as staff_name, a.store_cd, a.cust_num, a.subr_num, a.inv_num, a.inv_date, a.salesman_cd,        a.mkt_cd,pos_amt,
            pos_prod_cd, warehouse, item_cat,
            a.inv_date+1 as TXDATE, a.online_inv_flg, a.qty
            from 
            (
                select /*+ parallel(32) */ 
                                a.pos_shop_cd as store_cd, a.cust_num, a.subr_num, a.inv_num, a.inv_date, 
                                case when d.salesman_flg = 'Y'
                                then a.salesman_cd
                                else a.usr_id
                                end as salesman_cd, a.mkt_cd,
                listagg(b.pos_prod_cd, ', ') within group (order by a.inv_num) as pos_prod_cd,
                listagg(b.warehouse, ', ') within group (order by a.inv_num) as warehouse,
                listagg(c.item_cat, ', ') within group (order by a.inv_num) as item_cat,
                coalesce(sum(net_amt),0) as pos_amt
                ,TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD') as TXDATE, a.online_inv_flg, sum(b.qty) as qty
                from 
                ${etlvar::ADWDB}.pos_inv_header a
                left join ${etlvar::ADWDB}.pos_inv_detail b
                on a.inv_num = b.inv_num
                left join ${etlvar::ADWDB}.pos_prod_ref c
                on  b.POS_PROD_CD = c.POS_PROD_CD 
                                left join ${etlvar::ADWDB}.fes_usr_info d
                                on a.salesman_cd = d.usr_name
                                left join ${etlvar::ADWDB}.fes_usr_info e
                                on a.usr_id = e.usr_name
                where b.warehouse in ('AP', 'PP')
                and a.inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                and b.pos_prod_cd not in ('CRPP','CRRT','JDC','CRDP','CRCP','SIMTEENTRANSITH3','MONTHLYPLANREBATE','LDFD')
                and item_cat not in ('OTHER','ACC','FIXED','PC3')
                                and a.inv_num like 'I%'
                and a.salesman_cd not like 'CA%'
                and a.pos_shop_cd in (select store_cd from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP)
                group by 
                                a.pos_shop_cd, a.cust_num, a.subr_num, a.inv_num, a.inv_date, 
                                (
                                case when d.salesman_flg = 'Y'
                                then a.salesman_cd
                                else a.usr_id
                                end
                                ), a.mkt_cd, a.online_inv_flg
            ) a
                        left join ${etlvar::MIGDB}.U_RBD_KPI_REPORT_STAFF_NAME d
                        on a.salesman_cd = d.usr_name
                        where a.inv_num not in 
                        (select inv_num from ${etlvar::ADWDB}.pos_return_header  where trx_date >=  trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH'))
        ) x
) a
left join ${etlvar::ADWDB}.pos_inv_header b
on a.inv_num = b.inv_num
and a.cust_num = b.cust_num
and a.subr_num = b.subr_num
;
COMMIT;

----Accessories
INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_ACCESORY_T001 
(
        STAFF_NAME 
        ,STORE_CD 
        ,CUST_NUM 
        ,SUBR_NUM 
        ,INV_NUM 
        ,INV_DATE 
        ,SALESMAN_CD 
        ,MKT_CD 
        ,POS_AMT 
        ,WAREHOUSE 
        ,POS_PROD_CD 
        ,ITEM_CAT 
        ,RECPT_NUM 
        ,COUPON_DISCOUNT_AMT
        ,TXDATE
        ,ONLINE_INV_FLG
        ,QTY
        ,ORIG_SALESMAN_CD
        ,ORIG_USR_ID
        ,FA_INV_FLG
)
select /*+ parallel(32) */ 
DISTINCT
         a.STAFF_NAME
    ,a.STORE_CD 
    ,a.CUST_NUM 
    ,a.SUBR_NUM 
    ,a.INV_NUM 
    ,a.INV_DATE 
    ,a.SALESMAN_CD 
    ,a.MKT_CD 
    ,a.POS_AMT 
    ,a.WAREHOUSE 
    ,a.POS_PROD_CD 
    ,a.ITEM_CAT 
    ,coalesce(a.RECPT_NUM,' ')
    ,coalesce(a.COUPON_DISCOUNT_AMT,0)
    ,a.TXDATE
        ,a.ONLINE_INV_FLG
        ,a.QTY
        ,b.salesman_cd as ORIG_SALESMAN_CD
        ,b.usr_id as ORIG_USR_ID
        ,case when (b.salesman_cd like 'FA%' or b.usr_id like 'FA%')
        then 'Y'
        else 'N'
        end as fa_flg
from (
    select /*+ parallel(32) */ 
        a.STAFF_NAME, 
        a.STORE_CD, 
        a.CUST_NUM, 
        a.SUBR_NUM, 
        a.INV_NUM, 
        a.INV_DATE, 
        a.SALESMAN_CD, 
        a.MKT_CD, 
        a.POS_AMT, 
        a.WAREHOUSE,
        a.POS_PROD_CD,
        a.ITEM_CAT, 
        coalesce(a.RECPT_NUM,' ') as RECPT_NUM,
        coalesce(a.COUPON_DISCOUNT_AMT,0) as COUPON_DISCOUNT_AMT,
        a.TXDATE,
                a.online_inv_flg,
                a.QTY
        from 
    (
        select /*+ parallel(32) */ 
                nvl(d.full_name,' ') as staff_name,a.*, a.inv_date+1 as TXDATE,c.recpt_num,c.coupon_discount_amt
        from
        (
            select /*+ parallel(32) */ 
                        a.pos_shop_cd as store_cd, a.cust_num, a.subr_num, a.inv_num, a.inv_date, 
                        case when d.salesman_flg = 'Y'
                        then a.salesman_cd
                        else a.usr_id
                        end as salesman_cd, a.mkt_cd,
                        coalesce(sum(net_amt),0) as pos_amt,
                        listagg(b.warehouse,', ') within group (order by a.inv_num) as warehouse,
            listagg(b.pos_prod_cd,', ') within group (order by a.inv_num) as pos_prod_cd,
            listagg(c.item_cat,', ') within group (order by a.inv_num) as item_cat, a.online_inv_flg, sum(b.qty) as qty
                        from 
            ${etlvar::ADWDB}.pos_inv_header a
            left join ${etlvar::ADWDB}.pos_inv_detail b
            on a.inv_num = b.inv_num
            left join ${etlvar::ADWDB}.pos_prod_ref c
            on  b.POS_PROD_CD = c.POS_PROD_CD 
                        left join ${etlvar::ADWDB}.fes_usr_info d
                        on a.salesman_cd = d.usr_name
                        left join ${etlvar::ADWDB}.fes_usr_info e
                        on a.usr_id = e.usr_name
            where 
            b.pos_prod_cd not in ('CRRT','CRPP','JDC','CRDP','CRCP','SIMTEENTRANSITH3','MONTHLYPLANREBATE','LDFD')
            and item_cat in ('ACC','FIXED')
                        and b.warehouse in ('AA')
            and a.salesman_cd not like 'CA%'
                        and a.inv_num like 'I%'
            and a.pos_shop_cd in (select store_cd from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP)
            and a.inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
            group by a.pos_shop_cd, a.cust_num, a.subr_num, a.inv_num, a.inv_date, 
                        case when d.salesman_flg = 'Y'
                        then a.salesman_cd
                        else a.usr_id
                        end, a.mkt_cd, a.online_inv_flg
        ) a
        left join 
        (
                select /*+ parallel(32) */ 
                                x.recpt_num, X.inv_num, coupon_discount_amt
                from ${etlvar::ADWDB}.pos_recpt_detail x
                inner join (
                select recpt_num, sum(amt)  as coupon_discount_amt
                from ${etlvar::ADWDB}.pos_recpt_payment
                where PAYMENT_CD like '%COUP%' 
                and recpt_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                group by recpt_num
                ) y
                on x.recpt_num = y.recpt_num
                where x.recpt_Date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
        ) c
        on a.inv_num = c.inv_num
                left join ${etlvar::MIGDB}.U_RBD_KPI_REPORT_STAFF_NAME d
                on a.salesman_cd = d.usr_name
                where a.inv_num not in 
                (select inv_num from ${etlvar::ADWDB}.pos_return_header  where trx_date >=  trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH'))
    ) a
) a
left join ${etlvar::ADWDB}.pos_inv_header b
on a.inv_num = b.inv_num
and a.cust_num = b.cust_num
and a.subr_num = b.subr_num
;
COMMIT;


--Prepaid Voucher
INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_PREP_VOU_T001
(
        STAFF_NAME
        ,STORE_CD 
        ,CUST_NUM 
        ,SUBR_NUM 
        ,INV_NUM 
        ,INV_DATE 
        ,SALESMAN_CD 
        ,MKT_CD 
        ,POS_AMT 
        ,WAREHOUSE 
        ,POS_PROD_CD 
        ,ITEM_CAT
        ,TXDATE
        ,ONLINE_INV_FLG
        ,QTY
        ,ORIG_SALESMAN_CD
        ,ORIG_USR_ID
        ,FA_INV_FLG
)
select /*+ parallel(32) */ 
DISTINCT
     a.STAFF_NAME 
    ,a.STORE_CD 
    ,a.CUST_NUM 
    ,a.SUBR_NUM 
    ,a.INV_NUM 
    ,a.INV_DATE 
    ,a.SALESMAN_CD 
    ,a.MKT_CD 
    ,a.POS_AMT 
    ,a.WAREHOUSE 
    ,a.POS_PROD_CD 
    ,a.ITEM_CAT 
    ,a.TXDATE
        ,a.ONLINE_INV_FLG
        ,a.QTY
        ,b.salesman_cd as ORIG_SALESMAN_CD
        ,b.usr_id as ORIG_USR_ID
        ,case when (b.salesman_cd like 'FA%' or b.usr_id like 'FA%')
        then 'Y'
        else 'N'
        end as fa_flg
    from 
(
    select /*+ parallel(32) */ 
        nvl(d.full_name,' ') as staff_name,a.*
    from
        (
        select /*+ parallel(32) */ 
                a.pos_shop_cd as store_cd, a.cust_num, a.subr_num, a.inv_num, a.inv_date, 
                case when d.salesman_flg = 'Y'
                then a.salesman_cd
                else a.usr_id
                end as salesman_cd,     a.mkt_cd, 
        coalesce(sum(net_amt),0) as pos_amt,
        listagg(b.warehouse,', ') within group (order by a.inv_num) as warehouse,
        listagg(c.item_cat,', ') within group (order by a.inv_num)as item_cat,
        listagg(b.pos_prod_cd,', ') within group (order by a.inv_num)as pos_prod_cd
        ,a.inv_date+1 as TXDATE, a.online_inv_flg, sum(b.qty) as qty
        from 
        ${etlvar::ADWDB}.pos_inv_header a
        left join ${etlvar::ADWDB}.pos_inv_detail b
        on a.inv_num = b.inv_num
        left join ${etlvar::ADWDB}.pos_prod_ref c
        on  b.POS_PROD_CD = c.POS_PROD_CD 
        left join ${etlvar::ADWDB}.fes_usr_info d
        on a.salesman_cd = d.usr_name
                left join ${etlvar::ADWDB}.fes_usr_info e
        on a.usr_id = e.usr_name
        where 
        b.pos_prod_cd not in ('CRPP','CRDP','AALGGD330COUPON','AASSE1100COUPON','MONTHLYPLANREBATE','LDFD')
        and item_cat in ('PC3')
                and a.inv_num like 'I%'
        and a.salesman_cd not like 'CA%'
        and a.pos_shop_cd in (select store_cd from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP)
        and a.inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
        group by 
        d.full_name, 
                a.pos_shop_cd, a.cust_num, a.subr_num, a.inv_num, a.inv_date, 
                case when d.salesman_flg = 'Y'
                then a.salesman_cd
                else a.usr_id
                end, a.mkt_cd, a.online_inv_flg
    ) a
        left join ${etlvar::MIGDB}.U_RBD_KPI_REPORT_STAFF_NAME d
        on a.salesman_cd = d.usr_name
        where a.inv_num 
        not in (select inv_num from ${etlvar::ADWDB}.pos_return_header  where trx_date >=  trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH'))
) a
left join ${etlvar::ADWDB}.pos_inv_header b
on a.inv_num = b.inv_num
and a.cust_num = b.cust_num
and a.subr_num = b.subr_num
;
COMMIT;

--Other
INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_OTHER_T001
(
        STAFF_NAME 
        ,STORE_CD 
        ,CUST_NUM 
        ,SUBR_NUM 
        ,INV_NUM 
        ,INV_DATE 
        ,SALESMAN_CD 
        ,MKT_CD 
        ,POS_AMT 
        ,WAREHOUSE 
        ,POS_PROD_CD 
        ,ITEM_CAT
        ,TXDATE
        ,ONLINE_INV_FLG
        ,QTY
        ,ORIG_SALESMAN_CD
        ,ORIG_USR_ID
        ,FA_INV_FLG
)
select /*+ parallel(32) */ 
DISTINCT
     a.STAFF_NAME
    ,a.STORE_CD
    ,a.CUST_NUM
    ,a.SUBR_NUM
    ,a.INV_NUM
    ,a.INV_DATE
    ,a.SALESMAN_CD
    ,a.MKT_CD
    ,a.POS_AMT
    ,a.WAREHOUSE
    ,a.POS_PROD_CD 
    ,a.ITEM_CAT 
    ,a.TXDATE
        ,a.ONLINE_INV_FLG
        ,a.QTY
        ,b.salesman_cd as ORIG_SALESMAN_CD
        ,b.usr_id as ORIG_USR_ID
        ,case when (b.salesman_cd like 'FA%' or b.usr_id like 'FA%')
        then 'Y'
        else 'N'
        end as fa_flg
    from 
(
    select /*+ parallel(32) */ 
        nvl(b.full_name,' ') as staff_name,a.*
    from 
    (
        select /*+ parallel(32) */ 
                a.pos_shop_cd as store_cd, a.cust_num, a.subr_num, a.inv_num, a.inv_date, 
                case when c.salesman_flg = 'Y'
                then a.salesman_cd
                else a.usr_id
                end as salesman_cd,
                a.mkt_cd,
        coalesce(sum(net_amt),0) as pos_amt,
        listagg(b.pos_prod_cd,', ') within group (order by a.inv_num) as pos_prod_cd,
        listagg(b.warehouse,', ') within group (order by a.inv_num) as warehouse,
        listagg(e.item_cat,', ') within group (order by a.inv_num) as item_cat,
                a.online_inv_flg
        ,a.inv_date+1 as TXDATE, sum(b.qty) as qty
        from 
        ${etlvar::ADWDB}.pos_inv_header a
        left join ${etlvar::ADWDB}.pos_inv_detail b
        on a.inv_num = b.inv_num
        left join ${etlvar::ADWDB}.pos_prod_ref e
        on  b.POS_PROD_CD = e.POS_PROD_CD 
        left join ${etlvar::ADWDB}.fes_usr_info c
        on a.salesman_cd = c.usr_name
                left join ${etlvar::ADWDB}.fes_usr_info d
        on a.usr_id = d.usr_name
        where 
        b.pos_prod_cd not in ('CRPP','CRDP','MONTHLYPLANREBATE','LDFD') --LDFD = 'HANDSET REDEMPTION'
        and item_cat in ('OTHER')
        and a.salesman_cd not like 'CA%'
                and a.inv_num like 'I%'
        and a.pos_shop_cd in (select store_cd from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP)
        and a.inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and  TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
        group by a.pos_shop_cd, d.full_name, a.cust_num, a.subr_num, a.inv_num, a.inv_date,              
                case when c.salesman_flg = 'Y'
                then a.salesman_cd
                else a.usr_id
                end,     a.mkt_cd, a.online_inv_flg
    ) a 
        left join ${etlvar::MIGDB}.U_RBD_KPI_REPORT_STAFF_NAME b
        on a.salesman_cd = b.usr_name
        where a.inv_num 
        not in (select inv_num from ${etlvar::ADWDB}.pos_return_header  where trx_date >=  trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH'))
) a
left join ${etlvar::ADWDB}.pos_inv_header b
on a.inv_num = b.inv_num
and a.cust_num = b.cust_num
and a.subr_num = b.subr_num
;
COMMIT;

--Refresh image
DELETE FROM ${etlvar::MIGADWDB}.RBD_KPI_REPORT_SERV_REV_DTL
where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1;

DELETE FROM ${etlvar::MIGADWDB}.RBD_KPI_REPORT_HS_DTL
where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1;

DELETE FROM ${etlvar::MIGADWDB}.RBD_KPI_REPORT_SIM_DTL
where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1; 

DELETE FROM ${etlvar::MIGADWDB}.RBD_KPI_REPORT_ACCESORY_DTL 
where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1; 

DELETE FROM ${etlvar::MIGADWDB}.RBD_KPI_REPORT_PREPA_VOU_DTL
where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1; 

DELETE FROM ${etlvar::MIGADWDB}.RBD_KPI_REPORT_OTHER_DTL
where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1;
 
DELETE FROM ${etlvar::MIGADWDB}.RBD_KPI_REPORT_VAS_FEE_DTL
where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1; 

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

        $OUTPUT_FILE_PATH = "${etlvar::ETL_OUTPUT_DIR}/${etlvar::ETLSYS}/U_RBD_KPI_REPORT_UAT";
        $FILE_PREFIX = "Hitrate";

    }
    else
    {
        ##  PRODUCTION  ##
        $TDUSR = "${etlvar::TDUSR}";
        $TDPWD = "${etlvar::TDPWD}";
        $TDDSN = $ENV{"AUTO_DSN"};

        $OUTPUT_FILE_PATH = "${etlvar::ETL_OUTPUT_DIR}/${etlvar::ETLSYS}/U_RBD_KPI_REPORT_UAT";
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














