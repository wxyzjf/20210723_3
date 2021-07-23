/opt/etl/prd/etl/APP/RBD/Z_RBD_KPI_REPORT_DTL_UAT/bin> cat  u_rbd_kpi_report_dtl0055.pl
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

declare

    channel_cost_data_ready_ind VARCHAR2(12) := 'N';
        commission_data_ready_ind VARCHAR2(12) := 'N';

begin

    select 
    case when count(distinct period) = 1
    then 'Y'
    else 'N'
    end channel_cost_data_ready_ind    
    into channel_cost_data_ready_ind
    from ${etlvar::ADWDB}.RBD_SALES_CHANNEL_COST
    where period = add_months(trunc(to_date('${etlvar::TXDATE}','YYYY-MM-DD'),'MONTH'),-1)
    ;

        select 
        case when count(*) > 0
        then 'Y'
        else 'N'
        end as commission_data_ready_ind
        into commission_data_ready_ind
        from PRD_BIZ_SUMM_VW.VW_RBD_QUERY_ALL  
        where trx_month = add_months(trunc(to_date('${etlvar::TXDATE}','YYYY-MM-DD'),'MONTH'),-1)
        ;
    
    if (channel_cost_data_ready_ind = 'N' or commission_data_ready_ind ='N')
        then 
                        DBMS_OUTPUT.PUT_LINE('Either Last month''s RBD sales channel cost or RBD commission data are not ready yet, not refreshing the contribution report');
        else 
            DBMS_OUTPUT.PUT_LINE('Both last month''s RBD sales channel cost and RBD commission data are ready, refreshing the contribution report');     

                        EXECUTE IMMEDIATE 'TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T3';
                        EXECUTE IMMEDIATE 'TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T4';


                        --Row no = 1
                        --Start of Sales revenue
                        --a.) Postpaid
                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T3
                        (
                                store_cd
                                ,staff_id
                                ,staff_name
                                ,RowName 
                                ,POSTPAID 
                                ,TRX_MONTH
                                ,ROW_NO 
                        )
                        select 
                        a.store_cd, 
                        coalesce(a.staff_id,' ') as staff_id,
                        coalesce(b.full_name,' ') as full_name,
                        'Sales revenue' as ROWNAME, 
                        coalesce(REVENUE,0) as revenue, 
                        coalesce(TRX_MONTH,trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')) as trx_month,
                        coalesce(ROW_NO,1) as row_no
                        from (
                                select store_cd, staff_id, 'Sales revenue' as rowName, sum(revenue*discount_rate) as revenue, trx_month, 1 as row_no
                                from (
                                        select 
                                        --Postpaid
                                        a.store_cd,
                                        c.staff_id,
                                        rate_plan_grp,
                                        TOTAL_SERVICE_REVENUE+VAS_FEE as revenue,
                                        b.discount_rate,
                                        trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') as trx_month
                                        from (
                                                select 
                                                case when x.orig_create_by like 'FA%' and x.orig_handle_by not like 'FA%'
                                                and store_cd in (select store_cd from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP where daily_sales_sms='Y')
                                                then x.orig_handle_by
                                                else x.create_by
                                                end as usr_name,
                                                x.* from 
                                                ${etlvar::MIGADWDB}.RBD_KPI_REPORT_SERV_REV_DTL x
                                                where cast(substr(rate_plan_grp,1,2) as decimal) <= 12
                                                and inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                        ) a
                                        left join ${etlvar::ADWDB}.RBD_KPI_REPORT_DIS_RATE b
                                        on upper(a.channel_effort) = upper(b.channel)
                                        left join ${etlvar::MIGDB}.U_RBD_KPI_REPORT_STAFF_NAME c
                                        on a.usr_name = c.usr_name
                                )
                                group by store_cd, staff_id, trx_month
                        ) a
                        left join (
                                select distinct staff_id, full_name  from ${etlvar::MIGDB}.u_rbd_kpi_report_staff_name 
                        ) b
                        on a.staff_id = b.staff_id
                        ;

                        commit;

                        --b.) Handset
                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T3
                        (
                                store_cd
                                ,staff_id
                                ,staff_name
                                ,RowName 
                                ,HANDSET 
                                ,TRX_MONTH
                                ,ROW_NO 
                        )
                        select 
                        a.store_cd, 
                        coalesce(a.staff_id,' ') as staff_id,
                        coalesce(b.full_name,' ') as full_name,
                        'Sales revenue' as ROWNAME, 
                        coalesce(REVENUE,0) as revenue, 
                        coalesce(TRX_MONTH,trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')) as trx_month,
                        coalesce(ROW_NO,1) as row_no
                        from (
                                select store_cd, staff_id, 'Sales revenue' as rowName, sum(revenue*discount_rate) as revenue, trx_month, 1 as row_no
                                from (
                                        select 
                                        --Postpaid
                                        store_cd,
                                        staff_id, 
                                        revenue,
                                        discount_rate
                                        ,trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') as trx_month
                                        from (
                                                select
                                                store_cd,
                                                staff_id,
                                                inv_date,
                                                case when salesman_cd like 'FA%'
                                                then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel='Street fighter')
                                                when online_inv_flg in ('Y','B')
                                                then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel='Online')
                                                when store_cd in (select store_cd from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP where daily_sales_sms='Y')
                                                then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel='RBD')
                                                else (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel='Others')
                                                end as discount_rate,
                                            pos_amt+total_trade_in_amt+total_hs_rebate_amt as revenue 
                                                from (
                                                        select 
                                                        c.staff_id, 
                                                        a.* 
                                                        from (
                                                                select 
                                                                case when store_cd in (select store_cd from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP where daily_sales_sms ='Y')
                                                                then 'Y'
                                                                else 'N'
                                                                end as store_flg
                                                                ,a.* 
                                                                from ${etlvar::MIGADWDB}.RBD_KPI_REPORT_HS_DTL a
                                                        ) a
                                                        left join ${etlvar::MIGDB}.U_RBD_KPI_REPORT_STAFF_NAME c
                                                        on (
                                                        case when orig_salesman_cd like 'FA%' and orig_usr_id not like 'FA%'
                                                        and a.store_flg ='Y'
                                                        then a.orig_usr_id
                                                        else a.salesman_cd
                                                        end
                                                        ) = c.usr_name
                                                ) a 
                                                where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                        )
                                )
                                group by store_cd, staff_id, trx_month
                        ) a
                        left join (
                                select distinct staff_id, full_name  from ${etlvar::MIGDB}.u_rbd_kpi_report_staff_name 
                        ) b
                        on a.staff_id = b.staff_id
                        ;

                        commit;

                        --c.) Accessory
                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T3
                        (
                                store_cd
                                ,staff_id
                                ,staff_name
                                ,RowName 
                                ,ACCESSORY 
                                ,TRX_MONTH
                                ,ROW_NO 
                        )
                        select 
                        a.store_cd, 
                        coalesce(a.staff_id,' ') as staff_id,
                        coalesce(b.full_name,' ') as full_name,
                        'Sales revenue' as ROWNAME, 
                        coalesce(REVENUE,0) as revenue, 
                        coalesce(TRX_MONTH,trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')) as trx_month,
                        coalesce(ROW_NO,1) as row_no
                        from (
                                select store_cd, staff_id, 'Sales revenue' as rowName, sum(revenue*discount_rate) as revenue, trx_month, 1 as row_no
                                from (
                                        select 
                                        --Postpaid
                                        store_cd,
                                        staff_id, 
                                        revenue,
                                        discount_rate
                                        ,trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') as trx_month
                                        from (
                                                select
                                                store_cd,
                                                staff_id,
                                                inv_date,
                                                case when salesman_cd like 'FA%'
                                                then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel='Street fighter')
                                                when online_inv_flg in ('Y','B')
                                                then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel='Online')
                                                when store_cd in (select store_cd from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP where daily_sales_sms='Y')
                                                then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel='RBD')
                                                else (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel='Others')
                                                end as discount_rate,
                                                ---MOD_SR0002530---
                                                ---invole discount in revenue
                                           --pos_amt as revenue 
                                           pos_amt + coupon_discount_amt as revenue 
                                                from (
                                                        select 
                                                        c.staff_id, a.* 
                                                        from (
                                                                select a.*, 
                                                                case when store_cd in (select store_cd from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP where daily_sales_sms ='Y')
                                                                then 'Y'
                                                                else 'N'
                                                                end as store_flg
                                                                from ${etlvar::MIGADWDB}.RBD_KPI_REPORT_ACCESORY_DTL a
                                                        ) a
                                                        left join ${etlvar::MIGDB}.U_RBD_KPI_REPORT_STAFF_NAME c
                                                        on (
                                                        case when orig_salesman_cd like 'FA%' and orig_usr_id not like 'FA%'
                                                        and a.store_flg ='Y'
                                                        then a.orig_usr_id
                                                        else a.salesman_cd
                                                        end
                                                        ) = c.usr_name
                                                ) a 
                                                where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                        )
                                )
                                group by store_cd, staff_id, trx_month
                        ) a
                        left join (
                                select distinct staff_id, full_name  from ${etlvar::MIGDB}.u_rbd_kpi_report_staff_name 
                        ) b
                        on a.staff_id = b.staff_id
                        ;

                        commit;

                        --d.) Prepaid (SIM and Voucher)
                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T3
                        (
                                store_cd
                                ,staff_id
                                ,staff_name
                                ,RowName 
                                ,PREPAID 
                                ,TRX_MONTH
                                ,ROW_NO 
                        )
                        select 
                        a.store_cd, 
                        coalesce(a.staff_id,' ') as staff_id,
                        coalesce(b.full_name,' ') as full_name,
                        'Sales revenue' as ROWNAME, 
                        coalesce(REVENUE,0) as revenue, 
                        coalesce(TRX_MONTH,trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')) as trx_month,
                        coalesce(ROW_NO,1) as row_no
                        from (
                                select store_cd, staff_id, 'Sales revenue' as rowName, sum(revenue*discount_rate) as revenue, trx_month, 1 as row_no
                                from (
                                        select 
                                        --Postpaid
                                        store_cd, 
                                        staff_id, 
                                        revenue,
                                        discount_rate
                                        ,trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') as trx_month
                                        from (
                                                select
                                                store_cd, 
                                                staff_id,
                                                inv_date,
                                                case when salesman_cd like 'FA%'
                                                then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel='Street fighter')
                                                when online_inv_flg in ('Y','B')
                                                then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel='Online')
                                                when store_cd in (select store_cd from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP where daily_sales_sms='Y')
                                                then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel='RBD')
                                                else (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel='Others')
                                                end as discount_rate,
                                           pos_amt as revenue 
                                                from (
                                                        select c.staff_id, x.* from (
                                                                select 
                                                                case when store_cd in (select store_cd from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP where daily_sales_sms ='Y')
                                                                then 'Y'
                                                                else 'N'
                                                                end as store_flg
                                                                ,a.* 
                                                                from ${etlvar::MIGADWDB}.RBD_KPI_REPORT_SIM_DTL a
                                                                where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                                                union all
                                                                select 
                                                                case when store_cd in (select store_cd from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP where daily_sales_sms ='Y')
                                                                then 'Y'
                                                                else 'N'
                                                                end as store_flg
                                                                ,b.* 
                                                                from ${etlvar::MIGADWDB}.RBD_KPI_REPORT_PREPA_VOU_DTL b
                                                                where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                                        ) x
                                                        left join ${etlvar::MIGDB}.U_RBD_KPI_REPORT_STAFF_NAME c
                                                        on (
                                                        case when orig_salesman_cd like 'FA%' and orig_usr_id not like 'FA%'
                                                        and x.store_flg ='Y'
                                                        then x.orig_usr_id
                                                        else x.salesman_cd
                                                        end
                                                        ) = c.usr_name
                                                ) a 
                                        )
                                )
                                group by store_cd, staff_id, trx_month
                        ) a
                        left join (
                                select distinct staff_id, full_name  from ${etlvar::MIGDB}.u_rbd_kpi_report_staff_name 
                        ) b
                        on a.staff_id = b.staff_id
                        ;

                        commit;

                        --e.) HP
                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T3
                        (
                                store_cd
                                ,staff_id
                                ,staff_name
                                ,RowName 
                                ,HP 
                                ,TRX_MONTH
                                ,ROW_NO 
                        )
                        select 
                        a.store_cd, 
                        coalesce(a.staff_id,' ') as staff_id,
                        coalesce(b.full_name,' ') as full_name,
                        'Sales revenue' as ROWNAME, 
                        coalesce(REVENUE,0) as revenue, 
                        coalesce(TRX_MONTH,trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')) as trx_month,
                        coalesce(ROW_NO,1) as row_no
                        from (
                                select store_cd, staff_id, 'Sales revenue' as rowName, sum(revenue*discount_rate) as revenue, trx_month, 1 as row_no
                                from (
                                                select 
                                                --HP
                                                        store_cd,
                                                        c.staff_id,
                                                        CHANNEL_EFFORT, 
                                                        rate_plan_grp,
                                                        TOTAL_SERVICE_REVENUE+VAS_FEE as revenue,
                                                        coalesce(b.discount_rate,1) as discount_rate,
                                                        trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') as trx_month
                                                from (
                                                        select 
                                                        case when x.orig_create_by like 'FA%' and x.orig_handle_by not like 'FA%'
                                                        and store_cd in (select store_cd from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP where daily_sales_sms='Y')
                                                        then x.orig_handle_by
                                                        else x.create_by
                                                        end as usr_name
                                                        ,x.*
                                                        from 
                                                        ${etlvar::MIGADWDB}.RBD_KPI_REPORT_SERV_REV_DTL x
                                                        left join (
                                                                select * from ${etlvar::ADWDB}.rbd_kpi_margin_contribute 
                                                                where grp_name not in (
                                                                'Handset',
                                                                'Accesory',
                                                                'Prepaid'
                                                                )
                                                        ) y
                                                        on cast(substr(x.rate_plan_grp,1,2) as decimal) = cast(substr(y.grp_name,1,2) as decimal)
                                                        where cast(substr(rate_plan_grp,1,2) as decimal) > 16
                                                        and inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                                ) a
                                                left join ${etlvar::ADWDB}.RBD_KPI_REPORT_DIS_RATE b
                                                on upper(a.channel_effort) = upper(b.channel)
                                                left join ${etlvar::MIGDB}.U_RBD_KPI_REPORT_STAFF_NAME c
                                                on a.usr_name = c.usr_name
                                        )
                                        group by store_cd, staff_id, trx_month
                        ) a
                        left join (
                                select distinct staff_id, full_name  from ${etlvar::MIGDB}.u_rbd_kpi_report_staff_name 
                        ) b
                        on a.staff_id = b.staff_id
                        ;

                        commit;

                        --f.) FBB
                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T3
                        (
                                store_cd
                                ,staff_id
                                ,staff_name
                                ,RowName 
                                ,FBB 
                                ,TRX_MONTH
                                ,ROW_NO 
                        )
                        select 
                        a.store_cd, 
                        coalesce(a.staff_id,' ') as staff_id,
                        coalesce(b.full_name,' ') as full_name,
                        'Sales revenue' as ROWNAME, 
                        coalesce(REVENUE,0) as revenue, 
                        coalesce(TRX_MONTH,trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')) as trx_month,
                        coalesce(ROW_NO,1) as row_no
                        from (
                                select store_cd, staff_id, 'Sales revenue' as rowName, sum(revenue*discount_rate) as revenue, trx_month, 1 as row_no
                                from (
                                                select 
                                                --FBB
                                                        store_cd,
                                                        c.staff_id,
                                                        CHANNEL_EFFORT, 
                                                        rate_plan_grp,
                                                        TOTAL_SERVICE_REVENUE+VAS_FEE as revenue,
                                                        coalesce(b.discount_rate,1) as discount_rate,
                                                        trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') as trx_month
                                                from (
                                                        select 
                                                        case when x.orig_create_by like 'FA%' and x.orig_handle_by not like 'FA%'
                                                        and store_cd in (select store_cd from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP where daily_sales_sms='Y')
                                                        then x.orig_handle_by
                                                        else x.create_by
                                                        end as usr_name
                                                        ,x.*
                                                        from 
                                                        ${etlvar::MIGADWDB}.RBD_KPI_REPORT_SERV_REV_DTL x
                                                        where cast(substr(rate_plan_grp,1,2) as decimal) between 13 and 16
                                                        and inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                                ) a
                                                left join ${etlvar::ADWDB}.RBD_KPI_REPORT_DIS_RATE b
                                                on upper(a.channel_effort) = upper(b.channel)
                                                left join ${etlvar::MIGDB}.U_RBD_KPI_REPORT_STAFF_NAME c
                                                on a.usr_name = c.usr_name
                                        )
                                        group by store_cd, staff_id, trx_month
                        ) a
                        left join (
                                select distinct staff_id, full_name  from ${etlvar::MIGDB}.u_rbd_kpi_report_staff_name 
                        ) b
                        on a.staff_id = b.staff_id
                        ;

                        commit;

                        --Combine into single record
                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T4
                        (
                                store_cd
                                ,staff_id
                                ,staff_name
                                ,RowName 
                                ,Store_Total 
                                ,POSTPAID 
                                ,HANDSET 
                                ,ACCESSORY 
                                ,PREPAID 
                                ,HP 
                                ,FBB 
                                ,TRX_MONTH 
                                ,ROW_NO 
                        )
                        select 
                                store_cd
                                ,staff_id
                                ,staff_name
                                ,RowName 
                                ,sum(POSTPAID)+sum(HANDSET)+sum(ACCESSORY)+sum(PREPAID)+sum(HP)+sum(FBB) 
                                ,sum(POSTPAID)
                                ,sum(HANDSET)
                                ,sum(ACCESSORY)
                                ,sum(PREPAID)
                                ,sum(HP)
                                ,sum(FBB) 
                                ,TRX_MONTH 
                                ,1 as ROW_NO
                        from 
                        ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T3 a
                        where RowName = 'Sales revenue'
                        and trx_month = trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')
                        group by store_cd, staff_id, staff_name, rowname, TRX_MONTH 
                        ;

                        commit;

                        --End of Sales revenue

                        --Row no = 2
                        --Start of Cost before sales channel cost
                        --a.) Postpaid
                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T3
                        (
                                store_cd
                                ,staff_id
                                ,staff_name
                                ,RowName 
                                ,POSTPAID 
                                ,TRX_MONTH
                                ,ROW_NO 
                        )
                        select 
                        a.store_cd, 
                        coalesce(a.staff_id,' ') as staff_id,
                        coalesce(b.full_name,' ') as full_name,
                        'Cost before sales channel cost' as ROWNAME, 
                        coalesce(REVENUE,0) as revenue, 
                        coalesce(TRX_MONTH,trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')) as trx_month,
                        coalesce(ROW_NO,2) as row_no
                        from (
                                select store_cd, staff_id, 'Cost before sales channel cost' as rowName, 
                                sum(revenue) as revenue, trx_month, 2 as row_no
                                from (
                                        select 
                                        --Postpaid
                                        store_cd
                                        ,staff_id 
                                        ,revenue * b.margin as revenue 
                                        ,b.margin
                                        ,trx_month
                                        from (
                                                select
                                                store_cd,
                                                c.staff_id, rate_plan_grp, sum(total_service_revenue+vas_fee) as revenue
                                                ,trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') as trx_month
                                                from (
                                                        select 
                                                        case when x.orig_create_by like 'FA%' and x.orig_handle_by not like 'FA%'
                                                        and store_cd in (select store_cd from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP where daily_sales_sms='Y')
                                                        then x.orig_handle_by
                                                        else x.create_by
                                                        end as usr_name,
                                                        x.* from
                                                        ${etlvar::MIGADWDB}.RBD_KPI_REPORT_SERV_REV_DTL x
                                                ) a
                                                left join ${etlvar::MIGDB}.U_RBD_KPI_REPORT_STAFF_NAME c
                                                on a.usr_name = c.usr_name
                                                where cast(substr(rate_plan_grp,1,2) as decimal) <= 12
                                                and inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                                group by store_cd, c.staff_id, rate_plan_grp, trunc(inv_date,'MONTH')
                                        ) a
                                        left join (
                                                select * from ${etlvar::ADWDB}.RBD_KPI_MARGIN_CONTRIBUTE 
                                                where grp_name not in (
                                                'Handset',
                                                'Accesory',
                                                'Prepaid'
                                                )
                                        ) b
                                        on cast(substr(a.rate_plan_grp,1,2) as decimal) = cast(substr(b.grp_name,1,2) as decimal)
                                )
                                group by store_cd, staff_id, trx_month
                        ) a
                        left join (
                                select distinct staff_id, full_name  from ${etlvar::MIGDB}.u_rbd_kpi_report_staff_name 
                        ) b
                        on a.staff_id = b.staff_id
                        ;

                        commit;

                        --b.) Handset
                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T3
                        (
                                store_cd
                                ,staff_id
                                ,staff_name
                                ,RowName 
                                ,HANDSET 
                                ,TRX_MONTH
                                ,ROW_NO 
                        )
                        select 
                        a.store_cd, 
                        coalesce(a.staff_id,' ') as staff_id,
                        coalesce(b.full_name,' ') as full_name,
                        'Cost before sales channel cost' as ROWNAME, 
                        coalesce(REVENUE,0) as revenue, 
                        coalesce(TRX_MONTH,trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')) as trx_month,
                        2 as row_no
                        from (
                                select 
                                STORE_CD, STAFF_ID, STAFF_NAME,row_no,TRX_MONTH
                                ,handset * (select margin from ${etlvar::ADWDB}.RBD_KPI_MARGIN_CONTRIBUTE where grp_name ='Handset') as revenue
                                from (
                                        select * 
                                        from
                                        ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T3
                                        where rowname ='Sales revenue'
                                )
                                where trx_month = trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')
                        ) a
                        left join (
                                select distinct staff_id, full_name  from ${etlvar::MIGDB}.u_rbd_kpi_report_staff_name 
                        ) b
                        on a.staff_id = b.staff_id
                        ;

                        commit;

                        --c.) Accessory
                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T3
                        (
                                store_cd
                                ,staff_id
                                ,staff_name
                                ,RowName 
                                ,ACCESSORY 
                                ,TRX_MONTH
                                ,ROW_NO 
                        )
                        select 
                        a.store_cd, 
                        coalesce(a.staff_id,' ') as staff_id,
                        coalesce(b.full_name,' ') as full_name,
                        'Cost before sales channel cost' as ROWNAME, 
                        coalesce(REVENUE,0) as revenue, 
                        coalesce(TRX_MONTH,trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')) as trx_month,
                        2 as row_no
                        from (
                                select 
                                STORE_CD, STAFF_ID, STAFF_NAME,row_no,TRX_MONTH
                                --,Accessory * (select margin from ${etlvar::ADWDB}.RBD_KPI_MARGIN_CONTRIBUTE where grp_name ='Accessory') as revenue
                                ,Accessory * (select margin from ${etlvar::ADWDB}.RBD_KPI_MARGIN_CONTRIBUTE where grp_name ='Accesory') as revenue
                                from (
                                        select * 
                                        from
                                        ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T3
                                        where rowname ='Sales revenue'
                                )
                                where trx_month = trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')
                        ) a
                        left join (
                                select distinct staff_id, full_name  from ${etlvar::MIGDB}.u_rbd_kpi_report_staff_name 
                        ) b
                        on a.staff_id = b.staff_id
                        ;

                        commit;

                        --d.) Prepaid (SIM and Voucher)
                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T3
                        (
                                store_cd
                                ,staff_id
                                ,staff_name
                                ,RowName 
                                ,PREPAID 
                                ,TRX_MONTH
                                ,ROW_NO 
                        )
                        select 
                        a.store_cd, 
                        coalesce(a.staff_id,' ') as staff_id,
                        coalesce(b.full_name,' ') as full_name,
                        'Cost before sales channel cost' as ROWNAME, 
                        coalesce(REVENUE,0) as revenue, 
                        coalesce(TRX_MONTH,trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')) as trx_month,
                        2 as row_no
                        from (
                                select 
                                STORE_CD, STAFF_ID, STAFF_NAME,row_no,TRX_MONTH
                                ,Prepaid * (select margin from ${etlvar::ADWDB}.RBD_KPI_MARGIN_CONTRIBUTE where grp_name ='Prepaid') as revenue
                                from (
                                        select * 
                                        from
                                        ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T3
                                        where rowname ='Sales revenue'
                                )
                                where trx_month = trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')
                        ) a
                        left join (
                                select distinct staff_id, full_name  from ${etlvar::MIGDB}.u_rbd_kpi_report_staff_name 
                        ) b
                        on a.staff_id = b.staff_id
                        ;

                        commit;

                        --e.) HP
                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T3
                        (
                                store_cd
                                ,staff_id
                                ,staff_name
                                ,RowName 
                                ,HP 
                                ,TRX_MONTH
                                ,ROW_NO 
                        )
                        select 
                        a.store_cd, 
                        coalesce(a.staff_id,' ') as staff_id,
                        coalesce(b.full_name,' ') as full_name,
                        'Cost before sales channel cost' as ROWNAME, 
                        coalesce(REVENUE,0) as revenue, 
                        coalesce(TRX_MONTH,trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')) as trx_month,
                        coalesce(ROW_NO,2) as row_no
                        from (
                                select store_cd, staff_id, 'Cost before sales channel cost' as rowName, 
                                sum(revenue) as revenue, trx_month, 2 as row_no
                                from (
                                        select 
                                        --Postpaid
                                        store_cd
                                        ,staff_id 
                                        ,revenue * b.margin as revenue 
                                        ,b.margin
                                        ,trx_month
                                        from (
                                                select
                                                store_cd,
                                                c.staff_id, rate_plan_grp, sum(total_service_revenue+vas_fee) as revenue
                                                ,trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') as trx_month
                                                from (
                                                        select 
                                                        case when x.orig_create_by like 'FA%' and x.orig_handle_by not like 'FA%'
                                                        and store_cd in (select store_cd from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP where daily_sales_sms='Y')
                                                        then x.orig_handle_by
                                                        else x.create_by
                                                        end as usr_name,
                                                        x.* from
                                                        ${etlvar::MIGADWDB}.RBD_KPI_REPORT_SERV_REV_DTL x
                                                ) a
                                                left join ${etlvar::MIGDB}.U_RBD_KPI_REPORT_STAFF_NAME c
                                                on a.usr_name = c.usr_name
                                                where cast(substr(rate_plan_grp,1,2) as decimal) > 16
                                                and inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                                group by store_cd, c.staff_id, rate_plan_grp, trunc(inv_date,'MONTH')
                                        ) a
                                        left join (
                                                select * from ${etlvar::ADWDB}.RBD_KPI_MARGIN_CONTRIBUTE 
                                                where grp_name not in (
                                                'Handset',
                                                'Accesory',
                                                'Prepaid'
                                                )
                                        ) b
                                        on cast(substr(a.rate_plan_grp,1,2) as decimal) = cast(substr(b.grp_name,1,2) as decimal)
                                )
                                group by store_cd, staff_id, trx_month
                        ) a
                        left join (
                                select distinct staff_id, full_name  from ${etlvar::MIGDB}.u_rbd_kpi_report_staff_name 
                        ) b
                        on a.staff_id = b.staff_id
                        ;

                        commit;


                        --f.) FBB
                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T3
                        (
                                store_cd
                                ,staff_id
                                ,staff_name
                                ,RowName 
                                ,FBB 
                                ,TRX_MONTH
                                ,ROW_NO 
                        )
                        select 
                        a.store_cd, 
                        coalesce(a.staff_id,' ') as staff_id,
                        coalesce(b.full_name,' ') as full_name,
                        'Cost before sales channel cost' as ROWNAME, 
                        coalesce(REVENUE,0) as revenue, 
                        coalesce(TRX_MONTH,trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')) as trx_month,
                        coalesce(ROW_NO,2) as row_no
                        from (
                                select store_cd, staff_id, 'Cost before sales channel cost' as rowName, 
                                sum(revenue) as revenue, trx_month, 2 as row_no
                                from (
                                        select 
                                        --Postpaid
                                        store_cd
                                        ,staff_id 
                                        ,revenue * b.margin as revenue 
                                        ,b.margin
                                        ,trx_month
                                        from (
                                                select
                                                store_cd,
                                                c.staff_id, rate_plan_grp, sum(total_service_revenue+vas_fee) as revenue
                                                ,trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') as trx_month
                                                from (
                                                        select 
                                                        case when x.orig_create_by like 'FA%' and x.orig_handle_by not like 'FA%'
                                                        and store_cd in (select store_cd from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP where daily_sales_sms='Y')
                                                        then x.orig_handle_by
                                                        else x.create_by
                                                        end as usr_name,
                                                        x.* from
                                                        ${etlvar::MIGADWDB}.RBD_KPI_REPORT_SERV_REV_DTL x
                                                ) a
                                                left join ${etlvar::MIGDB}.U_RBD_KPI_REPORT_STAFF_NAME c
                                                on a.usr_name = c.usr_name
                                                where cast(substr(rate_plan_grp,1,2) as decimal) between 13 and 16
                                                and inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                                group by store_cd, c.staff_id, rate_plan_grp, trunc(inv_date,'MONTH')
                                        ) a
                                        left join (
                                                select * from ${etlvar::ADWDB}.RBD_KPI_MARGIN_CONTRIBUTE 
                                                where grp_name not in (
                                                'Handset',
                                                'Accesory',
                                                'Prepaid'
                                                )
                                        ) b
                                        on cast(substr(a.rate_plan_grp,1,2) as decimal) = cast(substr(b.grp_name,1,2) as decimal)
                                )
                                group by store_cd, staff_id, trx_month
                        ) a
                        left join (
                                select distinct staff_id, full_name  from ${etlvar::MIGDB}.u_rbd_kpi_report_staff_name 
                        ) b
                        on a.staff_id = b.staff_id
                        ;

                        commit;

                        --Combine into single record
                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T4
                        (
                                store_cd
                                ,staff_id
                                ,staff_name
                                ,RowName 
                                ,Store_Total 
                                ,POSTPAID 
                                ,HANDSET 
                                ,ACCESSORY 
                                ,PREPAID 
                                ,HP 
                                ,FBB 
                                ,TRX_MONTH 
                                ,ROW_NO 
                        )
                        select 
                                STORE_CD
                                ,staff_id
                                ,staff_name
                                ,'Cost before sales channel cost' as RowName 
                                ,sum(POSTPAID)+sum(HANDSET)+sum(ACCESSORY)+sum(PREPAID)+sum(HP)+sum(FBB)
                                ,sum(POSTPAID)
                                ,sum(HANDSET)
                                ,sum(ACCESSORY)
                                ,sum(PREPAID)
                                ,sum(HP)
                                ,sum(FBB) 
                                ,TRX_MONTH 
                                ,2 as ROW_NO
                        from 
                        ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T3
                        where RowName = 'Cost before sales channel cost'
                        group by STORE_CD, staff_id, staff_name, TRX_MONTH
                        ;

                        --End of Cost before sales channel cost


                        --Sales channel cost
                        --Manpower costs
                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T4
                        (
                                store_cd
                                ,staff_id
                                ,staff_name
                                ,RowName 
                                ,Store_Total 
                                ,TRX_MONTH 
                                ,ROW_NO 
                        )
                        select
                                STORE_CD
                                ,staff_id
                                ,staff_name
                                ,'Manpower costs' as expenses_nature
                                ,coalesce(total_package,0) as total
                                ,trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') as trx_month
                                ,5 as ROW_NO
                        from (
                                select store_cd, staff_id, staff_name, total_package
                                from (
                                        select distinct
                                        'dummy' as DUMMY, a.STORE_CD, c.cost_center_code, a.staff_id, a.staff_name
                                        ,total_package
                                        from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T4 a
                                        left join (
                                                select b.store_code, a.staff_no, a.total_package
                                                from ${etlvar::ADWDB}.rbd_kpi_report_se_list a
                                                left join ${etlvar::ADWDB}.RBD_KPI_COST_CENTER_CD b
                                                on a.dept_code = b.cost_center_code
                                                where trx_month = trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')
                                        ) b
                                        on a.staff_id = b.staff_no
                                        and a.store_cd = b.store_code
                                        left join PRD_ADW.rbd_kpi_cost_center_cd c
                                        on a.store_cd = c.store_code
                                )
                        )
                        ;

                        commit;

                        --Sales channel cost
                        --Commission
                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T4
                        (
                                store_cd
                                ,staff_id
                                ,staff_name
                                ,RowName 
                                ,Store_Total 
                                ,TRX_MONTH 
                                ,ROW_NO 
                        )
                        select
                                STORE_CD
                                ,staff_id
                                ,staff_name
                                ,'Commission' as expenses_nature
                                ,coalesce(commission,0) as total
                                ,trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') as trx_month
                                ,3 as ROW_NO
                        from (
                                select store_cd, staff_id, staff_name, commission
                                from (
                                        select distinct
                                        a.store_cd, a.staff_id, a.staff_name
                                        ,commission
                                        from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T4 a
                                        left join (
                                                select shop_cd, staff_code, SUM(COMMISSION_PAYABLE) as commission   
                                                from PRD_BIZ_SUMM_VW.VW_RBD_QUERY_ALL
                                                where trx_month = trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')
                                                group by trx_month, staff_code, shop_cd
                                        ) b
                                        on a.staff_id = b.staff_code
                                        and a.store_cd = b.shop_cd
                                )
                        )
                        ;

                        commit;

                        --Premium cost
                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T4
                        (
                                store_cd
                                ,staff_id
                                ,staff_name
                                ,RowName 
                                ,Store_Total 
                                ,TRX_MONTH 
                                ,ROW_NO 
                        )
                        select 
                        a.store_cd
                        , a.staff_id
                        , nvl(b.full_name,' ')
                        ,'Premium cost' as row_name
                        ,coalesce(a.premium_cost,0) as premium_cost
                        ,trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') as trx_month
                        ,4 as row_no
                        from (
                                select
                                store_cd, staff_id, sum(unit_cost) as premium_cost
                                from (
                                        select 
                                        a.store_cd, a.staff_id, a.staff_name, b.INV_NUM,
                                        c.pos_prod_cd, c.UNIT_COST * b.discount_rate as unit_cost
                                        from 
                                        (
                                                select distinct
                                                store_cd, staff_id, staff_name
                                                from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T4
                                        ) a 
                                        left join (
                                                select x.store_cd, y.staff_id, inv_num, discount_rate
                                                from (
                                                        select 
                                                        case when orig_create_by like 'FA%' and orig_handle_by not like 'FA%'
                                                        and store_cd in (select store_cd from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP where daily_sales_sms='Y')
                                                        then orig_handle_by
                                                        else create_by
                                                        end as usr_name, order_num as inv_num, store_cd
                                                        ,discount_rate
                                                        from (
                                                                select b.discount_rate, a.* from
                                                                ${etlvar::MIGADWDB}.RBD_KPI_REPORT_SERV_REV_DTL a
                                                                left join ${etlvar::ADWDB}.rbd_kpi_report_dis_rate b
                                                                on upper(a.channel_effort) = upper(b.channel)
                                                                where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                                        )
                                                        union
                                                        select 
                                                        case when orig_salesman_cd like 'FA%' and orig_usr_id not like 'FA%'
                                                        and store_cd in (select store_cd from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP where daily_sales_sms='Y')
                                                        then orig_usr_id
                                                        else salesman_cd
                                                        end as usr_name, inv_num, store_cd
                                                        ,case when FA_INV_FLG ='Y'
                                                        then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'Street fighter')
                                                        when ONLINE_INV_FLG in ('Y','B')
                                                        then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'Online')
                                                        when STORE_CD in (select store_cd from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP where daily_sales_sms='Y')
                                                        then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'RBD')
                                                        else (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'Others')
                                                        end as discount_rate
                                                        from ${etlvar::MIGADWDB}.RBD_KPI_REPORT_HS_DTL a
                                                        where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                                        union
                                                        select 
                                                        case when orig_salesman_cd like 'FA%' and orig_usr_id not like 'FA%'
                                                        and store_cd in (select store_cd from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP where daily_sales_sms='Y')
                                                        then orig_usr_id
                                                        else salesman_cd
                                                        end as usr_name, inv_num, store_cd 
                                                        ,case when FA_INV_FLG ='Y'
                                                        then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'Street fighter')
                                                        when ONLINE_INV_FLG in ('Y','B')
                                                        then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'Online')
                                                        when STORE_CD in (select store_cd from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP where daily_sales_sms='Y')
                                                        then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'RBD')
                                                        else (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'Others')
                                                        end as discount_rate
                                                        from ${etlvar::MIGADWDB}.RBD_KPI_REPORT_SIM_DTL a
                                                        where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                                        union
                                                        select 
                                                        case when orig_salesman_cd like 'FA%' and orig_usr_id not like 'FA%'
                                                        and store_cd in (select store_cd from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP where daily_sales_sms='Y')
                                                        then orig_usr_id
                                                        else salesman_cd
                                                        end as usr_name, inv_num, store_cd 
                                                        ,case when FA_INV_FLG ='Y'
                                                        then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'Street fighter')
                                                        when ONLINE_INV_FLG in ('Y','B')
                                                        then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'Online')
                                                        when STORE_CD in (select store_cd from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP where daily_sales_sms='Y')
                                                        then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'RBD')
                                                        else (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'Others')
                                                        end as discount_rate
                                                        from ${etlvar::MIGADWDB}.RBD_KPI_REPORT_ACCESORY_DTL a
                                                        where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                                        union
                                                        select 
                                                        case when orig_salesman_cd like 'FA%' and orig_usr_id not like 'FA%'
                                                        and store_cd in (select store_cd from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP where daily_sales_sms='Y')
                                                        then orig_usr_id
                                                        else salesman_cd
                                                        end as usr_name, inv_num, store_cd 
                                                        ,case when FA_INV_FLG ='Y'
                                                        then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'Street fighter')
                                                        when ONLINE_INV_FLG in ('Y','B')
                                                        then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'Online')
                                                        when STORE_CD in (select store_cd from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP where daily_sales_sms='Y')
                                                        then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'RBD')
                                                        else (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'Others')
                                                        end as discount_rate
                                                        from ${etlvar::MIGADWDB}.RBD_KPI_REPORT_PREPA_VOU_DTL a
                                                        where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                                        union
                                                        select 
                                                        case when orig_salesman_cd like 'FA%' and orig_usr_id not like 'FA%'
                                                        and store_cd in (select store_cd from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP where daily_sales_sms='Y')
                                                        then orig_usr_id
                                                        else salesman_cd
                                                        end as usr_name, inv_num, store_cd 
                                                        ,case when FA_INV_FLG ='Y'
                                                        then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'Street fighter')
                                                        when ONLINE_INV_FLG in ('Y','B')
                                                        then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'Online')
                                                        when STORE_CD in (select store_cd from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP where daily_sales_sms='Y')
                                                        then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'RBD')
                                                        else (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'Others')
                                                        end as discount_rate
                                                        from ${etlvar::MIGADWDB}.RBD_KPI_REPORT_OTHER_DTL a
                                                        where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                                ) x
                                                left join ${etlvar::MIGDB}.u_rbd_kpi_report_staff_name y
                                                on x.usr_name = y.usr_name
                                        ) b
                                        on a.staff_id = b.staff_id
                                        and a.store_cd = b.store_cd
                                        left join (
                                                select inv_num, pos_prod_cd, b.*
                                                from ${etlvar::ADWDB}.pos_inv_detail a
                                                inner join ${etlvar::ADWDB}.rbd_kpi_premium_cost b
                                                on a.pos_prod_cd = b.product_code
                                                where trunc(b.trx_month,'MONTH') = trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')
                                        ) c
                                        on b.inv_num = c.inv_num
                                )
                                group by STORE_CD, staff_id
                        ) a
                        left join (
                                select distinct staff_id, full_name  from ${etlvar::MIGDB}.u_rbd_kpi_report_staff_name 
                        ) b
                        on a.staff_id = b.staff_id
                        ;

                        commit;

                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T4
                        (
                                store_cd
                                ,staff_id
                                ,staff_name
                                ,RowName 
                                ,Store_Total 
                                ,TRX_MONTH 
                                ,ROW_NO 
                        )
                        SELECT 
                        store_cd
                        ,staff_id
                        ,staff_name
                        ,'Contribution'
                        ,coalesce(sum(
                                case when row_no = 2
                                then store_total
                                when row_no between 3 and 8
                                then store_total * -1
                                else 0
                                end
                        ),0)
                        ,trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')
                        ,9 as row_no
                        from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T4
                        where trx_month = trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')
                        group by store_cd, staff_id, staff_name
                        ;

                        commit;

                        update ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T4
                        set 
                        POSTPAID = -999
                        ,HANDSET = -999
                        ,ACCESSORY = -999
                        ,PREPAID = -999
                        ,HP = -999
                        ,FBB = -999
                        where ROW_NO between 3 and 9
                        ;

                        commit;

                        DELETE FROM ${etlvar::MIGDB}.RBD_KPI_Contribution_by_staff
                        WHERE TRX_MONTH = trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')
                        ;

                        DELETE FROM ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T4
                        WHERE TRX_MONTH = trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')
                        and row_no = -1
                        ;

                        INSERT INTO ${etlvar::MIGDB}.RBD_KPI_Contribution_by_staff
                        (
                                staff_id
                                ,store_cd
                                ,staff_name
                                ,RowName 
                                ,Staff_Total 
                                ,POSTPAID 
                                ,HANDSET 
                                ,ACCESSORY 
                                ,PREPAID 
                                ,HP 
                                ,FBB 
                                ,TRX_MONTH 
                                ,ROW_NO 
                        )
                        SELECT 
                                staff_id
                                ,STORE_CD
                                ,staff_name
                                ,RowName 
                                ,Store_Total 
                                ,POSTPAID 
                                ,HANDSET 
                                ,ACCESSORY 
                                ,PREPAID 
                                ,HP 
                                ,FBB 
                                ,TRX_MONTH 
                                ,ROW_NO 
                        FROM ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T4 ;

                        commit;

        end if;

end
;
/

quit;
--EXIT;

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














