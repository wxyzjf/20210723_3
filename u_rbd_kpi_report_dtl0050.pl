/opt/etl/prd/etl/APP/RBD/Z_RBD_KPI_REPORT_DTL_UAT/bin> cat u_rbd_kpi_report_dtl0050.pl
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
    
    --if (channel_cost_data_ready_ind = 'N' or commission_data_ready_ind ='N')
    if (1=0)
        then 
                        DBMS_OUTPUT.PUT_LINE('Either Last month''s RBD sales channel cost or RBD commission data are not ready yet, not refreshing the contribution report');
        else 
            DBMS_OUTPUT.PUT_LINE('Both last month''s RBD sales channel cost and RBD commission data are ready, refreshing the contribution report');    

                        EXECUTE IMMEDIATE 'TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T1';
                        EXECUTE IMMEDIATE 'TRUNCATE TABLE ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T2';

                        --Row no = 1
                        --Start of Sales revenue
                        --a.) Postpaid
                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T1
                        (
                                STORE_CD 
                                ,RowName 
                                ,POSTPAID 
                                ,TRX_MONTH
                                ,ROW_NO 
                        )
                        select store_cd, 'Sales revenue' as rowName, sum(revenue*discount_rate) as revenue, 
                        trx_month, 
                        1 as row_no
                        from (
                                select 
                                --Postpaid
                                STORE_CD, 
                                CHANNEL_EFFORT, 
                                rate_plan_grp,
                                TOTAL_SERVICE_REVENUE+VAS_FEE as revenue,
                                coalesce(discount_rate,1) as discount_rate,
                                trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') as trx_month
                                from (
                                        select * 
                                        from 
                                        ${etlvar::MIGADWDB}.RBD_KPI_REPORT_SERV_REV_DTL x
                                        where cast(substr(rate_plan_grp,1,2) as decimal) <= 12
                                        and inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                ) a
                                left join ${etlvar::ADWDB}.rbd_kpi_report_dis_rate b
                                on upper(a.channel_effort) = upper(b.channel)
                        )
                        group by store_cd, trx_month
                        ;

                        commit;

                        --b.) Handset
                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T1
                        (
                                STORE_CD 
                                ,RowName 
                                ,HANDSET 
                                ,TRX_MONTH
                                ,ROW_NO 
                        )
                        select store_cd, 'Sales revenue' as rowName, sum(revenue*discount_rate) as revenue,
                        trx_month, 
                        1 as row_no
                        from (
                                select 
                                --Postpaid
                                STORE_CD, 
                                revenue,
                                discount_rate
                                ,trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') as trx_month
                                from (
                                        select
                                        store_cd,
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
                                        from 
                                        ${etlvar::MIGADWDB}.RBD_KPI_REPORT_HS_DTL a 
                                        where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1     
                                )
                        )
                        group by store_cd, trx_month
                        ;

                        commit;

                        --c.) Accessory
                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T1
                        (
                                STORE_CD 
                                ,RowName 
                                ,ACCESSORY 
                                ,TRX_MONTH
                                ,ROW_NO 
                        )
                        select store_cd, 'Sales revenue' as rowName, sum(revenue*discount_rate) as revenue, 
                        trx_month, 
                        1 as row_no
                        from (
                                select 
                                --Postpaid
                                STORE_CD, 
                                revenue,
                                discount_rate
                                ,trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') as trx_month
                                from (
                                        select
                                        store_cd,
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
                                ---Invole redeem amt in accessory ---
                                   --pos_amt as revenue 
                                   pos_amt + COUPON_DISCOUNT_AMT as revenue 
                                        from 
                                        ${etlvar::MIGADWDB}.RBD_KPI_REPORT_ACCESORY_DTL a 
                                        where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                )
                        )
                        group by store_cd, trx_month
                        ;

                        commit;

                        --d.) Prepaid (SIM and Voucher)
                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T1
                        (
                                STORE_CD 
                                ,RowName 
                                ,PREPAID 
                                ,TRX_MONTH
                                ,ROW_NO 
                        )
                        select store_cd, 'Sales revenue' as rowName, sum(revenue*discount_rate) as revenue, 
                        trx_month, 
                        1 as row_no
                        from (
                                select 
                                --Postpaid
                                STORE_CD, 
                                revenue,
                                discount_rate,
                                trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') as trx_month
                                from (
                                        select
                                        store_cd,
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
                                        select * from ${etlvar::MIGADWDB}.RBD_KPI_REPORT_SIM_DTL 
                                        where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1  
                                        union all
                                        select * from ${etlvar::MIGADWDB}.RBD_KPI_REPORT_PREPA_VOU_DTL 
                                        where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                        ) a
                                )
                        )
                        group by store_cd, trx_month
                        ;

                        commit;

                        --e.) HP
                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T1
                        (
                                STORE_CD 
                                ,RowName 
                                ,HP 
                                ,TRX_MONTH
                                ,ROW_NO 
                        )
                        select store_cd, 'Sales revenue' as rowName, sum(revenue*discount_rate) as revenue, 
                        trx_month, 
                        1 as row_no
                        from (
                                        select 
                                        --HP
                                                STORE_CD, 
                                                CHANNEL_EFFORT, 
                                                rate_plan_grp,
                                                TOTAL_SERVICE_REVENUE+VAS_FEE as revenue,
                                                coalesce(discount_rate,1) as discount_rate,
                                                trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') as trx_month
                                        from (
                                                select x.* from 
                                                ${etlvar::MIGADWDB}.RBD_KPI_REPORT_SERV_REV_DTL x
                                                where cast(substr(rate_plan_grp,1,2) as decimal) > 16
                                                and inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                        ) a
                                        left join ${etlvar::ADWDB}.rbd_kpi_report_dis_rate b
                                        on upper(a.channel_effort) = upper(b.channel)
                                )
                                group by store_cd, trx_month
                        ;

                        commit;


                        --f.) FBB
                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T1
                        (
                                STORE_CD 
                                ,RowName 
                                ,FBB 
                                ,TRX_MONTH
                                ,ROW_NO 
                        )
                        select store_cd, 'Sales revenue' as rowName, sum(revenue*discount_rate) as revenue, 
                        trx_month, 
                        1 as row_no
                        from (
                                        select 
                                        --FBB
                                                STORE_CD, 
                                                CHANNEL_EFFORT, 
                                                rate_plan_grp,
                                                TOTAL_SERVICE_REVENUE+VAS_FEE as revenue,
                                                coalesce(discount_rate,1) as discount_rate,
                                                trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') as trx_month
                                        from (
                                                select x.* from 
                                                ${etlvar::MIGADWDB}.RBD_KPI_REPORT_SERV_REV_DTL x
                                                where cast(substr(rate_plan_grp,1,2) as decimal) between 13 and 16
                                                and inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                        ) a
                                        left join ${etlvar::ADWDB}.rbd_kpi_report_dis_rate b
                                        on upper(a.channel_effort) = upper(b.channel)
                                )
                                group by store_cd, trx_month
                        ;

                        commit;



                        --g.) OTHERS
                        ---MOD_SR0002530---
                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T1
                        (
                                STORE_CD
                                ,RowName
                                ,OTHERS
                                ,TRX_MONTH
                                ,ROW_NO
                        )
                        select 
                                x.store_code
                                , 'Sales revenue' as rowName
                                ,nvl(a.revenue,0)
                                ,trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') as trx_month
                                , 1 as row_no
                        from (select distinct store_code from prd_adw.rbd_kpi_cost_center_cd )x
                        left outer join 
                         (select store_cd
                                , sum(pos_amt) as revenue
                          from ${etlvar::MIGADWDB}.RBD_KPI_REPORT_OTHER_DTL x
                         where 
                           inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') 
                                        and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                         group by store_cd )a
                        on x.store_code = a.store_cd;
                        commit; 

                        --Combine into single record
                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T2
                        (
                                STORE_CD 
                                ,RowName 
                                ,Store_Total 
                                ,POSTPAID 
                                ,HANDSET 
                                ,ACCESSORY 
                                ,PREPAID 
                                ,HP 
                                ,FBB 
                        ---MOD_SR0002530---
                        ---Add Others 
                                ,OTHERS
                                ,TRX_MONTH 
                                ,ROW_NO 
                        )
                        select 
                                STORE_CD 
                                ,'Sales revenue' as RowName 
                        ---MOD_SR0002530---
                                ,sum(POSTPAID)+sum(HANDSET)+sum(ACCESSORY)+sum(PREPAID)+sum(HP)+sum(FBB)+sum(OTHERS)
                                ,sum(POSTPAID)
                                ,sum(HANDSET)
                                ,sum(ACCESSORY)
                                ,sum(PREPAID)
                                ,sum(HP)
                                ,sum(FBB) 
                        ---MOD_SR0002530---
                        ---Add Others 
                                ,sum(OTHERS)
                                ,TRX_MONTH 
                                ,1 as ROW_NO
                        from 
                        ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T1
                        where RowName = 'Sales revenue'
                        and trx_month = trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')
                        group by STORE_CD, TRX_MONTH
                        ;

                        commit;

                        --End of Sales revenue

                        --Row no = 2
                        --Start of Cost before sales channel cost
                        --a.) Postpaid
                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T1
                        (
                                STORE_CD 
                                ,RowName 
                                ,POSTPAID 
                                ,TRX_MONTH
                                ,ROW_NO 
                        )
                        select store_cd, 'Cost before sales channel cost' as rowName, 
                        sum(revenue), trx_month, 
                        2 as row_no
                        from (
                                select 
                                --Postpaid
                                STORE_CD 
                                ,revenue * b.margin as revenue 
                                ,b.margin
                                ,trx_month
                                from (
                                        select
                                                store_cd
                                                , rate_plan_grp
                                                , sum(total_service_revenue+vas_fee) as revenue
                                                , trunc(inv_date,'MONTH') as trx_month
                                        from ${etlvar::MIGADWDB}.RBD_KPI_REPORT_SERV_REV_DTL a
                                        left join ${etlvar::ADWDB}.RBD_KPI_REPORT_DIS_RATE b
                                        on upper(a.channel_effort) = upper(b.channel)
                                        where cast(substr(rate_plan_grp,1,2) as decimal) <= 12
                                        and inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                        group by store_cd, rate_plan_grp, trunc(inv_date,'MONTH')
                                ) a
                                left join (
                                        select * from ${etlvar::ADWDB}.RBD_KPI_MARGIN_CONTRIBUTE 
                    where grp_name not in (
                    'Handset',
                    'Accesory',
                    'Prepaid'
                    )
                                ) b
                                on cast(substr(a.rate_plan_grp,1,2) as decimal) = cast(substr(B.grp_name,1,2) as decimal)
                        )
                        group by store_cd, trx_month
                        ;

                        commit;

                        --b.) Handset
                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T1
                        (
                                STORE_CD 
                                ,RowName 
                                ,HANDSET 
                                ,TRX_MONTH
                                ,ROW_NO 
                        )
                        select store_cd, 'Cost before sales channel cost' as rowName,
                        handset * (select margin from ${etlvar::ADWDB}.RBD_KPI_MARGIN_CONTRIBUTE where grp_name ='Handset'), 
                        trx_month, 
                        2 as row_no
                        from (
                                select * from
                                ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T1
                                where rowname ='Sales revenue'
                        )
                        ;

                        commit;

                        --c.) Accessory
                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T1
                        (
                                STORE_CD 
                                ,RowName 
                                ,ACCESSORY 
                                ,TRX_MONTH
                                ,ROW_NO 
                        )
                        select store_cd, 'Cost before sales channel cost' as rowName,
                        accessory * (select margin from ${etlvar::ADWDB}.RBD_KPI_MARGIN_CONTRIBUTE where grp_name ='Accesory'), 
                        --accessory * (select margin from ${etlvar::ADWDB}.RBD_KPI_MARGIN_CONTRIBUTE where grp_name ='Accessory'), 
                        trx_month, 
                        2 as row_no
                        from (
                                select * from
                                ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T1
                                where rowname ='Sales revenue'
                        )
                        ;

                        commit;

                        --d.) Prepaid (SIM and Voucher)
                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T1
                        (
                                STORE_CD 
                                ,RowName 
                                ,PREPAID 
                                ,TRX_MONTH
                                ,ROW_NO 
                        )
                        select store_cd, 'Cost before sales channel cost' as rowName,
                        prepaid * (select margin from ${etlvar::ADWDB}.RBD_KPI_MARGIN_CONTRIBUTE where grp_name ='Prepaid'), 
                        trx_month, 
                        2 as row_no
                        from (
                                select * from
                                ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T1
                                where rowname ='Sales revenue'
                        )
                        ;

                        commit;

                        --e.) HP
                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T1
                        (
                                STORE_CD 
                                ,RowName 
                                ,HP 
                                ,TRX_MONTH
                                ,ROW_NO 
                        )
                        select store_cd, 'Cost before sales channel cost' as rowName, 
                        sum(revenue), trx_month, 
                        2 as row_no
                        from (
                                select 
                                --Postpaid
                                STORE_CD 
                                ,revenue * b.margin as revenue 
                                ,b.margin
                                ,trx_month
                                from (
                                        select
                                        store_cd, rate_plan_grp, sum(total_service_revenue+vas_fee) as revenue, trunc(inv_date,'MONTH') as trx_month
                                        from ${etlvar::MIGADWDB}.RBD_KPI_REPORT_SERV_REV_DTL a
                                        left join ${etlvar::ADWDB}.RBD_KPI_REPORT_DIS_RATE b
                                        on upper(a.channel_effort) = upper(b.channel)
                                        where cast(substr(rate_plan_grp,1,2) as decimal) > 16
                                        and inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                        group by store_cd, rate_plan_grp, trunc(inv_date,'MONTH')
                                ) a
                                left join (
                                        select * from ${etlvar::ADWDB}.RBD_KPI_MARGIN_CONTRIBUTE 
                    where grp_name not in (
                    'Handset',
                    'Accesory',
                    'Prepaid'
                    )
                                ) b
                                on cast(substr(a.rate_plan_grp,1,2) as decimal) = cast(substr(B.grp_name,1,2) as decimal)
                        )
                        group by store_cd, trx_month
                        ;

                        commit;


                        --f.) FBB
                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T1
                        (
                                STORE_CD 
                                ,RowName 
                                ,FBB 
                                ,TRX_MONTH
                                ,ROW_NO 
                        )
                        select store_cd, 'Cost before sales channel cost' as rowName, 
                        sum(revenue), trx_month, 
                        2 as row_no
                        from (
                                select 
                                --Postpaid
                                STORE_CD 
                                ,revenue * b.margin as revenue 
                                ,b.margin
                                ,trx_month
                                from (
                                        select
                                        store_cd, rate_plan_grp, sum(total_service_revenue+vas_fee) as revenue, trunc(inv_date,'MONTH') as trx_month
                                        from ${etlvar::MIGADWDB}.RBD_KPI_REPORT_SERV_REV_DTL a
                                        left join ${etlvar::ADWDB}.RBD_KPI_REPORT_DIS_RATE b
                                        on upper(a.channel_effort) = upper(b.channel)
                                        where cast(substr(rate_plan_grp,1,2) as decimal) between 13 and 16
                                        and inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                                        group by store_cd, rate_plan_grp, trunc(inv_date,'MONTH')
                                ) a
                                left join (
                                        select * from ${etlvar::ADWDB}.RBD_KPI_MARGIN_CONTRIBUTE 
                    where grp_name not in (
                    'Handset',
                    'Accesory',
                    'Prepaid'
                    )
                                ) b
                                on cast(substr(a.rate_plan_grp,1,2) as decimal) = cast(substr(B.grp_name,1,2) as decimal)
                        )
                        group by store_cd, trx_month
                        ;

                        commit;

                        --g.) OTHERS
                        ---MOD_SR0002530---
                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T1
                        (
                                STORE_CD
                                ,RowName
                                ,OTHERS
                                ,TRX_MONTH
                                ,ROW_NO
                        )
                        select a.store_code
                              ,'Cost before sales channel cost'
                              ,nvl(x.revenue,0)
                              ,trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') as trx_month
                              ,2 as row_no
                        from (select distinct store_code from prd_adw.rbd_kpi_cost_center_cd )a
                        left outer join
                        (select store_cd
                                , sum(pos_amt) *  (select margin 
                                                        from ${etlvar::ADWDB}.RBD_KPI_MARGIN_CONTRIBUTE 
                                                        where grp_name ='Others')as revenue
                          from ${etlvar::MIGADWDB}.RBD_KPI_REPORT_OTHER_DTL x
                         where
                           inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') 
                                and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
                         group by store_cd )x
                         on a.store_code = x.store_cd;
                        commit;


                        --Combine into single record
                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T2
                        (
                                STORE_CD 
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
                        ---MOD_SR0002530---
                                ,OTHERS
                        )
                        select 
                                STORE_CD 
                                ,'Cost before sales channel cost' as RowName 
                        ---MOD_SR0002530---
                                ,sum(POSTPAID)+sum(HANDSET)+sum(ACCESSORY)+sum(PREPAID)+sum(HP)+sum(FBB) + sum(OTHERS)
                                ,sum(POSTPAID)
                                ,sum(HANDSET)
                                ,sum(ACCESSORY)
                                ,sum(PREPAID)
                                ,sum(HP)
                                ,sum(FBB) 
                                ,TRX_MONTH 
                                ,2 as ROW_NO
                        ---MOD_SR0002530---
                                ,sum(OTHERS)
                        from 
                        ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T1
                        where RowName = 'Cost before sales channel cost'
                        group by STORE_CD, TRX_MONTH
                        ;

                        commit;

                        --End of Cost before sales channel cost


                        --Sales channel cost
                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T2
                        (
                                STORE_CD 
                                ,RowName 
                                ,Store_Total 
                                ,TRX_MONTH 
                                ,ROW_NO 
                        )
                        select 
                                a.store_code 
                                ,a.expenses_nature
                                ,coalesce(b.Total,0) as total
                                ,coalesce(b.TRX_MONTH ,trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')) as trx_month
                                ,a.ROW_NO as row_no
                        from (
                                select a.store_code, b.expenses_nature, b.row_no 
                                from (
                                        select distinct 'dummy' as dummy, x.store_code from ${etlvar::ADWDB}.rbd_kpi_cost_center_cd x
                                ) a
                                left join (
                                        select distinct 'dummy' as dummy, expenses_nature,
                                        case when upper(EXPENSES_NATURE)=upper('Commission')
                                        then 3
                                        ---MOD_SR0002530---
                                        ---Item c13a -----
                                        when upper(EXPENSES_NATURE)=upper('Marketing cost')
                                        then 3.1
                                        when upper(EXPENSES_NATURE)=upper('Rental '||Chr(38)||' utilities')
                                        then 6
                                        when upper(EXPENSES_NATURE)=upper('Manpower costs')
                                        then 5
                                        when upper(EXPENSES_NATURE)=upper('MIS expenses')
                                        then 7
                                        when upper(EXPENSES_NATURE)=upper('Administration expenses')
                                        then 8
                                        else -1
                                        end as row_no
                                        from ${etlvar::ADWDB}.RBD_KPI_ORACLE_CODE
                                        where upper(expenses_nature) not in upper('Headcount')
                                ) b
                                on a.dummy = b.dummy
                        ) a
                        left join (
                                select
                                store_code
                                ,expenses_nature
                                ,sum(net_trx_amt) as total
                                ,trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') as trx_month
                                from(
                                                select distinct
                                                a.cost_center_code, a.store_code, b.period, account_cd, account_cd_desc, net_trx_amt, c.EXPENSES_NATURE
                                                from 
                                                ${etlvar::ADWDB}.rbd_kpi_cost_center_cd a
                                                left join ${etlvar::ADWDB}.RBD_SALES_CHANNEL_COST b
                                                on a.cost_center_code = b.cost_center
                                                left join ${etlvar::ADWDB}.RBD_KPI_ORACLE_CODE c
                                                on b.ACCOUNT_CD between c.ACCT_CODE_START and ACCT_CODE_END
                                                where b.period = trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')
                                )
                                group by store_code, expenses_nature
                        ) b
                        on a.store_code = b.store_code
                        and a.expenses_nature = b.expenses_nature
                        ;

                        commit;
                        ---MOD_SR0002530---
                        --- account cd hardcode refer to fin advise----
                        ---c13 b-----
                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T2
                        (
                                STORE_CD
                                ,RowName
                                ,Store_Total
                                ,TRX_MONTH
                                ,ROW_NO
                        )
                        select
                                b.store_code
                                ,'Marketing cost'
                                ,sum(nvl(a.net_trx_amt,0)) as total
                                ,trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') as trx_month
                                ,3.2 ROW_NO
                        from prd_adw.rbd_kpi_cost_center_cd b
                        left outer join prd_adw.RBD_SALES_CHANNEL_COST a
                            on b.cost_center_code = a.cost_center
                                and a.period = trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')
                                and a.account_cd in ('09179010' ,'09179030')
                        group by b.store_code;
                        commit;

                        ---MOD_SR0002530---
                        ---c14 -----
                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T2
                        (
                                STORE_CD
                                ,RowName
                                ,Store_Total
                                ,TRX_MONTH
                                ,ROW_NO
                        )
                        select a.store_code
                                ,'SIM costs' as expense_nature
                                ,nvl(x.store_total,0)
                                ,trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') as trx_month
                                ,8.1 as row_no
                        from (select distinct store_code from prd_adw.rbd_kpi_cost_center_cd )a
                        left outer join (
                            Select   t.STORE_CD
                                   , COUNT(t.store_cd) *nvl((select sim_cost 
                                        from ${etlvar::MIGDB}.RBD_KPI_SIM_COST 
                                  where trx_mth =trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')),0) as store_total
                        From PRD_BIZ_SUMM_VW.VW_RBD_STORE_ORDER_M2D_DTL_TB t
                        Where t.MOBILE_TYPE='Mobile' 
                          and t.connection_type='New Activation'
                          and trunc(t.report_date,'MM') =  trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') 
                        Group By t.STORE_CD) x
                        on a.store_code = x.store_cd;
                        commit;
                         ---MOD_SR0002530---
                        ---c16 depreciation
        Insert into ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T2
         (
            STORE_CD
            ,RowName
            ,Store_Total
            ,TRX_MONTH
            ,ROW_NO
        )
        Select
            c.STORE_CD
            ,'store depreciation' ROWNAME
            ,sum(a.deprn_amt) as STORE_TOTAL
            ,trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')  as TRX_MONTH
            ,8.3 as ROW_NO
            from mig_adw.RBD_KPI_DEPRECIATION a
                ,prd_adw.RBD_KPI_ORACLE_CODE b
                ,mig_adw.RBD_KPI_LOCATION c
           where a.account_cd between b.acct_code_start and b.acct_code_end
             and b.expenses_nature ='Headcount'
             and a.location_code = c.location
             and c.trx_mth = trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')
             and a.trx_mth = trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')
           group by c.store_cd;
        commit;
                        ---MOD_SR0002530---
                        ---c15 headcount preparation---
        Insert into ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T2
         (  
            STORE_CD
            ,RowName
            ,Store_Total
            ,TRX_MONTH
            ,ROW_NO
        )
        Select
            cc.STORE_code
            ,'store headcount' ROWNAME
            ,sum(nvl(hc.net_actv,0)) as STORE_TOTAL
            ,trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')  as TRX_MONTH
            ,-1 as ROW_NO
          from prd_adw.rbd_kpi_cost_center_cd cc
          left outer join mig_adw.RBD_KPI_HEADCOUNT hc
            on cc.cost_center_code = substr(hc.acct_flexfield,instr(hc.acct_flexfield,'.',1,1)+ 1
                                    ,instr(hc.acct_flexfield,'.',1,2)-instr(hc.acct_flexfield,'.',1,1) -1)
            and hc.trx_mth = trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')                                    
         group by cc.store_code ;
        commit;
                        ---MOD_SR0002530---
                        ---c15 shared overhead---
                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T2
                        (
                                STORE_CD
                                ,RowName
                                ,Store_Total
                                ,TRX_MONTH
                                ,ROW_NO
                        )
                            Select   a.STORE_CODE
                                ,'shared overhead' as expense_nature
                                , sum(nvl(net_trx_amt,0))*nvl(max(a2.ptg),0) as store_total
                                ,trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') as trx_month
                                ,8.2
                        from ( select distinct store_code from prd_adw.rbd_kpi_cost_center_cd )a 
                        left outer join (
                            Select t2.store_cd
                                ,t2.store_total/sum(t2.store_total) over (partition by 1) ptg
                            from mig_adw.U_RBD_KPI_REPORT_CONTRIBUTE_T2 t2
                                where t2.rowname='store headcount'
                        )a2
                                on a.store_code = a2.store_cd
                        left outer join
                        (        
                        ---i---
select                                
                                sum(net_trx_amt) as net_trx_amt
                             --    b.cost_centre
                             --   , 'RBD General' 
                             --   , b.period
                             --   , account_cd
                             --   , account_cd_desc
                             --   , net_trx_amt
                             --   , c.EXPENSES_NATURE
                        from mig_adw.RBD_KPI_ODR b
                            ,mig_adw.RBD_KPI_SHARE_AC_CD c
                        where b.ACCOUNT_CD between c.ACCT_CODE_START and ACCT_CODE_END
                          and c.trx_mth = trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')                            
                          and upper(c.expenses_nature) in (upper('Administration expenses'),upper('MIS expenses'))
                          and b.period = trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')
                          and b.cost_centre='06519'
                        union 
                        --- ii ---
select
                                sum(b.net_trx_amt) as net_trx_amt
                             --    b.cost_centre as cost_center_code
                             --   , 'RBD General' as store_code
                             --   , b.period
                             --   , b.account_cd
                             --   , b.account_cd_desc
                             --   , b.net_trx_amt
                             --   , c.EXPENSES_NATURE
                        from 
                            mig_adw.RBD_KPI_ODR b
                            ,mig_adw.RBD_KPI_SHARE_AC_CD c
                        where b.ACCOUNT_CD between c.ACCT_CODE_START and ACCT_CODE_END
                           and c.trx_mth = trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')
                           and upper(c.expenses_nature) in (upper( 'Marketing expense'))
                           and b.cost_centre not in(select a.cost_center_code from prd_adw.rbd_kpi_cost_center_cd a)
                           and b.period = trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')
                           and b.project_code in (select project_code from mig_adw.RBD_KPI_PROJECT )
                     --   group by
                     --      b.cost_centre
                     --           , b.period
                     --           , b.account_cd
                     --           , b.account_cd_desc
                     --           , b.net_trx_amt
                     --           , c.EXPENSES_NATURE 
                        union
                        --- iii---
                        select
                                sum(b.net_trx_amt) as net_trx_amt
                      --           b.cost_centre as cost_center_code
                      --          , 'RBD General'  as store_code
                      --          , b.period
                      --          , b.account_cd
                      --          , b.account_cd_desc
                      --          , b.net_trx_amt
                      --          , c.EXPENSES_NATURE
                        from 
                             mig_adw.RBD_KPI_ODR b
                            ,mig_adw.RBD_KPI_SHARE_AC_CD c
                        where b.ACCOUNT_CD between c.ACCT_CODE_START and ACCT_CODE_END
                           and c.trx_mth = trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')
                           and upper(c.expenses_nature) in (upper( 'Marketing expense2'))
                           and b.cost_centre not in(select a.cost_center_code from prd_adw.rbd_kpi_cost_center_cd a)
                           and b.period = trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')
                     --   group by
                     --      b.cost_centre
                     --           , b.period
                     --           , b.account_cd
                     --           , b.account_cd_desc
                     --           , b.net_trx_amt
                     --           , c.EXPENSES_NATURE
                )x
        on 1=1 
        group by a.STORE_CODE;
                commit;
                        ---MOD_SR0002530---
                        ---Remark Premium cost ----
--                      --Premium cost
--                      INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T2
--                      (
--                              STORE_CD 
--                              ,RowName 
--                              ,Store_Total 
--                              ,TRX_MONTH 
--                              ,ROW_NO 
--                      )
--                      select 
--                      store_cd
--                      ,'Premium cost' as row_name
--                      ,coalesce(premium_cost,0) as premium_cost
--                      ,trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') as trx_month
--                      ,4 as row_no
--                      from (
--                              select
--                              store_cd, sum(unit_cost) as premium_cost
--                              from (
--                                      select 
--                                      a.store_cd, b.INV_NUM,
--                                      c.pos_prod_cd, c.UNIT_COST * b.discount_rate as unit_cost
--                                      from 
--                                      (
--                                              select distinct
--                                              store_cd
--                                              from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T2
--                                      ) a 
--                                      left join (
--                                              select x.store_cd, inv_num, discount_rate
--                                              from (
--                                                      select 
--                                                      inv_num, store_cd
--                                                      ,discount_rate
--                                                      from (
--                                                              select b.discount_rate, store_cd, order_num as inv_num 
--                                                              from
--                                                              ${etlvar::MIGADWDB}.RBD_KPI_REPORT_SERV_REV_DTL a
--                                                              left join ${etlvar::ADWDB}.rbd_kpi_report_dis_rate b
--                                                              on upper(a.channel_effort) = upper(b.channel)
--                                                              where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
--                                                      )
--                                                      union
--                                                      select 
--                                                      store_cd, inv_num
--                                                      ,case when FA_INV_FLG ='Y'
--                                                      then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'Street fighter')

--                                                      then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'Online')
--                                                      when STORE_CD in (select store_cd from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP where daily_sales_sms='Y')
--                                                      then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'RBD')
--                                                      else (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'Others')
--                                                      end as discount_rate
--                                                      from ${etlvar::MIGADWDB}.RBD_KPI_REPORT_HS_DTL a
--                                                      where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
--                                                      union
--                                                      select 
--                                                      store_cd, inv_num
--                                                      ,case when FA_INV_FLG ='Y'
--                                                      then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'Street fighter')
--                                                      when ONLINE_INV_FLG in ('Y','B')
--                                                      then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'Online')
--                                                      when STORE_CD in (select store_cd from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP where daily_sales_sms='Y')
--                                                      then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'RBD')
--                                                      else (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'Others')
--                                                      end as discount_rate
--                                                      from ${etlvar::MIGADWDB}.RBD_KPI_REPORT_SIM_DTL a
--                                                      where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
--                                                      union
--                                                      select 
--                                                      store_cd, inv_num
--                                                      ,case when FA_INV_FLG ='Y'
--                                                      then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'Street fighter')
--                                                      when ONLINE_INV_FLG in ('Y','B')
--                                                      then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'Online')
--                                                      when STORE_CD in (select store_cd from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP where daily_sales_sms='Y')
--                                                      then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'RBD')
--                                                      else (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'Others')
--                                                      end as discount_rate
--                                                      from ${etlvar::MIGADWDB}.RBD_KPI_REPORT_ACCESORY_DTL a
--                                                      where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
--                                                      union
--                                                      select 
--                                                      store_cd, inv_num
--                                                      ,case when FA_INV_FLG ='Y'
--                                                      then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'Street fighter')
--                                                      when ONLINE_INV_FLG in ('Y','B')
--                                                      then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'Online')
--                                                      when STORE_CD in (select store_cd from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP where daily_sales_sms='Y')
--                                                      then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'RBD')
--                                                      else (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'Others')
--                                                      end as discount_rate
--                                                      from ${etlvar::MIGADWDB}.RBD_KPI_REPORT_PREPA_VOU_DTL a
--                                                      where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
--                                                      union
--                                                      select 
--                                                      store_cd, inv_num
--                                                      ,case when FA_INV_FLG ='Y'
--                                                      then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'Street fighter')
--                                                      when ONLINE_INV_FLG in ('Y','B')
--                                                      then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'Online')
--                                                      when STORE_CD in (select store_cd from ${etlvar::MIGDB}.U_RBD_KPI_REPORT_TX_001_TMP where daily_sales_sms='Y')
--                                                      then (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'RBD')
--                                                      else (select discount_rate from ${etlvar::ADWDB}.rbd_kpi_report_dis_rate where channel = 'Others')
--                                                      end as discount_rate
--                                                      from ${etlvar::MIGADWDB}.RBD_KPI_REPORT_OTHER_DTL a
--                                                      where inv_date between trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH') and TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1
--                                              ) x
--                                      ) b
--                                      on a.store_cd = b.store_cd
--                                      left join (
--                                              select inv_num, pos_prod_cd, b.*
--                                              from ${etlvar::ADWDB}.pos_inv_detail a
--                                              inner join ${etlvar::ADWDB}.rbd_kpi_premium_cost b
--                                              on a.pos_prod_cd = b.product_code
--                                              where trunc(b.trx_month,'MONTH') = trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')
--                                      ) c
--                                      on b.inv_num = c.inv_num
--                              )
--                              group by STORE_CD
--                      )
--                      ;
--
--                      commit;

                        INSERT INTO ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T2
                        (
                                STORE_CD 
                                ,RowName 
                                ,Store_Total 
                                ,TRX_MONTH 
                                ,ROW_NO 
                        )
                        SELECT 
                        store_cd
                        ,'Contribution'
                        ,coalesce(sum(
                                case when row_no = 2
                                then store_total
                        ---MOD_SR0002530---
                        ---- Invole new item into it change row_no from 8 to 8.3
                                --when row_no between 3 and 8
                                when row_no between 3 and 8.3
                                then store_total * -1
                                else 0
                                end
                        ),0)
                        ,trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')
                        ,9 as row_no
                        from
                        ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T2
                        where trx_month = trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')
                        group by store_cd
                        ;

                        commit;

                        update ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T2
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

                        DELETE FROM  ${etlvar::MIGADWDB}.RBD_KPI_Contribution_by_store
                        WHERE TRX_MONTH = trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')
                        ;

                        commit;

                        DELETE FROM  ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T2
                        WHERE TRX_MONTH = trunc(TO_DATE('${etlvar::TXDATE}', 'YYYY-MM-DD')-1,'MONTH')
                        and row_no = -1
                        ;

                        commit;

                        INSERT INTO  ${etlvar::MIGADWDB}.RBD_KPI_Contribution_by_store
                        (
                                STORE_CD 
                                ,RowName 
                                ,Store_Total 
                                ,POSTPAID 
                                ,HANDSET 
                                ,ACCESSORY 
                                ,PREPAID 
                                ,HP 
                                ,FBB 
                                ,TRX_MONTH 
                                ,OTHERS
                                ,ROW_NO 
                        )
                        SELECT 
                                STORE_CD 
                                ,RowName 
                                ,Store_Total 
                                ,POSTPAID 
                                ,HANDSET 
                                ,ACCESSORY 
                                ,PREPAID 
                                ,HP 
                                ,FBB 
                                ,TRX_MONTH 
                                ,OTHERS
                                ,ROW_NO 
                        FROM 
                         ${etlvar::MIGDB}.U_RBD_KPI_REPORT_CONTRIBUTE_T2
                        ;

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














