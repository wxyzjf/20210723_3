/opt/etl/prd/etl/APP/ADW/Y_B_RETENT_CORP_COMM_RPT/bin> cat b_retent_corp_comm_rpt0020.pl
######################################################
#   $Header: /CVSROOT/SmarTone-Vodafone/Code/ETL/APP/ADW/B_POS_INV_DETAIL/bin/b_pos_inv_detail0010.pl,v 1.1 2005/12/14 01:04:05 MichaelNg Exp $
#   Purpose: For prepare the retention comm rpt
#   Param  : TX_Date = 2016-03-01 , report range = 2016-02-01 ~ 2016-02-29 
#
#
######################################################


##my $ETLVAR = $ENV{"AUTO_ETLVAR"};require $ETLVAR;
my $ETLVAR = "/opt/etl/prd/etl/APP/ADW/Y_B_RETENT_CORP_COMM_RPT/bin/master_dev.pl";
require $ETLVAR;

my $MASTER_TABLE = ""; #Please input the final target ADW table name here

sub runSQLPLUS{
    my $rc = open(SQLPLUS, "| sqlplus /\@${etlvar::TDDSN}");
    ##my $rc = open(SQLPLUS, "| cat > a.sql");
    unless ($rc){
        print "Cound not invoke SQLPLUS command\n";
        return -1;
    }


    print SQLPLUS<<ENDOFINPUT;
        --${etlvar::LOGON_TD}
        ${etlvar::SET_MAXERR}
        ${etlvar::SET_ERRLVL_1}
        ${etlvar::SET_ERRLVL_2}
set echo on
---------------------------------------------------------------------------------------------------------

set define on;
set linesize 2000
alter session force parallel dml parallel 30;

define comm_mth=to_date('$etlvar::F_D_MONTH[0]','YYYY-MM-DD');
define rpt_mth=add_months(to_date('$etlvar::F_D_MONTH[0]','YYYY-MM-DD'),-3);
define rpt_s_date=add_months(to_date('$etlvar::F_D_MONTH[0]','YYYY-MM-DD'),-3);
define rpt_e_date=add_months(to_date('$etlvar::F_D_MONTH[0]','YYYY-MM-DD'),-2)-1;

execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_CORP_COMM_002A_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_CORP_COMM_002A02_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_CORP_COMM_002A03_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_CORP_COMM_002B01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_CORP_COMM_002C01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_CORP_COMM_002C02_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_CORP_COMM_002D_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_CORP_COMM_002_T');
prompt 'b_retent_corp_comm_002A_t [Preparing billing first serval bill inv list within the valid ld period]';

 insert into mig_adw.b_retent_corp_comm_002a_t(
         rpt_mth
        ,comm_mth
        ,case_id        
        ,ld_inv_num
        ,ld_cust_num
        ,ld_subr_num
        ,ld_cd
        ,ld_mkt_cd
        ,ld_start_date
        ,ld_expired_date
        ,start_date
        ,end_date
        ,bill_inv_num
        ,bill_inv_date        
        ,bill_inv_subr
        ,bill_inv_cust
        ,bill_inv_rnk       
        ) 
 select tc.rpt_mth
        ,tc.comm_mth
        ,tc.case_id        
        ,tc.ld_inv_num
        ,tc.ld_cust_num
        ,tc.ld_subr_num
        ,tc.ld_cd
        ,tc.ld_mkt_cd
        ,tc.ld_start_date
        ,tc.ld_expired_date
        ,tc.start_date
        ,tc.end_date
        ,ph.inv_num as bill_inv_num
        ,ph.inv_date as bill_inv_date        
        ,ph.subr_num as bill_inv_subr
        ,ph.cust_num as bill_inv_cust        
        ,rank() over (partition by tc.case_id order by ph.inv_date) as bill_inv_rnk        
 from (
 Select   t.case_id
         ,t.comm_mth
         ,t.rpt_mth
         ,t.ld_inv_num
         ,t.ld_cd
         ,t.ld_mkt_cd
        ,sl.subr_num as ld_subr_num
        ,sl.cust_num as ld_cust_num
        ,sl.start_date
        ,sl.end_date
        ,sl.ld_start_date
        ,sl.ld_expired_date
 from mig_adw.b_retent_corp_comm_001_t t
     ,prd_adw.subr_ld_hist sl
  where t.ld_inv_num = sl.inv_num
    and sl.start_date <= t.comm_mth
 ) tc,prd_adw.inv_header ph 
where tc.ld_subr_num = ph.subr_num
  and tc.ld_cust_num = ph.cust_num
  and ph.inv_date between tc.start_date and tc.end_date
  and ph.inv_date between tc.ld_start_date and tc.ld_expired_date
  and ph.inv_date <= add_months(tc.comm_mth,1)-1
order by tc.case_id;
commit;

prompt 'b_retent_corp_comm_002A02_t [combine the first 4 invoice list ]';
insert into  mig_adw.b_retent_corp_comm_002a02_t
(
         rpt_mth
        ,comm_mth
        ,case_id 
        ,ld_inv_num
        ,map_bill_inv_num
        ,map_bill_inv_date
        ,map_bill_subr
        ,map_bill_cust
        ,map_bill_inv_rnk
        ,bill_inv_list
)
select  
         t.rpt_mth
        ,t.comm_mth
        ,t.case_id 
        ,t.ld_inv_num
        ,nvl(max(decode(t.bill_inv_rnk,4,t.bill_inv_num,3,t.bill_inv_num)),' ') as map_bill_inv_num
        ,nvl(max(decode(t.bill_inv_rnk,4,t.bill_inv_date,3,t.bill_inv_date)),date '1900-01-01') as map_bill_inv_date
        ,nvl(max(decode(t.bill_inv_rnk,4,t.bill_inv_subr)),' ') as map_bill_subr
        ,nvl(max(decode(t.bill_inv_rnk,4,t.bill_inv_cust)),' ') as map_bill_cust
        ,nvl(max(decode(t.bill_inv_rnk,4,t.bill_inv_rnk)),0) as map_bill_inv_rnk
        ,listagg('['||t.bill_inv_num||']-'||to_char(t.bill_inv_date,'yyyymmdd'), ';') within group (order by t.bill_inv_rnk)  as bill_inv_list
from mig_adw.b_retent_corp_comm_002a_t t
where t.bill_inv_rnk <=4
group by 
         t.rpt_mth
        ,t.comm_mth
        ,t.case_id
        ,t.ld_inv_num;
commit;

prompt 'b_retent_corp_comm_002A03_t [map the rebate by 4 invoice date ]';
insert into  mig_adw.b_retent_corp_comm_002a03_t
(
         rpt_mth
        ,comm_mth
        ,case_id
        ,ld_inv_num
        ,map_bill_inv_num
        ,map_bill_inv_date
        ,map_bill_subr
        ,map_bill_cust
        ,map_bill_inv_rnk
        ,bill_inv_list
        ,rebate_amt
        ,json_rmk
)
select 
         tt.rpt_mth
        ,tt.comm_mth
        ,tt.case_id
        ,tt.ld_inv_num
        ,tt.map_bill_inv_num
        ,tt.map_bill_inv_date
        ,tt.map_bill_subr
        ,tt.map_bill_cust
        ,tt.map_bill_inv_rnk
        ,tt.bill_inv_list
        ,sum(tt.rebate_amt) as rebate_amt
        ,',"INV_REBTE":"'||listagg(substr(tt.rebate_type,1,instr(tt.rebate_type,'-')-1)||'='||tt.rebate_amt,';') within group (order by  tt.rebate_type)||'"'
from(
    Select 
          t.rpt_mth
        ,t.comm_mth
        ,t.case_id
        ,t.ld_inv_num
        ,t.map_bill_inv_num
        ,t.map_bill_inv_date
        ,t.map_bill_subr
        ,t.map_bill_cust
        ,t.map_bill_inv_rnk
        ,t.bill_inv_list
        ,sh.transaction_type rebate_type
            ,sh.transaction_amount rebate_amt
            ,'SAL_LED_HIST' 
    from mig_adw.b_retent_corp_comm_002a02_t t
        , prd_adw.SAL_LED_HISTORY sh
    where t.map_bill_subr = sh.subr_num
      and t.map_bill_cust = sh.cust_num
      and trunc(t.map_bill_inv_date,'MM') = sh.trx_hist_month
      and sh.transaction_type like 'PAY%'
      and not exists (SELECT 'x'
                       FROM prd_adw.NON_REV_LEDGE_TYPE_REF b
                       WHERE b.trx_category='PAY'
                       AND   sh.transaction_type like b.trx_type_cd
      )
      and trx_hist_month between &rpt_s_date and add_months(&rpt_s_date,+5)
    union all
    Select
                t.rpt_mth
        ,t.comm_mth
        ,t.case_id
        ,t.ld_inv_num
        ,t.map_bill_inv_num
        ,t.map_bill_inv_date
        ,t.map_bill_subr
        ,t.map_bill_cust
        ,t.map_bill_inv_rnk
        ,t.bill_inv_list
        ,sl.sal_trx_type_cd rebate_type
            ,sl.sal_led_amt rebate_amt
            ,'SAL_LED' 
    from mig_adw.b_retent_corp_comm_002a02_t t
        , prd_adw.sal_led sl
    where t.map_bill_subr = sl.subr_num
      and t.map_bill_cust = sl.cust_num
      and sl.sal_trx_type_cd like 'PAY%'
      and trunc(t.map_bill_inv_date,'MM') = trunc(sl.trx_date,'MM')
      and not exists (SELECT 'x'
                       FROM prd_adw.NON_REV_LEDGE_TYPE_REF b
                       WHERE b.trx_category='PAY'
                       AND   sl.sal_trx_type_cd like b.trx_type_cd
    )
      and sl.trx_date between &rpt_s_date and add_months(&rpt_s_date,+5)
)tt 
group by tt.case_id
        ,tt.rpt_mth
        ,tt.comm_mth
        ,tt.case_id
        ,tt.ld_inv_num
        ,tt.map_bill_inv_num
        ,tt.map_bill_inv_date
        ,tt.map_bill_subr
        ,tt.map_bill_cust
        ,tt.map_bill_inv_rnk
        ,tt.bill_inv_list ;
commit;

prompt 'b_retent_corp_comm_002B01_t [ Mapping the hs subsidy ]';
insert into mig_adw.B_RETENT_CORP_COMM_002B01_T
(       rpt_mth
        ,comm_mth
        ,case_id
        ,ld_inv_num
        ,hs_subsidy_amt
)
Select   t.rpt_mth
         ,t.comm_mth
         ,t.case_id
         ,t.ld_inv_num
         ,sh.net_subsidy_amt
 from mig_adw.b_retent_corp_comm_001_t t
     ,prd_biz_summ_vw.vw_hs_subsidy sh
 where t.ld_inv_num = sh.inv_num;
commit;

prompt 'B_RETENT_CORP_COMM_002C01_T [ Mapping the sales info ]';
insert into mig_adw.B_RETENT_CORP_COMM_002C01_T
(
        rpt_mth 
        ,comm_mth
        ,case_id
        ,ld_inv_num
        ,salesman_cd
        ,sales_team
)
select
        t.rpt_mth 
        ,t.comm_mth
        ,t.case_id
        ,t.ld_inv_num
        ,case when ov.salesman_cd is not null
              then ov.salesman_cd
         else ph.salesman_cd
         end as salesman_cd
        ,' ' as sales_team
from  mig_adw.b_retent_corp_comm_001_t t
left outer join prd_adw.pos_inv_header ph
        on t.ld_inv_num = ph.inv_num
left outer join MIG_ADW.RETENT_CORP_COMM_OVERR_HIST ov
        on t.ld_inv_num = ov.inv_num
           and ov.COMM_MTH = &comm_mth;


prompt 'B_RETENT_CORP_COMM_002C02_T [ Mapping the sales info ]';
insert into mig_adw.B_RETENT_CORP_COMM_002C02_T
(
        rpt_mth 
        ,comm_mth
        ,case_id
        ,ld_inv_num
        ,salesman_cd
        ,sales_team
)
select
        t.rpt_mth 
        ,t.comm_mth
        ,t.case_id
        ,t.ld_inv_num
        ,t.salesman_cd
        ,nvl(r.sale_team,' ')
from  mig_adw.B_RETENT_CORP_COMM_002C01_T t
left outer join mig_adw.retent_corp_comm_salesman r
        on t.salesman_cd = r.salesman_cd 
           and r.COMM_MTH = &comm_mth;


prompt 'B_RETENT_CORP_COMM_002D_T [ mkt POS creditback amount ]';
insert into mig_adw.b_retent_corp_comm_002d_t
(             
        case_id
       ,mkt_cb_amt
        ,json_rmk
)
select tbl.case_id 
        ,-1 * sum(tbl.cb_amt) as ttl_cb_amt
        ,',"MKT_REBTE":"'||listagg(tbl.credit_back_cd||'='||tbl.cb_amt,';') within group (order by  tbl.credit_back_cd)||'"'
from
        (
        --------SCB simonly scb from mkt_cd ----------
        select   t.case_id
                ,t.pos_inv_num
                ,t.subr_num
                ,t.cust_num
                ,t.pos_mkt_cd
                ,t.pos_inv_date
                ,s.credit_back_cd
                ,max(s.credit_back_amt) cb_amt
        from mig_adw.b_retent_corp_comm_001_t t                
                    ,prd_adw.pos_crbk_detail p
                    ,prd_adw.srvcb s
        where
         t.subr_num = p.subr_num
         and t.cust_num = p.cust_num
         and t.pos_mkt_cd = s.mkt_cd
         and p.credit_cd = s.credit_back_cd
         and &rpt_e_date between s.start_date and s.end_date
         and abs(t.pos_inv_date - p.crbk_date ) <=1
         and (p.inv_num = t.pos_inv_num or p.inv_num =' ')
        ------ skip the family sub sim  case due to the LD override by main LD----         
        group by
                 t.case_id
                 ,t.pos_inv_num
                ,t.subr_num
                ,t.cust_num
                ,t.pos_mkt_cd
                ,t.pos_inv_date
                ,s.credit_back_cd
        --------FES HS BONUS credit back --------------
        union all
        select
                t.case_id
                ,t.pos_inv_num
                ,t.subr_num
                ,t.cust_num
                ,t.pos_mkt_cd
                ,t.pos_inv_date
                ,bh.credit_back_cd
                ,sum(bh.credit_back_amt * d.qty)  as cb_amt
        from mig_adw.b_retent_corp_comm_001_t t
                ,prd_adw.pos_inv_detail d
                ,prd_adw.fes_hs_bonus_hist bh
        where t.pos_inv_num = d.inv_num
              and t.pos_mkt_cd = bh.mkt_cd
              and d.pos_prod_cd = bh.prod_cd
              and &rpt_e_date between bh.start_date and bh.end_date
               ------ skip the family sub sim  case due to the LD override by main LD----             
        group by t.case_id
                ,t.subr_num
                ,t.cust_num
                ,t.pos_inv_num
                ,t.pos_inv_date
                ,t.pos_mkt_cd
                ,bh.credit_back_cd
        ) tbl
        group by tbl.case_id ;
commit;
prompt 'B_RETENT_CORP_COMM_002_T : [ combine to target table ]';
insert into mig_adw.B_RETENT_CORP_COMM_002_T
(
    rpt_mth
    ,comm_mth
    ,case_id
    ,ld_inv_num
    ,map_bill_inv_num
    ,map_bill_inv_date
    ,map_bill_subr
    ,map_bill_cust
    ,map_bill_inv_rnk
    ,bill_inv_list
    ,rebate_amt
    ,hs_subsidy_amt
    ,salesman_cd
    ,sales_team
    ,mkt_cb_amt
    ,inv_cb_amt
    ,rebate_flg
    ,json_rmk
)
select 
    t.rpt_mth
    ,t.comm_mth
    ,t.case_id
    ,t.ld_inv_num
    ,case when mc.case_id is not null then ' ' else nvl(inv.map_bill_inv_num, ' ') end map_bill_inv_num
    ,case when mc.case_id is not null then date '1900-01-01' else nvl(inv.map_bill_inv_date,date '1900-01-01') end map_bill_inv_date
    ,case when mc.case_id is not null then ' ' else nvl(inv.map_bill_subr,' ') end map_bill_subr
    ,case when mc.case_id is not null then ' ' else nvl(inv.map_bill_cust,' ') end map_bill_cust
    ,case when mc.case_id is not null then 0  else nvl(inv.map_bill_inv_rnk,0) end map_bill_inv_rnk
    ,case when mc.case_id is not null then ' ' else nvl(inv.bill_inv_list,' ') end bill_inv_list
    ,case when t.pos_mkt_type ='NON_PLAN_MKT' then nvl(mc.mkt_cb_amt,0)
          when mc.case_id is not null then mc.mkt_cb_amt 
          else nvl(inv.rebate_amt,0) 
     end rebate_amt
    ,nvl(hs.hs_subsidy_amt,0)
    ,nvl(sa.salesman_cd,' ')
    ,nvl(sa.sales_team,' ')
    ,nvl(mc.mkt_cb_amt,0)
    ,nvl(inv.rebate_amt,0)
    ,case when t.pos_mkt_type ='NON_PLAN_MKT' then 'MKT_REBATE'
          when mc.case_id is not null then 'MKT_REBATE'
          else 'INV_REBATE'
     end rebate_flg
    ,t.json_rmk||',"POS_MKT_TYPE"="'||t.pos_mkt_type||'"'||nvl(case when t.pos_mkt_type ='NON_PLAN_MKT' then mc.json_rmk
          when mc.case_id is not null then mc.json_rmk
          else inv.json_rmk
     end,' ') json_rmk
from mig_adw.B_RETENT_CORP_COMM_001_T t
left outer join mig_adw.B_RETENT_CORP_COMM_002A03_T inv
        on t.case_id = inv.case_id
left outer join mig_adw.B_RETENT_CORP_COMM_002B01_T hs
        on t.case_id = hs.case_id
left outer join mig_adw.B_RETENT_CORP_COMM_002C02_T sa
        on t.case_id = sa.case_id
left outer join mig_adw.B_RETENT_CORP_COMM_002D_T mc
        on t.case_id = mc.case_id ;
commit;


quit;
---------------------------------------------------------
commit;
  exit;
ENDOFINPUT

    close(SQLPLUS);
    my $RET_CODE = $? >> 8;
    if ($RET_CODE != 0){
        return 1;
    }else{
        return 0;
    }
}


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
etlvar::genFirstDayOfMonth($etlvar::TXDATE);
my $ret = runSQLPLUS();
my $post = etlvar::postProcess();

exit($ret);

