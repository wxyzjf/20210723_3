/opt/etl/prd/etl/APP/ADW/Y_SHKDP_RPT/bin> cat b_shkdp_rpt0040.pl
######################################################
#   $Header: /CVSROOT/SmarTone-Vodafone/Code/ETL/APP/ADW/B_POS_INV_DETAIL/bin/b_pos_inv_detail0010.pl,v 1.1 2005/12/14 01:04:05 MichaelNg Exp $
#   Purpose: For prepare the retention comm rpt
#   Param  : TX_Date = 2016-03-01 , report range = 2016-02-01 ~ 2016-02-29 
#
#
######################################################


##my $ETLVAR = $ENV{"AUTO_ETLVAR"};require $ETLVAR;
my $ETLVAR = "/opt/etl/prd/etl/APP/ADW/Y_B_RETENT_UPG_COMM_RPT/bin/master_dev.pl";
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
---------------------------------------------------------------------------------------------------------
set linesize 2000
alter session force parallel query parallel 30;
alter session force parallel dml parallel 30;

execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'SHKDP_RPT_004A01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'SHKDP_RPT_004A02_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'SHKDP_RPT_004B01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'SHKDP_RPT_004C01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'SHKDP_RPT_004C02_T');
--execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'SHKDP_RPT_004_T');

set define on;
define opt_date=to_date('$etlvar::F_D_MONTH[0]','YYYY-MM-DD');


--------------------------------------------------------------------------------------------------------

prompt 'Step SHKDP_RPT_004A01_T: [ Prepaing rule addon mkt code list map value ] ';
----Only focus on non plan mkt code list . deduplicate and sorting by mkt code
insert into mig_adw.SHKDP_RPT_004A01_T
(   rule_id
    ,addon_map_mkt_lst)
select t.rule_id
      ,listagg( t.mkt_cd,',') within group (order by mkt_cd) addon_map_mkt_lst
from      
(     
    ---- Deduplicate mkt code in this level---
      select  distinct 
            r.rule_id
           ,r.mkt_lst
      --     ,r.mkt_cnt
      --     ,c.mkt_num           
           ,regexp_substr(r.mkt_lst,'[^,]+', 1, mkt_num) mkt_cd            
      from
        (
            select       ru.rule_id
                    ,decode(ru.hs_mkt_cd,' ','',','||ru.hs_mkt_cd)
                    ||decode(ru.fup_mkt_cd,' ','',','||ru.fup_mkt_cd)
                    ||decode(ru.vas_mkt_cd,' ','',','||ru.vas_mkt_cd)
                    ||decode(ru.vas2_mkt_cd,' ','',','||ru.vas2_mkt_cd)
                    ||decode(ru.vas3_mkt_cd,' ','',','||ru.vas3_mkt_cd) as mkt_lst                    
                    ,regexp_count(decode(ru.hs_mkt_cd,' ','',','||ru.hs_mkt_cd)
                    ||decode(ru.fup_mkt_cd,' ','',','||ru.fup_mkt_cd)
                    ||decode(ru.vas_mkt_cd,' ','',','||ru.vas_mkt_cd)
                    ||decode(ru.vas2_mkt_cd,' ','',','||ru.vas2_mkt_cd)
                    ||decode(ru.vas3_mkt_cd,' ','',','||ru.vas3_mkt_cd),',') as mkt_cnt
            from mig_adw.shkdp_rpt_rule_ref ru           
        )r 
        ,(select rownum as mkt_num from dual connect by rownum<100) c
     where 1=1 and r.mkt_cnt >= mkt_num
) t
where t.mkt_cd is not null 
  and t.mkt_cd <> ' '   
group by t.rule_id ;
commit;

prompt 'Step SHKDP_RPT_004A02_T: [ Combine addon_map_mkt_lst to rule info] ';
----- Add mapping to rule temp rule table 
insert into mig_adw.SHKDP_RPT_004A02_T
(
         rule_id
        ,plan_cd
        ,plan_mkt_cd
        ,ld_cd
        ,hs_mkt_cd
        ,fup_mkt_cd
        ,vas_mkt_cd
        ,vas2_mkt_cd
        ,vas3_mkt_cd
        ,map_mkt_cd_lst
        ,net_price
        ,plan_mix_nature
        ,plan_mix_sub_nature
        ,addon_map_mkt_lst
        ,create_ts
        ,refresh_ts
)select
         r.rule_id
        ,r.plan_cd
        ,r.plan_mkt_cd
        ,r.ld_cd
        ,r.hs_mkt_cd
        ,r.fup_mkt_cd
        ,r.vas_mkt_cd
        ,r.vas2_mkt_cd
        ,r.vas3_mkt_cd
        ,r.map_mkt_cd_lst
        ,r.net_price
        ,r.plan_mix_nature
        ,r.plan_mix_sub_nature
        ,nvl(t.addon_map_mkt_lst,' ')
        ,sysdate create_ts
        ,sysdate refresh_ts
from mig_adw.SHKDP_RPT_RULE_REF r
left outer join mig_adw.SHKDP_RPT_004A01_T t
        on r.rule_id = t.rule_id;
commit;
        
        
prompt 'Step SHKDP_RPT_004B01_T: [ Combine addon_map_mkt_lst to rule info] ';
insert into mig_adw.SHKDP_RPT_004B01_T
(
         trx_id
        ,af_online_pos_inv_num
        ,af_rate_plan_cd
        ,af_plan_mkt_cd 
        ,af_addon_map_mkt_lst
        ,map_rule_id
        ,map_rule_net_price
        ,map_plan_mix_nature
        ,map_plan_mix_sub_nature)
select
         t.trx_id
        ,af.af_online_pos_inv_num
        ,af.af_rate_plan_cd
        ,af.af_plan_mkt_cd 
        ,af.af_addon_map_mkt_lst
        ,nvl(max(ru.rule_id)keep (dense_rank first order by length(ru.addon_map_mkt_lst) desc),' ')     as map_rule_id
        ,nvl(max(ru.net_price)keep (dense_rank first order by length(ru.addon_map_mkt_lst) desc),0)     as map_rule_net_price
        ,nvl(max(ru.plan_mix_nature)keep (dense_rank first order by length(ru.addon_map_mkt_lst) desc),' ')  as  map_plan_mix_nature
        ,nvl(max(ru.plan_mix_sub_nature)keep (dense_rank first order by length(ru.addon_map_mkt_lst) desc),' ') as map_plan_mix_sub_nature
 from mig_adw.SHKDP_RPT_001_T t 
  left outer join mig_adw.SHKDP_RPT_003_T af
                on t.trx_id = af.trx_id
  left outer join mig_adw.SHKDP_RPT_004A02_T ru
        on      af.af_rate_plan_cd = ru.plan_cd
                and af.af_plan_mkt_cd = ru.plan_mkt_cd
                and af.af_addon_map_mkt_lst = ru.addon_map_mkt_lst
group by t.trx_id
        ,af.af_online_pos_inv_num
        ,af.af_rate_plan_cd
        ,af.af_plan_mkt_cd
        ,af.af_addon_map_mkt_lst ;
commit;

prompt 'Step SHKDP_RPT_004C01_T: [ Combine all info ] ';

insert into mig_adw.SHKDP_RPT_004C01_T
(
         batch_id
        ,trx_id
        ,cust_num
        ,subr_num
        ,batch_create_date
        ,contact_start_date
        ,eff_start_date
        ,eff_end_date
        ,shk_tier1
        ,shk_tier2
        ,shk_tier3
        ,shk_tier4
        ,shk_tier5
        ,shk_sale_tag
        ,pr_last_mod_date
        ,pr_final_status
        ,pr_comm_by
        ,pr_call_pgm_id
        ,bf_ld_exp_date
        ,bf_rate_plan_cd
        ,bf_plan_tariff
        ,bf_subr_sw_on_date
        ,bf_nomination_flg
        ,bf_avg_net_rev
        ,bf_other_amt
        ,bf_other_lst
        ,bf_json_rmk
        ,af_channel_effort
        ,af_online_pos_inv_num
        ,af_rate_plan_cd
        ,af_plan_tariff
        ,af_plan_mkt_cd
        ,af_mkt_cd_lst
        ,af_map_key_id_lst
        ,af_addon_map_mkt_lst
        ,af_json_rmk
        ,af_net_price
        ,ru_rule_id
        ,ru_rule_net_price
        ,ru_plan_mix_nature
        ,ru_plan_mix_sub_nature
        ,cl_sale_type
        ,cl_reach
        ,cl_hit
        ,cl_except
        ,shk_tier
)select
        t.batch_id
        ,t.trx_id
        ,t.cust_num
        ,t.subr_num
        ,t.batch_create_date
        ,t.contact_start_date
        ,t.eff_start_date
        ,t.eff_end_date
        ,t.shk_tier1
        ,t.shk_tier2
        ,t.shk_tier3
        ,t.shk_tier4
        ,t.shk_tier5
        ,t.shk_sale_tag
        ,bf.pr_last_mod_date
        ,bf.pr_final_status
        ,bf.pr_comm_by
        ,bf.pr_call_pgm_id
        ,bf.bf_ld_exp_date
        ,bf.bf_rate_plan_cd
        ,bf.bf_plan_tariff
        ,bf.bf_subr_sw_on_date
        ,bf.bf_nomination_flg
        ,bf.bf_avg_net_rev
        ,bf.bf_other_amt
        ,bf.bf_other_lst
        ,bf.bf_json_rmk
        ,af.af_channel_effort
        ,af.af_online_pos_inv_num
        ,af.af_rate_plan_cd
        ,af.af_plan_tariff
        ,af.af_plan_mkt_cd
        ,af.af_mkt_cd_lst
        ,af.af_map_key_id_lst
        ,af.af_addon_map_mkt_lst
        ,af.af_json_rmk
        ,ru.map_rule_net_price
        ,ru.map_rule_id ru_rule_id
        ,ru.map_rule_net_price  as ru_rule_net_price
        ,ru.map_plan_mix_nature         as ru_plan_mix_nature
        ,ru.map_plan_mix_sub_nature as ru_plan_mix_sub_nature
        ,' ' as cl_sale_type
        ,' ' as cl_reach
        ,' ' as cl_hit
        ,' ' as cl_except
        ,t.shk_tier
from mig_adw.SHKDP_RPT_001_T t
left outer join mig_adw.shkdp_rpt_002_t bf
        on t.trx_id = bf.trx_id
left outer join mig_adw.shkdp_rpt_003_t af
        on t.trx_id = af.trx_id
left outer join mig_adw.SHKDP_RPT_004B01_T ru
        on t.trx_id = ru.trx_id;
commit;

prompt 'Step SHKDP_RPT_004C02_T: [ process calculation ] ';
insert into mig_adw.SHKDP_RPT_004C02_T
(
         batch_id
        ,trx_id
        ,cust_num
        ,subr_num
        ,batch_create_date
        ,contact_start_date
        ,eff_start_date
        ,eff_end_date
        ,shk_tier1
        ,shk_tier2
        ,shk_tier3
        ,shk_tier4
        ,shk_tier5
        ,shk_sale_tag
        ,pr_last_mod_date
        ,pr_final_status
        ,pr_comm_by
        ,pr_call_pgm_id
        ,bf_ld_exp_date
        ,bf_rate_plan_cd
        ,bf_plan_tariff
        ,bf_subr_sw_on_date
        ,bf_nomination_flg
        ,bf_avg_net_rev
        ,bf_other_amt
        ,bf_other_lst
        ,bf_json_rmk
        ,af_channel_effort
        ,af_online_pos_inv_num
        ,af_rate_plan_cd
        ,af_plan_tariff
        ,af_plan_mkt_cd
        ,af_mkt_cd_lst
        ,af_map_key_id_lst
        ,af_addon_map_mkt_lst
        ,af_json_rmk
        ,af_net_price
        ,ru_rule_id
        ,ru_rule_net_price
        ,ru_plan_mix_nature
        ,ru_plan_mix_sub_nature
        ,cl_sale_type
        ,cl_reach
        ,cl_hit
        ,cl_except
        ,shk_tier
)select
         t.batch_id
        ,t.trx_id
        ,t.cust_num
        ,t.subr_num
        ,t.batch_create_date
        ,t.contact_start_date
        ,t.eff_start_date
        ,t.eff_end_date
        ,t.shk_tier1
        ,t.shk_tier2
        ,t.shk_tier3
        ,t.shk_tier4
        ,t.shk_tier5
        ,t.shk_sale_tag
        ,t.pr_last_mod_date
        ,t.pr_final_status
        ,t.pr_comm_by
        ,t.pr_call_pgm_id
        ,t.bf_ld_exp_date
        ,t.bf_rate_plan_cd
        ,t.bf_plan_tariff
        ,t.bf_subr_sw_on_date
        ,t.bf_nomination_flg
        ,t.bf_avg_net_rev
        ,t.bf_other_amt
        ,t.bf_other_lst
        ,t.bf_json_rmk
        ,t.af_channel_effort
        ,t.af_online_pos_inv_num
        ,t.af_rate_plan_cd
        ,t.af_plan_tariff
        ,t.af_plan_mkt_cd
        ,t.af_mkt_cd_lst
        ,t.af_map_key_id_lst
        ,t.af_addon_map_mkt_lst
        ,t.af_json_rmk
        ,t.af_net_price
        ,t.ru_rule_id
        ,t.ru_rule_net_price
        ,t.ru_plan_mix_nature
        ,t.ru_plan_mix_sub_nature
        ,case when t.ru_rule_id<> ' ' and t.bf_avg_net_rev > t.af_net_price then 'DOWN_SELL' 
              when t.ru_rule_id<> ' ' and t.bf_avg_net_rev = t.af_net_price then 'FLAT_SELL' 
              when t.ru_rule_id<> ' ' and t.bf_avg_net_rev < t.af_net_price then 'UP_SELL' 
                else ' '
         end cl_sale_type
        ,case when upper(t.pr_final_status) in (upper('Accept Offer'),upper('Consider'),upper('Wait Fulfill'),upper('Reject'),upper('Not Available')) then 'REACH' 
              else 'NON_REACH'
         end cl_reach
        ,case when upper(t.pr_final_status) in(upper('Accept Offer')) and t.af_channel_effort ='Outbound' then 'HIT:OUTBOUND' 
              when upper(t.pr_final_status) in(upper('Accept Offer')) and t.af_channel_effort ='Inbound' then 'HIT:INBOUND' 
                --- no pr ----
              when t.ru_rule_id<> ' ' then 'HIT:'||upper(t.af_channel_effort)
              else ' '
         end cl_hit
        ,case when t.ru_rule_id = ' ' and t.af_channel_effort <> ' ' then 'EXCEPT_RULE_UNMAP'
              else ' '
         end cl_except
        ,t.shk_tier
from mig_adw.SHKDP_RPT_004C01_T t;
commit;

-------------------------------------------------------
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'SHKDP_RPT_H');

insert into mig_adw.SHKDP_RPT_H
select * from mig_adw.SHKDP_RPT_004C02_T;
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

