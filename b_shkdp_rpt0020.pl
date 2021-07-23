/opt/etl/prd/etl/APP/ADW/Y_SHKDP_RPT/bin> cat b_shkdp_rpt0020.pl
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


execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'SHKDP_RPT_002B01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'SHKDP_RPT_002C01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'SHKDP_RPT_002D01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'SHKDP_RPT_002_T');

set define on;
define opt_date=to_date('$etlvar::F_D_MONTH[0]','YYYY-MM-DD');


--------------------------------------------------------------------------------------------------------
prompt 'Step SHKDP_RPT_002B01_T: [ Get BF rate plan code ,net rev ] ';
 
---------------Tracking before case 
insert into mig_adw.SHKDP_RPT_002B01_T
(    trx_id
    ,cust_num
    ,subr_num
    ,trx_month
    ,ld_exp_date
    ,rate_plan_grp
    ,rate_plan_cd
    ,plan_tariff
    ,subr_sw_on_date
    ,nomination_flg
    ,avg_net_rev
    ,avg_net_accs_rev
    ,avg_net_thereafter_rev
    ,avg_net_idd_rev
    ,avg_net_outbd_roam_rev
    ,avg_net_vdrs_rev
    ,avg_other_rev
    ,avg_hs_amort_amt
    ,avg_mou
    ,avg_date_usg_gb
    ,create_ts
    ,refresh_ts
)select
    t.trx_id
    ,t.cust_num
    ,t.subr_num
    ,s.trx_month
    ,s.latest_ld_exp_date_sim_or_hso as ld_exp_date
    ,s.current_rate_plan_group as rate_plan_grp
    ,s.rate_plan_card_type as rate_plan_cd
    ,s.rate_plan_tariff as plan_tariff
    ,s.subr_sw_on_date as subr_sw_on_date
    ,s.nomination_flg as nomination_flg
    ,sum((s.lm_net_revenue+s.c_2lm_net_rev+s.c_3lm_net_rev)/3) as avg_net_rev
    ,sum((s.net_access_rev_lm+s.net_access_rev_2lm +net_access_rev_3lm)/3) as avg_net_accs_rev
    ,sum((net_thereafter_rev_lm+s.net_thereafter_rev_2lm+s.net_thereafter_rev_3lm)/3) as avg_net_thereafter_rev
    ,sum((s.net_idd_rev_lm+s.net_idd_rev_2lm+s.net_idd_rev_3lm)/3) avg_net_idd_rev
    ,sum((s.net_outbound_roam_rev_lm+s.net_outbound_roam_rev_2lm+s.net_outbound_roam_rev_3lm)/3) as avg_net_outbd_roam_rev
    ,sum((s.net_vrs_drs_rev_non_voice_lm+s.net_vrs_drs_rev_non_voice_2lm+s.net_vrs_drs_rev_non_voice_3lm)/3) as avg_net_vdrs_rev
    ,sum((s.other_revenue_lm+s.other_revenue_2lm+s.other_revenue_3lm)/3)  as avg_other_rev
    ,sum((s.amort_amt_lm+s.amort_amt_2lm+s.amort_amt_3lm)/3)as avg_hs_amort_amt
    ,sum((s.lm_mou+s.l2m_mou+s.l3m_mou)/3) as avg_mou
    ,sum((s.ggsn_lm_total_volume+s.ggsn_l2m_total_volume+s.ggsn_l3m_total_volume)/3/1024/1024) as avg_date_usg_gb
    ,sysdate create_ts
    ,sysdate refresh_ts
from MIG_ADW.SHKDP_RPT_001_T t
    ,PRD_BIZ_SUMM_VW.VW_SHK_POSTPAID_MASTER_SUMM s
where t.cust_num = s.cust_num
  and t.subr_num = s.subr_num
  and s.trx_month = add_months(trunc(t.contact_start_date,'MM'),-1)
group by 
    t.trx_id
    ,t.cust_num
    ,t.subr_num
    ,s.trx_month
    ,s.latest_ld_exp_date_sim_or_hso
    ,s.current_rate_plan_group
    ,s.rate_plan_card_type
    ,s.rate_plan_tariff
    ,s.subr_sw_on_date
    ,s.nomination_flg; 
commit;    

prompt 'Step SHKDP_RPT_002C01_T: [ Get PR module ] ';
------------ PR --------
insert into mig_adw.SHKDP_RPT_002C01_T
(   TRX_ID
    ,CUST_NUM
    ,SUBR_NUM
    ,LAST_MOD_DATE
    ,PR_FINAL_STATUS
    ,COMM_BY
    ,CALL_PGM_ID
    ,CREATE_TS
    ,REFRESH_TS
)
Select 
    t.TRX_ID
    ,t.CUST_NUM
    ,t.SUBR_NUM
    ,max(pr.Lst_Mod_Date)keep (dense_rank first order by pr.lst_mod_date desc) LAST_MOD_DATE
    ,max(pr.Pr_Status)keep (dense_rank first order by pr.lst_mod_date desc) PR_FINAL_STATUS
    ,max(pr.Comm_By)keep (dense_rank first order by pr.lst_mod_date desc) COMM_BY
    ,max(pr.Program_Id) keep (dense_rank first order by pr.lst_mod_date desc) as CALL_PGM_ID
    ,sysdate CREATE_TS
    ,sysdate REFRESH_TS
from MIG_ADW.SHKDP_RPT_001_T t
    ,PRD_BIZ_SUMM_VW.VW_PR_STATUS pr    
where t.cust_num = pr.cust_num
  and t.subr_num = pr.subr_num
  and pr.lst_mod_date >= t.contact_start_date
  and( pr.program_id in (
      select r.dict_val
      from mig_adw.shkdp_rpt_dict_ref r
      where r.dict_grp ='PRG_ID:PR-PRG_LST'
      and &opt_date between r.eff_start_date and r.eff_end_date)
-----Uat -----
        or pr.program_id like 'PRS%'
-----Uat logic ----
    )
group by
    t.TRX_ID
    ,t.CUST_NUM
    ,t.SUBR_NUM;
   -- ,pr.Program_Id;
commit;
----------------------------------------------Bill bf fup --------------------------------
prompt 'Step SHKDP_RPT_002D01_T [Get BF FUP from BILL] ';
insert into mig_adw.SHKDP_RPT_002D01_T
(   TRX_ID
    ,CUST_NUM
    ,SUBR_NUM
    ,SRC_TYPE
    ,BILL_CD
    ,BILL_CD_DESC
    ,BILL_CD_TARIFF
    ,BILL_CD_START_DATE
    ,BILL_CD_END_DATE
    ,MKT_CD
    ,LD_INV_NUM
    ,LD_START_DATE
    ,LD_EXP_DATE
    ,LD_ORIG_EXP_DATE
    ,CREATE_TS
    ,REFRESH_TS)
select 
     t.TRX_ID
    ,t.CUST_NUM
    ,t.SUBR_NUM
    ,'BILL_CD:BF-FUP' SRC_TYPE
    ,nvl(max(bs.BILL_SERV_CD) keep (dense_rank first order by bs.Bill_Start_Date desc),' ') 
    ,nvl(max(bs.BILL_CD_DESC) keep (dense_rank first order by bs.Bill_Start_Date desc),' ')
    ,nvl(max(bs.Bill_Rate) keep (dense_rank first order by bs.Bill_Start_Date desc),0) BILL_CD_TARIFF
    ,nvl(max(bs.Bill_Start_Date) keep (dense_rank first order by bs.Bill_Start_Date desc),date '2999-12-31') BILL_CD_START_DATE
    ,nvl(max(bs.Bill_End_Date) keep (dense_rank first order by bs.Bill_Start_Date desc),date '2999-12-31') BILL_CD_END_DATE
    ,' ' MKT_CD
    ,' ' LD_INV_NUM
    ,date '2999-12-31' LD_START_DATE
    ,date '2999-12-31' LD_EXP_DATE
    ,date '2999-12-31' LD_ORIG_EXP_DATE
    ,sysdate CREATE_TS
    ,sysdate REFRESH_TS
from mig_adw.SHKDP_RPT_001_T t
left outer join prd_biz_summ_vw.VW_PREPD_POSTPAID_SUBR_N1 n
    on t.cust_num = n.cust_num and t.subr_num = n.subr_num
left outer join PRD_BIZ_SUMM_VW.VW_PREPD_POSTPAID_CUST c
    on t.cust_num = c.cust_num
left outer join PRD_BIZ_SUMM_VW.VW_BILL_SERVS bs
    on t.cust_num = bs.cust_num and t.subr_num = bs.subr_num
left outer join prd_biz_summ_vw.VW_RATE_PLAN_REF gp
    on n.Rate_Plan_Cd = gp.Rate_Plan_Cd
left outer join prd_biz_summ_vw.vw_prepd_demo_card de
    on t.subr_num = de.Msisdn and n.sim  = de.sim
where n.D_Active_Subr_Flg='Y'
  and case  when (n.Prepost_Type<>'PO' and de.Msisdn<>' ') then 'N' 
            when nvl(gp.Rate_Plan_Grp, ' ') in ('Add-on Numbers', 'Non Revenue Mobile Plan', 'Non Revenue BB Line', 'DHL Special Project') then 'N' 
            when nvl(n.Cust_Type_Cd, ' ') in ('SMC', 'LPUC1', 'HKBU1', 'RWSU1', 'HKBU2', 'GCU1') then 'N' else 'Y' end ='Y' 
  and n.Prepost_Type='PO' 
  and SUBSTR(bs.Bill_Serv_Cd,1 ,3) in ('FUP') 
  and SUBSTR(bs.Bill_Serv_Cd,4 ,1) in ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9') 
  and SUBSTR(bs.Bill_Serv_Cd,5 ,1) in ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9') 
  and bs.Bill_Start_Date < t.contact_start_date
  and bs.Bill_Serv_cd is not null
group by 
     t.TRX_ID
    ,t.CUST_NUM
    ,t.SUBR_NUM
; 
commit;
----------------------------------------------Mkt bf fup --------------------------------
prompt 'Step SHKDP_RPT_002D01_T [Get BF FUP from MKT ] ';

insert into mig_adw.SHKDP_RPT_002D01_T
(   TRX_ID
    ,CUST_NUM
    ,SUBR_NUM
    ,SRC_TYPE
    ,BILL_CD
    ,BILL_CD_DESC
    ,BILL_CD_TARIFF
    ,BILL_CD_START_DATE
    ,BILL_CD_END_DATE
    ,MKT_CD
    ,LD_INV_NUM
    ,LD_START_DATE
    ,LD_EXP_DATE
    ,LD_ORIG_EXP_DATE
    ,CREATE_TS
    ,REFRESH_TS)
select 
     t.TRX_ID
    ,t.CUST_NUM
    ,t.SUBR_NUM
    ,'MKT_CD:BF-FUP' SRC_TYPE
    ,' ' BILL_SERV_CD 
    ,' ' BILL_CD_DESC
    ,0 Bill_Rate
    ,date '2999-12-31' Bill_Start_Date 
    ,date '2999-12-31' Bill_End_Date 
    ,nvl(max(pg.mkt_cd)keep (dense_rank first order by pg.D_Ld_Expired_Date desc), ' ')  MKT_CD
    ,nvl(max(pg.inv_num) keep (dense_rank first order by pg.D_Ld_Expired_Date desc), ' ')  LD_INV_NUM
    ,nvl(max(sl.ld_start_date)  keep (dense_rank first order by pg.D_Ld_Expired_Date desc),date '2999-12-31') LD_START_DATE
    ,nvl(max(pg.D_Ld_Expired_Date) keep (dense_rank first order by pg.D_Ld_Expired_Date desc),date '2999-12-31')   LD_EXP_DATE
    ,nvl(max(pg.d_org_ld_exp_date) keep (dense_rank first order by pg.D_Ld_Expired_Date desc),date '2999-12-31')  LD_ORIG_EXP_DATE
    ,sysdate CREATE_TS
    ,sysdate REFRESH_TS
from mig_adw.SHKDP_RPT_001_T t
left outer join prd_biz_summ_vw.VW_PREPD_POSTPAID_SUBR_N1 n
    on t.cust_num = n.cust_num and t.subr_num = n.subr_num
left outer join PRD_BIZ_SUMM_VW.VW_PREPD_POSTPAID_CUST c
    on t.cust_num = c.cust_num
left outer join PRD_BIZ_SUMM_VW.vw_subr_ld_post_paygo pg
    on t.cust_num = pg.cust_num  
    and t.subr_num = pg.subr_num
left outer join PRD_BIZ_SUMM_VW.VW_SUBR_LD_ST_DATE sl
    on pg.inv_num = sl.inv_num
where 
    pg.mkt_cd in (
        select dict_val from mig_adw.SHKDP_RPT_DICT_REF where dict_grp ='MKT_CD:BF-FUP'
    )
  and pg.inv_date < t.contact_start_date
  and pg.mkt_cd is not null
group by 
     t.TRX_ID
    ,t.CUST_NUM
    ,t.SUBR_NUM;
commit;     


prompt 'Step SHKDP_RPT_002_T [Prepare result table ] ';
----left outer join for fullset
insert into mig_adw.shkdp_rpt_002_t
(    
     trx_id
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
)
select     
    t.trx_id
    ,nvl(pr.last_mod_date,date '2999-12-31')    as pr_last_mod_date
    ,nvl(pr.pr_final_status,' ')                as pr_final_status
    ,nvl(pr.comm_by,' ')                        as pr_comm_by
    ,nvl(pr.call_pgm_id,' ')                    as pr_call_pgm_id
    ,nvl(bf.ld_exp_date ,date '2999-12-31')     as bf_ld_exp_date
    ,nvl(bf.rate_plan_cd,' ')                   as bf_rate_plan_cd
    ,nvl(bf.plan_tariff,0)                      as bf_plan_tariff
    ,nvl(bf.subr_sw_on_date,date '2999-12-31')  as bf_subr_sw_on_date
    ,nvl(bf.nomination_flg,' ')                 as bf_nomination_flg
    ,nvl(bf.avg_net_rev,0)                      as bf_avg_net_rev
    ,nvl(prb.bf_other_amt,0)                    as bf_other_amt
    ,nvl(prb.other_list ,' ')                   as bf_other_lst
    ,nvl(prb.json_rmk,' ')                      as bf_json_rmk
from    mig_adw.SHKDP_RPT_001_T t
left outer join mig_adw.SHKDP_RPT_002C01_T pr    
    on t.trx_id = pr.trx_id
left outer join mig_adw.SHKDP_RPT_002B01_T bf
    on t.trx_Id = bf.trx_id
left outer join (
    Select tt.trx_id
       ,sum(tt.bill_cd_tariff) bf_other_amt
       ,listagg('"'||tt.src_type||'":"'||case when tt.src_type like 'BILL_CD%' 
            then tt.bill_cd||';'||tt.bill_cd_tariff||';'||'NA'
            when tt.src_type like 'MKT_CD%'
            then tt.mkt_cd||';'|| 0||';'||ld_inv_num
            else ' '
            end||'"',',') within group(order by tt.src_type) as json_rmk
       ,listagg(case when tt.src_type like 'BILL_CD%' then tt.bill_cd
             when tt.src_type like 'MKT_CD%' then tt.mkt_cd
             else ' '
             end,',')within group(order by tt.src_type) as other_list
    from mig_adw.SHKDP_RPT_002D01_T tt
    group by tt.trx_id 
) prb
    on t.trx_id = prb.trx_id;
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

