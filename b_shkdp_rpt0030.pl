/opt/etl/prd/etl/APP/ADW/Y_SHKDP_RPT/bin> cat b_shkdp_rpt0030.pl
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

execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'SHKDP_RPT_003A01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'SHKDP_RPT_003B01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'SHKDP_RPT_003_T');

set define on;
define opt_date=to_date('$etlvar::F_D_MONTH[0]','YYYY-MM-DD');

--------------------------------------------------------------------------------------------------------
prompt 'Step SHKDP_RPT_003A_T: [ Check channel ] ';
----------------------------------------------channel af fup --------------------------------
insert into mig_adw.SHKDP_RPT_003A01_T
(    TRX_ID
    ,CUST_NUM
    ,SUBR_NUM
    ,KEY_CAT
    ,CHANNEL_EFFORT
    ,ONLINE_POS_INV_NUM
    ,ONLINE_RENT_VIA_RBD_FLG
    ,STORE_CD
    ,POS_INV_DATE
    ,MKT_CD
    ,NEW_RATE_PLAN_CD
    ,NEW_LD_CD
    ,FREE_DATA_ENTITLE
    ,EFF_DATE
    ,NEW_RATE_PLAN_CD_TARIFF
    ,CREATE_TS
    ,REFRESH_TS
)
select 
     t.trx_id
    ,t.cust_num
    ,t.subr_num
    ,max(u.TRX_TYPE)keep(dense_rank first order by u.inv_date desc) as key_cat
    ,max(u.CHANNEL_EFFORT)keep(dense_rank first order by u.inv_date desc) as CHANNEL_EFFORT
    ,max(u.ONLINE_INV)keep(dense_rank first order by u.inv_date desc) as ONLINE_POS_INV_NUM 
    ,max(u.ONLINE_RETENT_PORTAL_FLG)keep(dense_rank first order by u.inv_date desc) as ONLINE_RENT_VIA_RBD_FLG
    ,max(u.STORE_CD)keep(dense_rank first order by u.inv_date desc) as STORE_CD
    ,max(u.INV_DATE)keep(dense_rank first order by u.inv_date desc) as POS_INV_DATE
    ,max(u.MKT_CD)keep(dense_rank first order by u.inv_date desc) as MKT_CD
    ,max(u.NEW_RATE_PLAN_CD)keep(dense_rank first order by u.inv_date desc) as NEW_RATE_PLAN_CD
    ,max(u.NEW_LD_CD)keep(dense_rank first order by u.inv_date desc) as NEW_LD_CD
    ,max(u.FREE_DATA_ENTITLE)keep(dense_rank first order by u.inv_date desc)as FREE_DATA_ENTITLE
    ,max(u.EFF_DATE)keep(dense_rank first order by u.inv_date desc) as EFF_DATE
    ,max(r.BILL_RATE)keep(dense_rank first order by u.inv_date desc) as NEW_RATE_PLAN_TARIFF
    ,sysdate
    ,sysdate
from mig_adw.SHKDP_RPT_001_T t
--left outer join prd_biz_summ_vw.VW_PREPD_POSTPAID_SUBR_N1 n
--    on t.cust_num = n.cust_num and t.subr_num = n.subr_num
--left outer join PRD_BIZ_SUMM_VW.VW_PREPD_POSTPAID_CUST c
--    on t.cust_num = c.cust_num
    ,PRD_BIZ_SUMM_VW.VW_RBD_ALL_CHANNEL_ACT_RE_TX_U u
left outer join  prd_adw.bill_serv_ref r
        on u.new_rate_plan_cd = r.bill_serv_cd
 where
    t.cust_num = u.cust_num 
   and t.subr_num = u.subr_num  
   and u.TRX_TYPE in ('RETENTION', 'UPGRADE') 
   and u.INV_DATE>=t.contact_start_date
group by   
     t.trx_id
    ,t.cust_num
    ,t.subr_num;

-------------------------All case tracked by mkt code ----------------------------------------
prompt 'Step SHKDP_RPT_003B01_T: [ Check mkt ] ';
--MKT_CD:AF-5GLITE
--MKT_CD:AF-5G_PREM
--MKT_CD:AF-ASIA_DAY_PASS
--MKT_CD:AF-FREE_CALL_GUARD
--MKT_CD:AF-FREE_TALK_PACK
--MKT_CD:AF-FUP
--MKT_CD:AF-HS_COUPON
--MKT_CD:AF-NETFLIX
--MKT_CD:BF-FUP    
insert into mig_adw.SHKDP_RPT_003B01_T
(
     trx_id
    ,cust_num
    ,subr_num
    ,map_type
    ,src_type
    ,map_key_id
    ,mkt_cd
    ,create_ts
    ,refresh_ts
)
select 
     t.trx_id
    ,t.cust_num
    ,t.subr_num
    ,'OM_CHG_PLAN'map_type
    ,dt.dict_grp as src_type
    ,o.case_id as map_key_id
    ,o.mkt_cd mkt_cd
    ,sysdate create_ts
    ,sysdate refresh_ts 
from mig_adw.SHKDP_RPT_001_T t
    ,PRD_BIZ_SUMM_VW.VW_OM_CHG_PLAN o
    ,(
        select distinct dict_grp,dict_val 
        from mig_adw.SHKDP_RPT_DICT_REF 
        where dict_grp in (
         'MKT_CD:AF-5GLITE'
        ,'MKT_CD:AF-5G_PREM'
        ,'MKT_CD:AF-ASIA_DAY_PASS'
        ,'MKT_CD:AF-FREE_CALL_GUARD'
        ,'MKT_CD:AF-FREE_TALK_PACK'
        ,'MKT_CD:AF-FUP'
        ,'MKT_CD:AF-HS_COUPON'
        ,'MKT_CD:AF-NETFLIX'        
        )
     ) dt
where t.subr_num = o.subr_num
  and t.cust_num = o.cust_num  
  and o.create_date>=t.contact_start_date
  and o.mkt_cd = dt.dict_val;
commit;


prompt 'Step SHKDP_RPT_003_T: [ combine result table 1 trx_id 1 record ] ';
------left outer join for fullset
insert into mig_adw.SHKDP_RPT_003_T
(
    trx_id
    ,AF_CHANNEL_EFFORT
    ,AF_ONLINE_POS_INV_NUM
    ,AF_RATE_PLAN_CD
    ,AF_PLAN_TARIFF
    ,AF_PLAN_MKT_CD
    ,AF_LD_CD
    ,AF_MKT_CD_LST
    ,AF_MAP_KEY_ID_LST
    ,AF_JSON_RMK    
    ,AF_ADDON_MAP_MKT_LST
)
select 
     t.trx_id
    ,nvl(ch.channel_effort,' ')                         as AF_CHANNEL_EFFORT
    ,nvl(ch.online_pos_inv_num,' ')             as AF_ONLINE_POS_INV_NUM
    ,nvl(ch.new_rate_plan_cd,' ')               as AF_RATE_PLAN_CD
    ,nvl(ch.new_rate_plan_cd_tariff,0)          as AF_PLAN_TARIFF
    ,nvl(ch.mkt_cd,' ')                         as AF_PLAN_MKT_CD                
    ,nvl(ch.new_ld_cd,' ')                      as AF_LD_CD
    ,nvl(ad.mkt_cd_lst,' ')             as AF_MKT_CD_LST
    ,nvl(ad.map_key_id_lst,' ')         as AF_MAP_KEY_ID_LST
    ,nvl(ad.json_rmk,' ')               as AF_JSON_RMK    
    ,nvl(ad2.addon_map_mkt_lst,' ')     as AF_ADDON_MAP_MKT_LST
  from  mig_adw.SHKDP_RPT_001_T t
  left outer join mig_adw.SHKDP_RPT_003A01_T ch
    on t.trx_id =ch.trx_id
  left outer join (
          Select tt.trx_id
           ,listagg(tt.mkt_cd,',') within group(order by tt.mkt_cd) mkt_cd_lst
           ,listagg(tt.map_key_id,',') within group(order by tt.mkt_cd) map_key_id_lst
           ,listagg('"'||tt.src_type||'":"'
           ||case             
             when tt.src_type like 'MKT_CD%'
             then tt.mkt_cd||'='|| 0 ||';'||tt.map_key_id
             else ' '
              end||'"',',') within group(order by tt.mkt_cd) as json_rmk 
    from mig_adw.SHKDP_RPT_003B01_T tt
    group by tt.trx_id
  )  ad
  on t.trx_id = ad.trx_id
  left outer join (
        select tt.trx_id
                ,listagg(mkt_cd,',') within group (order by mkt_cd) addon_map_mkt_lst
        from (
                Select distinct trx_id ,mkt_cd                
                from mig_adw.SHKDP_RPT_003B01_T 
        )tt   
        group by tt.trx_id
  )ad2
  on t.trx_id = ad2.trx_id ;
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

