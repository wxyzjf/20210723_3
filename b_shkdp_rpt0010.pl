/opt/etl/prd/etl/APP/ADW/Y_SHKDP_RPT/bin> cat b_shkdp_rpt0010.pl
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

--execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'B_RETENT_UPG_COMM_001A_T');

execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL(p_table_schema=>'${etlvar::MIGDB}',p_table_name=>'SHKDP_RPT_001_T');

set define on;
define rpt_mth=to_date('$etlvar::F_D_MONTH[0]','YYYY-MM-DD');
define rpt_s_date=to_date('$etlvar::F_D_MONTH[0]','YYYY-MM-DD');
define rpt_e_date=add_months(to_date('$etlvar::F_D_MONTH[0]','YYYY-MM-DD'),1)-1;


--------------------------------------------------------------------------------------------------------
prompt 'Step SHKDP_RPT_001_T: [ Prepaing base monitoring profile ] ';

insert into mig_adw.SHKDP_RPT_001_T
(
    BATCH_ID,
    TRX_ID,
    FST_CUST_NUM,
    FST_SUBR_NUM,
    CUST_NUM,
    SUBR_NUM,
    CONTACT_START_DATE,
    EFF_START_DATE,
    EFF_END_DATE,
    CREATE_TS,
    REFRESH_TS,
    SHK_TIER1,
    SHK_TIER2,
    SHK_TIER3,
    SHK_TIER4,
    SHK_TIER5,
    BATCH_CREATE_DATE,
    SHK_SALE_TAG,
    SHK_TIER
)    
select
    t.BATCH_ID,
    t.TRX_ID,
    t.FST_CUST_NUM,
    t.FST_SUBR_NUM,
    t.CUST_NUM,
    t.SUBR_NUM,
    t.CONTACT_START_DATE,
    t.EFF_START_DATE,
    t.EFF_END_DATE,
    t.CREATE_TS,
    t.REFRESH_TS,
    t.SHK_TIER1,
    t.SHK_TIER2,
    t.SHK_TIER3,
    t.SHK_TIER4,
    t.SHK_TIER5,
    t.BATCH_CREATE_DATE,
    t.SHK_SALE_TAG,
    t.SHK_TIER2
from MIG_ADW.X_SHKDP_RPT t;




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

