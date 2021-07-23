/opt/etl/prd/etl/APP/RBD/Z_RBD_KPI_REPORT_DTL_UAT/bin> cat u_rbd_kpi_report_dtl0035.pl
######################################################
#   $Header: /CVSROOT/SmarTone-Vodafone/Code/ETL/APP/ADW/B_SUBR_LIFE_STYLE_HIST/bin/b_subr_life_style_hist0020.pl,v 1.1 2005/12/14 01:04:16 MichaelNg Exp $
#   Purpose:
#   
#
######################################################


##my $ETLVAR = $ENV{"AUTO_ETLVAR"};
my $ETLVAR = "/opt/etl/prd/etl/APP/RBD/O_U_RBD_KPI_REPORT_DTL_UAT/bin/master_dev.pl";
require $ETLVAR;


#We need to have variable input for the program to start
if ($#ARGV < 0){
    print("Syntax : perl <Script Name> <System Name>_<Job Name>_<TXDATE>.dir>\n");
    print("Example: perl d_cust_info001.pl adw_d_cust_info_20051010.dir\n");
    exit(1);
}

my $TARGET_DB = "$etlvar::MIGADWDB";
my $TARGET_TABLE = "RBD_KPI_REPORT_PREPA_VOU_DTL";
my $SOURCE_DB = "$etlvar::MIGDB";
my $SOURCE_TABLE = "U_RBD_KPI_REPORT_PREP_VOU_T001";
my $SCRIPT_TYPE = $etlvar::APPEND_SCRIPT;


#Call the function we want to run
open(STDERR, ">&STDOUT");

my $pre = etlvar::preProcess($ARGV[0]);
my $rc = etlvar::getTXDate($TARGET_TABLE);
my $ret = etlvar::runGenScript($TARGET_DB,$TARGET_TABLE,$SOURCE_DB,$SOURCE_TABLE,$SCRIPT_TYPE,$etlvar::TXDATE);
if ($ret == 0){
    $ret = etlvar::updateJobTXDate($TARGET_TABLE);
}
my $post = etlvar::postProcess($TARGET_TABLE);
 
exit($ret);
