/opt/etl/prd/etl/APP/RBD/Z_RBD_KPI_REPORT_DTL_UAT/bin> cat u_rbd_kpi_report_dtl0026.pl
######################################################
#   $Header: /CVSROOT/SmarTone-Vodafone/Code/ETL/APP/ADW/B_SUBR_LIFE_STYLE_HIST/bin/b_subr_life_style_hist0020.pl,v 1.1 2005/12/14 01:04:16 MichaelNg Exp $
#   Purpose:
#   
#
######################################################


##my $ETLVAR = $ENV{"AUTO_ETLVAR"};require $ETLVAR;
my $ETLVAR = "/opt/etl/prd/etl/APP/RBD/Z_RBD_KPI_REPORT_DTL_UAT/bin/master_dev.pl";


#We need to have variable input for the program to start
if ($#ARGV < 0){
    print("Syntax : perl <Script Name> <System Name>_<Job Name>_<TXDATE>.dir>\n");
    print("Example: perl d_cust_info001.pl adw_d_cust_info_20051010.dir\n");
    exit(1);
}

my $TARGET_DB = "$etlvar::MIGADWDB";
my $TARGET_TABLE = "RBD_KPI_REPORT_TRX_DTL";
my $SOURCE_DB = "$etlvar::MIGDB";
my $SOURCE_TABLE = "U_RBD_KPI_REPORT_TRX_TMP002";
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
/opt/etl/prd/etl/APP/RBD/Z_RBD_KPI_REPORT_DTL_UAT/bin> ls -l
total 764
-rw-r--r-- 1 adwbat dstage   1174 Nov 17  2020 master_dev.pl
-rw-r--r-- 1 adwbat dstage   4439 Nov 26 17:25 run.ksh
-rw-r--r-- 1 adwbat dstage 255530 Nov 17  2020 run.log
-rw-r--r-- 1 adwbat dstage  38249 Nov 26 18:31 _sqlcmd.sql
drwxr-xr-x 6 adwbat dstage   4096 Nov 17  2020 tmp
drwxr-xr-x 2 adwbat dstage    163 Nov 17  2020 tmp2
-rw-r--r-- 1 adwbat dstage  59414 Nov 17  2020 u_rbd_kpi_report_dtl0010.pl
-rw-r--r-- 1 adwbat dstage  76171 Nov 17  2020 u_rbd_kpi_report_dtl0020.pl
-rw-r--r-- 1 adwbat dstage  21714 Nov 17  2020 u_rbd_kpi_report_dtl0025.pl
-rw-r--r-- 1 adwbat dstage   1310 Nov 17  2020 u_rbd_kpi_report_dtl0026.pl
-rw-r--r-- 1 adwbat dstage  74381 Nov 17  2020 u_rbd_kpi_report_dtl0030.pl
-rw-r--r-- 1 adwbat dstage  70935 Nov 17  2020 u_rbd_kpi_report_dtl0030.pl.20191023
-rw-r--r-- 1 adwbat dstage   1282 Nov 17  2020 u_rbd_kpi_report_dtl0031.pl
-rw-r--r-- 1 adwbat dstage   1270 Nov 17  2020 u_rbd_kpi_report_dtl0032.pl
-rw-r--r-- 1 adwbat dstage   1272 Nov 17  2020 u_rbd_kpi_report_dtl0033.pl
-rw-r--r-- 1 adwbat dstage   1284 Nov 17  2020 u_rbd_kpi_report_dtl0034.pl
-rw-r--r-- 1 adwbat dstage   1284 Nov 17  2020 u_rbd_kpi_report_dtl0035.pl
-rw-r--r-- 1 adwbat dstage   1278 Nov 17  2020 u_rbd_kpi_report_dtl0036.pl
-rw-r--r-- 1 adwbat dstage   1279 Nov 17  2020 u_rbd_kpi_report_dtl0037.pl
-rw-r--r-- 1 adwbat dstage  17105 Nov 17  2020 u_rbd_kpi_report_dtl0040.pl
-rw-r--r-- 1 adwbat dstage  45828 Nov 23 10:42 u_rbd_kpi_report_dtl0050.pl
-rw-r--r-- 1 adwbat dstage  46199 Nov 17  2020 u_rbd_kpi_report_dtl0055.pl
