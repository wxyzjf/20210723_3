/opt/etl/prd/etl/APP/ADW/Y_SHKDP_RPT/bin/el_test> cat b_shkdp_rpt0050.pl
######################################################
#   $Header: /CVSROOT/smartone/Code/ETL/APP/ADW/B_HS_STK_ALLOCATE/bin/b_hs_stk_allocate0010.pl,v 1.1 2005/12/14 01:03:59 MichaelNg Exp $
#   Purpose:
#
#
######################################################

my $ETLVAR = $ENV{"AUTO_ETLVAR"};require $ETLVAR;

my $MASTER_TABLE = "";#Please input the final target ADW table name here

my $ENV;

my $OUTPUT_FILE_PATH, $DEST_DIR,
   $OUTPUT_FILE_NAME_1;$OUTPUT_FILE_NAME_2;

my $FTP_FROM_HOST,$FTP_FROM_USERNAME,$FTP_FROM_PASSWORD;
my $FTP_TO_HOST,$FTP_TO_PORT,$FTP_TO_USERNAME,$FTP_TO_PASSWORD,$FTP_TO_DEST_PATH;

my $PROCESS_DATE;
my $PROCESS_DATE_LAST_YYYYMM;
my $CURR_MONTH_START;
my $CURR_MONTH_END;
my $LAST_MONTH_START;
my $LAST_MONTH_END;
my $NEXT_MONTH_START;

#########################################################################################################
#########################################################################################################
#########################################################################################################

sub initParam{

    $ENV = $ENV{"ETL_ENV"};

    use Date::Manip;

    #my $PROCESS_DATE = &UnixDate("${etlvar::TXDATE}", "%Y%m%d");

    #my $PROCESS_DATE_YYYYMM = &UnixDate("${etlvar::TXDATE}", "%Y%m");
     my $PROCESS_DATE_YYYYMM = "202012";


    # ------------------------------------------------------------------#
    #  Please define the parameters for this job below.                 #
    # ------------------------------------------------------------------#

    #$OUTPUT_FILE_PATH = ${etlvar::ETL_OUTPUT_DIR}."/".${etlvar::ETLSYS}."/".${etlvar::ETLJOBNAME};
    $OUTPUT_FILE_PATH = "/opt/etl/prd/etl/APP/ADW/Y_SHKDP_RPT/bin/el_test";
    
        if (! -d $OUTPUT_FILE_PATH) {
            system("mkdir ${OUTPUT_FILE_PATH}");
        }

#   system("rm -f ${OUTPUT_FILE_PATH}/*.txt");

    $OUTPUT_FILE_NAME_1 = "SHKDP_RPT_5_rpt1_".$PROCESS_DATE_YYYYMM.".csv";
    $OUTPUT_COMPLETE_1 = "SHKDP_RPT_5_rpt1_".$PROCESS_DATE_YYYYMM.".csv.complete";

    $OUTPUT_FILE_NAME_2 = "SHKDP_RPT_5_rpt2_".$PROCESS_DATE_YYYYMM.".csv";
    $OUTPUT_COMPLETE_2 = "SHKDP_RPT_5_rpt2_".$PROCESS_DATE_YYYYMM.".csv.complete";


    if ($ENV eq "DEV")
    {
        ##  DEVELOPMENT  ##

        # PUT action
        $FTP_TO_HOST = "";                                         # Please define
        $FTP_TO_PORT = "";                                         # Please define
        $FTP_TO_USERNAME = "";                                     # Please define
        $FTP_TO_PASSWORD = "";                                     # Please define
        $FTP_TO_DEST_PATH = "";                                    # Please define

        # GET action   (ONLY  FOR  DEVELOPMENT)
        $FTP_FROM_HOST = "${etlvar::DSSVR}";
        $FTP_FROM_USERNAME = "${etlvar::DSUSR}";
        $FTP_FROM_PASSWORD = "${etlvar::DSPWD}";
    }
    else
    {
        ##  PRODUCTION  ##

        # PUT action
        $FTP_TO_HOST = "";                                         # Please define
        $FTP_TO_PORT = "";                                         # Please define
        $FTP_TO_USERNAME = "";                                     # Please define
        $FTP_TO_PASSWORD = "";                                     # Please define
        $FTP_TO_DEST_PATH = "";                                    # Please define
    }
}

#########################################################################################################
#########################################################################################################
#########################################################################################################

sub runSQLPLUS_EXPORT{

  ##my $SQLCMD_FILE="${etlvar::AUTO_GEN_TEMP_PATH}u_bm_ld_hs_subsidy0020_sqlcmd.sql";
  my $SQLCMD_FILE="/opt/etl/prd/etl/APP/ADW/Y_SHKDP_RPT/bin/el_test/b_shkdp_rpt0050_sqlcmd.sql";
  open SQLCMD, ">" . $SQLCMD_FILE || die "Cannot open file" ;

  print SQLCMD<<ENDOFINPUT;

        ${etlvar::LOGON_TD}
        ${etlvar::SET_MAXERR}
        ${etlvar::SET_ERRLVL_1}
        ${etlvar::SET_ERRLVL_2}
--Please type your SQL statement here

---------------------------------------------------------------------------------------------------

execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL2(P_schema_tbl_name=>'MIG_ADW.Y_SHKDP_RPT_005A01_T');
execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL2(P_schema_tbl_name=>'MIG_ADW.Y_SHKDP_RPT_005B01_T');



--execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL2(P_schema_tbl_name=>'${etlvar::TMPDB}.Y_SHKDP_RPT_005A01_T');
--execute ${etlvar::UTLDB}.ETL_UTILITY.TRUNCATE_TBL2(P_schema_tbl_name=>'${etlvar::TMPDB}.Y_SHKDP_RPT_005B01_T');


---------------------------------------------------------------------------------------------------
-------------------------25 rows  26 col-------------------------
--6 Size
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,6 as row_num
                ,3 + row_number() over(order by shk_tier) as col_num
                ,count(*) as disp_val
                ,'Size' as calc_row_key
                ,shk_tier as calc_col_key
                from mig_adw.SHKDP_RPT_H
              group by shk_tier;
   --26

              
--7 Size (5G potential customer)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,7 as row_num
                ,3 + row_number() over(order by shk_tier) as col_num
                ,sum(decode(shk_sale_tag,'5G_POTENTIAL',1,0)) as disp_val
                ,'Size (5G potential customer)' as calc_row_key
                ,shk_tier as calc_col_key
                from mig_adw.SHKDP_RPT_H
              group by shk_tier;
   --26   

              
              
              
--8 Reached
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,8 as row_num
                ,3 + row_number() over(order by shk_tier) as col_num
                ,sum(decode(CL_REACH,'REACH',1,0)) as disp_val
                ,'Reached' as calc_row_key
                ,shk_tier as calc_col_key
                from mig_adw.SHKDP_RPT_H
              group by shk_tier;
   --26  


--10 Hit
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,10 as row_num
                ,3 + row_number() over(order by shk_tier) as col_num
                ,sum(decode(substr(CL_HIT,1,3),'HIT',1,0)) as disp_val
                ,'Hit' as calc_row_key
                ,shk_tier as calc_col_key
                from mig_adw.SHKDP_RPT_H
              group by shk_tier;
   --26  
              
              

--12 Hit (from 5G potential customer)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,12 as row_num
                ,3 + row_number() over(order by shk_tier) as col_num
                ,sum(case when substr(CL_HIT,1,3)='HIT' and shk_sale_tag='5G_POTENTIAL' then 1 else 0 end) as disp_val
                ,'Hit (from 5G potential customer)' as calc_row_key
                ,shk_tier as calc_col_key
                from mig_adw.SHKDP_RPT_H
              group by shk_tier;
   --26 



--20 Before
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,20 as row_num
                ,3 + row_number() over(order by shk_tier) as col_num
--                ,case when count(shk_tier) = 0 then 0 else sum(bf_avg_net_rev)/count(shk_tier) end as disp_val
                ,case when sum(decode(substr(CL_HIT,1,3),'HIT',1,0)) = 0 
                      then 0 
                 else sum(decode(substr(CL_HIT,1,3),'HIT',bf_avg_net_rev,0))
                      /sum(decode(substr(CL_HIT,1,3),'HIT',1,0)) 
                 end as disp_val
                ,'Before' as calc_row_key
                ,shk_tier as calc_col_key
                from mig_adw.SHKDP_RPT_H
              group by shk_tier;
   --26  
              
--21 After (accept order)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,21 as row_num
                ,3 + row_number() over(order by shk_tier) as col_num
--                ,case when count(shk_tier) = 0 then 0 else sum(af_net_price)/count(shk_tier) end as disp_val
                ,case when sum(decode(substr(CL_HIT,1,3),'HIT',1,0)) = 0 
                      then 0 
                 else sum(decode(substr(CL_HIT,1,3),'HIT',af_net_price,0))
                      /sum(decode(substr(CL_HIT,1,3),'HIT',1,0)) 
                 end as disp_val
                ,'After (accept order)' as calc_row_key
                ,shk_tier as calc_col_key
                from mig_adw.SHKDP_RPT_H
              group by shk_tier;
   --26  
              


--28 Upsell(count)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,28 as row_num
                ,3 + row_number() over(order by shk_tier) as col_num
                ,sum(decode(cl_sale_type,'UP_SELL',1,0)) as disp_val
                ,'Upsell(count)' as calc_row_key
                ,shk_tier as calc_col_key
                from mig_adw.SHKDP_RPT_H
              group by shk_tier;
   --26  

--29 Flatsell(count)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,29 as row_num
                ,3 + row_number() over(order by shk_tier) as col_num
                ,sum(decode(cl_sale_type,'FLAT_SELL',1,0)) as disp_val
                ,'Flatsell(count)' as calc_row_key
                ,shk_tier as calc_col_key
                from mig_adw.SHKDP_RPT_H
              group by shk_tier;
   --26  

--30 Downsell(count)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,30 as row_num
                ,3 + row_number() over(order by shk_tier) as col_num
                ,sum(decode(cl_sale_type,'DOWN_SELL',1,0)) as disp_val
                ,'Downsell(count)' as calc_row_key
                ,shk_tier as calc_col_key
                from mig_adw.SHKDP_RPT_H
              group by shk_tier;
   --26  

--31 Total(count)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,31 as row_num
                ,3 + row_number() over(order by shk_tier) as col_num
                ,sum(decode(substr(cl_sale_type,-4),'SELL',1,0)) as disp_val
                ,'Total(count)' as calc_row_key
                ,shk_tier as calc_col_key
                from mig_adw.SHKDP_RPT_H
              group by shk_tier;
   --26

--33 Upsell magnitude
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,33 as row_num
                ,3 + row_number() over(order by shk_tier) as col_num
                ,to_char(round(
                case when sum(decode(substr(cl_sale_type,-4),'SELL',af_plan_tariff,0)) = 0 then 0 else 
                sum(decode(cl_sale_type,'UP_SELL',bf_plan_tariff,0)) / sum(decode(substr(cl_sale_type,-4),'SELL',af_plan_tariff,0))
                end * 100,2),'fm9990.00')||'%' as disp_val
                ,'Upsell magnitude' as calc_row_key
                ,shk_tier as calc_col_key
                from mig_adw.SHKDP_RPT_H 
              group by shk_tier;
   --26  
              



--34 Downsell magnitude
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,34 as row_num
                ,3 + row_number() over(order by shk_tier) as col_num
                ,to_char(round(
                case when sum(decode(substr(cl_sale_type,-4),'SELL',af_plan_tariff,0)) = 0 then 0 else 
                sum(decode(cl_sale_type,'DOWN_SELL',bf_plan_tariff,0)) / sum(decode(substr(cl_sale_type,-4),'SELL',af_plan_tariff,0))
                end * 100,2),'fm9990.00')||'%' as disp_val
                ,'Downsell magnitude' as calc_row_key
                ,shk_tier as calc_col_key
                from mig_adw.SHKDP_RPT_H 
              group by shk_tier;
   --26  
              

--35 Overall magnitude
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,35 as row_num
                ,3 + row_number() over(order by shk_tier) as col_num
                ,to_char(round(
                case when sum(decode(substr(cl_sale_type,-4),'SELL',af_plan_tariff,0)) = 0 then 0 else 
                sum(decode(substr(cl_sale_type,-4),'SELL',bf_plan_tariff,0)) / sum(decode(substr(cl_sale_type,-4),'SELL',af_plan_tariff,0))
                end * 100,2),'fm9990.00')||'%' as disp_val
                ,'Overall magnitude' as calc_row_key
                ,shk_tier as calc_col_key
                from mig_adw.SHKDP_RPT_H 
              group by shk_tier;
   --26  



---------------------------------------------------------------------------------------------------------------

--23 Upsell(%)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,23 as row_num
                ,3 + row_number() over(order by CALC_COL_KEY) as col_num
                ,case when sum(decode(CALC_ROW_KEY,'Total(count)',to_number(DISP_VAL),0)) = 0 
                      then 0 
                 else sum(decode(CALC_ROW_KEY,'Upsell(count)',to_number(DISP_VAL),0)) 
                      / sum(decode(CALC_ROW_KEY,'Total(count)',to_number(DISP_VAL),0)) 
                 end as disp_val
                ,'Upsell(%)' as calc_row_key
                ,CALC_COL_KEY as calc_col_key
                from MIG_ADW.Y_SHKDP_RPT_005A01_T 
              group by CALC_COL_KEY;
   --26  
              
--24 Flatsell(%)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,24 as row_num
                ,3 + row_number() over(order by CALC_COL_KEY) as col_num
                ,case when sum(decode(CALC_ROW_KEY,'Total(count)',to_number(DISP_VAL),0)) = 0 
                      then 0 
                 else sum(decode(CALC_ROW_KEY,'Flatsell(count)',to_number(DISP_VAL),0)) 
                      / sum(decode(CALC_ROW_KEY,'Total(count)',to_number(DISP_VAL),0)) 
                 end as disp_val
                ,'Flatsell(%)' as calc_row_key
                ,CALC_COL_KEY as calc_col_key
                from MIG_ADW.Y_SHKDP_RPT_005A01_T 
              group by CALC_COL_KEY;
   --26  
              
--25 Downsell(%)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,25 as row_num
                ,3 + row_number() over(order by CALC_COL_KEY) as col_num
                ,case when sum(decode(CALC_ROW_KEY,'Total(count)',to_number(DISP_VAL),0)) = 0 
                      then 0 
                 else sum(decode(CALC_ROW_KEY,'Downsell(count)',to_number(DISP_VAL),0)) 
                      / sum(decode(CALC_ROW_KEY,'Total(count)',to_number(DISP_VAL),0)) 
                 end as disp_val
                ,'Downsell(%)' as calc_row_key
                ,CALC_COL_KEY as calc_col_key
                from MIG_ADW.Y_SHKDP_RPT_005A01_T 
              group by CALC_COL_KEY;
   --26  
              
              


--5 PTD(Plan)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,5 as row_num
                ,3 + row_number() over(order by CALC_COL_KEY) as col_num
                ,case when sum(decode(CALC_ROW_KEY,'Size',to_number(DISP_VAL),0)) = 0 
                      then 0 
                 else sum(decode(CALC_ROW_KEY,'Hit',to_number(DISP_VAL),0)) 
                      / sum(decode(CALC_ROW_KEY,'Size',to_number(DISP_VAL),0)) 
                 end as disp_val
                ,'PTD(Plan)' as calc_row_key
                ,CALC_COL_KEY as calc_col_key
                from MIG_ADW.Y_SHKDP_RPT_005A01_T 
              group by CALC_COL_KEY;
   --26  

--9 Reach %
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,9 as row_num
                ,3 + row_number() over(order by CALC_COL_KEY) as col_num
                ,case when sum(decode(CALC_ROW_KEY,'Size',to_number(DISP_VAL),0)) = 0 
                      then 0
                 else sum(decode(CALC_ROW_KEY,'Reached',to_number(DISP_VAL),0)) 
                      / sum(decode(CALC_ROW_KEY,'Size',to_number(DISP_VAL),0)) 
                 end as disp_val
                ,'Reach %' as calc_row_key
                ,CALC_COL_KEY as calc_col_key
                from MIG_ADW.Y_SHKDP_RPT_005A01_T 
              group by CALC_COL_KEY;
   --26 

--11 Hit/Reach%
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,11 as row_num
                ,3 + row_number() over(order by CALC_COL_KEY) as col_num
                ,case when sum(decode(CALC_ROW_KEY,'Reached',to_number(DISP_VAL),0)) = 0 
                      then 0 
                 else sum(decode(CALC_ROW_KEY,'Hit',to_number(DISP_VAL),0)) 
                      / sum(decode(CALC_ROW_KEY,'Reached',to_number(DISP_VAL),0)) 
                 end as disp_val
                ,'Hit/Reach%' as calc_row_key
                ,CALC_COL_KEY as calc_col_key
                from MIG_ADW.Y_SHKDP_RPT_005A01_T 
              group by CALC_COL_KEY;
   --26 

--17 Net service revenue1 (before)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,17 as row_num
                ,3 + row_number() over(order by CALC_COL_KEY) as col_num
                ,sum(decode(CALC_ROW_KEY,'Size',to_number(DISP_VAL),0)) * sum(decode(CALC_ROW_KEY,'Before',to_number(DISP_VAL),0)) as disp_val
                ,'Net service revenue1 (before)' as calc_row_key
                ,CALC_COL_KEY as calc_col_key
                from MIG_ADW.Y_SHKDP_RPT_005A01_T 
              group by CALC_COL_KEY;
   --26  

--18 Net service revenue (after)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,18 as row_num
                ,3 + row_number() over(order by CALC_COL_KEY) as col_num
                ,sum(decode(CALC_ROW_KEY,'Hit',to_number(DISP_VAL),0)) * sum(decode(CALC_ROW_KEY,'After (accept order)',to_number(DISP_VAL),0)) as disp_val
                ,'Net service revenue (after)' as calc_row_key
                ,CALC_COL_KEY as calc_col_key
                from MIG_ADW.Y_SHKDP_RPT_005A01_T 
              group by CALC_COL_KEY;
   --26  



--4 Gap(Plan)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,4 as row_num
                ,3 + row_number() over(order by CALC_COL_KEY) as col_num
                ,sum(decode(CALC_ROW_KEY,'PTD(Plan)',to_number(DISP_VAL),0)) - 0.5 as disp_val
                ,'Gap(Plan)' as calc_row_key
                ,CALC_COL_KEY as calc_col_key
                from MIG_ADW.Y_SHKDP_RPT_005A01_T 
              group by CALC_COL_KEY;
   --26    
              

--16 PTD(Revenue)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,16 as row_num
                ,3 + row_number() over(order by CALC_COL_KEY) as col_num
                ,case when sum(decode(CALC_ROW_KEY,'Net service revenue (after)',to_number(DISP_VAL),0)) = 0 
                      then 0 
                 else sum(decode(CALC_ROW_KEY,'Net service revenue1 (before)',to_number(DISP_VAL),0)) 
                      / sum(decode(CALC_ROW_KEY,'Net service revenue (after)',to_number(DISP_VAL),0)) 
                 end as disp_val
                ,'PTD(Revenue)' as calc_row_key
                ,CALC_COL_KEY as calc_col_key
                from MIG_ADW.Y_SHKDP_RPT_005A01_T 
              group by CALC_COL_KEY;
   --26  


--15 Gap(Revenue)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,15 as row_num
                ,3 + row_number() over(order by CALC_COL_KEY) as col_num
                ,sum(decode(CALC_ROW_KEY,'PTD(Revenue)',to_number(DISP_VAL),0)) - 0.5 as disp_val
                ,'Gap(Revenue)' as calc_row_key
                ,CALC_COL_KEY as calc_col_key
                from MIG_ADW.Y_SHKDP_RPT_005A01_T 
              group by CALC_COL_KEY;
   --26  
-------------------------25 rows  'Total' col-------------------------
--6 Size
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,6 as row_num
                ,3 as col_num
                ,count(*) as disp_val 
                ,'Size' as calc_row_key
                ,'Total' as calc_col_key
                from mig_adw.SHKDP_RPT_H;
                  

              
--7 Size (5G potential customer)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,7 as row_num
                ,3 as col_num
                ,sum(decode(shk_sale_tag,'5G_POTENTIAL',1,0)) as disp_val
                ,'Size (5G potential customer)' as calc_row_key
                ,'Total' as calc_col_key
                from mig_adw.SHKDP_RPT_H;
                    

              
              
              
--8 Reached
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,8 as row_num
                ,3 as col_num
                ,sum(decode(CL_REACH,'REACH',1,0)) as disp_val
                ,'Reached' as calc_row_key
                ,'Total' as calc_col_key
                from mig_adw.SHKDP_RPT_H;
                   


--10 Hit
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,10 as row_num
                ,3 as col_num
                ,sum(decode(substr(CL_HIT,1,3),'HIT',1,0)) as disp_val
                ,'Hit' as calc_row_key
                ,'Total' as calc_col_key
                from mig_adw.SHKDP_RPT_H;
                   
              
              

--12 Hit (from 5G potential customer)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,12 as row_num
                ,3 as col_num
                ,sum(case when substr(CL_HIT,1,3)='HIT' and shk_sale_tag='5G_POTENTIAL' then 1 else 0 end) as disp_val
                ,'Hit (from 5G potential customer)' as calc_row_key
                ,'Total' as calc_col_key
                from mig_adw.SHKDP_RPT_H;
                  



--20 Before
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,20 as row_num
                ,3 as col_num
--                ,case when count(shk_tier) = 0 then 0 else sum(bf_avg_net_rev)/count(shk_tier) end as disp_val
                ,case when sum(decode(substr(CL_HIT,1,3),'HIT',1,0)) = 0 
                      then 0 
                 else sum(decode(substr(CL_HIT,1,3),'HIT',bf_avg_net_rev,0))
                      /sum(decode(substr(CL_HIT,1,3),'HIT',1,0)) 
                 end as disp_val
                ,'Before' as calc_row_key
                ,'Total' as calc_col_key
                from mig_adw.SHKDP_RPT_H;
                   
--21 After (accept order)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,21 as row_num
                ,3 as col_num
--                ,case when count(shk_tier) = 0 then 0 else sum(af_net_price)/count(shk_tier) end as disp_val
                ,case when sum(decode(substr(CL_HIT,1,3),'HIT',1,0)) = 0 
                      then 0 
                 else sum(decode(substr(CL_HIT,1,3),'HIT',af_net_price,0))
                      /sum(decode(substr(CL_HIT,1,3),'HIT',1,0)) 
                 end as disp_val
                ,'After (accept order)' as calc_row_key
                ,'Total' as calc_col_key
                from mig_adw.SHKDP_RPT_H;
                   
              


--28 Upsell(count)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,28 as row_num
                ,3 as col_num
                ,sum(decode(cl_sale_type,'UP_SELL',1,0)) as disp_val
                ,'Upsell(count)' as calc_row_key
                ,'Total' as calc_col_key
                from mig_adw.SHKDP_RPT_H;
                   

--29 Flatsell(count)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,29 as row_num
                ,3 as col_num
                ,sum(decode(cl_sale_type,'FLAT_SELL',1,0)) as disp_val
                ,'Flatsell(count)' as calc_row_key
                ,'Total' as calc_col_key
                from mig_adw.SHKDP_RPT_H;
                   

--30 Downsell(count)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,30 as row_num
                ,3 as col_num
                ,sum(decode(cl_sale_type,'DOWN_SELL',1,0)) as disp_val
                ,'Downsell(count)' as calc_row_key
                ,'Total' as calc_col_key
                from mig_adw.SHKDP_RPT_H;
                   

--31 Total(count)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,31 as row_num
                ,3 as col_num
                ,sum(decode(substr(cl_sale_type,-4),'SELL',1,0)) as disp_val
                ,'Total(count)' as calc_row_key
                ,'Total' as calc_col_key
                from mig_adw.SHKDP_RPT_H;
                 

--33 Upsell magnitude
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,33 as row_num
                ,3 as col_num
                ,to_char(round(
                case when sum(decode(substr(cl_sale_type,-4),'SELL',af_plan_tariff,0)) = 0 then 0 else 
                sum(decode(cl_sale_type,'UP_SELL',bf_plan_tariff,0)) / sum(decode(substr(cl_sale_type,-4),'SELL',af_plan_tariff,0))
                end * 100,2),'fm9990.00')||'%' as disp_val
                ,'Upsell magnitude' as calc_row_key
                ,'Total' as calc_col_key
                from mig_adw.SHKDP_RPT_H;
                   
              



--34 Downsell magnitude
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,34 as row_num
                ,3 as col_num
                ,to_char(round(
                case when sum(decode(substr(cl_sale_type,-4),'SELL',af_plan_tariff,0)) = 0 then 0 else 
                sum(decode(cl_sale_type,'DOWN_SELL',bf_plan_tariff,0)) / sum(decode(substr(cl_sale_type,-4),'SELL',af_plan_tariff,0))
                end * 100,2),'fm9990.00')||'%' as disp_val
                ,'Downsell magnitude' as calc_row_key
                ,'Total' as calc_col_key
                from mig_adw.SHKDP_RPT_H;
                   
              

--35 Overall magnitude
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,35 as row_num
                ,3 as col_num
                ,to_char(round(
                case when sum(decode(substr(cl_sale_type,-4),'SELL',af_plan_tariff,0)) = 0 then 0 else 
                sum(decode(substr(cl_sale_type,-4),'SELL',bf_plan_tariff,0)) / sum(decode(substr(cl_sale_type,-4),'SELL',af_plan_tariff,0))
                end * 100,2),'fm9990.00')||'%' as disp_val
                ,'Overall magnitude' as calc_row_key
                ,'Total' as calc_col_key
                from mig_adw.SHKDP_RPT_H;
                   



---------------------------------------------------------------------------------------------------------------

--23 Upsell(%)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,23 as row_num
                ,3 as col_num
                ,case when sum(decode(CALC_ROW_KEY,'Total(count)',to_number(DISP_VAL),0)) = 0 
                      then 0 
                 else sum(decode(CALC_ROW_KEY,'Upsell(count)',to_number(DISP_VAL),0)) 
                      / sum(decode(CALC_ROW_KEY,'Total(count)',to_number(DISP_VAL),0)) 
                 end as disp_val
                ,'Upsell(%)' as calc_row_key
                ,'Total' as calc_col_key
                from MIG_ADW.Y_SHKDP_RPT_005A01_T;
                   
              
              
--24 Flatsell(%)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,24 as row_num
                ,3 as col_num
                ,case when sum(decode(CALC_ROW_KEY,'Total(count)',to_number(DISP_VAL),0)) = 0 
                      then 0 
                 else sum(decode(CALC_ROW_KEY,'Flatsell(count)',to_number(DISP_VAL),0)) 
                      / sum(decode(CALC_ROW_KEY,'Total(count)',to_number(DISP_VAL),0)) 
                 end as disp_val
                ,'Flatsell(%)' as calc_row_key
                ,'Total' as calc_col_key
                from MIG_ADW.Y_SHKDP_RPT_005A01_T;
                   
              
--25 Downsell(%)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,25 as row_num
                ,3 as col_num
                ,case when sum(decode(CALC_ROW_KEY,'Total(count)',to_number(DISP_VAL),0)) = 0 
                      then 0 
                 else sum(decode(CALC_ROW_KEY,'Downsell(count)',to_number(DISP_VAL),0)) 
                      / sum(decode(CALC_ROW_KEY,'Total(count)',to_number(DISP_VAL),0)) 
                 end as disp_val
                ,'Downsell(%)' as calc_row_key
                ,'Total' as calc_col_key
                from MIG_ADW.Y_SHKDP_RPT_005A01_T;
                   
              
              


--5 PTD(Plan)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,5 as row_num
                ,3 as col_num
                ,case when sum(decode(CALC_ROW_KEY,'Size',to_number(DISP_VAL),0)) = 0 
                      then 0 
                 else sum(decode(CALC_ROW_KEY,'Hit',to_number(DISP_VAL),0)) 
                      / sum(decode(CALC_ROW_KEY,'Size',to_number(DISP_VAL),0)) 
                 end as disp_val
                ,'PTD(Plan)' as calc_row_key
                ,'Total' as calc_col_key
                from MIG_ADW.Y_SHKDP_RPT_005A01_T;
                   

--9 Reach %
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,9 as row_num
                ,3 as col_num
                ,case when sum(decode(CALC_ROW_KEY,'Size',to_number(DISP_VAL),0)) = 0 
                      then 0
                 else sum(decode(CALC_ROW_KEY,'Reached',to_number(DISP_VAL),0)) 
                      / sum(decode(CALC_ROW_KEY,'Size',to_number(DISP_VAL),0)) 
                 end as disp_val
                ,'Reach %' as calc_row_key
                ,'Total' as calc_col_key
                from MIG_ADW.Y_SHKDP_RPT_005A01_T;
                  

--11 Hit/Reach%
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,11 as row_num
                ,3 as col_num
                ,case when sum(decode(CALC_ROW_KEY,'Reached',to_number(DISP_VAL),0)) = 0 
                      then 0 
                 else sum(decode(CALC_ROW_KEY,'Hit',to_number(DISP_VAL),0)) 
                      / sum(decode(CALC_ROW_KEY,'Reached',to_number(DISP_VAL),0)) 
                 end as disp_val
                ,'Hit/Reach%' as calc_row_key
                ,'Total' as calc_col_key
                from MIG_ADW.Y_SHKDP_RPT_005A01_T;
                  

--17 Net service revenue1 (before)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,17 as row_num
                ,3 as col_num
                ,sum(decode(CALC_ROW_KEY,'Size',to_number(DISP_VAL),0)) * sum(decode(CALC_ROW_KEY,'Before',to_number(DISP_VAL),0)) as disp_val
                ,'Net service revenue1 (before)' as calc_row_key
                ,'Total' as calc_col_key
                from MIG_ADW.Y_SHKDP_RPT_005A01_T;
                   

--18 Net service revenue (after)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,18 as row_num
                ,3 as col_num
                ,sum(decode(CALC_ROW_KEY,'Hit',to_number(DISP_VAL),0)) * sum(decode(CALC_ROW_KEY,'After (accept order)',to_number(DISP_VAL),0)) as disp_val
                ,'Net service revenue (after)' as calc_row_key
                ,'Total' as calc_col_key
                from MIG_ADW.Y_SHKDP_RPT_005A01_T;
                   



                   

--4 Gap(Plan)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,4 as row_num
                ,3 as col_num
                ,sum(decode(CALC_ROW_KEY,'PTD(Plan)',to_number(DISP_VAL),0)) - 0.5 as disp_val
                ,'Gap(Plan)' as calc_row_key
                ,'Total' as calc_col_key
                from MIG_ADW.Y_SHKDP_RPT_005A01_T where calc_col_key = 'Total';
                     
              

--16 PTD(Revenue)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,16 as row_num
                ,3 as col_num
                ,case when sum(decode(CALC_ROW_KEY,'Net service revenue (after)',to_number(DISP_VAL),0)) = 0 
                      then 0 
                 else sum(decode(CALC_ROW_KEY,'Net service revenue1 (before)',to_number(DISP_VAL),0)) 
                      / sum(decode(CALC_ROW_KEY,'Net service revenue (after)',to_number(DISP_VAL),0)) 
                 end as disp_val
                ,'PTD(Revenue)' as calc_row_key
                ,'Total' as calc_col_key
                from MIG_ADW.Y_SHKDP_RPT_005A01_T;
                   


--15 Gap(Revenue)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,15 as row_num
                ,3 as col_num
                ,sum(decode(CALC_ROW_KEY,'PTD(Revenue)',to_number(DISP_VAL),0)) - 0.5 as disp_val
                ,'Gap(Revenue)' as calc_row_key
                ,'Total' as calc_col_key
                from MIG_ADW.Y_SHKDP_RPT_005A01_T where calc_col_key = 'Total';
                   
-----------------------------10 rows col 1-26--------------------------------------
--1 KPI
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,1 as row_num
                ,3 + row_number() over(order by shk_tier) as col_num
                ,shk_tier as disp_val
                ,'KPI' as calc_row_key
                ,shk_tier as calc_col_key
                from mig_adw.SHKDP_RPT_H
              group by shk_tier;

--2 Recontract Plan Rate
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,2 as row_num
                ,3 + row_number() over(order by shk_tier) as col_num
                ,' ' as disp_val
                ,'Recontract Plan Rate' as calc_row_key
                ,shk_tier as calc_col_key
                from mig_adw.SHKDP_RPT_H
              group by shk_tier;

--3 Target(Plan)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,3 as row_num
                ,3 + row_number() over(order by shk_tier) as col_num
                ,'50%' as disp_val
                ,'Target(Plan)' as calc_row_key
                ,shk_tier as calc_col_key
                from mig_adw.SHKDP_RPT_H
              group by shk_tier;

--13 Recontract Revenue Rate
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,13 as row_num
                ,3 + row_number() over(order by shk_tier) as col_num
                ,' ' as disp_val
                ,'Recontract Revenue Rate' as calc_row_key
                ,shk_tier as calc_col_key
                from mig_adw.SHKDP_RPT_H
              group by shk_tier;

--14 Target(Revenue)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,14 as row_num
                ,3 + row_number() over(order by shk_tier) as col_num
                ,'50%' as disp_val
                ,'Target(Revenue)' as calc_row_key
                ,shk_tier as calc_col_key
                from mig_adw.SHKDP_RPT_H
              group by shk_tier;
              
--19 Avg Net Service Revenue Per User
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,19 as row_num
                ,3 + row_number() over(order by shk_tier) as col_num
                ,' ' as disp_val
                ,'Avg Net Service Revenue Per User' as calc_row_key
                ,shk_tier as calc_col_key
                from mig_adw.SHKDP_RPT_H
              group by shk_tier;

--22 Sales status (%)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,22 as row_num
                ,3 + row_number() over(order by shk_tier) as col_num
                ,' ' as disp_val
                ,'Sales status (%)' as calc_row_key
                ,shk_tier as calc_col_key
                from mig_adw.SHKDP_RPT_H
              group by shk_tier;

--26 Total(%)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,26 as row_num
                ,3 + row_number() over(order by shk_tier) as col_num
                ,' ' as disp_val
                ,'Total(%)' as calc_row_key
                ,shk_tier as calc_col_key
                from mig_adw.SHKDP_RPT_H
              group by shk_tier;
              
--27 Sales status (count)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,27 as row_num
                ,3 + row_number() over(order by shk_tier) as col_num
                ,' ' as disp_val
                ,'Sales status (count)' as calc_row_key
                ,shk_tier as calc_col_key
                from mig_adw.SHKDP_RPT_H
              group by shk_tier;
              
--32 Magnitude
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,32 as row_num
                ,3 + row_number() over(order by shk_tier) as col_num
                ,' ' as disp_val
                ,'Magnitude' as calc_row_key
                ,shk_tier as calc_col_key
                from mig_adw.SHKDP_RPT_H
              group by shk_tier;


-----------------------------10 rows 'Total' col--------------------------------------
--1 KPI
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,1 as row_num
                ,3  as col_num
                ,'Total' as disp_val
                ,'KPI' as calc_row_key
                ,'Total' as calc_col_key
                from dual;


--2 Recontract Plan Rate
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,2 as row_num
                ,3 as col_num
                ,' ' as disp_val
                ,'Recontract Plan Rate' as calc_row_key
                ,'Total' as calc_col_key
                from dual;
              

--3 Target(Plan)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,3 as row_num
                ,3 as col_num
                ,'50%' as disp_val
                ,'Target(Plan)' as calc_row_key
                ,'Total' as calc_col_key
                from dual;
              

--13 Recontract Revenue Rate
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,13 as row_num
                ,3 as col_num
                ,' ' as disp_val
                ,'Recontract Revenue Rate' as calc_row_key
                ,'Total' as calc_col_key
                from dual;
              

--14 Target(Revenue)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,14 as row_num
                ,3 as col_num
                ,'50%' as disp_val
                ,'Target(Revenue)' as calc_row_key
                ,'Total' as calc_col_key
                from dual;
              
              
--19 Avg Net Service Revenue Per User
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,19 as row_num
                ,3 as col_num
                ,' ' as disp_val
                ,'Avg Net Service Revenue Per User' as calc_row_key
                ,'Total' as calc_col_key
                from dual;
              

--22 Sales status (%)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,22 as row_num
                ,3 as col_num
                ,' ' as disp_val
                ,'Sales status (%)' as calc_row_key
                ,'Total' as calc_col_key
                from dual;
              

--26 Total(%)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,26 as row_num
                ,3 as col_num
                ,' ' as disp_val
                ,'Total(%)' as calc_row_key
                ,'Total' as calc_col_key
                from dual;
              
              
--27 Sales status (count)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,27 as row_num
                ,3 as col_num
                ,' ' as disp_val
                ,'Sales status (count)' as calc_row_key
                ,'Total' as calc_col_key
                from dual;
              
              
--32 Magnitude
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,32 as row_num
                ,3 as col_num
                ,' ' as disp_val
                ,'Magnitude' as calc_row_key
                ,'Total' as calc_col_key
                from dual;
              

-----------------------------all rows 'KPI' col--------------------------------------
--1 KPI
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,1 as row_num
                ,1 as col_num
                ,'KPI' as disp_val
                ,'KPI' as calc_row_key
                ,'KPI' as calc_col_key
                from dual;


--2 Recontract Plan Rate
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,2 as row_num
                ,1 as col_num
                ,'Recontract Plan Rate' as disp_val
                ,'Recontract Plan Rate' as calc_row_key
                ,'KPI' as calc_col_key
                from dual;

--3 Target(Plan)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,3 as row_num
                ,1 as col_num
                ,'Target(Plan)' as disp_val
                ,'Target(Plan)' as calc_row_key
                ,'KPI' as calc_col_key
                from dual;
--4 Gap(Plan)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,4 as row_num
                ,1 as col_num
                ,'Gap(Plan)' as disp_val
                ,'Gap(Plan)' as calc_row_key
                ,'KPI' as calc_col_key
                from dual;
                
--5 PTD(Plan)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,5 as row_num
                ,1 as col_num
                ,'PTD(Plan)' as disp_val
                ,'PTD(Plan)' as calc_row_key
                ,'KPI' as calc_col_key
                from dual;

--6 Size
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,6 as row_num
                ,1 as col_num
                ,'Size' as disp_val
                ,'Size' as calc_row_key
                ,'KPI' as calc_col_key
                from dual;


--7 Size (5G potential customer)

insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,7 as row_num
                ,1 as col_num
                ,'Size (5G potential customer)' as disp_val
                ,'Size (5G potential customer)' as calc_row_key
                ,'KPI' as calc_col_key
                from dual;
                
--8 Reached
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,8 as row_num
                ,1 as col_num
                ,'Reached' as disp_val
                ,'Reached' as calc_row_key
                ,'KPI' as calc_col_key
                from dual;
                
--9 Reach %
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,9 as row_num
                ,1 as col_num
                ,'Reach %' as disp_val
                ,'Reach %' as calc_row_key
                ,'KPI' as calc_col_key
                from dual;

--10 Hit
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,10 as row_num
                ,1 as col_num
                ,'Hit' as disp_val
                ,'Hit' as calc_row_key
                ,'KPI' as calc_col_key
                from dual;
                
--11 Hit/Reach%
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,11 as row_num
                ,1 as col_num
                ,'Hit/Reach%' as disp_val
                ,'Hit/Reach%' as calc_row_key
                ,'KPI' as calc_col_key
                from dual;
                
--12 Hit (from 5G potential customer)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,12 as row_num
                ,1 as col_num
                ,'Hit (from 5G potential customer)' as disp_val
                ,'Hit (from 5G potential customer)' as calc_row_key
                ,'KPI' as calc_col_key
                from dual;

--13 Recontract Revenue Rate
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,13 as row_num
                ,1 as col_num
                ,'Recontract Revenue Rate' as disp_val
                ,'Recontract Revenue Rate' as calc_row_key
                ,'KPI' as calc_col_key
                from dual;

--14 Target(Revenue)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,14 as row_num
                ,1 as col_num
                ,'Target(Revenue)' as disp_val
                ,'Target(Revenue)' as calc_row_key
                ,'KPI' as calc_col_key
                from dual;
              
--15 Gap(Revenue)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,15 as row_num
                ,1 as col_num
                ,'Gap(Revenue)' as disp_val
                ,'Gap(Revenue)' as calc_row_key
                ,'KPI' as calc_col_key
                from dual;
                
--16 PTD(Revenue)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,16 as row_num
                ,1 as col_num
                ,'PTD(Revenue)' as disp_val
                ,'PTD(Revenue)' as calc_row_key
                ,'KPI' as calc_col_key
                from dual;
                
--17 Net service revenue1 (before)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,17 as row_num
                ,1 as col_num
                ,'Net service revenue1 (before)' as disp_val
                ,'Net service revenue1 (before)' as calc_row_key
                ,'KPI' as calc_col_key
                from dual;
                
                
--18 Net service revenue (after)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,18 as row_num
                ,1 as col_num
                ,'Net service revenue (after)' as disp_val
                ,'Net service revenue (after)' as calc_row_key
                ,'KPI' as calc_col_key
                from dual;
                
--19 Avg Net Service Revenue Per User
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,19 as row_num
                ,1 as col_num
                ,'Avg Net Service Revenue Per User' as disp_val
                ,'Avg Net Service Revenue Per User' as calc_row_key
                ,'KPI' as calc_col_key
                from dual;

--20 Before
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,20 as row_num
                ,1 as col_num
                ,'Before' as disp_val
                ,'Before' as calc_row_key
                ,'KPI' as calc_col_key
                from dual;
                
--21 After (accept order)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,21 as row_num
                ,1 as col_num
                ,'After (accept order)' as disp_val
                ,'After (accept order)' as calc_row_key
                ,'KPI' as calc_col_key
                from dual;


--22 Sales status (%)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,22 as row_num
                ,1 as col_num
                ,'Sales status (%)' as disp_val
                ,'Sales status (%)' as calc_row_key
                ,'KPI' as calc_col_key
                from dual;
--23 Upsell(%)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,23 as row_num
                ,1 as col_num
                ,'Upsell(%)' as disp_val
                ,'Upsell(%)' as calc_row_key
                ,'KPI' as calc_col_key
                from dual;
                
--24 Flatsell(%)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,24 as row_num
                ,1 as col_num
                ,'Flatsell(%)' as disp_val
                ,'Flatsell(%)' as calc_row_key
                ,'KPI' as calc_col_key
                from dual;
                
--25 Downsell(%)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,25 as row_num
                ,1 as col_num
                ,'Downsell(%)' as disp_val
                ,'Downsell(%)' as calc_row_key
                ,'KPI' as calc_col_key
                from dual;
                
--26 Total(%)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,26 as row_num
                ,1 as col_num
                ,'Total(%)' as disp_val
                ,'Total(%)' as calc_row_key
                ,'KPI' as calc_col_key
                from dual;
              
--27 Sales status (count)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,27 as row_num
                ,1 as col_num
                ,'Sales status (count)' as disp_val
                ,'Sales status (count)' as calc_row_key
                ,'KPI' as calc_col_key
                from dual;
                
--28 Upsell(count)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,28 as row_num
                ,1 as col_num
                ,'Upsell(count)' as disp_val
                ,'Upsell(count)' as calc_row_key
                ,'KPI' as calc_col_key
                from dual;
                
--29 Flatsell(count)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,29 as row_num
                ,1 as col_num
                ,'Flatsell(count)' as disp_val
                ,'Flatsell(count)' as calc_row_key
                ,'KPI' as calc_col_key
                from dual;
                
--30 Downsell(count)

insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,30 as row_num
                ,1 as col_num
                ,'Downsell(count)' as disp_val
                ,'Downsell(count)' as calc_row_key
                ,'KPI' as calc_col_key
                from dual;
--31 Total(count)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,31 as row_num
                ,1 as col_num
                ,'Total(count)' as disp_val
                ,'Total(count)' as calc_row_key
                ,'KPI' as calc_col_key
                from dual;
                
--32 Magnitude
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,32 as row_num
                ,1 as col_num
                ,'Magnitude' as disp_val
                ,'Magnitude' as calc_row_key
                ,'KPI' as calc_col_key
                from dual;

--33 Upsell magnitude
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,33 as row_num
                ,1 as col_num
                ,'Upsell magnitude' as disp_val
                ,'Upsell magnitude' as calc_row_key
                ,'KPI' as calc_col_key
                from dual;

--34 Downsell magnitude
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,34 as row_num
                ,1 as col_num
                ,'Downsell magnitude' as disp_val
                ,'Downsell magnitude' as calc_row_key
                ,'KPI' as calc_col_key
                from dual;

--35 Overall magnitude
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,35 as row_num
                ,1 as col_num
                ,'Overall magnitude' as disp_val
                ,'Overall magnitude' as calc_row_key
                ,'KPI' as calc_col_key
                from dual;

-----------------------------all rows 'Formula' col--------------------------------------
--1 KPI
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,1 as row_num
                ,2 as col_num
                ,'Formula' as disp_val
                ,'KPI' as calc_row_key
                ,'Formula' as calc_col_key
                from dual;


--2 Recontract Plan Rate
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,2 as row_num
                ,2 as col_num
                ,' ' as disp_val
                ,'Recontract Plan Rate' as calc_row_key
                ,'Formula' as calc_col_key
                from dual;

--3 Target(Plan)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,3 as row_num
                ,2 as col_num
                ,'1' as disp_val
                ,'Target(Plan)' as calc_row_key
                ,'Formula' as calc_col_key
                from dual;
--4 Gap(Plan)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,4 as row_num
                ,2 as col_num
                ,'2=3-1' as disp_val
                ,'Gap(Plan)' as calc_row_key
                ,'Formula' as calc_col_key
                from dual;
                
--5 PTD(Plan)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,5 as row_num
                ,2 as col_num
                ,'3=8/4' as disp_val
                ,'PTD(Plan)' as calc_row_key
                ,'Formula' as calc_col_key
                from dual;

--6 Size
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,6 as row_num
                ,2 as col_num
                ,'4' as disp_val
                ,'Size' as calc_row_key
                ,'Formula' as calc_col_key
                from dual;


--7 Size (5G potential customer)

insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,7 as row_num
                ,2 as col_num
                ,'5' as disp_val
                ,'Size (5G potential customer)' as calc_row_key
                ,'Formula' as calc_col_key
                from dual;
                
--8 Reached
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,8 as row_num
                ,2 as col_num
                ,'6' as disp_val
                ,'Reached' as calc_row_key
                ,'Formula' as calc_col_key
                from dual;
                
--9 Reach %
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,9 as row_num
                ,2 as col_num
                ,'7=6/4' as disp_val
                ,'Reach %' as calc_row_key
                ,'Formula' as calc_col_key
                from dual;

--10 Hit
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,10 as row_num
                ,2 as col_num
                ,'8' as disp_val
                ,'Hit' as calc_row_key
                ,'Formula' as calc_col_key
                from dual;
                
--11 Hit/Reach%
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,11 as row_num
                ,2 as col_num
                ,'9=8/6' as disp_val
                ,'Hit/Reach%' as calc_row_key
                ,'Formula' as calc_col_key
                from dual;
                
--12 Hit (from 5G potential customer)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,12 as row_num
                ,2 as col_num
                ,'10' as disp_val
                ,'Hit (from 5G potential customer)' as calc_row_key
                ,'Formula' as calc_col_key
                from dual;

--13 Recontract Revenue Rate
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,13 as row_num
                ,2 as col_num
                ,' ' as disp_val
                ,'Recontract Revenue Rate' as calc_row_key
                ,'Formula' as calc_col_key
                from dual;

--14 Target(Revenue)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,14 as row_num
                ,2 as col_num
                ,'12=1' as disp_val
                ,'Target(Revenue)' as calc_row_key
                ,'Formula' as calc_col_key
                from dual;
              
--15 Gap(Revenue)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,15 as row_num
                ,2 as col_num
                ,'13=14-12' as disp_val
                ,'Gap(Revenue)' as calc_row_key
                ,'Formula' as calc_col_key
                from dual;
                
--16 PTD(Revenue)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,16 as row_num
                ,2 as col_num
                ,'14=15/16' as disp_val
                ,'PTD(Revenue)' as calc_row_key
                ,'Formula' as calc_col_key
                from dual;
                
--17 Net service revenue1 (before)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,17 as row_num
                ,2 as col_num
                ,'15=4*17' as disp_val
                ,'Net service revenue1 (before)' as calc_row_key
                ,'Formula' as calc_col_key
                from dual;
                
                
--18 Net service revenue (after)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,18 as row_num
                ,2 as col_num
                ,'16=8*18' as disp_val
                ,'Net service revenue (after)' as calc_row_key
                ,'Formula' as calc_col_key
                from dual;
                
--19 Avg Net Service Revenue Per User
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,19 as row_num
                ,2 as col_num
                ,' ' as disp_val
                ,'Avg Net Service Revenue Per User' as calc_row_key
                ,'Formula' as calc_col_key
                from dual;

--20 Before
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,20 as row_num
                ,2 as col_num
                ,'17' as disp_val
                ,'Before' as calc_row_key
                ,'Formula' as calc_col_key
                from dual;
                
--21 After (accept order)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,21 as row_num
                ,2 as col_num
                ,'18' as disp_val
                ,'After (accept order)' as calc_row_key
                ,'Formula' as calc_col_key
                from dual;


--22 Sales status (%)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,22 as row_num
                ,2 as col_num
                ,' ' as disp_val
                ,'Sales status (%)' as calc_row_key
                ,'Formula' as calc_col_key
                from dual;
--23 Upsell(%)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,23 as row_num
                ,2 as col_num
                ,'15=20/23' as disp_val
                ,'Upsell(%)' as calc_row_key
                ,'Formula' as calc_col_key
                from dual;
                
--24 Flatsell(%)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,24 as row_num
                ,2 as col_num
                ,'15=21/23' as disp_val
                ,'Flatsell(%)' as calc_row_key
                ,'Formula' as calc_col_key
                from dual;
                
--25 Downsell(%)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,25 as row_num
                ,2 as col_num
                ,'15=22/23' as disp_val
                ,'Downsell(%)' as calc_row_key
                ,'Formula' as calc_col_key
                from dual;
                
--26 Total(%)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,26 as row_num
                ,2 as col_num
                ,' ' as disp_val
                ,'Total(%)' as calc_row_key
                ,'Formula' as calc_col_key
                from dual;
              
--27 Sales status (count)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,27 as row_num
                ,2 as col_num
                ,' ' as disp_val
                ,'Sales status (count)' as calc_row_key
                ,'Formula' as calc_col_key
                from dual;
                
--28 Upsell(count)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,28 as row_num
                ,2 as col_num
                ,'20' as disp_val
                ,'Upsell(count)' as calc_row_key
                ,'Formula' as calc_col_key
                from dual;
                
--29 Flatsell(count)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,29 as row_num
                ,2 as col_num
                ,'21' as disp_val
                ,'Flatsell(count)' as calc_row_key
                ,'Formula' as calc_col_key
                from dual;
                
--30 Downsell(count)

insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,30 as row_num
                ,2 as col_num
                ,'22' as disp_val
                ,'Downsell(count)' as calc_row_key
                ,'Formula' as calc_col_key
                from dual;
--31 Total(count)
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,31 as row_num
                ,2 as col_num
                ,'23' as disp_val
                ,'Total(count)' as calc_row_key
                ,'Formula' as calc_col_key
                from dual;
                
--32 Magnitude
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,32 as row_num
                ,2 as col_num
                ,' ' as disp_val
                ,'Magnitude' as calc_row_key
                ,'Formula' as calc_col_key
                from dual;

--33 Upsell magnitude
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,33 as row_num
                ,2 as col_num
                ,'24' as disp_val
                ,'Upsell magnitude' as calc_row_key
                ,'Formula' as calc_col_key
                from dual;

--34 Downsell magnitude
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,34 as row_num
                ,2 as col_num
                ,'25' as disp_val
                ,'Downsell magnitude' as calc_row_key
                ,'Formula' as calc_col_key
                from dual;

--35 Overall magnitude
insert into MIG_ADW.Y_SHKDP_RPT_005A01_T
             select
                'rpt_tag_1' as rpt_tag
                ,35 as row_num
                ,2 as col_num
                ,'26' as disp_val
                ,'Overall magnitude' as calc_row_key
                ,'Formula' as calc_col_key
                from dual;
commit;
---------------------------------------------------------------------------------------------------

update MIG_ADW.Y_SHKDP_RPT_005A01_T tmpa set DISP_VAL = (select to_char(round(DISP_VAL * 100,2),'fm9999999990.00')||'%' as DISP_VAL
                                                   from MIG_ADW.Y_SHKDP_RPT_005A01_T tmpb where tmpa.col_num = tmpb.col_num and tmpa.row_num = tmpb.row_num
)where row_num in (4,5,9,11,15,16,23,24,25)  and col_num not in (1,2);


update MIG_ADW.Y_SHKDP_RPT_005A01_T tmpa set DISP_VAL = (select to_char(round(DISP_VAL,2),'fm9999999990.00') as DISP_VAL
                                                   from MIG_ADW.Y_SHKDP_RPT_005A01_T tmpb where tmpa.col_num = tmpb.col_num and tmpa.row_num = tmpb.row_num
)where row_num in (17,18,20,21)  and col_num not in (1,2);
commit;
---------------------------------------------------------------------------------------------------
update MIG_ADW.Y_SHKDP_RPT_005A01_T 
set DISP_VAL = '-' || DISP_VAL
where ROW_NUM = 34 and  COL_NUM not in (1,2);
commit;
---------------------------------------------------------------------------------------------------
--SET SERVEROUTPUT ON;
declare
    v_first_sql varchar2(1000);

    v_last1_sql varchar2(1000);
    v_last2_sql varchar2(1000);
    v_tmp1_sql varchar2(1000);
    v_tmp2_sql varchar2(1000);
    v_cnt number(18);
    v_num number(18);
    v_tmp_last1_sql varchar2(1000);
    v_tmp_last2_sql varchar2(1000);
    v_sql varchar2(1000);
begin 
    v_first_sql := 'select "';
    v_last1_sql := '" from (
  select disp_val,col_num,row_num
  from MIG_ADW.Y_SHKDP_RPT_005A01_T where RPT_TAG = ' || '''' ||'rpt_tag_1' || '''' || '
)pivot(
  max(disp_val) for col_num in (';
    v_last2_sql := ')
)
order by to_number(ROW_NUM)';
  
    v_tmp1_sql := '"' || ' || ' || '''' || ',' || '''' ||' || "'; 
    
    v_tmp2_sql := ',';
    --v_cnt := 3;
    v_num := 1;

    
    select count(distinct(calc_col_key)) into v_cnt from MIG_ADW.Y_SHKDP_RPT_005A01_T;
    
    while v_num <= v_cnt loop
        --DBMS_OUTPUT.PUT_LINE(v_num);
        v_tmp_last1_sql := v_tmp_last1_sql || v_num; 
        --DBMS_OUTPUT.PUT_LINE(v_tmp_last1_sql);
        
        
        v_tmp_last2_sql := v_tmp_last2_sql || v_num; 
        --DBMS_OUTPUT.PUT_LINE(v_tmp_last2_sql); 
        
        exit when v_num = v_cnt;
        v_tmp_last1_sql := v_tmp_last1_sql || v_tmp1_sql;
        v_tmp_last2_sql := v_tmp_last2_sql || v_tmp2_sql;
        
        v_num := v_num + 1;
        
        
    end loop;


    v_sql := v_first_sql || v_tmp_last1_sql || v_last1_sql || v_tmp_last2_sql || v_last2_sql || ';';
    
    --DBMS_OUTPUT.PUT_LINE(v_sql);
    --execute immediate v_sql;
    delete from MIG_ADW.Y_SHKDP_RPT_005_T;
    insert into MIG_ADW.Y_SHKDP_RPT_005_T values(v_sql);
    commit;
end;
/

commit;

---------------------------------------------------------------------------------------------------

${etlvar::SPOOLOPT}
set termout off;
--.SET RETLIMIT 0 200;
SET LINE 2000;
SPOOL '/opt/etl/prd/etl/APP/ADW/Y_SHKDP_RPT/bin/el_test/1.sql';
select spool_str from MIG_ADW.Y_SHKDP_RPT_005_T;

spool off;

spool '${OUTPUT_FILE_PATH}/${OUTPUT_FILE_NAME_1}';
@/opt/etl/prd/etl/APP/ADW/Y_SHKDP_RPT/bin/el_test/1.sql
spool off;

commit;
---------------------------------------------------------------------------------------------------
insert into MIG_ADW.Y_SHKDP_RPT_005B01_T
select * from (
with tbl as (
    Select
             h.batch_id
            ,h.shk_tier
            ,decode(h.ru_plan_mix_nature,' ','UNMAP',h.ru_plan_mix_nature)ru_plan_mix_nature
            ,decode(h.ru_plan_mix_sub_nature,' ','UNMAP',h.ru_plan_mix_sub_nature)ru_plan_mix_sub_nature
            ,count(h.subr_num) cnt_sub_nature                      
         from mig_adw.SHKDP_RPT_h h
         where h.cl_hit like 'HIT%'
         group by h.batch_id
                 ,h.shk_tier
                 ,h.ru_plan_mix_nature
                 ,h.ru_plan_mix_sub_nature
)
select 
       ttg.ru_plan_mix_nature
      ,ttg.ru_plan_mix_sub_nature
      ,ttg.rnk_nature
      ,ttg.rnk_sub_nature
      ,decode(label_type,'NATURE_HEADER',ttg.ru_plan_mix_nature,ttg.ru_plan_mix_sub_nature) display_nature
      ,ttg.batch_id
      ,ttg.shk_tier      
      ,decode(rnk_sub_nature,0,' ',to_char(round(nvl(tt.ptg,'0') * 100,2),'fm9999999990.00')||'%') ptg
from(----- prepare the label order---     
        select distinct
           tg.batch_id
          ,'NATURE_HEADER' label_type  
          ,tg.ru_plan_mix_nature
          ,' ' as ru_plan_mix_sub_nature
          ,dense_rank()over (partition by tg.batch_id order by tg.ru_plan_mix_nature) rnk_nature
          ,0 rnk_sub_nature     
          ,tmp.shk_tier
     from tbl tg,(select distinct shk_tier from  mig_adw.SHKDP_RPT_h union select 'TOTAL' from dual) tmp
    union all 
    select distinct
           tg.batch_id
          ,'NATURE_DTL' label_type
          ,tg.ru_plan_mix_nature
          ,tg.ru_plan_mix_sub_nature
          ,dense_rank()over (partition by tg.batch_id order by tg.ru_plan_mix_nature) rnk_nature
          ,dense_rank()over (partition by tg.batch_id ,tg.ru_plan_mix_nature order by tg.ru_plan_mix_nature,tg.ru_plan_mix_sub_nature) rnk_sub_nature
          ,tmp.shk_tier
     from tbl tg,(select distinct shk_tier from  mig_adw.SHKDP_RPT_h union select 'TOTAL' from dual) tmp
     ) ttg
left outer join(------ Counting the detail              
  select   t.batch_id
            ,t.shk_tier
            ,t.ru_plan_mix_nature
            ,t.ru_plan_mix_sub_nature
            ,case when sum(t.cnt_sub_nature) over(partition by t.batch_id,t.shk_tier) = 0
                  then 0
                  else t.cnt_sub_nature / sum(t.cnt_sub_nature) over(partition by t.batch_id,t.shk_tier)
             end as ptg
    from tbl t
    union all  
    select   distinct t.batch_id
            ,'TOTAL'  as shk_tier
            ,t.ru_plan_mix_nature
            ,t.ru_plan_mix_sub_nature            
            ,case when sum(t.cnt_sub_nature) over(partition by t.batch_id) = 0 
                  then 0
                  else sum(t.cnt_sub_nature) over (partition by t.batch_id,t.ru_plan_mix_nature,t.ru_plan_mix_sub_nature)   
                       / sum(t.cnt_sub_nature) over(partition by t.batch_id) 
             end as ptg
    from tbl t               
 )tt
 on ttg.label_type='NATURE_DTL' and ttg.ru_plan_mix_nature =tt.ru_plan_mix_nature and ttg.ru_plan_mix_sub_nature = tt.ru_plan_mix_sub_nature
    and ttg.shk_tier = tt.shk_tier
 order by ttg.rnk_nature,ttg.rnk_sub_nature,ttg.shk_tier
);

commit;
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
--SET SERVEROUTPUT ON;
declare
    cursor shk_tier_cur
       is 
       select distinct SHK_TIER
       from MIG_ADW.Y_SHKDP_RPT_005B01_T 
       where SHK_TIER <> 'TOTAL' order by SHK_TIER; 

    v_first_sql varchar2(1000);
    v_first2_sql varchar2(1000);
    v_first3_sql varchar2(1000);
    v_last1_sql varchar2(1000);
    v_last2_sql varchar2(1000);
    v_last3_sql varchar2(1000);
    v_last4_sql varchar2(1000);
    v_tmp1_sql varchar2(1000);
    v_tmp2_sql varchar2(1000);
    v_tmp3_sql varchar2(1000);
    v_tmp4_sql varchar2(1000);
    v_cnt number(18);
    v_num number(18);
    v_tmp_last1_sql varchar2(1000);
    v_tmp_last2_sql varchar2(1000);
    v_tmp_last3_sql varchar2(1000);
    v_tmp_last4_sql varchar2(1000);
    v_sql varchar2(2000);
    v_sql2 varchar2(2000);
    v_sql3 varchar2(2000);
    v_sql_total varchar2(3000);
begin 
    v_first_sql := 'select ' || '''' || '"' || '''' || ' || ' || 'DISPLAY_NATURE' || ' || ' 
                   || '''' || '"' || '''' || ' || ' || '''' || ',' || '''' 
                   || ' || ' || '"' || '''' || 'TOTAL' || '''' || '"' ||' || ' || '''' || ',' || ''''
                   || ' || "' || '''';
    v_last1_sql := '''' || '"
from (
select * 
from (
  select SHK_TIER
,DISPLAY_NATURE,rnk_nature,rnk_sub_nature,decode(RNK_SUB_NATURE,0,null,ptg) as ptg
  from MIG_ADW.Y_SHKDP_RPT_005B01_T
)pivot(
  max(ptg) for SHK_TIER in (' || '''' || 'TOTAL' || '''' || ',' || ''''; 
    v_last2_sql := ''''||')
)
order by to_number(rnk_nature),to_number(rnk_sub_nature)
)
';

    v_first2_sql := 'select ' || '''' || 'Row Labels' || '''' || ' || ' || '''' || ',' || '''' 
                    || ' || ' || '''' || 'Total' || '''' || ' || ' || '''' || ',' || '''' || ' || ' || '''';  
    v_last3_sql := '''' || ' from dual 
union all 
';

    
    v_first3_sql := 'union all
select ' || '''' || 'Total' || '''' || ' || ' || '''' || ',' || '''' || ' || ' || '''' || '100%' || '''' || ' ';

    v_last4_sql := 'from dual';
     

    v_tmp1_sql := '''' || '"' || ' || ' || '''' || ',' || '''' ||' || "' || ''''; 
    
    v_tmp2_sql := '''' || ',' || '''';
    
    v_tmp3_sql := '''' || ' || ' || '''' || ',' || '''' || ' || ' || '''';
    
    v_tmp4_sql := '|| ' || '''' || ',' || '''' || ' || ' || '''' || '100%' || '''' || ' ';
    
    select count(distinct SHK_TIER) into v_cnt
        from MIG_ADW.Y_SHKDP_RPT_005B01_T 
    where SHK_TIER <> 'TOTAL'; 
    v_num := 1;
    
    for v_shk_tier in shk_tier_cur
    loop
        --DBMS_OUTPUT.PUT_LINE(v_shk_tier.SHK_TIER);
        
        v_tmp_last1_sql := v_tmp_last1_sql || v_shk_tier.SHK_TIER; 
        --DBMS_OUTPUT.PUT_LINE(v_tmp_last1_sql);
        
        v_tmp_last2_sql := v_tmp_last2_sql || v_shk_tier.SHK_TIER; 
        --DBMS_OUTPUT.PUT_LINE(v_tmp_last2_sql); 
        
        v_tmp_last3_sql := v_tmp_last3_sql || v_shk_tier.SHK_TIER; 
        --DBMS_OUTPUT.PUT_LINE(v_tmp_last3_sql); 
        
        v_tmp_last4_sql := v_tmp_last4_sql || v_tmp4_sql;
        --DBMS_OUTPUT.PUT_LINE(v_tmp_last4_sql); 
        
        exit when v_num = v_cnt;
        v_tmp_last1_sql := v_tmp_last1_sql || v_tmp1_sql;
        v_tmp_last2_sql := v_tmp_last2_sql || v_tmp2_sql;
        v_tmp_last3_sql := v_tmp_last3_sql || v_tmp3_sql;

        
        v_num := v_num + 1;
        
        
    end loop;
   
    v_sql2 := v_first2_sql || v_tmp_last3_sql || v_last3_sql;
    --DBMS_OUTPUT.PUT_LINE(v_sql2);  
    
    v_sql := v_first_sql || v_tmp_last1_sql || v_last1_sql || v_tmp_last2_sql || v_last2_sql;
    --DBMS_OUTPUT.PUT_LINE(v_sql);
     
    v_sql3 := v_first3_sql || v_tmp_last4_sql || v_last4_sql;
    --DBMS_OUTPUT.PUT_LINE(v_sql3);  
    
      
    v_sql_total := v_sql2 || v_sql || v_sql3 || ';';
    --DBMS_OUTPUT.PUT_LINE(v_sql_total);    
    
    --execute immediate v_sql;
    delete from MIG_ADW.Y_SHKDP_RPT_005_T;
    insert into MIG_ADW.Y_SHKDP_RPT_005_T values(v_sql_total);
    commit;
end;
/

commit;
---------------------------------------------------------------------------------------------------
${etlvar::SPOOLOPT}
set termout off;
--.SET RETLIMIT 0 200;
SET LINE 2000;
SPOOL '/opt/etl/prd/etl/APP/ADW/Y_SHKDP_RPT/bin/el_test/2.sql';
select spool_str from MIG_ADW.Y_SHKDP_RPT_005_T;

spool off;

spool '${OUTPUT_FILE_PATH}/${OUTPUT_FILE_NAME_2}';
@/opt/etl/prd/etl/APP/ADW/Y_SHKDP_RPT/bin/el_test/2.sql
spool off;

commit;
---------------------------------------------------------------------------------------------------

COMMIT;

exit;

ENDOFINPUT
  close(SQLCMD);
  print("sqlplus /\@${etlvar::TDDSN} \@$SQLCMD_FILE");
  my $ret = system("sqlplus /\@${etlvar::TDDSN} \@$SQLCMD_FILE");
  if ($ret != 0)
  {
    return (1);
  }

}




#########################################################################################################
#########################################################################################################
#########################################################################################################

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

## Disable the Perl buffer, Print the log message immediately
$| = 1;

initParam();

my $ret = 0;

##################################################################################

# RUN SQL and EXPORT FILE
if ($ret == 0)
{
    $ret = runSQLPLUS_EXPORT();
}

##################################################################################

## REMOVE FILE HEADER
if ($ret == 0)
{
    system("touch ${OUTPUT_FILE_PATH}/${OUTPUT_COMPLETE_1}");
    system("touch ${OUTPUT_FILE_PATH}/${OUTPUT_COMPLETE_2}");
}




my $post = etlvar::postProcess();

exit($ret);



















