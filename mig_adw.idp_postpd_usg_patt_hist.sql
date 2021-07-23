declare
   fid SYS.UTL_FILE.FILE_TYPE;
   loc VARCHAR2(100);
   file VARCHAR2(100);
   line VARCHAR2(2000);
begin
   fid := UTL_FILE.FOPEN ('EXP_STAGE','idp_call_log_details.out', 'W');
   FOR rec IN (SELECT CUST_ID,CUST_NUM,SUBR_NUM,USAGE_TRANSACTION_DATE,OUTGOING_CALLS_CNT,
                      INCOMING_CALLS_CNT,ROAMING_CALLS_CNT,INTERNATIONAL_CALLS_CNT,
                      DATA_CONSUMED,LOCAL_SMS_CNT,ROAMING_SMS_CNT,LOCAL_CALLS_MINUTES,
                      VAS_ACTIVATION_CNT,VAS_DEACTIVATIONS_CNT,SERVICES_SUBSCRIPTION_CNT,
                      DAY_PLAN_USED_CNT,VOICE_ROAMING_CALLS_MINUTES,CREATE_TS,REFRESH_TS
               from mig_adw.idp_postpd_usg_patt_hist)
   LOOP
      line := rec.CUST_ID||','||
              rec.CUST_NUM||','||
              rec.SUBR_NUM||','||
              rec.to_char(USAGE_TRANSACTION_DATE,'YYYY-MM-DD')||','||
              rec.OUTGOING_CALLS_CNT||','||
              rec.INCOMING_CALLS_CNT||','||
              rec.ROAMING_CALLS_CNT||','||
              rec.INTERNATIONAL_CALLS_CNT||','||
              rec.DATA_CONSUMED||','||
              rec.LOCAL_SMS_CNT||','||
              rec.ROAMING_SMS_CNT||','||
              rec.LOCAL_CALLS_MINUTES||','||
              rec.VAS_ACTIVATION_CNT||','||
              rec.VAS_DEACTIVATIONS_CNT||','||
              rec.SERVICES_SUBSCRIPTION_CNT||','||
              rec.DAY_PLAN_USED_CNT||','||
              rec.VOICE_ROAMING_CALLS_MINUTES||','||
              rec.to_char(CREATE_TS,'YYYY-MM-DD HH24:MI:SS')||','||
              rec.to_char(REFRESH_TS,'YYYY-MM-DD HH24:MI:SS');
         UTL_FILE.PUT_LINE (fid, line);
   END LOOP;
   UTL_FILE.FCLOSE (fid);
EXCEPTION
   WHEN OTHERS THEN UTL_FILE.FCLOSE (fid);
END;
/










