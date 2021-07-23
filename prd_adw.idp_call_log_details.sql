declare
   fid SYS.UTL_FILE.FILE_TYPE;
   loc VARCHAR2(100);
   file VARCHAR2(100);
   line VARCHAR2(2000);
begin
   fid := UTL_FILE.FOPEN ('EXP_STAGE','idp_call_log_details.out', 'W');
   FOR rec IN (SELECT CUST_ID,CUST_NUM,SUBR_NUM,A_NUMBER,CALL_START_DATE,CALL_START_TIME,CALL_END_DATE,
                      CALL_END_TIME,CALL_DURATION,DIALED_DIGITS,ORGN_OPERATOR_CODE,ORGN_OPERATOR_GRP,
                      DSTN_OPERATOR_CODE,DSTN_OPERATOR_GRP,X_CO_ORGN_CELL,Y_CO_ORGN_CELL,X_CO_TERM_CELL,
                      Y_CO_TERM_CELL,RANAP_CAUSE_CD,DISCONNECT_PARTY,ORIGIN_SITE_NAME,TERM_SITE_NAME,
                      HANDSET_BRAND,HANDSET_MODEL,HANDSET_OS,IMEI,BSS_MAP,RATE_PLAN_CODE,DATA_VOLUME,
                      SUBSCRIPTION_TYPE,SERVICE_ID,NW_TYPE,REC_CREATE_DATE,REC_UPD_DATE,CREATE_TS,REFRESH_TS,
                      CALL_REC_TYPE_CD,CALL_SERV_TYPE_CD
               from prd_adw.idp_call_log_details)
   LOOP
      line := rec.CUST_ID||','||
              rec.CUST_NUM||','||
              rec.SUBR_NUM||','||
              rec.A_NUMBER||','||
              rec.to_char(CALL_START_DATE,'YYYY-MM-DD')||','||
              rec.CALL_START_TIME||','||
              rec.to_char(CALL_END_DATE,'YYYY-MM-DD')||','||
              rec.CALL_END_TIME||','||
              rec.CALL_DURATION||','||
              rec.DIALED_DIGITS||','||
              rec.ORGN_OPERATOR_CODE||','||
              rec.ORGN_OPERATOR_GRP||','||
              rec.DSTN_OPERATOR_CODE||','||
              rec.DSTN_OPERATOR_GRP||','||
              rec.X_CO_ORGN_CELL||','||
              rec.Y_CO_ORGN_CELL||','||
              rec.X_CO_TERM_CELL||','||
              rec.Y_CO_TERM_CELL||','||
              rec.RANAP_CAUSE_CD||','||
              rec.DISCONNECT_PARTY||','||
              rec.ORIGIN_SITE_NAME||','||
              rec.TERM_SITE_NAME||','||
              rec.HANDSET_BRAND||','||
              rec.HANDSET_MODEL||','||
              rec.HANDSET_OS||','||
              rec.IMEI||','||
              rec.BSS_MAP||','||
              rec.RATE_PLAN_CODE||','||
              rec.DATA_VOLUME||','||
              rec.SUBSCRIPTION_TYPE||','||
              rec.SERVICE_ID||','||
              rec.NW_TYPE||','||
              rec.to_char(REC_CREATE_DATE,'YYYY-MM-DD')||','||
              rec.to_char(REC_UPD_DATE,'YYYY-MM-DD')||','||
              rec.to_char(CREATE_TS,'YYYY-MM-DD HH24:MI:SS')||','||
              rec.to_char(REFRESH_TS,'YYYY-MM-DD HH24:MI:SS')||','||
              rec.CALL_REC_TYPE_CD||','||
              rec.CALL_SERV_TYPE_CD;
         UTL_FILE.PUT_LINE (fid, line);
   END LOOP;
   UTL_FILE.FCLOSE (fid);
EXCEPTION
   WHEN OTHERS THEN UTL_FILE.FCLOSE (fid);
END;
/






















