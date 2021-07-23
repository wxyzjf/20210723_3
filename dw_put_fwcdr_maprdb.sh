/app/HDSBAT/reload/fw [37]> cat dw_put_fwcdr_maprdb.sh
export DWHOME=${DWHOME:-/app/HDSBAT}
export SCRIPTDIR=${DWHOME}/script

scriptname=`basename $0 |  cut -f1 -d'.'`
. ${SCRIPTDIR}/dw_master_hds_reload.sh $scriptname N

jobmsg="Running $scriptname"

#startup "${jobmsg}"

getdate 1 filedate "%Y%m%d"
getdate 0 optdate "%Y%m%d"

FWCDRDIR=${DATADIR}/FWCDR

check_status dw_load_fwcdr.status "Check dw_load_fwcdr Status"

echo "hadoop fs -mkdir maprfs://bigdatamr08-10g:7222/HDS_VOL_HIVE/FWCDR/start_date=$filedate" | tee -a ${LOGFILE}
hadoop fs -mkdir maprfs://bigdatamr08-10g:7222/HDS_VOL_HIVE/FWCDR/start_date=$filedate
if [ $? -ne 0 ];then
  echo "Failed to create folder in new env" | tee -a ${LOGFILE}
  errhandle "Failed to create folder in new env"
  exit 99
fi

hadoop fs -chmod 777 maprfs://bigdatamr08-10g:7222/HDS_VOL_HIVE/FWCDR/start_date=$filedate

echo "hadoop distcp /HDS_VOL_HIVE/FWCDR/start_date=${filedate}/fwcdr_ldr*gz maprfs://bigdatamr08-10g.hksmartone.com/HDS_VOL_HIVE/FWCDR/start_date=${filedate}" | tee -a ${LOGFILE}
hadoop distcp /HDS_VOL_HIVE/FWCDR/start_date=${filedate}/fwcdr_ldr*gz maprfs://bigdatamr08-10g.hksmartone.com/HDS_VOL_HIVE/FWCDR/start_date=${filedate}
if [ $? -ne 0 ];then
  echo "Failed to copy file to new env" | tee -a ${LOGFILE}
  errhandle "Failed to copy file to new env"
  exit 99
fi

chgdir /app/HDSBAT/reload/fw/complete
rm dw_fw_cdr*.complete
touch dw_fw_cdr_$optdate.complete
lftp sftp://bigdatamr09 -u mapr,Smart2019 <<FOftp
        set ftp:ssl-allow true
        set ftp:ssl-force true
        set ftp:ssl-protect-data true
        set ftp:ssl-protect-list true
        set ftps:initial-prot
        set xfer:clobber on
        cd /opt/reload/input/FWCDR
        put dw_fw_cdr_$optdate.complete
        quit
FOftp
if [ $? -ne 0 ];then
  echo "Failed to create complete file" | tee -a ${LOGFILE}
  errhandle "Failed to create complete file"
  exit 99
fi

cleanup "${jobmsg}"
  


hadoop distcp /HDS_VOL_HIVE/reload/FWCDR/start_date=20200512/fwcdr_ldr_20200512_20200512165709_p1_000.gz maprfs://bigdatamr08-10g.hksmartone.com/HDS_VOL_HIVE/TEST/FWCDR/start_date=20200512



/HDS_VOL_HIVE/reload/FWCDR/start_date=20200512/fwcdr_ldr_20200512_20200512165709_p1_000.gz





fwcdr_ldr_20200512_20200512165709_p1_000.gz


/HDS_VOL_HIVE/reload/FWCDR/start_date=20200512/fwcdr_ldr_20200512_20200512165709_p1_000.gz

/HDS_VOL_HIVE/TEST/FWCDR/start_date=20200512


