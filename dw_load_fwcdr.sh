/app/HDSBAT/script [32]> cat dw_load_fwcdr.sh
export DWHOME=${DWHOME:-/app/HDSBAT}
export SCRIPTDIR=${DWHOME}/script

scriptname=`basename $0 |  cut -f1 -d'.'`
. ${SCRIPTDIR}/dw_master_hds.sh $scriptname N

jobmsg="Loading FIRE WALL MD CDR File"

startup "${jobmsg}"

getdate 1 filedate "%Y%m%d"
opttime=`date +"%Y%m%d%H%M%S"`

FWCDRDIR=${DATADIR}/FWCDR
FWSRCDIR=/nfs/eo_logsearch/MD_DATA/FW/FWCDR_RELOAD

PROC_NUM=3

## Clean Up Last Round Loading Files ##
rm ${FWCDRDIR}/complete/fw_cdr*gz
rm ${FWCDRDIR}/fwcdr*gz

CheckFileCnt "${FWSRCDIR}" "fw_cdr" "filecnt"

#getfile_sftp  hdnn01 "hdsbat,smart123" "HDSBAT/cvtdata/FWCDR/complete/$filedate" "fw_cdr*gz" ${INPUTDIR}/FWCDR
dispatch_file ${PROC_NUM} ${FWSRCDIR} ${FWCDRDIR}/process "fw_cdr*gz"

for d in `seq $PROC_NUM`
do
{
  PROCLOG=${LOGDIR}/${scriptname}_proc_$d.log
  PROCLIST=${FWCDRDIR}/fwcdr_proc_$d.lst
  PROCBAD=${FWCDRDIR}/fwcdr_proc_$d.bad
  cat /dev/null > ${PROCBAD}
  cat /dev/null > ${PROCLOG}
  cat /dev/null > ${PROCLIST}

  filedate=`ls ${FWCDRDIR}/process/proc_${d} | head -1 | cut -c 8-15`
  cvtdate=`date +'%Y%m%d%H%M%S'`

  perl $SCRIPTDIR/dw_fwcdr_convert.pl ${FWCDRDIR}/process/proc_${d} ${PROCLIST} ${PROCBAD} \
       | split -d -a3 -l 180000000 --filter='gzip >> $FILE.gz' - ${FWCDRDIR}/fwcdr_ldr_${filedate}_${cvtdate}_p${d}_
  if [ $? -ne 0 ];then
    echo "Process $d Convert or Split Files error" | tee -a ${PROCLOG}
    echo "Move Error Files to ${ERRORDIR}/ " | tee -a ${PROCLOG}

    mv  ${FWCDRDIR}/process/proc_${d}/* ${ERRORDIR}/

    echo "Remove Already Splited files fwcdr_${d}_split*" | tee -a ${PROCLOG}
    errhandle "Process $d Convert or Split Files error"
    exit 

  else
    mv ${FWCDRDIR}/process/proc_${d}/* ${FWCDRDIR}/complete
  fi
##  done
}&
done
wait_subproc

### Error file checking ###
res=`find ${FWCDRDIR}/error -maxdepth 1 -name "fwcdr*gz"|wc -l`
if [ $res -ne 0 ]; then
  mv ${FWCDRDIR}/error/fwcdr*gz ${FWCDRDIR}/error/err_backup
  echo "AWK convert file failed"|tee -a $LOGFILE
  errhandle "AWK convert file failed"
  exit 1
fi
### ###

cat /dev/null >${TMPDIR}/dw_load_fwcdr.sql
chgdir ${FWCDRDIR}
for cvt_file_name in `ls fwcdr*.gz`
do
        start_date=`echo $cvt_file_name | awk -F'_' '{print $3}'`

        echo "alter table h_fw_cdr add if not exists partition (start_date='$start_date'); " >>${TMPDIR}/dw_load_fwcdr.sql
        echo "load data LOCAL inpath '${FWCDRDIR}/${cvt_file_name}' into table h_fw_cdr partition(start_date='$start_date');" >>${TMPDIR}/dw_load_fwcdr.sql
done

echo "Start run hive sql `date`"| tee -a $LOGFILE
runhivesql ${TMPDIR}/dw_load_fwcdr.sql

cleanup "${jobmsg}"












