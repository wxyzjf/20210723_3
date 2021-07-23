[eloisewu@bigdataetl01 script]$ cat dw_load_cf_cdr.sh
export DWHOME=${DWHOME:-/app/HDSBAT}
export SCRIPTDIR=${DWHOME}/script

scriptname=`basename $0 |  cut -f1 -d'.'`
. ${SCRIPTDIR}/dw_master_hds.sh $scriptname N

jobmsg="Loading Call Forward CDR File"

startup "${jobmsg}"

getdate 0 optdate "%Y%m%d"
getdate 14 clsdate "%Y%m%d"
CFDIR=${DATADIR}/CF_CDR

PROC_NUM=1

## create process dir ##
for p in `seq $PROC_NUM`;
do
if [ $p -le $PROC_NUM ];then
  if [ ! -d "${CFDIR}/process/proc_${p}" ];then
    mkdir -p ${CFDIR}/process/proc_${p}
  fi
fi
done  

test -d ${CFDIR}/complete/$optdate || mkdir -p ${CFDIR}/complete/$optdate

#### Housekeep files ####
##rm -rf ${CFDIR}/complete/$clsdate

## Clean Up Last Round Loading Files ##
rm ${CFDIR}/cf_proc_*
rm ${CFDIR}/CF*.gz

echo "[Step 1 Prepare Dispatch Files  .... ]  `date`" | tee -a $LOGFILE

dispatch_file ${PROC_NUM} ${INPUTDIR}/CF_CDR ${CFDIR}/process "GSMOWNCF*DAT.gz"
function dispatch_file_head_filter {
        MVprocnum=$1
        MVsrcdir=$2
        MVdestdir=$3
        MVpattern=$4
        MVfilter=$5
        MVhead=$6

        MVproc_cnt=0;
        for fd in `seq $MVprocnum`
        do
                if [ ! -d ${MVdestdir}/proc_${fd} ];then
                        mkdir ${MVdestdir}/proc_${fd}
                        if [ $? -ne 0 ];then
                                echo "failed to create directory $destdir/proc_$fd"|tee -a $LOGFILE
                                exit 0
                        fi
                fi
        done
        ##for f in `ls -S $MVsrcdir/$MVpattern`
        chgdir $MVsrcdir
        for f in `find $MVsrcdir  -maxdepth 1 -name "$MVpattern"|grep -v "$MVfilter"|head -n${MVhead}`
        do
                MVdcnt=`expr $MVproc_cnt % $MVprocnum + 1`
                MVproc_cnt=`expr $MVproc_cnt + 1`
                mv $f ${MVdestdir}/proc_${MVdcnt}
                if [ $? -ne 0 ];then
                        echo "Dispatch File error"|tee -a $LOGFILE
                        exit 0
                fi
        done
        echo "Dispatch File Finish"|tee -a $LOGFILE
}


## polling data files to each process dir ##

#dispatch_file_head ${PROC_NUM} ${INPUTDIR}/CF_CDR ${CFDIR}/process "GSMOWNCF*DAT.gz" 10000
dispatch_file ${PROC_NUM} ${INPUTDIR}/CF_CDR ${CFDIR}/process "GSMOWNCF*DAT.gz"

echo "[Step 1.1 convert Files  .... ]  `date`" | tee -a $LOGFILE

## Convert & Split Files ##

CVTTIME=`date +'%Y%m%d%H%M%S'`

for d in `seq $PROC_NUM`
do
  {
     PROCLOG=${LOGDIR}/cf_proc_$d.log
     PROCLIST=${CFDIR}/cf_proc_$d.lst
     PROCBAD=${CFDIR}/cf_proc_$d.bad
     cat /dev/null > ${PROCBAD}
     cat /dev/null > ${PROCLOG}
     cat /dev/null > ${PROCLIST}

     perl $SCRIPTDIR/dw_cf_convert.pl ${CFDIR}/process/proc_$d ${CVTTIME} ${PROCLIST} ${PROCBAD} 200000000 CF proc_$d

     if [ $? -ne 0 ];then
       echo "Process $d Convert or split files failed" | tee -a ${PROCLOG}
       echo "Move error files to ${CFDIR}/error " | tee -a ${PROCLOG}

       mv  ${CFDIR}/process/proc_${d}/*.gz ${CFDIR}/error

       echo "Remove Already Splited files cf_${d}_split*" | tee -a ${PROCLOG}
       errhandle "Process $d Convert or Split Files error"
       exit 1
     else
       mv ${CFDIR}/process/proc_${d}/*.gz ${CFDIR}/complete/$optdate
     fi
     cp ${PROCLIST} ${LOGDIR}/cf_proc_$d.${currdate}.log
  }&
done
wait_subproc

echo "[Step 1.1 End of convert Files  .... ]  `date`" | tee -a $LOGFILE


echo "[Step 1.2 Prepare hive sql  .... ]  `date`" | tee -a $LOGFILE

cat /dev/null >${TMPDIR}/dw_load_cf.sql

chgdir ${CFDIR}
for cvt_file_name in `ls CF*.gz|sort`
do
        part_time=`echo $cvt_file_name|awk -F '_' '{print $2}'`
        echo "alter table h_cf_cdr add if not exists partition (part_key='$part_time') LOCATION '/HDS_VOL_HIVE/cf/part_key=$part_time'; " >>${TMPDIR}/dw_load_cf.sql
        echo "load data LOCAL inpath '/app/HDSBAT/cvtdata/CF_CDR/$cvt_file_name' into table h_cf_cdr partition(part_key='$part_time');" >>${TMPDIR}/dw_load_cf.sql
done

echo "[Step 1.3 runhivesql .... ]  `date`" | tee -a $LOGFILE

runhivesql ${TMPDIR}/dw_load_cf.sql

echo "Job Finish On `date`"|tee -a $LOGFILE

cleanup "${jobmsg}"
exit 0

