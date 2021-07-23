[mapr@bigdatamr10 script]$ cat dw_load_fwcdr_parquet.sh
#!/bin/ksh


export HOMEDIR=/home/mapr/el/test/bigdataetl02/daily

LOGDIR=${HOMEDIR}/log
scriptname=`basename $0 |  cut -f1 -d'.'`
currdate=`date +"%Y%m%d%H%M%S"`
LOGFILE=${LOGDIR}/${scriptname}_${currdate}.log
ERRFILE=${LOGDIR}/${scriptname}_${currdate}.err
SCRIPTDIR=${HOMEDIR}/script


jobmsg="${scriptname} Loading"
echo "`date`:$jobmsg" | tee -a $LOGFILE

echo "spark-submit --master yarn  ${SCRIPTDIR}/dw_load_fwcdr_parquet.py >> ${LOGFILE}" >> ${LOGFILE}

#echo "$? : `date`" | tee -a ${LOGFILE}

/opt/mapr/spark/spark-2.3.3/bin/spark-submit --master yarn  ${SCRIPTDIR}/dw_load_fwcdr_parquet.py >> ${LOGFILE}

if [ $? -ne 0 ];then
        echo "Load fwcdr Error at `date`\n" | tee -a ${LOGFILE}
        echo "Failure for ${scriptname} : Load fwcdr Error at `date`\n" | tee -a ${ERRFILE}
        maillist="eloise_wu@smartone.com"
        mailx -s "Failure for loading fwcdr" ${maillist} <<EOF
            Please check the loading job ${scriptname}, thanks.
EOF
        exit 1
fi
echo "Load fwcdr Success at `date`\n" | tee -a ${LOGFILE}
#delete hdfs file

logdate=`date -d "`date` -7 day " +%Y%m%d`
#rm ${LOGDIR}/${scriptname}_${logdate}*.log
#rm ${LOGDIR}/${scriptname}_${logdate}*.err

echo "${jobmsg} Was Finished On `date`" | tee -a ${LOGFILE}




