[eloisewu@bigdataetl01 script]$ cat dw_cf_convert.pl
#!/usr/bin/perl
#
# $Locker$
#
# $Log$
#use DBI;
perl $SCRIPTDIR/dw_cf_convert.pl ${CFDIR}/process/proc_$d ${CVTTIME} ${PROCLIST} ${PROCBAD} 200000000 CF proc_$d

use File::Basename;
use Time::Local;

unshift(@INC, "/home/etladm/script");

## Install Environment ##
my $file;
my @dirc;
my $dir = $ARGV[0];
$MarkTime = $ARGV[1];
$cvtlog = $ARGV[2];
my $badfile = $ARGV[3];
my $file_count = $ARGV[4];
my $file_prefix = $ARGV[5];
my $fold_prefix = $ARGV[6];

my $cnt=0;
my $filecnt=1;
my %ary_fh;
my %ary_cnt;
my %ary_name;
my $fh;


## write down which file be converted to cvtlist ##
open CVTLIST, ">> $cvtlog" or die "ERROR - Could not open $cvtlog\n";

opendir(DIR,$dir) or die "Can't open $dir $!\n";
@dirc = readdir(DIR);

foreach $file(@dirc)
{
    #next if $file =~ /^\./;
    next if $file !~ /DAT.gz$/;
    $srcfile = "$dir/$file";

    $filename = basename($srcfile);
    $filename =~ s/\s//g;

    print CVTLIST $filename."\n";
    #open OUT, ">> $outfile" or die "unable to open $outfile $!\n";
    open BAD, ">> $badfile" or die "unable to open $badfile $!\n";
    open IN, "gzip -dc $srcfile |" or die "Can't open $filename\n";

    ###Src file CF Structure###
    my ($F_RECORD_TYPE,$F_IMSI,$F_DIAL_DIGIT,$F_REMARK0,$F_CALL_START_DATE,$F_CALL_START_TIME,$F_DURATION,$F_REMARK1,$F_FORWARD_NO,$F_CALLER_NO,$F_REMARK2,$F_MSISDN);

    ###Dest Table Structure ###
    my ($TBL_RECORD_TYPE,$TBL_IMSI,$TBL_DIAL_DIGIT,$TBL_REMARK0,$TBL_CALL_START_DATE,$TBL_CALL_START_TIME,$TBL_DURATION,$TBL_REMARK1,$TBL_FORWARD_NO,$TBL_CALLER_NO,$TBL_REMARK2,$TBL_MSISDN);

    $TBL_RECORD_TYPE = \$F_RECORD_TYPE;
    $TBL_IMSI = \$F_IMSI;
    $TBL_DIAL_DIGIT = \$F_DIAL_DIGIT;
    $TBL_REMARK0 = \$F_REMARK0;
    $TBL_CALL_START_DATE = \$F_CALL_START_DATE;
    $TBL_CALL_START_TIME = \$F_CALL_START_TIME;
    $TBL_DURATION = \$F_DURATION;
    $TBL_REMARK1 = \$F_REMARK1;
    $TBL_FORWARD_NO = \$F_FORWARD_NO;
    $TBL_CALLER_NO = \$F_CALLER_NO;
    $TBL_REMARK2 = \$F_REMARK2;
    $TBL_MSISDN = \$F_MSISDN;
 
    while ( $Line=<IN> )
    {
        ### checking ###
        chomp($line);
        if (($Line =~ /^[10]/) || ($Line =~ /^[90]/))
        {
          next;
        }else {
        ($F_RECORD_TYPE,$F_IMSI,$F_DIAL_DIGIT,$F_REMARK0,$F_CALL_START_DATE,$F_CALL_START_TIME,$F_DURATION,$F_REMARK1,$F_FORWARD_NO,$F_CALLER_NO,$F_REMARK2,$F_MSISDN)=unpack A2A15A24A35A6A6A6A62A18A18A90A18,$Line;

        trim(\$F_RECORD_TYPE);
        trim(\$F_IMSI);
        trim(\$F_DIAL_DIGIT);
        trim(\$F_REMARK0);
        trim(\$F_CALL_START_DATE);
        trim(\$F_CALL_START_TIME);
        trim(\$F_DURATION);
        trim(\$F_REMARK1);
        trim(\$F_FORWARD_NO);
        trim(\$F_CALLER_NO);
        trim(\$F_REMARK2);
        trim(\$F_MSISDN);

        if(length($F_CALL_START_DATE)<8) {
          $filedate="20".$F_CALL_START_DATE;
        }

        #if (&ChkDate($TBL_CALL_START_DATE)!=1){
        #        print OUT $Line."\n";
        #        next;
        #}

        if (length($$TBL_CALL_START_DATE)==6)
        {
                 
                if( $ary_cnt{$filedate} >= $file_count ){
                        delete($ary_cnt{$filedate});
                        $ary_cnt{$filedate}=$cnt;

                        $filenamecnt=$ary_name{$filedate};
                        $filenamecnt +=1;
                        if($filenamecnt<10){
                                $filenamecnt="0$filenamecnt";
                        }
                        $ary_name{$filedate}=$filenamecnt;
                        close($ary_fh{$filedate});
                        $file_time=`date +'%Y%m%d%H%M%S'`;
                        chomp($file_time);
#open("$ary_fh{$filedate}","|gzip > $targetdir/$file_prefix\_$filedate\_$file_time\_$filenamecnt.gz");
open("$ary_fh{$filedate}","|gzip > /app/HDSBAT/cvtdata/CF_CDR/$file_prefix\_$filedate\_$file_time\_$filenamecnt\_$fold_prefix.gz");
                }
                if(!defined($ary_cnt{$filedate})){
                        $ary_cnt{$filedate} = $cnt;
                }
                if(!defined($ary_name{$filedate})){
                        $ary_name{$filedate} = $filecnt;
                }
                if (!defined($ary_fh{$filedate})){
                        $ary_fh{$filedate} = $filedate;
                        $filenamecnt="0$filecnt";
                        $file_time=`date +'%Y%m%d%H%M%S'`;
                        chomp($file_time);
#open("$ary_fh{$filedate}","|gzip > $targetdir/$file_prefix\_$filedate\_$file_time\_$filenamecnt.gz");
open("$ary_fh{$filedate}","|gzip > /app/HDSBAT/cvtdata/CF_CDR/$file_prefix\_$filedate\_$file_time\_$filenamecnt\_$fold_prefix.gz");
                }
                $fh=$ary_fh{$filedate};
                $rocord_cnt=$ary_cnt{$filedate};
                $rocord_cnt +=1;
                delete($ary_cnt{$filedate});
                $ary_cnt{$filedate}=$rocord_cnt;

                print $fh "$$TBL_RECORD_TYPE"."|"."$$TBL_IMSI"."|"."$$TBL_DIAL_DIGIT"."|"."$$TBL_CALL_START_DATE"."|"."$$TBL_CALL_START_TIME"."|"."$$TBL_DURATION"."|"."$$TBL_FORWARD_NO"."|"."$$TBL_CALLER_NO"."|"."$$TBL_MSISDN"."\n";
        }
        else
        {
                print BAD "$$TBL_RECORD_TYPE"."|"."$$TBL_IMSI"."|"."$$TBL_DIAL_DIGIT"."|"."$$TBL_CALL_START_DATE"."|"."$$TBL_CALL_START_TIME"."|"."$$TBL_DURATION"."|"."$$TBL_FORWARD_NO"."|"."$$TBL_CALLER_NO"."|"."$$TBL_MSISDN"."\n";
        }

    }

}

close(IN);
close(BAD);
close(OUT);
}

for $k (keys %ary_fh){
    close($k);
    close($ary_fh{$filedate});
}
close CVTLIST;
closedir(DIR);


############################# sub function ############################
sub trim
{
    my ($res)=@_;
    $$res =~s/^[\s]+//go;
    $$res =~s/[\s]+$//go;
}

### 010.000.000.001 =>10.0.0.1###
sub FmtIP
{
    my ($srcIP)=@_;
    $$srcIP= (substr($$srcIP,0,3)+0).".".(substr($$srcIP,3,3)+0).".".(substr($$srcIP,6,3)+0).".".(substr($$srcIP,9,3)+0);
}

sub FmtDate
{
    my($dstr)=@_;
    $$dstr=substr($$dstr,0,4)."-".substr($$dstr,4,2)."-".substr($$dstr,6,2);
}

##if $$para is not a int then set as 0 else set as original
sub FmtInt
{
    my ($para)=@_;
    if (!($$para =~ /^\d+$/o))
    {
        $$para=0;
    }
        $$para += 0;
}

sub ChkDate
{
    my ($para)= @_;
    if ($$para =~ /20[0-9][0-9]\-(0[1-9]|10|11|12)\-([0-2][1-9]|10|20|30|31)/o)
    {
        return 1;
    }
    else
    {
        return -1;
    }
}

