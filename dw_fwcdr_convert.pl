/app/HDSBAT/script [34]> cat dw_fwcdr_convert.pl
#!/usr/bin/perl
#
use File::Basename;
use Time::Local;

unshift(@INC, "/home/etladm/script");

  perl $SCRIPTDIR/dw_fwcdr_convert.pl ${FWCDRDIR}/process/proc_${d} ${PROCLIST} ${PROCBAD} \
       | split -d -a3 -l 180000000 --filter='gzip >> $FILE.gz' - ${FWCDRDIR}/fwcdr_ldr_${filedate}_${cvtdate}_p${d}_

#die "Usage : $0 <filename>  <badfile>" if @ARGV != 2;

my $file;
my @dirc;
my $dir = $ARGV[0];
#my $batchno=$ARGV[1];
my $delimiter = '\|';
#my $MarkTime = `date +'%Y-%m-%d %H:%M:%S'` or die "fetch date error";
#$MarkTime = substr($MarkTime,0,19);
#$MarkTime = $ARGV[2] if defined $ARGV[2];
my $cvtlog = "dw_fwcdr_cdr.lst";
$cvtlog = $ARGV[1] if defined $ARGV[1];
my $badfile = $ARGV[2];

my ($new_src,$src,@ary_src,$new_start_datetime);

## write down which file be converted to cvtlist##
open CVTLIST, ">>$cvtlog" or die "ERROR - Could not open $cvtlog\n";

opendir(DIR,$dir) or die "Can't open $dir !";
@dirc = readdir(DIR);

###row number ####
my $rno = 0;
foreach $file(@dirc)
{
  next if $file =~ /^\./;
  next if $file !~ /^/;
  next if $file !~ /.gz$/;
  $xmlfile = "$dir/$file";

  $filename = basename($xmlfile);
  $filename =~ s/\s//g;

  print CVTLIST $filename."\n";
  open BAD, ">>$badfile" or die "unable to open $badfile $!";
  open IN, "gzip -dc $xmlfile |" or die "Can't open $filename\n";
  
  my ($f_batchno,$f_send_volume,$f_receive_volume,$f_start_datetime,$f_service,$f_src,$f_dst);
  my ($tbl_batchno,$tbl_send_volume,$tbl_receive_volume,$tbl_start_date,$tbl_start_time,$tbl_service,$tbl_src,$tbl_dst);
  
  my $tmp_start_date = "";
  my $tmp_start_time = "";
  my $tmp_null = "";
  #$tbl_batchno = \$f_batchno;
  $tbl_send_volume = \$f_send_volume;
  $tbl_receive_volume = \$f_receive_volume;
  $tbl_start_date = \$tmp_start_date;
  $tbl_start_time = \$tmp_start_time; 
  $tbl_service = \$f_service;
  $tbl_src = \$f_src;
  $tbl_dst = \$f_dst;
  
  while ( $line=<IN> )
  {  
    patch_quota(\$line);
    chomp $line;
    ($f_batchno,$f_send_volume,$f_receive_volume,$f_start_datetime,$f_service,$f_src,$f_dst) = split $delimiter,$line;
  
    $tmp_start_date = substr($f_start_datetime,0,10);
    $tmp_start_time = substr($f_start_datetime,11,8);
    $tmp_start_time =~ s/://g;

    #### re-create batchno ####
    $new_src="";
    @ary_src=split('\.',$f_src);
    foreach $str (@ary_src){
      $new_src=$new_src.lpad($str,3).".";
    }
    $new_src=~s/.$//;

    $new_start_datetime=FmtDT($f_start_datetime);

    $tbl_batchno=$new_src."_".$new_start_datetime;

    #####delete the row delimiter###
    ##chomp($$tbl_content_type);

    $rno = $rno + 1;
    if ($$tbl_start_date =~ /^20[0-9][0-9]-(0[1-9]|10|11|12)-(0[1-9]|[1-2][0-9]|3[0-1])$/o )
    {

      #print $tbl_batchno."|".$rno."|".$$tbl_send_volume."|".$$tbl_receive_volume."|".$$tbl_start_date."|".$$tbl_start_time."|".$$tbl_service."|".$$tbl_src."|".$$tbl_dst."\n";
      #print $tbl_batchno."_".$$tbl_dst."|".$f_start_datetime."|".$$tbl_src."|{\"send_volume\":\"".$$tbl_send_volume."\",\"receive_volume\":\"".$$tbl_receive_volume."\",\"service\":\"".$$tbl_service."\",\"dst\":\"".$$tbl_dst."\"}\n";
      
      
      print $tbl_batchno."_".$$tbl_dst."|".$f_start_datetime."|".$$tbl_src."|".$$tbl_send_volume."|".$$tbl_receive_volume."|".$$tbl_service."|".$$tbl_dst."\n";
      
      #($f_batchno,$f_send_volume,$f_receive_volume,$f_start_datetime,$f_service,$f_src,$f_dst) = split $delimiter,$line;
      
    }
    else 
    {
      #print BAD $tbl_batchno."|".$rno."|".$$tbl_send_volume."|".$$tbl_receive_volume."|".$$tbl_start_date."|".$$tbl_start_time."|".$$tbl_service."|".$$tbl_src."|".$$tbl_dst."\n";
      print BAD $tbl_batchno."_".$$tbl_dst."|".$f_start_datetime."|".$$tbl_src."|{\"send_volume\":\"".$$tbl_send_volume."\",\"receive_volume\":\"".$$tbl_receive_volume."\",\"service\":\"".$$tbl_service."\",\"dst\":\"".$$tbl_dst."\"}\n";
    }    
  }
  
  close IN;
  close BAD;

}

close CVTLIST;
closedir(DIR);

########### Define the sub functions ###########
sub trim
{
  my ($res)=@_;
  $$res =~s/^[\s]+//go;
  $$res =~s/[\s]+$//go;
}
sub patch_quota
{
  my ($res)=@_;
  $$res =~ s/\"//go;
}
sub lpad {
  my ($num, $len) = @_;
  return '0' x ($len - length $num) . $num;
}
sub FmtDT
{
    my($dstr)=@_;
    $dstr=substr($dstr,0,4).substr($dstr,5,2).substr($dstr,8,2).substr($dstr,11,2).substr($dstr,14,2).substr($dstr,17,2);
}
