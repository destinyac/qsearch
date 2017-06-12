#/usr/bin/perl -w 
use Time::Local qw(timelocal);
use POSIX qw(strftime);   

our @timePatten = (qw(^.*?(?<yyyy>\d+)\-(?<mm>\d+)\-(?<dd>\d+)\s+(?<hh>\d+):(?<ii>\d+):(?<ss>\d+)\.(?<ccc>\d+).*),qw($+{yyyy}-$+{mm}-$+{dd} $+{hh}:$+{ii}:$+{ss}.$+{ccc}));
our $maxTimes = 0;
our $searchLogRange = 20971520;
our $searchFileType = 0 ;
our $maxSearchLine = 10000;

sub getMaybeFile
{
    my($baseFileName,$time) = @_;

    my @result = ();
    my ($sec,$min,$hour,$day,$mon,$year) = localtime($time);
    if( $min <= 3 ){
        push(@result,$baseFileName.'.'.strftime("%Y%m%d%H", localtime($time-240)));
    }

    if(strftime("%Y%m%d%H", localtime($time)) != strftime("%Y%m%d%H", localtime())){
        push(@result,$baseFileName. '.'.strftime("%Y%m%d%H", localtime($time)));
    }else{
        push(@result,$baseFileName);
    }

    if(strftime("%Y%m%d%H", localtime()) != strftime("%Y%m%d%H", localtime(time()-300)) && 
        (!grep { $_ eq $baseFileName } @result)
      ){
        push(@result,$baseFileName);
    }
    return @result;
}

sub mystrtomicotime
{
    my($strTime) = @_;
    my($year,$mon,$mday,$hour,$min,$sec,$micosec) = split /\-|\s|:|\./,$strTime; 
    unless( $mon =~ /^\d+$/ ){
        my @monthName =('January','February','March','April','May','June','July','August','September','October','November','December');
        for (my $index = 0; $index < @monthName; $index++) {
            if($monthName[$index]=~/^$mon/i){
                $mon = $index + 1;
                last;
            }
        }
    }
    return 1000 * timelocal($sec,$min,$hour,$mday,$mon-1,$year) + $micosec; 
}

sub getRealTime
{
    my($fp,$pos) = @_;
    my @match=();
    seek $fp ,$pos,0 ;
    readline($fp);
    my $str = readline($fp);    
    return 0 unless ($str =~s/$timePatten[0]/$+{yyyy}-$+{mm}-$+{dd} $+{hh}:$+{ii}:$+{ss}.$+{ccc}/);
    return mystrtomicotime($str) ;
}

sub leftRightSearch
{
    my($fp,$leftTime,$leftPostion,$rightTime,$rightPostion,$searchTime) = @_;
    our $maxTimes;
    if( ($rightPostion  - $leftPostion < 1048576) || ($maxTimes++ >= 20) ){
            return ($leftPostion,$rightPostion);
    }
    my $perSize = ($rightPostion - $leftPostion)/($rightTime - $leftTime);
    my $possiblePosition =  $leftPostion + int($perSize*(($searchTime - $leftTime)));
    my $possibleTime =  getRealTime($fp,$possiblePosition);
    if($possibleTime == 0 ){
        return ($rightPostion,$rightPostion) if($possiblePosition > $rightPostion );
        return ($leftPostion,$leftPostion)   if($possiblePosition < $leftPostion );
        return ($possiblePosition,$possiblePosition);
    }

    my $tmpStep = int($perSize * ($searchTime- $possibleTime)*2);
    my $maybePosRealTime = getRealTime($fp,$possiblePosition + $tmpStep);

    #快速定位法：search_time 在 定位的maybePosRealTime 和 possibleTime 中间时,
    if(abs($maybePosRealTime - $searchTime) + abs($possibleTime - $searchTime) == abs($possibleTime - $maybePosRealTime) ){
        if($maybePosRealTime > $possibleTime ){
            return leftRightSearch($fp,$possibleTime,$possiblePosition,$maybePosRealTime,$possiblePosition + $tmpStep,$searchTime);
        }
        else{
            return leftRightSearch($fp,$maybePosRealTime,$possiblePosition + $tmpStep,$possibleTime,$possiblePosition,$searchTime);
        }
    }

    #推测失败，则退化为严格二分查找
    my $midPosition =  int(($leftPostion + $rightPostion)/2);
    my $midTime =  getRealTime($fp,$midPosition);
    if($midTime >= $searchTime ){
        return leftRightSearch($fp,$leftTime,$leftPostion,$midTime,$midPosition,$searchTime);
    }
    return leftRightSearch($fp,$midTime,$midPosition,$rightTime,$rightPostion,$searchTime);
}

sub computeLogPositon
{
    my($time,$fileName) = @_;
    my @fileStat = stat($fileName);
    my $fsize= $fileStat[7];
    open (FILE, $fileName) || die ("Could not open file:".$fileName);

    my $logTime = $time;
    #获取文件最后一行的时间
    my $lastLine = `tail -n1 $fileName`;
    return 0 unless ($lastLine =~s/$timePatten[0]/$+{yyyy}-$+{mm}-$+{dd} $+{hh}:$+{ii}:$+{ss}.$+{ccc}/);
    my $curTime = mystrtomicotime($lastLine);

    my $firstLine = `head -n50 $fileName |tail  -n1`;
    return 0 unless ($firstLine =~s/$timePatten[0]/$+{yyyy}-$+{mm}-$+{dd} $+{hh}:$+{ii}:$+{ss}.$+{ccc}/);
    my $firstTime = mystrtomicotime($firstLine);
    my @position = leftRightSearch(*FILE,$firstTime,0,$curTime,$fsize,$logTime);
    close(FILE);
    return @position;
}

sub quickSearchFile
{
    my ($fileName,$command,$time,$preSearchLogSize) = @_;
    my @pos = computeLogPositon($time,$fileName);
    unless(@pos){
        return 'error';
    }
    $startOffset = int( $pos[0] - $preSearchLogSize/2 );
    if($startOffset < 0 ){
        $startOffset = 0;
    }
    my $searchTlp = "tail %s -c +".($startOffset)."|head -c ".(($pos[1]-$pos[0])+$preSearchLogSize).'|'.$command.'|head -n'.$maxSearchLine ;
    my $searchCmd  = sprintf($searchTlp,$fileName);
    return `$searchCmd`;
}

if( @ARGV < 3){
    print '
     perl qsearch.pl fileName command strTime [searchFileType searchLogRange logTimePatten ] 
      help:
        fileName       : search file name 
        command        : exec cmd . 
        strTime        : search time ,time\'s fmt is "YYYY-MM-DD HH:II:SS.xxx"
        searchFileType : 0（defautl）:auto detect maby file ; otherwise will not .
        searchLogRange : search log range size 
                         default : 20971520
        logTimePatten  : match the time in the log file .
                         time fmt must is "YYYY-MM-DD HH:II:SS.xxx"
      usage:
         perl qsearch.pl /home/log.log "grep request_info" "2017-11-03 09:01:01.023"
         perl qsearch.pl /home/log.log "grep request_info|wc -l"  "2017-11-03 09:01:01.023" 1 20971520 
';
    exit;
}

my @outout =();
my $lineStr =''; 

$searchFileType = int($ARGV[3]) if(defined $ARGV[3]);
$searchLogRange = int($ARGV[4]) if((defined $ARGV[4]) && int($ARGV[4]) > 0  );
$timePatten[0] = $ARGV[5] if((defined $ARGV[5]) );


if( $searchFileType == 0 ){
    my @searchFiles =  getMaybeFile($ARGV[0],mystrtomicotime($ARGV[2])/1000);
    foreach my $searchFile  (@searchFiles) {
        next unless $searchFile;
        @outout= quickSearchFile($searchFile,$ARGV[1],mystrtomicotime($ARGV[2]),$searchLogRange);
        foreach $lineStr (@outout) {  
            print $searchFile,":",$lineStr;
        }
    }
}
else {
    @outout= quickSearchFile($ARGV[0],$ARGV[1],mystrtomicotime($ARGV[2]),$searchLogRange);
    foreach $lineStr (@outout) {  
        print $ARGV[0],":",$lineStr;
    }
}
