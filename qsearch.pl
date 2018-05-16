#/usr/bin/perl -w 
use IPC::Open2;
use threads;
use threads::shared;
use Thread::Semaphore;
use Time::Local qw(timelocal);
use Time::HiRes qw/time/;
use POSIX qw(strftime);
use Sys::Hostname;
use POSIX;
use IO::Compress::Gzip qw(gzip $GzipError) ;
use IO::Uncompress::Gunzip qw(gunzip $GunzipError) ;
use IO::Compress::Gzip qw(:flush :level :strategy) ;

our $gzIndexInfo;
our @timePatten = (qw(^.*?(?<yyyy>\d+)\-(?<mm>\d+)\-(?<dd>\d+)\s+(?<hh>\d+):(?<ii>\d+):(?<ss>\d+)\.(?<ccc>\d+).*),qw($+{yyyy}-$+{mm}-$+{dd} $+{hh}:$+{ii}:$+{ss}.$+{ccc}));
our $maxTimes = 0;
our $searchLogRange = 20971520;
our $searchFileType = 0 ;
our $maxSearchLine = 10000;

#############bloomfilter funtion ################
sub bloomFilterCheck{
        my($bloomMask,$m,$k,$key) = @_;
        my $h = time33($key);
        my $delta = ($h >> 17) | ($h << 15);  # Rotate right 17 bits
        for (my $j = 0; $j < $k; $j++) {
            return 0 if(vec( $bloomMask, $h % $m, 1 )  == 0 );
            $h += $delta;
        }
        return 1;
}

sub bloomFilterCreate{
        my($keys,$p,$maxlength) = @_;
        my $keynum = @$keys;
        my $lnp = log($p);
        my $m = ceil(-$keynum*$lnp/(0.4804));
        my $k = ceil(-$lnp/0.6931);
        $m = $maxlength if($m > $maxlength );
        my $bloomMask = pack( "b*", '0' x $m );
        foreach my $key ( @$keys ) {
                my $h = time33($key);
                my $delta = ($h >> 17) | ($h << 15);  # Rotate right 17 bits
                for (my $j = 0; $j < $k; $j++) {
                    vec( $bloomMask, $h % $m, 1 ) = 1;
                    $h += $delta;
                }
        }
        return ($bloomMask,$m,$k);
}

sub time33 {
        my ($string) = @_;
        my $hval=0;
        foreach my $c (unpack('L*', $string)) {
           $hval= (($hval <<5) + $hval + $c) & 0xffffffff;
        }
        return $hval;
}
#############bloomfilter funtion ################

#############gzip random access funtion ################
our  $semaphore;
sub GzFastReadAt{
    my $fp        = shift;
    my $indexInfo = shift;
    my $offset    = shift;
    my $size      = shift;
    my $maxThread = shift;
    $maxThread = 1  if($maxThread == 0 );
    $maxThread = 10 if($maxThread > 10 );

    return '' if($offset > $indexInfo->{'filesize'});
    my $offsetInfo        = $indexInfo->{'offsetInfo'};
    my $sizeInfo          = $indexInfo->{'sizeInfo'};

    my $startPachnum      = ceil(($offset+1)/$indexInfo->{'pachsize'})-1;
    my $endPachnum        = ceil(($offset+$size)/$indexInfo->{'pachsize'})-1;
    $endPachnum = 0  if ($endPachnum < 0 ); 
    my $fileBaseOffset    = $indexInfo->{'pachsize'} * $startPachnum;

#print ($startPachnum,"\t",$endPachnum,"\n");
    my $thread;
    my $pachnum    = $startPachnum;
    my @res:shared = 1;
    my $index      = 0;
    $semaphore     = new Thread::Semaphore($maxThread);

    my $splitThreadSize = ceil( ($endPachnum  - $startPachnum)/$maxThread) ;
    my $compressdSize   = 0;
    my $unCompressdSize = 0;
    my $resnum          = 0;

    $fp->seek(@$offsetInfo[$pachnum],0);
    for($pachnum = $startPachnum,$index = 0 ; $pachnum <= $endPachnum ; $pachnum ++,$index ++ )
    {
        $compressdSize   += @$sizeInfo[$pachnum];
        $unCompressdSize += $indexInfo->{'pachsize'};
        if( $index !=0 && ($index % $splitThreadSize) == 0 ){
            my $buffer;
            $semaphore->down();
            $fp->read($buffer,$compressdSize);
            $thread=threads->new(\&_GzUngzipbuffer,$buffer,$unCompressdSize,$res[$resnum++]); 
            $thread->detach();  
            $compressdSize   = 0; 
            $unCompressdSize = 0;
        }
    }

    if($compressdSize != 0 ){
        my $buffer;
        $semaphore->down();
        $fp->read($buffer,$compressdSize);
        $thread=threads->new(\&_GzUngzipbuffer,$buffer,$unCompressdSize,$res[$resnum++]); 
        $thread->detach();  
        $compressdSize   = 0; 
        $unCompressdSize = 0;
    }

    my $num=0;
    while($num<$maxThread)
    {    
        $semaphore->down();
        $num++;
    }

    return @res;
    ####待完善函数功能##
    #substr($res[0],0,$offset - $fileBaseOffset) = '';
    #return join('',@res);
}

sub _GzUngzipbuffer{
    my $buffer = shift;
    my $size   = shift;
    my $res    = \$_[0];
    my $zo = new IO::Uncompress::Gunzip \$buffer ,(-Append => 1,-MultiStream => 1,-BlockSize=>4096)
            or ( print  "gunzip failed: $GunzipError\n"  and return '' );
    $zo->read($$res, $size);
    $semaphore->up();
    $zo->close();
    return $$res;
}

sub GzReadAt{
    my $fp        = shift;
    my $indexInfo = shift;
    my $offset    = shift;
    my $size      = shift;


    return '' if($offset > $indexInfo->{'filesize'});
    my $offsetInfo        = $indexInfo->{'offsetInfo'};
    my $sizeInfo          = $indexInfo->{'sizeInfo'};
    my $pachnum           = ceil(($offset+1)/$indexInfo->{'pachsize'})-1;
    my $fileBaseOffset    = $indexInfo->{'pachsize'} * $pachnum;

    $fp->seek(@$offsetInfo[$pachnum],0);
    my $zo = new IO::Uncompress::Gunzip $fp ,(-Append => 1,-MultiStream => 1,-BlockSize=>4096)
            or ( print  "gunzip failed: $GunzipError\n"  and return '' );

    my $buffer = '';
    $zo->seek($offset - $fileBaseOffset,0);
    $zo->read($buffer, $size);
    $zo->close();
    return $buffer;
}

our %gunzipCache;
sub _GzReadPach{
    my $zo        = shift;
    my $pachsize  = shift;
    my $pachnum   = shift;
    my $buffer    = \$_[0];

    if($gunzipCache{$pachnum}){
        $$buffer .= $gunzipCache{$pachnum};
        return length($gunzipCache{$pachnum});
    }
    my $tmpbuffer;
    $zo->read($tmpbuffer,$pachsize);
    if((scalar keys %gunzipCache) < 20 ){
        $gunzipCache{$pachnum} = $tmpbuffer;
    }
    $$buffer .= $tmpbuffer;
    return length($tmpbuffer);
}

sub GzGetNextLine{
    my $fp        = shift;
    my $indexInfo = shift;
    my $offset    = shift;

    return '' if($offset > $indexInfo->{'filesize'});

    my $offsetInfo    = $indexInfo->{'offsetInfo'};
    my $sizeInfo      = $indexInfo->{'sizeInfo'};
    my $pachnum       = ceil(($offset+1)/$indexInfo->{'pachsize'})-1;
    my $fileBaseOffset= $indexInfo->{'pachsize'} * $pachnum;

    $fp->seek(@$offsetInfo[$pachnum],0);
    my $zo = new IO::Uncompress::Gunzip $fp ,(-Append => 1,-MultiStream => 1,-BlockSize=>4096)
             or ( print  "gunzip failed: $GunzipError\n"  and return '' );

    my $buffer;
    my $flag = 0;
    _GzReadPach($zo,$indexInfo->{'pachsize'},$pachnum++,$buffer);
    substr($buffer, 0, $offset - $fileBaseOffset) =  '';
    do{
        goto EXITWHILE  if ($buffer =~ s/^(.*?[\r\n])//s) ;
    }while(_GzReadPach($zo,$indexInfo->{'pachsize'},$pachnum++,$buffer) > 0 );

EXITWHILE:
    do{
        if ($buffer =~ s/^(.*?[\r\n])//s) {
            $zo->close();
            return $1 
        }
    }while(_GzReadPach($zo,$indexInfo->{'pachsize'},$pachnum++,$buffer) > 0 );

    $zo->close();
    return '';
}

sub GzGetLastLine{
    my $fp        = shift;
    my $indexInfo = shift;

    return '' if($offset > $indexInfo->{'filesize'});

    my $offsetInfo    = $indexInfo->{'offsetInfo'};
    my $sizeInfo      = $indexInfo->{'sizeInfo'};
    my $curPachIndex  = $indexInfo->{'pachcount'}-1;

    my $buffer;
    my $index;
    $fp->seek(@$offsetInfo[$curPachIndex],0);
    my $zo = new IO::Uncompress::Gunzip $fp ,(-Append => 1,-MultiStream => 1,-BlockSize=>4096)
                 or ( print  "gunzip failed: $GunzipError\n"  and return '' );
    _GzReadPach($zo,$indexInfo->{'pachsize'},$curPachIndex,$buffer);
    substr($buffer,length($buffer)-1) ='';

    do{
       goto NEDLABLE if(($index = rindex($buffer,"\n")) != -1);

       $zo->close();
       $curPachIndex --;
       if($curPachIndex >= 0 ){
           $fp->seek(@$offsetInfo[$curPachIndex],0);
           $zo = new IO::Uncompress::Gunzip $fp ,(-Append => 1,-MultiStream => 1,-BlockSize=>4096)
                         or ( print  "gunzip failed: $GunzipError\n"  and return '' );
            _GzReadPach($zo,$indexInfo->{'pachsize'},$curPachIndex,$buffer);
       }
    }while($curPachIndex >= 0 );

NEDLABLE:
    $zo->close();
    return substr($buffer,$index + 1);
}


sub GzGetIndexInfo{
#print(__LINE__,"\t",time(),"\n");
    my $fp = shift;
    my $magicstr    = 'xy!xy';
    #读取第一个giz包内容，找到最后一个gz包的文件偏移量
    $fp->seek(0,0);
    my $zo = new IO::Uncompress::Gunzip $fp 
            or ( print  "gunzip failed: $GunzipError\n"  and return 0 );
    my $indexOffset  = $zo->getHeaderInfo()->{Comment};

    if(substr($indexOffset,0,length($magicstr)) ne $magicstr ) {
        print "magic string is not match\n";
        return 0;
    }
    $indexOffset = substr($indexOffset,length($magicstr));
    $zo->close();
#print(__LINE__,"\t",time(),"\n");

    #读取最后连续的giz包内容，找到索引信息
    my $strGzipInfo ='';
    $fp->seek($indexOffset,0);
    $zo = new IO::Uncompress::Gunzip $fp 
            or die "gunzip failed: $GunzipError\n";
    do{
        $strGzipInfo .= unpack("x4A*",$zo->getHeaderInfo()->{ExtraFieldRaw});
    }while($zo->nextStream());

    return 0 if( 2* length($magicstr) > length($strGzipInfo) ||
                substr($strGzipInfo,0,length($magicstr))  ne $magicstr || 
                substr($strGzipInfo,-length($magicstr))  ne $magicstr
               );
#print(__LINE__,"\t",time(),"\n");

    my @gzipInfo = unpack ( "Q*", substr($strGzipInfo,length($magicstr),length($strGzipInfo)-2*length($magicstr)) );
    $zo->close();
#print(__LINE__,"\t",time(),"\n");

    my %out ;
    $out{'filesize'}  = shift(@gzipInfo);
    $out{'pachsize'}  = shift(@gzipInfo);
    $out{'pachcount'} = shift(@gzipInfo);
    my $offset        = shift(@gzipInfo);shift(@gzipInfo);shift(@gzipInfo);
    my @tempArray     ;
    my $tmpsize       = 0;
    my @offsetInfo ;
    my @sizeInfo ; 
    my @timeIndexInfo ;
    my $curSize ; 
    while(@gzipInfo){
        $curSize = shift(@gzipInfo);
        push(@offsetInfo,$offset+$tmpsize);
        push(@sizeInfo,$curSize);
        push(@timeIndexInfo,shift(@gzipInfo),shift(@gzipInfo));
        $tmpsize += $curSize;
    }
#print(__LINE__,"\t",time(),"\n");
    $out{'offsetInfo'}      = \@offsetInfo;
    $out{'sizeInfo'}        = \@sizeInfo;
    $out{'timeIndexInfo'}   = \@timeIndexInfo;
#print(__LINE__,"\t",time(),"\n");
    return \%out; 
}

sub GzGetPatchBloomFilterInfo{
    my $fp = shift;
    my $indexOffset = shift;
    my $magicstr    = 'xy!xy';
    $fp->seek($indexOffset,0);
    my $zo = new IO::Uncompress::Gunzip $fp 
            or die "gunzip failed: $GunzipError\n";
    return 0 unless ($zo->getHeaderInfo()->{ExtraFieldRaw});
    my $strBloomFilterInfo .= unpack("x4A*",$zo->getHeaderInfo()->{ExtraFieldRaw});

    return 0 if( 2* length($magicstr) > length($strBloomFilterInfo) ||
                substr($strBloomFilterInfo,0,length($magicstr))  ne $magicstr || 
                substr($strBloomFilterInfo,-length($magicstr))  ne $magicstr
               );

    $strBloomFilterInfo = substr($strBloomFilterInfo,length($magicstr),length($strBloomFilterInfo)-2*length($magicstr));
    return split ( /,/ ,$strBloomFilterInfo,3); #m,k,bloomMask
}

sub GzbloomPatchCheck{
    my $fp = shift;
    my $indexOffset = shift;
    my $searchKey = shift;
    return 1 unless $searchKey ;
    my @bloomFilterInfo = GzGetPatchBloomFilterInfo($fp,$indexOffset);
    return 1 unless @bloomFilterInfo ;
    return bloomFilterCheck($bloomFilterInfo[2],$bloomFilterInfo[0],$bloomFilterInfo[1],$searchKey);
}

#############gzip random access funtion ################


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
    my $str ;
    unless($gzIndexInfo){
        seek $fp ,$pos,0 ;
        readline($fp);
        $str = readline($fp);    
    }else{
        $str = GzGetNextLine($fp,$gzIndexInfo,$pos);
    }
    return 0 unless ($str =~s/$timePatten[0]/$+{yyyy}-$+{mm}-$+{dd} $+{hh}:$+{ii}:$+{ss}.$+{ccc}/);
    return mystrtomicotime($str) ;
}

sub leftRightSearch
{
	my($fp,$leftTime,$leftPostion,$rightTime,$rightPostion,$searchTime) = @_;
	our $maxTimes;
#print ($rightPostion,"\t",$leftPostion,"\t",$rightPostion  - $leftPostion,"\n");
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

    my $tmpvalue = 1;
    my $last = 0;
    while($last == 0 ){
        my $tmpSearchPositon = $possiblePosition + int($perSize * ($searchTime- $possibleTime)*$tmpvalue);
        if($tmpSearchPositon > $rightPostion){
            $tmpSearchPositon = $rightPostion;
            $last =1;
        }elsif ($tmpSearchPositon > $rightPostion){
            $tmpSearchPositon = $leftPostion;
            $last =1;
        }
        my $maybePosRealTime = getRealTime($fp,$tmpSearchPositon);

        #快速定位法：search_time 在 定位的maybePosRealTime 和 possibleTime 中间时,
        if(abs($maybePosRealTime - $searchTime) + abs($possibleTime - $searchTime) == abs($possibleTime - $maybePosRealTime) ){
            if($maybePosRealTime > $possibleTime ){
                return leftRightSearch($fp,$possibleTime,$possiblePosition,$maybePosRealTime,$tmpSearchPositon,$searchTime);
            }
            else{
                return leftRightSearch($fp,$maybePosRealTime,$tmpSearchPositon,$possibleTime,$possiblePosition,$searchTime);
            }
        }
        $tmpvalue *= 2;
    }
}

sub computeLogPositon
{
	my($time,$fileName,$file,$fsize,$gzIndexInfo) = @_;
    my $logTime = $time;
    #获取文件最后一行的时间
    my $lastLine ='';
    unless($gzIndexInfo){
       $lastLine = `tail -n1 $fileName`;
    }else{
       $lastLine = GzGetLastLine($file,$gzIndexInfo);
    }
    return 0 unless ($lastLine =~s/$timePatten[0]/$+{yyyy}-$+{mm}-$+{dd} $+{hh}:$+{ii}:$+{ss}.$+{ccc}/);
    my $curTime = mystrtomicotime($lastLine);

    my $firstLine ='';
    unless($gzIndexInfo){
        $firstLine = `head -n50 $fileName |tail  -n1`;
    }else{
        $firstLine = `zcat $fileName |head -n50|tail -n1`;
    }
    return 0 unless ($firstLine =~s/$timePatten[0]/$+{yyyy}-$+{mm}-$+{dd} $+{hh}:$+{ii}:$+{ss}.$+{ccc}/);
    my $firstTime = mystrtomicotime($firstLine);
#    print(__LINE__,"\t",time(),"\n");
    my @position = leftRightSearch($file,$firstTime,0,$curTime,$fsize,$logTime);
#    print(__LINE__,"\t",time(),"\n\n");
    return @position;
}

sub GzComputeLogPosition
{  
   my($time,$gzIndexInfo,$fp,$searchKey) = @_;
   my $offsetInfo    = $gzIndexInfo->{'offsetInfo'};
   my $sizeInfo      = $gzIndexInfo->{'sizeInfo'};
   my $pachcount     = $gzIndexInfo->{'pachcount'};
   my $timeIndexInfo = $gzIndexInfo->{'timeIndexInfo'};
   my $moreTimeSize  = 10000; #后向检索10s
   
   my $startIndex = $pachcount - 1;
   my $endIndex   = $pachcount - 1 ;
   for(my $index =0 ; $index < $pachcount ;$index ++ ){
      if( ($time-$moreTimeSize) < @$timeIndexInfo[$index*2]  ){
          $startIndex = $index  ;
          last;
      }
   }
   
   for(my $index =$pachcount - 1 ; $index >= $startIndex  ;$index -- ){
      if( ($time+$moreTimeSize) > @$timeIndexInfo[$index*2+1] ){
          $endIndex = $index ;
          last;
      } 
   }

   $startIndex = 0               if($startIndex <= 0  ) ;
   $endIndex   = $pachcount -1   if($endIndex >= $pachcount  ) ;

   my @out;
   for(my $index = $startIndex ; $index <= $endIndex ; $index++ ){
      if( defined($searchKey) ){
         if(GzbloomPatchCheck($fp,@$offsetInfo[$index],$searchKey) ){
             push(@out,$index);
             push(@out,++$index) if(GzbloomPatchCheck($fp,@$offsetInfo[$index],$searchKey.'lastone') && $index < $pachcount );
         }
      }else{
         push(@out,$index);
      }
   }
   return @out;
}

sub quickSearchFile
{
	my ($fileName,$command,$time,$preSearchLogSize) = @_;

    my @fileStat = stat($fileName);
    my $fsize= $fileStat[7];
    my $file = new IO::File "<".$fileName;
    $gzIndexInfo = 0;
    unless ( $file ){
        $fileName = $fileName.".gz";
        $file = new IO::File "<".$fileName  or die "Cannot open '$fileName': $!\n" ;
        $gzIndexInfo = GzGetIndexInfo($file);
        die "gzfile not match! '$fileName': $!\n" unless ($gzIndexInfo) ;
        $fsize = $gzIndexInfo->{'filesize'};
    }

#    print(__LINE__,"\t",time(),"\n");
    if($gzIndexInfo){
        my $searchKey;
        if($command=~/^\s*grep\s+(\-\w+){0,10}\s+(\w{32})\b/){
            $searchKey = $2;
        }

        @searchBatch = GzComputeLogPosition($time,$gzIndexInfo,$file,$searchKey);
        return '' unless(@searchBatch);

        #uncompress data 
        my @unCompressData ;
        my $lastPatch      = shift (@searchBatch);
        my $startOffset    = $lastPatch * $gzIndexInfo->{'pachsize'};
        my $readBufferSize = $gzIndexInfo->{'pachsize'};

        foreach my $patch (@searchBatch) {
            if($patch ==  $lastPatch + 1  ){
                $readBufferSize = $readBufferSize + $gzIndexInfo->{'pachsize'};
                $lastPatch      = $patch;
                next;
            }
#            print ($startOffset,"\t",$readBufferSize,"\n");
            push(@unCompressData,GzReadAt($file,$gzIndexInfo,$startOffset,$readBufferSize));
            $lastPatch      = $patch ;
            $startOffset    = $lastPatch * $gzIndexInfo->{'pachsize'};
            $readBufferSize = $gzIndexInfo->{'pachsize'};
        }  
 #       print ($startOffset,"\t",$readBufferSize,"\n");
        push(@unCompressData,GzReadAt($file,$gzIndexInfo,$startOffset,$readBufferSize));
        #uncompress data end

        local (*Reader, *Writer);
        my $pid = open2(\*Reader, \*Writer, $command."|head -n".$maxSearchLine);
        print Writer @unCompressData;
        close Writer;
        my $res    = '';
        my $buffer = '';
        while(read(Reader,$buffer,1048576)){
           $res .= $buffer;
        }
        close Reader;
        waitpid($pid,0);
        return $res;
    }

    unless(@pos){
       @pos =  computeLogPositon($time,$fileName,$file,$fsize,$gzIndexInfo);
       unless(@pos){
            return 'error';
       }

       $startOffset = int( $pos[0] - $preSearchLogSize/2 );
       if($startOffset < 0 ){
            $startOffset = 0;
       }
       $readBufferSize = ($pos[1]-$pos[0])+$preSearchLogSize;
 
    }
    close($file);
    if($readBufferSize > 20971520){
        $readBufferSize = 20971520;
    }

#    print(__LINE__,"\t",time(),"\n\n");

    my $searchTlp = '';
    unless($gzIndexInfo){
        $searchTlp = "perl -e '\$size=0;open (FILE, \"%s\");seek FILE,".($startOffset).",0;while(read(FILE ,\$buffer,20971520) && \$size <".$readBufferSize." ){\$size += 20971520;print \$buffer;}close FILE;'|".$command."|head -n".$maxSearchLine ;
        my $searchCmd  = sprintf($searchTlp,$fileName);
        return `$searchCmd`;
    }
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


my $host = hostname;
my @outout =();
my $lineStr =''; 

$searchFileType = int($ARGV[3]) if(defined $ARGV[3]);
$searchLogRange = int($ARGV[4]) if((defined $ARGV[4]) && int($ARGV[4]) > 0  );
$timePatten[0] = $ARGV[5] if((defined $ARGV[5]) );


if( $searchFileType == 0 ){
	my @searchFiles =  getMaybeFile($ARGV[0],mystrtomicotime($ARGV[2])/1000);
	foreach my $searchFile  (@searchFiles) {
		next unless $searchFile ;
        next unless (-e $searchFile || -e $searchFile.".gz" );
		@outout= quickSearchFile($searchFile,$ARGV[1],mystrtomicotime($ARGV[2]),$searchLogRange);
		foreach $lineStr (@outout) {  
		    print $host,":",$searchFile,":",$lineStr;
		}
	}
}
else {
	@outout= quickSearchFile($ARGV[0],$ARGV[1],mystrtomicotime($ARGV[2]),$searchLogRange);
	foreach $lineStr (@outout) {  
	    print $host,":",$ARGV[0],":",$lineStr;
	}
}

