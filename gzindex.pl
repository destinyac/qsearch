#! /usr/bin/perl -w
use Time::Local qw(timelocal);
use POSIX qw(strftime);
use strict;
use POSIX;
use IO::Compress::Gzip qw(gzip $GzipError) ;
use IO::Uncompress::Gunzip qw(gunzip $GunzipError) ;
use IO::Compress::Gzip qw(:flush :level :strategy) ;
my %opt=();

#
# Command line options processing
#
sub init()
{
    use Getopt::Std;
    my $opt_string = 'f:o:l:s:';
    getopts( "$opt_string", \%opt ) or usage();
    usage() unless $opt{f};
    print "[$opt{f}] file not exists!\n" and exit unless -e $opt{f};
}

#
# Message about this program and how to use it
#
sub usage()
{
    print STDERR << "EOF";

This program use gzip to compresse file ,which  can be random-accessed .

usage: $0  -f file [-o outfile] [-l level] [-s pachsize]

     -f        : input file  
     -o        : output file,default ".gz"
     -l        : comressed level [0-9],default 
     -s        : compressed pachsize (bytes),default 2097152

example: $0  -f file -o file.gz 

EOF
    exit;
}

init();


my $pachsize    = 2097152;#2M
if(defined $opt{s} && int($opt{s}) >= 1048576 ){
   $pachsize = int($opt{s});
}

my $EFMAXSIZE   = 60000; #65535
my $infile      = $opt{f};
my $magicstr    = 'xy!xy';
my $outfile     = $infile.'.gz';
our @timePatten = (qw(^[\s\S]*?(?<yyyy>\d{4})\-(?<mm>\d{2})\-(?<dd>\d{2})\s+(?<hh>\d{2}):(?<ii>\d{2}):(?<ss>\d{2})\.(?<ccc>\d{3})[\s\S]*),qw($+{yyyy}-$+{mm}-$+{dd} $+{hh}:$+{ii}:$+{ss}.$+{ccc}));
if(defined $opt{o}){
	$outfile    =  $opt{o};
}

if(-e $outfile ){
   open TMPFILE,">$outfile";  
   close TMPFILE;
}

my @filestat  = stat($infile);
my $filesize  = $filestat[7];
my $pachcount = ceil($filesize*1.0/$pachsize);
my $commentTpl   = $magicstr . ('x' x 100 ) . $magicstr;

my $z = new IO::Compress::Gzip $outfile,(-AutoClose => 1, -Append => 1, -Level=>Z_BEST_SPEED,
	                                     -Comment=>$commentTpl
	                                    )
        or die "IO::Compress::Gzip failed: $GzipError\n";
my @indexInfo ;
my $buffer = '';
my $lastIndex = 0;
my $curIndex = 0;
my $firstPatchIndex = 0;
my $startTime = 0; 
my $endTime  =0 ;
my $tbuffer = '';

#生成压缩文件(第一个gz包内容为空，gz包头部记录记录索引gz包的偏移量信息)
open (FILE, $infile);
do{
        $z->write($buffer);
        $z->flush();
        $curIndex =tell(*$z->{FH}) + length($z->mkTrailer());
        $firstPatchIndex = $curIndex - $lastIndex  if($firstPatchIndex == 0 ); 

        my @ret = getTimeInfo($buffer);
        $startTime = $ret[0];$endTime =$ret[1];
        if($startTime > $endTime){my $t = $endTime ; $endTime = $startTime;$startTime = $t;}  
        push(@indexInfo, ($curIndex - $lastIndex));
        push(@indexInfo,$startTime); 
        push(@indexInfo,$endTime); 

        $lastIndex = $curIndex;

        #处理边界问题
        goto ENDLABLE unless(read(FILE,$buffer,$pachsize));
        $tbuffer='';
        if(substr($buffer,-1,1) ne "\n"){
            $tbuffer = readline(FILE);
            seek(FILE,-length($tbuffer),1);
        }
}while(  $z->newStream(-AutoClose => 1, -Append => 1, -Level=>Z_BEST_SPEED,-Comment=>'',-ExtraField=>genBloomExtraFiled($buffer.$tbuffer) ) );

ENDLABLE:
#保存索引信息到尾部的连续gz包的ExtraField中
my $indexInfoStr = $magicstr.pack("Q*",$filesize,$pachsize,$pachcount,@indexInfo).$magicstr;
my $tmpoffset = 0; 
my @extraFiled;
while($tmpoffset < length($indexInfoStr) ){
	@extraFiled = ('01',substr($indexInfoStr,$tmpoffset,$EFMAXSIZE));
	$tmpoffset += $EFMAXSIZE;
	$z->newStream(-AutoClose => 1, -Append => 1, -Level=>Z_BEST_SPEED,-Comment=>'',-ExtraField=>\@extraFiled);
    $z->write('');
    $z->flush();
}
$z->close();

#把索引信息文件偏移量，记录在第一个gz包的comment中
my $tmpstr     = pack('A'.length($commentTpl),$magicstr.$lastIndex);
$buffer     ='';
open(FH,"+<$outfile") or die "compressed file can not open!\n";
read(FH,$buffer,$firstPatchIndex);
$buffer =~ s/$commentTpl/$tmpstr/;
#print $buffer."\n";
seek(FH,0,SEEK_SET); 
print FH $buffer; 
close(FH);



sub getTimeInfo{
    my $buffer = shift;
    my $index ;
    my $firstTimeStr;

    return (0,0)  unless($buffer);
    if( ($index = index($buffer,"\n")) != -1 && ($index = index($buffer,"\n",$index+1)) != -1){
       $firstTimeStr = substr($buffer,0,$index);
    }else{
       $firstTimeStr = $buffer;
    }

    $firstTimeStr =~s/$timePatten[0]/$+{yyyy}-$+{mm}-$+{dd} $+{hh}:$+{ii}:$+{ss}.$+{ccc}/;
    return (0,0) unless ($firstTimeStr=~/^\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$/);
    my $firstTime = mystrtomicotime($firstTimeStr);

    my $lastChar = substr($buffer,-1,1);
    substr($buffer,length($buffer)-1) = '' if( $lastChar eq "\r" ||  $lastChar eq  "\n") ; 
    return ($firstTime,$firstTime) if(($index = rindex($buffer,"\n")) == -1);

    my $lastTimeStr = substr($buffer,$index + 1);
    $lastTimeStr =~s/$timePatten[0]/$+{yyyy}-$+{mm}-$+{dd} $+{hh}:$+{ii}:$+{ss}.$+{ccc}/;
    return ($firstTime,mystrtomicotime($lastTimeStr)) if ($lastTimeStr=~/^\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$/ );

    substr($buffer,$index) = '';
    if(($index = rindex($buffer,"\n")) != -1){
        $lastTimeStr = substr($buffer,$index + 1);
        $lastTimeStr =~s/$timePatten[0]/$+{yyyy}-$+{mm}-$+{dd} $+{hh}:$+{ii}:$+{ss}.$+{ccc}/;
        return ($firstTime,mystrtomicotime($lastTimeStr)) if ($lastTimeStr=~/^\d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$/ );
    }

    return ($firstTime,$firstTime) ;
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


sub genBloomExtraFiled{
	my ($buffer) = @_;
	my @traceids;
	my @extraFiled;
	my $lastTraceid='';
	while($buffer=~/\|\|traceid=(.*?)\|\|/mg){
		if( $1  and  $1 ne $lastTraceid ){
			$lastTraceid = $1;
			push (@traceids,$1) ;
		}
	}
	push (@traceids,$lastTraceid.'lastone');#标记最后一个，后面对于这种需要特殊处理
	my ($bloomMask,$m,$k) = bloomFilterCreate(\@traceids,0.01,($EFMAXSIZE-100)*8);
	@extraFiled = ('01',$magicstr.join(',',$m,$k,$bloomMask).$magicstr);
	return \@extraFiled;
}

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


