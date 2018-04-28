#!/usr/bin/perl


# @Author: travel.liu
# @Date:   2018-04-28 16:18:00
# @Last Modified by:   travel.liu
# @Last Modified time: 2018-04-28 16:24:16
#

####################################################################################################
##      travel.liu 2018-04-28  1. modify function GetTraceName                                    ##
##                             2. add    function ConverNumber                                    ##
####################################################################################################

use strict;
use warnings;
use English;
use Sys::Hostname;
use Term::ANSIColor qw(:constants);
use Getopt::Long qw(:config no_ignore_case bundling);
use POSIX qw/strftime/;
use Data::Dumper;
use Env qw(PATH LD_LIBRARY_PATH DISPLAY);
use Fcntl qw(:mode);
# use Encode qw(from_to);

our $versionNum = 1.5;
our $trcfd;
our $traceFile;
our $dbstatus = 'NO';
our $dbversion;
our $undoSegFile=0;
our $undoSegBlock=0;
our %dbfname;
our @undoInfo;
our $rootdba;
## <<---------------------[ Begin Function ]--------------------------->>

sub MsgPrint{
    my ($type, $msg, $step) = @_;
    my $printMsg = $msg;
    if ($type eq "E" || $type eq "EE")
    {
        print BOLD, RED, "ERROR: ", RESET;
        if ($trcfd)
        {
            print $trcfd "ERROR: ";
        }
    }
    elsif ($type eq "I")
    {
        print BOLD, BLUE, "INFO: ", RESET;
        if ($trcfd)
        {
            print $trcfd "INFO: ";
        }
    }
    elsif ($type eq "W")
    {
        print BOLD, MAGENTA, "WARNING: ", RESET;
        if ($trcfd)
        {
            print $trcfd "WARNING: ";
        }
    }
    elsif ($type eq "S")
    {
        print BOLD, GREEN, "SUCCESS: ", RESET;
        if ($trcfd)
        {
            print $trcfd "SUCCESS: ";
        }
    }
    print "$printMsg\n";
    # Trace("$printMsg");
    if ($trcfd)
    {
        print $trcfd "$printMsg\n";
    }
}

sub Trace{
    my ($output) = @_;
    my ($sec, $min, $hour, $day, $month, $year) = (localtime)[ 0, 1, 2, 3, 4, 5 ];
    $month = $month + 1;
    $year  = $year + 1900;
    $output =~ s/%/%%/g;
    printf $trcfd "Debug: %04d-%02d-%02d %02d:%02d:%02d: $output\n", $year, $month, $day, $hour, $min, $sec;
}

sub DieTrap{
    my ($msg) = @_;
    # if ($trcfd)
    # {
    #     print $trcfd "$msg\n";
    # }
    MsgPrint("E","$msg");
    die("$msg\n");
}

sub InitLogfile{
    my ($dt, $hname, $logPrefix);
    # $runstartTime = time();
    $dt = strftime("%Y%m%d%H%M%S", localtime);
    $hname = GetHostName();
    $logPrefix = $hname;
    $traceFile = "/tmp/analyzeundo-$logPrefix.trc";
    open($trcfd, "> $traceFile");
    MsgPrint('I', "InitLogfile Logfile : $traceFile");
}

sub DoCleanExit{
    MsgPrint("I", "Log file is $traceFile ...");
    print "Exiting...\n";
    exit 0;
}

sub PrintVersionNum{
    print "\n\tanlyzeundo Version is $versionNum by Travel.liu. \n";
    print "Copyright (c) 2015-2016, Enmotech and/or its affiliates. All rights reserved.\n\n";
    exit 0;
}

sub ParseArgs{
    our ($opt_v);
    GetOptions(
      'version|v!'    => \$opt_v
    );
    if (defined $opt_v)
    {
        PrintVersionNum();
    }
}

sub TrimSpace{
    my $str = shift;
    $str =~ s/^[\s]+//;
    $str =~ s/\s+$//;
    $str =~ s/^\s*\n//;
    return $str;
}

sub GetHostName{
    my $host = hostname() or return "";
    my $shorthost;
    if ($host =~ /^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$/)
    {
        $shorthost = $host;
    }
    else
    {
        ($shorthost,) = split(/\./, $host);
    }
    $shorthost =~ tr/A-Z/a-z/;
    return $shorthost;
}

sub GetTraceName {
    my $sql = shift;
    my $traceFileName;

    # my $sql = "alter system set events 'immediate trace name file_hdrs level 3'";
    my $fineTraceSql = q^SELECT d.VALUE || '/' || RTRIM(i.INSTANCE, CHR(0)) || '_ora_' ||
           p.spid || '.trc' trace_file_name
      FROM (SELECT p.spid
              FROM v$mystat m, v$session s, v$process p
             WHERE m.statistic# = 1
               AND s.SID = m.SID
               AND p.addr = s.paddr) p,
           (SELECT t.INSTANCE
              FROM v$thread t, v$parameter v
             WHERE v.NAME = 'thread'
               AND (v.VALUE = '0' OR to_char(t.thread#) = v.VALUE)) i,
           (SELECT VALUE FROM v$parameter WHERE NAME = 'user_dump_dest') d
           ^;

    $sql = $sql . "\n".$fineTraceSql;

    my @sqlResult = ExecuteSQL($sql);

    $traceFileName = $sqlResult[0];

    return $traceFileName;
}

sub RunSystemCmd{
    my $rc  = 0;
    my $prc = 0;
    my @output;
    # print "@_\n";
    Trace("Executing cmd: @_");
    if (!open(CMD, "@_ 2>&1 |")) { $rc = -1; }
    else
    {
        @output = (<CMD>);
        close CMD;
        $prc = $CHILD_ERROR >> 8;    # get program return code right away
        chomp(@output);
    }
    if (scalar(@output) > 0)
    {
        Trace(join("\n>  ", ("Command output:", @output)), "\n>End Command output");
    }
    if ($prc != 0)
    {
        $rc = $prc;
    }
    elsif ($rc < 0 || ($rc = $CHILD_ERROR) < 0)
    {
        MsgPrint("E", "Failure in execution (rc=$rc, $CHILD_ERROR, $!) for command @_");
    }
    elsif ($rc & 127)
    {
        my $sig = $rc & 127;
        MsgPrint("E", "Failure with signal $sig from command: @_");
    }
    elsif ($rc)
    {
        Trace("Failure with return code $rc from command @_");
    }
    return ($rc, @output);
}

sub ExecuteSQL{
    my $sqlExec     = $_[0];
    my $sqltype     = $_[1];
    my $checkErrors = 1;

    if (defined $_[2])
    {
        $checkErrors = 0;
    }
    return ExecuteSqlSqlplus( $sqlExec, $sqltype, $checkErrors);
}

sub ExecuteSqlSqlplus{
    # Build SQL Scripts
    my ($sqlExec,$sqltype,$checkErrors) = @_;

    my $scriptsFile = '.sql_script.tmp';

    my $connectRole = "/ as sysdba";

    $sqlExec = $sqlExec . ";";

    open SQLSCRIPTTEMP, ">", $scriptsFile;
    printf SQLSCRIPTTEMP "%s\n", "SET LINES 11111 PAGES 0 TRIM ON TRIMS ON TI OFF TIMI OFF AUTOT OFF FEED OFF SERVEROUTPUT ON SIZE UNLIMITED";
    if (defined $sqltype)
    {
        printf SQLSCRIPTTEMP "%s\n","SET FEED ON";
    }
    printf SQLSCRIPTTEMP "%s\n%s\n",$sqlExec, "EXIT";
    close SQLSCRIPTTEMP;

    Trace("SQL-connectRole cmd: $connectRole");
    Trace("SQL-sqlExec cmd    :\n $sqlExec");

    my ($rc,@output ) = RunSystemCmd("sqlplus -s -L '$connectRole' \@$scriptsFile;");

    Trace("SQL-sqlResult cmd: \n''' @output ''' ");

    # Check Result File
    # @output =~ s/%/%%/g;

    if ($checkErrors == 1){
        foreach (@output){
            chomp;
            my $errorLine = $_;
            if ($errorLine =~ m/\AORA\-[0-9]{5}/){
                MsgPrint('E', "\033[31mSQL Error : [$errorLine]\033[0m");
            }
            elsif($errorLine =~ m/command\snot\sfound/){
                MsgPrint('E', "\033[31mCMD Error : SQL*Plus Not Found \033[0m");
            }
        }
    }
    # unlink $scriptsFile;
    # Result SQL Result
    return @output;
}

## <<----------------------[ End Function ]---------------------------->>

sub CheckVersion{
    my @sqlResult = ExecuteSQL("SELECT BANNER from v\$version where rownum = 1");
    # print "$sqlResult[0]\n";
    my @temp = split(/\s+/ ,$sqlResult[0]);
    $dbversion =  $temp[6];
    $dbversion =~ s/\.//g;
    MsgPrint('I', "Database Version is : $dbversion ");
    # MsgPrint('I', "Undo\$ Segment Block on datafile 1 block $undosegblock{$dbversion}");
}

sub CheckDBStatus{
    my @inst_status = ExecuteSQL("SELECT STATUS FROM V\$INSTANCE");
    if ($inst_status[0] eq "MOUNTED") {
        $dbstatus = "OK";
    }
    elsif ($inst_status[0] eq "OPEN")
    {
        $dbstatus = "OK";
    }
}

sub CheckTS{
    my $sql = "select rfile#,name from v\$datafile where ts#=0";
    my @sqlResult = ExecuteSQL($sql);
    foreach (@sqlResult)
    {
        my @temp = split(/\s+/);
        $dbfname{$temp[1]} = $temp[2];
        MsgPrint("I","Datafile rfile : $temp[1] name : $temp[2]" );
    }
}

sub DumpSegment{
    my ($filenum,$fileblock) = @_;
    my %blockmap;
    my $sql = "alter system dump datafile '".$dbfname{$filenum}."' block ". $fileblock .";";
    my $findMap = 0;
    my $tracefile = GetTraceName($sql);

    MsgPrint("I","Segment Block Dump Trace $tracefile");
    Trace("Segment Block Dump Trace : $tracefile");

    if (open(GRIDINSTLOG, "< $tracefile"))
    {
        while (<GRIDINSTLOG>)
        {
            chomp();
            next unless /\S/;
            if ($_ =~ /Extent Map/)
            {
                $findMap = 1;
            }
            if ($_ =~ /nfl = 1/)
            {
                $findMap = 0;
            }
            if ($findMap == 1) {
                if ($_ =~ /----/ || $_ =~ /Extent Map/) {
                    next
                } else {
                   my @temp = split(/\s+/,TrimSpace($_));
                   $blockmap{$temp[0]} = $temp[2];
                   Trace("Block Map RDBA $temp[0] Blocks $temp[2]");
                   MsgPrint("I","Block Map RDBA $temp[0] Blocks $temp[2]");
                }
            }
        }
        close(GRIDINSTLOG);
    }
    return %blockmap;
}

sub ParseDBA{
    my $key = shift;
    my $b=substr($key,2);
    my $fileNum  =  hex($b) >> 22;
    my $blockNum =  hex($b) & 4194303;
    return $fileNum , $blockNum;
}

sub DumpMap{
    my %Blockmap = @_;
    my @dumpsql;
    foreach my $key (sort keys %Blockmap)
    {
        my ($fileNum , $blockNum) = ParseDBA($key);
        Trace("Data Block Map RDBA $key($fileNum/$blockNum)");
        MsgPrint("I","Data Block Map RDBA $key($fileNum/$blockNum)");
        for my $i ( 0 .. $Blockmap{$key} )
        {
            MsgPrint("I","Will Dump Datafile : $dbfname{$fileNum} Block : ".(int($blockNum) + $i));
            push (@dumpsql,"alter system dump datafile '".$dbfname{$fileNum}."' block ". (int($blockNum) + $i) . ";")
        }
    }
    my $sql = '';
    foreach(@dumpsql)
    {
        $sql =  $sql.$_."\n";
    }
    # print "$sql\n";
    my $tracefile = GetTraceName($sql);

    MsgPrint("I","Data Block Dump Trace : $tracefile");
    Trace("Data Block Dump Trace : $tracefile");
    return $tracefile;
}

sub ConverNamebySql{
    my $sql = '';
    foreach my $name (@undoInfo)
    {
        Trace("Undo Segment Name: $name");
        $name =~ s/\s+//g;
        Trace("Covert Undo Segment Name: $name");
        my $sql1= "select UTL_RAW.CAST_TO_VARCHAR2('".$name."') from dual";
        $sql = $sql.$sql1."\nunion\n";
    }
    $sql .= "select UTL_RAW.CAST_TO_VARCHAR2('756e646f') from dual";
    my @sqlResult = ExecuteSQL($sql);

    my $PREFIX  = "_CORRUPTED_ROLLBACK_SEGMENTS = (";
    my $FSP     = ",";
    my $POSTFIX = ")";
    my $str;
    $str = "$PREFIX";
    foreach (@sqlResult)
    {
        if ($_ =~ /SYSTEM/ || $_ =~ /undo/) {
            next
        } else {
            $str = "$str$_$FSP";
        }
    }
    if ($str =~ /$FSP$/)
    {
        chop($str);
    }
    $str = "$str$POSTFIX";
    MsgPrint("I","$str");
    # 5f535953534d55323024
}

sub ConVertUndoSegName{
    my %undo;
    my $PREFIX  = "_CORRUPTED_ROLLBACK_SEGMENTS = (";
    my $FSP     = ",";
    my $POSTFIX = ")";
    my $str;
    $str = "$PREFIX";
    printf ("  %-30s=    %s\n", "UndoSegNum", "UndoSegName");
    for (my $i = 0; $i < $#undoInfo; $i += 2) {
        my $undoNum  = ConverNumber($undoInfo[$i]);
        my $undoName = ConverVarchar($undoInfo[$i + 1]);
        printf ("  %-30s=    %s\n", $undoNum, $undoName);
        if ($undoName =~ /SYSTEM/ || $undoName =~ /undo/) {
            next
        } else {
            $str = "$str$undoName$FSP";
        }
    }

    if ($str =~ /$FSP$/)
    {
        chop($str);
    }
    $str = "$str$POSTFIX";
    MsgPrint("I","$str");
}

sub DumpRootDBA {

    my $findMap = 0;
    my $findTBS = 0;
    my $tracefile = GetTraceName("alter system set events 'immediate trace name file_hdrs level 3';");

    MsgPrint("I","Datafile Header Block Dump Trace $tracefile");
    Trace("Datafile Header Block Dump Trace : $tracefile");

    if (open(GRIDINSTLOG, "< $tracefile"))
    {
        while (<GRIDINSTLOG>)
        {
            chomp();
            next unless /\S/;
            if (($_ =~ /Tablespace/)) {
                my @temp1 = split(/\s+/);
                Trace("Datafile Header Block : $_");
                if ($temp1[3] eq 'SYSTEM' and $temp1[4] eq "rel_fn:1") {
                    Trace("SYSTEM Datafile Header Block : $_");
                    $findTBS = 1
                }
                else
                {
                    $findTBS = 0
                }
            }
            if ($findTBS == 1 and $_ =~ /root dba:/)
            {
                Trace("Root DBA Block : $_");
                my @temp1 = split(/\s+/);
                my @temp2 = split(/:/,$temp1[3]);
                # print "$temp2[1]\n";
                $rootdba = $temp2[1];
            }
        }
        close(GRIDINSTLOG);
    }
}

sub ConverNumber {
    my $str = shift;
    my @str1 = split (/\s+/,$str);
    my $total;
    my $index;
    if (hex($str1[0]) == 0x80) {
        # 0
        $total = 0
    } elsif (hex($str1[0]) > 0x80) {
        # 正数
        $index = hex($str1[0]) - 0xc1;
        for (my $var = 1; $var <= $#str1; $var++) {
            $total += (hex($str1[$var]) - 0x01) * (100 ** $index);
            $index--;
        }
    }
    # elsif (hex($str1[0]) < 0x80) {
    #     # 负数
    #     $index = hex($str1[0]) - 0x3e;
    #     for (my $var = 1; $var <= $#str1; $var++) {
    #         $total += (hex($str1[$var]) - 0x01) * (100 ** -$index);
    #         $index--;
    #     }
    # }
    return $total;
}

sub ConverVarchar{
    my $str1 = shift;
    my @str2 = split (/\s+/,$str1);
    my $str;
    foreach my $item (@str2)
    {
        $str.=chr(hex($item));
    }
    return $str;
}

sub DumpBootStrap{
    my $findCol = 0;
    my $tabundosql = '';
    my ($fileNum , $blockNum) = ParseDBA($rootdba);
    Trace("BootStrap Block RDBA $rootdba($fileNum/$blockNum)");
    MsgPrint("I","BootStrap Block RDBA $rootdba($fileNum/$blockNum)");
    # my %bootmap =
    my $BS_trcfile = DumpMap(DumpSegment($fileNum,$blockNum));
    if (open(TRCLOG, "< $BS_trcfile"))
    {
        while (<TRCLOG>)
        {
            chomp();
            next unless /\S/;
            if ($_ =~ /43 52 45 41 54 45 20 54 41 42 4c 45 20 55 4e 44 4f 24/)
            {
                $findCol = 1
            }
            if ($_ =~ /tab 0/ or $_ =~ /end_of_block_dump/)
            {
                $findCol = 0;
            }
            if ($findCol == 1) {
                # print "$_\n";
                $tabundosql = $tabundosql.$_;
                # if ($_ =~ /----/ || $_ =~ /Extent Map/) {
                #     next
                # } else {
                #    my @temp = split(/\s+/,TrimSpace($_));
                #    $blockmap{$temp[0]} = $temp[2];
                #    Trace("Block Map RDBA $temp[0] Blocks $temp[2]");
                #    MsgPrint("I","Block Map RDBA $temp[0] Blocks $temp[2]");
                # }
            }
        }
        close(TRCLOG);
    }
    Trace("Dump Trace file Undo Sql : $tabundosql");
    my $undosql = ConverVarchar($tabundosql);
    Trace("Create Undo Sql : $undosql");
    MsgPrint("I","Create Undo Sql : $undosql");
    my $str;
    if($undosql =~ /\((FILE\s\d+\sBLOCK\s\d+)\)/){ $str=$1;}
    my @str1=split(/\s/,$str);
    # print "$str1[1],$str1[3]\n";
    $undoSegFile    = $str1[1];
    $undoSegBlock   = $str1[3];
}

sub DumpUndo{
    my $undo_trcfile = DumpMap(DumpSegment($undoSegFile,$undoSegBlock));

    if (open(GRIDINSTLOG, "< $undo_trcfile"))
    {
        my $findLine = 0;
        while (<GRIDINSTLOG>)
        {
            chomp();
            next unless /\S/;
            if ($_ =~ /col  [0|1]/)
            {
                if (length($_) > 12)
                {
                    my @str = split(/]/,$_);
                    push @undoInfo,TrimSpace($str[1]);

                }
            } elsif ($_ =~ /5f 53 59 53 53 4d/) {
                push @undoInfo,TrimSpace($_);
            }
        }
        close(GRIDINSTLOG);
    }
}
# ## <<-----------------------[ Begin Main ]----------------------------->>

ParseArgs();
InitLogfile();
CheckDBStatus();
if ($dbstatus eq 'NO') {
    DieTrap("Database Must be Mounted and Opened")
}
CheckVersion();
DumpRootDBA();
CheckTS();
DumpBootStrap();
DumpUndo();
ConVertUndoSegName();

