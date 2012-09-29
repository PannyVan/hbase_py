#!/usr/bin/perl -w

use strict;
use warnings;
use DBI;
use Data::Dumper;

my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time()-3600);
my $last_hour = sprintf("%04d%02d%02d%02d", $year+1900, $mon+1, $mday, $hour);

#print "$last_hour";
my $current_status = &get_current_status();

my $perfdata_dir = '/home/nagios/data/nagios_perf';
my $perfdata_backup_dir = '/home/nagios/data/nagios_perf_backup';
# 从nagios_perf目录读取所有sql文件
opendir(DIR, $perfdata_dir) || die "cant opendir $perfdata_dir: $!";
my @dots = grep { -f "$perfdata_dir/$_" } readdir(DIR);
closedir DIR;

my $result = "";
my $instance_result = "";
my $instance_status = 0;
# 对所有sql文件循环
foreach my $sql_file (sort @dots ) {
	my $instance_status = 1;
	# tail nagios_perf.sql.2010091409 
    my $year_mon_mday;
	my $file = $perfdata_dir . "/" . $sql_file;
	my $backup_file = $perfdata_backup_dir . "/" . $sql_file;

	my %h = ();
	my $now_hour = undef;
    # 定义now_hour和backup_file的值
	if ( $file =~ m/(\d{4})(\d{2})(\d{2})(\d{2})/ ) {
		$now_hour = sprintf("$1-$2-$3 $4:00:00");
        $year_mon_mday = $1 . $2 . $3;
        mkdir $perfdata_backup_dir . "/" . $year_mon_mday if (! -d $perfdata_backup_dir . "/" . $year_mon_mday);
	    $backup_file = $perfdata_backup_dir . "/" . $year_mon_mday . "/" . $sql_file;
	}

	die "now time not define\n" if ( ! $now_hour );
	open(INPUT, $file) or die "cant open\n";

	my %v = ();
	my %s = ();
    # 读取文件
	while(<INPUT>) {
		my $line = $_;
	
		# tianjin_active`mob1004`Foxyserver-8086`time`0.155`1284008397
        # 对每行进行正则匹配
		if ( $line =~ m{(.+?)`(.+?)`(.+?)`(.+?)`(.+?)`(.+?)$} ) {
			my $instance = $1;
			my $host_name = $2;
			my $service_description = $3;
			my $key = $4;
			my $value = $5;
			my $check_time = $6;

	
            # 跳过部分不需要计算的服务名称
			next if ( $service_description =~ m{^disk-|^swap|^connects} );
			next if ( $host_name =~ m{pnp-internal} );

            # 根据sql文件中第一条服务的check_time更新instance_ext中对应集群的时间(检查各集群是否正常更新)
			$instance_result .= "update instance_ext set perf_updated = '" . &timestamp_2_date($check_time) . "' where instance_name = '$instance';\n" if ($instance_status == 1);
			# print $instance_result;
            # 初始化max_time和min_time
			$h{$host_name}{$service_description}{'time'}{'max_time'} ||= 0;
			$h{$host_name}{$service_description}{'time'}{'min_time'} ||= 99999;
            # 如果该行perfdata数据为time,则计算该服务的总耗时,单次最大耗时,单次最小耗时,检查次数
			#if ( $key =~ m{time} ) {
			if ( $key eq 'time' ) {
				$value = 60 if ($value > 60);
				$h{$host_name}{$service_description}{'time'}{'time_all'} += $value;
				$h{$host_name}{$service_description}{'time'}{'max_time'} = $value if ( $h{$host_name}{$service_description}{'time'}{'max_time'} < $value );
				$h{$host_name}{$service_description}{'time'}{'min_time'} = $value if ( $h{$host_name}{$service_description}{'time'}{'min_time'} > $value );
				$h{$host_name}{$service_description}{'time'}{'time_number'} ++;
            # 如果该行perfdata数据为status,
			#} elsif ( $key =~ m{status} ) {
			} elsif ( $key eq 'status' ) {
                # 检查集群->主机->服务->notification_enable是否为0(表示当前禁用),如果禁用,则认为服务status=0,服务为可用
                if (exists $current_status->{$instance}->{$host_name}->{$service_description} && $current_status->{$instance}->{$host_name}->{$service_description}->{notifications_enabled} == 0  && ((time - $check_time) < 7200) ) {
			        $value = 0;
                }
                # 检查集群->主机->服务->servicedowntime是否为1(表示计划维护),如果为维护,则认为服务status=0,服务为可用
                if (exists $current_status->{$instance}->{$host_name}->{$service_description} && $current_status->{$instance}->{$host_name}->{$service_description}->{servicedowntime} == 1  && ((time - $check_time) < 7200) ) {
			        $value = 0;
                }

				$v{$host_name}{$service_description}{'start'} ||= 0;
				$h{$host_name}{$service_description}{'status'} ||= 0;

				# s 哈希存放当前主机+服务 是否第一次处理 来解决多perfdata文件之间空白时间默认OK
				# 导致完全不可用服务 可用性不是0%
				#if ( ! exists $s{$host_name}{$service_description} ) {
				#	if ( $value == 2 ) {
				#		my $guess_time = 300;
				#		if ( exists $current_status->{$instance}->{$host_name}->{$service_description}->{state_type} && $current_status->{$instance}->{$host_name}->{$service_description}->{state_type} == 0 ) {
				#			$guess_time = 60;
				#		}

				#		$h{$host_name}{$service_description}{'status'} += $guess_time;
				#	}
				#	$s{$host_name}{$service_description} = 1;
				#}
                # v 哈希存放主机+服务 是否第一次处理(start为0)并且status=2
                # 如果是则标记开始时间位,并将start置为非第一次(start为1)
				if ( ( $v{$host_name}{$service_description}{'start'} == 0 ) && ( $value == 2 ) ) {
					$v{$host_name}{$service_description}{'start'} = 1;
					$v{$host_name}{$service_description}{'start_time'} = $check_time;
				}
                # v 哈希存放主机+服务 判断是否可以开始计算(start=1 && value=2)
                # 如果条件符合,则计算失效时间,(当前的check_time减去上一次的check_time)
                # 得到失效时间,再将开始时间改为当前时间(为下一次计算做准备)
				if ( ( $v{$host_name}{$service_description}{'start'} == 1 ) && ( $value == 2 ) ) {
					$h{$host_name}{$service_description}{'status'} += ($check_time - $v{$host_name}{$service_description}{'start_time'});
					$v{$host_name}{$service_description}{'start_time'} = $check_time;
                # v 哈希存放主机+服务 判断业务是否恢复(start=1 && value=0)
                # 修改计算标记(start=0)
                # 但是我们仍然将从失效到恢复的这段时间记入到失效时间段中
				} elsif  ( ( $v{$host_name}{$service_description}{'start'} == 1 ) && ( $value == 0 ) ) {
					$v{$host_name}{$service_description}{'start'} = 0;
					$h{$host_name}{$service_description}{'status'} += ($check_time - $v{$host_name}{$service_description}{'start_time'});
				}
	
			}
=for
			if ( $service_description eq "万软" ) {
				print Dumper $h{'预装'}{'万软'};
			}
=cut
		}
		$instance_status = 0;
	}
	
	close(INPUT);

	foreach my $host_name ( keys %h ) {
		foreach my $service_description ( keys %{$h{$host_name}} ) {
			$h{$host_name}{$service_description}{'status'} ||= 0;
			$h{$host_name}{$service_description}{'time'}{'time_all'} ||= 0;
			$h{$host_name}{$service_description}{'time'}{'time_number'} ||= 0;
            # 判断主机+服务一小时失效时间是否大于3600秒(1小时)
            # 在解决一个服务完全不可用，可用性不为0%的情况,
            # 我们对每个服务第一次出现添加300秒,可能导致1小时内超过3600
			$h{$host_name}{$service_description}{'status'} = 3600 if ( $h{$host_name}{$service_description}{'status'} > 3600 );
			$h{$host_name}{$service_description}{'status'} = 0 if ( $h{$host_name}{$service_description}{'status'} < 0  );
			$h{$host_name}{$service_description}{'time'}{'min_time'} = 0 if ( $h{$host_name}{$service_description}{'time'}{'min_time'} == 99999 );
			$h{$host_name}{$service_description}{'time'}{'min_time'} = $h{$host_name}{$service_description}{'time'}{'max_time'} if ( $h{$host_name}{$service_description}{'time'}{'min_time'} > $h{$host_name}{$service_description}{'time'}{'max_time'} );
			$result .= sprintf("replace into status_hour (check_time, host_name, service_description, max_time, min_time, total_time, total_number, fail_time) values ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%d');\n", $now_hour, $host_name, $service_description, $h{$host_name}{$service_description}{'time'}{'max_time'}, $h{$host_name}{$service_description}{'time'}{'min_time'}, $h{$host_name}{$service_description}{'time'}{'time_all'}, $h{$host_name}{$service_description}{'time'}{'time_number'}, $h{$host_name}{$service_description}{'status'}); 
		}
	}
	system("/bin/mv $file $backup_file");
	chdir $perfdata_backup_dir;
	system("bzip2 -f $backup_file");
}


my $tmp_file = "/dev/shm/nss.sql";
open(NCR, "+> $tmp_file") or die "cant open the file\!";
print NCR $result;
print NCR $instance_result;
close(NCR);

system("/usr/bin/mysql -u ucmo -pucmoforucweb -D ucmo < $tmp_file");
unlink $tmp_file;




sub timestamp_2_date{
        my $timestamp = shift;
        if ($timestamp !~ m{\d{10}}){
                die "The timestamp must be 10 Numbers,example:1282111741\n";
        }
        my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime($timestamp);
        $mon += 1;
        $year += 1900;
        $sec = sprintf("%02d",$sec);
        $min = sprintf("%02d",$min);
        $hour = sprintf("%02d",$hour);
        $mday = sprintf("%02d",$mday);
        $mon = sprintf("%02d",$mon);
        $year = sprintf("%04d",$year);
        my $date = $year . "-" . $mon . "-" . $mday . " " . $hour . ":" . $min . ":" . $sec;
        return $date;
}

#
sub get_current_status {
    my $current_status;
    my $db_host = "localhost";
    my $db_name = "ucmo";
    my $db_port = "3306";
    my $db_user = "read";
    my $db_passwd = "ucweb\$RFV";
    my $dbh = DBI->connect("DBI:mysql:datebase=$db_name:host=$db_host:port=$db_port",$db_user,$db_passwd);
    die "Cant connect to $db_host" . "\n" if (not $dbh);

    my $sql = 'set names utf8;';
    my $sth = $dbh->prepare($sql);
    $sth->execute();

    $sql = 'select * from ' . $db_name . '.current_status;';
    $sth = $dbh->prepare($sql);
    $sth->execute();

    while (my $hash = $sth->fetchrow_hashref) {
        # $current_status->{$hash->{instance_name}}->{$hash->{host_name}}->{$hash->{service_description}}->{service_description} = $hash->{service_description};
        $current_status->{$hash->{instance_name}}->{$hash->{host_name}}->{$hash->{service_description}}->{notifications_enabled} = $hash->{notifications_enabled};
		# xinyong 20120801 1437 +
        $current_status->{$hash->{instance_name}}->{$hash->{host_name}}->{$hash->{service_description}}->{servicedowntime} = $hash->{servicedowntime};
    }
    $sth->finish;
    $dbh->disconnect;
    # print Dumper $current_status;
    return $current_status;
}
