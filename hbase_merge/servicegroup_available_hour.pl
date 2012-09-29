#!/usr/bin/perl -w

use strict;
use warnings;
use POSIX qw/strftime/;
use Data::Dumper;
use DBI;
use vars qw/$d/;

my $now = &get_now();
my $database = "ucmo";
my $hostname = "localhost";
my $port = "3306";
my $dsn = "DBI:mysql:database=$database;host=$hostname;port=$port";

my $user = "ucmo";
my $password = "ucmoforucweb";

print &get_now() . "\n";

my $dbh = DBI->connect($dsn, $user, $password);

my $sth = $dbh->prepare("set names utf8");
$sth->execute;

my @sl;
&parse(0);
&service();

# 只可以查询前1小时前的纪录，因为status_hour表只有前1小时数据
my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time()-3600);
my $last_hour = shift;
$last_hour ||= sprintf("%04d-%02d-%02d %02d:00:00", $year+1900, $mon+1, $mday, $hour);

# 判断当前时间和last_hour的差，必须大于last_hour 1小时15分钟(4500s)
if ( (&get_timestamp($now) - &get_timestamp($last_hour)) < 4500 ) {
    die "ERROR [current time less last_hour 4500s!]\n";
}

# 根据真实服务组数组进行循环
foreach my $servicegroup_name ( @sl ) {
    # 计算真实服务组1小时内单次max_time,单次min_time,
    # 总耗时total_time,总次数total_number,平均失败时间fail_time
    # 基于status_hour表中数据,关联服务组名查询得到结果
	my $sql1 = sprintf("select max(max_time) max_time, min(min_time) min_time, sum(total_time) total_time, sum(total_number) total_number, avg(fail_time) fail_time from status_hour a, servicegroup_members b, servicegroup c where a.host_name = b.host_name and a.service_description = b.service_description and b.servicegroup_id = c.servicegroup_id and c.servicegroup_name = '%s' and a.check_time = '%s';", $servicegroup_name, $last_hour);

	$sth = $dbh->prepare($sql1);
	$sth->execute;

	my $d = $sth->fetchrow_hashref;
    # 不存在或者全部为空则提示服务组异常
    unless ( (exists $d->{max_time} && exists $d->{min_time} && exists $d->{total_time} && exists $d->{total_number} && exists $d->{fail_time}) && ($d->{max_time} || $d->{min_time} || $d->{total_time} || $d->{total_number} || $d->{fail_time}) ) {
        print "ERROR [servicegroup_name:$servicegroup_name]\n";
        next;
    }
    # 将从status_hour结合service_group查询得到服务组的数据写入到servicegroup_status_hour表中
	my $sql2 = sprintf("replace into servicegroup_status_hour (check_time, servicegroup_name, max_time, min_time, total_time, total_number, fail_time) values ('%s', '%s', '%s', '%s', '%s', '%s', '%s')", $last_hour, $servicegroup_name, $d->{max_time}, $d->{min_time}, $d->{total_time}, $d->{total_number}, $d->{fail_time});

	$sth = $dbh->prepare($sql2);
	$sth->execute;
}


# 从数据库中查询真实服务组(servicegroup=y),并Push到@sl数组中
# 如果非真实服务组(servicegroup=n),则再传入id进行递归
sub parse {
	my $t = shift;

	my $sth1 = $dbh->prepare("select * from servicegroup_browse where parent_id = $t;");
	$sth1->execute;

	while (my $data = $sth1->fetchrow_hashref) {
		if ( $data->{servicegroup} eq 'y' ) {
			push @sl, $data->{value};
		} 
		if ( $data->{servicegroup} eq 'n' ) {
			&parse($data->{id});
		}
	}
}


# 从数据库中查询真实服务组(service_catetory=业务),并Push到@sl数组中
sub service {
	my $t = shift;

	# my $sth1 = $dbh->prepare("select servicegroup_name from servicegroup where service_catetory = '业务';");
	my $sth1 = $dbh->prepare("select servicegroup_name from servicegroup where service_catetory = '业务' or service_catetory = '运维';");
	$sth1->execute;

	while (my $data = $sth1->fetchrow_hashref) {
		push @sl, $data->{servicegroup_name};
	}
}

sub get_now {
    my $now = strftime ("%Y-%m-%d %H:%M:%S",localtime);
    return $now;
}

sub get_timestamp {
    my ($time,$timestamp);
    $time = shift;
    $timestamp = `/bin/date -d "$time" +%s`;
    chomp ($timestamp);
    return $timestamp;
}
