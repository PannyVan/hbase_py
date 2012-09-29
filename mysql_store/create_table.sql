CREATE TABLE `t_error` (
      `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
      `jkid` varchar(64) NOT NULL,
      `errdt` datetime NOT NULL,
      `indb` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      `msg` varchar(1024) NOT NULL,
      PRIMARY KEY (`id`),
      KEY `i_jkid` (`jkid`),
      KEY `i_errdt` (`errdt`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
