CREATE TABLE `geo_country_totals` (
  `account_id` bigint(20) NOT NULL,
  `redirection_id` bigint(20) NOT NULL,
  `country_code` varchar(255) NOT NULL,
  `clicks` int(11) DEFAULT NULL,
  PRIMARY KEY (`account_id`,`redirection_id`, `country_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
