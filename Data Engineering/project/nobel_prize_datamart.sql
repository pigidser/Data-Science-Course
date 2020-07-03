CREATE TABLE `nobel_prizes` (
  `year` varchar(4),
  `prize` varchar(1000),
  `motivation` varchar(1000),
  `laureate_id` int,
  `prize_share` varchar(5)
);

CREATE TABLE `nobel_laureates` (
  `laureate_id` int,
  `category` varchar(100),
  `type` varchar(100),
  `full_name` varchar(200),
  `birth_date` date,
  `birth_city_id` int
);

CREATE TABLE `cities` (
  `city_id` int,
  `city` varchar(100),
  `country_id` int
);

CREATE TABLE `countries` (
  `country_id` int,
  `country` varchar(100),
  `region` varchar(100),
  `population` int,
  `area` numeric,
  `pop_density` numeric,
  `net_migration` numeric,
  `infant_mrtality` numeric,
  `gdp_per_capita` int,
  `literacy` numeric
);

ALTER TABLE `nobel_prizes` ADD FOREIGN KEY (`laureate_id`) REFERENCES `nobel_laureates` (`laureate_id`);

ALTER TABLE `nobel_laureates` ADD FOREIGN KEY (`birth_city_id`) REFERENCES `cities` (`city_id`);

ALTER TABLE `cities` ADD FOREIGN KEY (`country_id`) REFERENCES `countries` (`country_id`);
