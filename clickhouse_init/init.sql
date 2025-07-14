CREATE TABLE airflare.ikt_airflare_return (
	id				int,
	depart_date		date,
	iata_origin			char(3),
	city_name_origin	varchar(30),
	country_origin		varchar(30),
	iata_destination	char(3),
	city_name_destination	varchar(30),
	country_destination		varchar(30),
	trip_class		int,
	value			int,
	gate			varchar(20),
	duration		int,
	distance		int,
	number_of_changes	int,
	date_of_extraction	 date,
	day_before_departure	int
)
ENGINE = MergeTree()
ORDER BY (depart_date, iata_origin, iata_destination, day_before_departure);

CREATE TABLE airflare.ikt_airflare_outbound (
	id				int,
	depart_date		date,
	iata_origin			char(3),
	city_name_origin	varchar(30),
	country_origin		varchar(30),
	iata_destination	char(3),
	city_name_destination	varchar(30),
	country_destination		varchar(30),
	trip_class		int,
	value			int,
	gate			varchar(20),
	duration		int,
	distance		int,
	number_of_changes	int,
	date_of_extraction	 date,
	day_before_departure	int
)
ENGINE = MergeTree()
ORDER BY (depart_date, iata_origin, iata_destination, day_before_departure);