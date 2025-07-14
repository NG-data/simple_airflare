#!/bin/bash
set -e



psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname=postgres <<-EOSQL
    CREATE DATABASE airflare;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname=airflare <<-EOSQL
CREATE TABLE iata_codes (
	id				int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	iata			char(3),
	city_name		varchar(30),
	country			varchar(20)
);

INSERT INTO iata_codes (iata, city_name, country)
VALUES ('IKT', 'Irkutsk', 'Russia'),
('HKT', 'Phuket', 'Thailand'),
('MOW', 'Moscow (metropolitan area)', 'Russia'),
('AER', 'Sochi', 'Russia'),
('LED', 'Saint Petersburg', 'Russia'),
('VVO', 'Vladivostok', 'Russia'),
('MLE', 'Malé', 'Maldives'),
('DXB', 'Dubai', 'United Arab Emirates'),
('IST', 'Istanbul', 'Turkey'),
('AYT', 'Antalya', 'Turkey'),
('NHA', 'Nha Trang', 'Vietnam'),
('SGN', 'Ho Chi Minh City', 'Vietnam'),
('HAN', 'Hanoi', 'Vietnam'),
('TYO', 'Tokyo (metropolitan area)', 'Japan'),
('KUL', 'Kuala Lumpur', 'Malaysia'),
('PKX', 'Beijing Daxing', 'China'),
('PEK', 'Beijing Capital', 'China'),
('RGK', 'Gorno-Altaysk', 'Russia');

CREATE TABLE ikt_airflare_outbound (
	id				int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	depart_date		date,
	origin			int,
	destination		int,
	trip_class		int,
	value			int,
	gate			varchar(20),
	duration		int,
	distance		int,
	number_of_changes	int,
	date_of_extraction	 date,
	day_before_departure	int,
	FOREIGN KEY (origin) REFERENCES iata_codes(id),
	FOREIGN KEY (destination) REFERENCES iata_codes(id)
);

CREATE TABLE ikt_airflare_return (
	id				int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	depart_date		date,
	origin			int,
	destination		int,
	trip_class		int,
	value			int,
	gate			varchar(20),
	duration		int,
	distance		int,
	number_of_changes	int,
	date_of_extraction	 date,
	day_before_departure	int,
	FOREIGN KEY (origin) REFERENCES iata_codes(id),
	FOREIGN KEY (destination) REFERENCES iata_codes(id)
);
EOSQL