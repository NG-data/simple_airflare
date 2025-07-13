ATTACH TABLE _ UUID '087ca4c7-efae-41ee-81e4-0d6b070cb2ed'
(
    `id` Int32,
    `depart_date` Date,
    `iata_origin` String,
    `city_name_origin` String,
    `country_origin` String,
    `iata_destination` String,
    `city_name_destination` String,
    `country_destination` String,
    `trip_class` Int32,
    `value` Int32,
    `gate` String,
    `duration` Int32,
    `distance` Int32,
    `number_of_changes` Int32,
    `date_of_extraction` Date,
    `day_before_departure` Int32
)
ENGINE = MergeTree
ORDER BY (depart_date, iata_origin, iata_destination, day_before_departure)
SETTINGS index_granularity = 8192
