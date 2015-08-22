
--create table data_science.agg_custom_example_from_dmf_agg_dw_clicks_pb
USE data_science;

CREATE TABLE IF NOT EXISTS data_science.agg_custom_example_from_dmf_agg_dw_clicks_pb
(
date_time bigint,
auction_id_64 bigint,
user_id_64 bigint,
tag_id int,
venue_id int,
inventory_source_id int,
session_frequency int
)
PARTITIONED BY (dy string, dm string, dd string, dh string)
STORED AS PARQUET
LOCATION '/data_science/aggs/agg_custom_example_from_dmf_agg_dw_clicks_pb';

