
--Example insert overwrite to custom agg table

--compression
SET hive.exec.compress.output=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.output.compression.type=BLOCK;

--merging small files
SET hive.merge.mapredfiles=true;
SET hive.merge.mapfiles=true;

USE data_science;

INSERT OVERWRITE TABLE
data_science.agg_custom_example_from_dmf_agg_dw_clicks_pb
PARTITION
(dy = '2015', dm ='06', dd = '10', dh = '00')
SELECT
date_time,
auction_id_64,
user_id_64,
tag_id,
venue_id,
inventory_source_id,
session_frequency
FROM dmf.agg_dw_clicks_pb
WHERE dh = '2015-06-10 00';

