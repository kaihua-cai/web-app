#!/usr/bin/env bash

echo 'Current sizes per partition for data_science.agg_custom_example_from_dmf_agg_dw_clicks_pb:'

hive -e "ANALYZE TABLE data_science.agg_custom_example_from_dmf_agg_dw_clicks_pb \
        PARTITION(dy,dm,dd,dh) COMPUTE STATISTICS ;"

