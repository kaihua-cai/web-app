#!/bin/bash
### Custom aggregation for table: hourly: data_science.agg_custom_example_from_dmf_agg_dw_clicks_pb
# crontab -e
15 * * * * bash /home/$USER/dev/custom_agg_example/custom_agg_cron_job.sh >> /home/$USER/agg_logs/custom_agg_error.log 2>&1