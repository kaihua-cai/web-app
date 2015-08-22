import subprocess
import logging
import datetime
from os.path import expanduser
import beeline_utils as beeline

CLUSTER_NAME = "hadoop-f"
DESTINATION_AGG_TABLE = "data_science.agg_custom_example_from_dmf_agg_dw_clicks_pb"
LOOKBACK_DAY = 30


def setup_logging(filename_log, log_name='default', is_debug=False):
    # Setup standard file logging

    log = logging.getLogger(log_name)

    logging.basicConfig(level=logging.DEBUG if is_debug else logging.INFO,
                        format='%(asctime)s:%(levelname)s:%(message)s',
                        filename=filename_log,
                        filemode='a')

    return log


# # DEPRECATED METHOD:
# def hive_execute(query):
#     hive_command = "hive -S -e \"" + \
#                    query.replace('\n', ' ').replace('\t', ' ') + '\"\n'
#     logging.info("Running the following Hive command:\n" + hive_command)
#     return subprocess.call(hive_command, shell=True)

def update_custom_agg(dt_str):
    dy = dt_str[0:4]
    dm = dt_str[0:7]
    dd = dt_str[0:10]
    dh = dt_str

    hive_agg_click_q = """
    SELECT
    date_time,
    auction_id_64,
    user_id_64,
    tag_id,
    venue_id,
    inventory_source_id,
    session_frequency
    FROM dmf.agg_dw_clicks_pb
    WHERE dh = '%s';
    """ % (dh)

    insert_click = """
    INSERT OVERWRITE TABLE
        %s
    PARTITION
       (dy = '%s', dm = '%s', dd = '%s', dh = '%s')
    """ % (DESTINATION_AGG_TABLE, dy, dm, dd, dh)

    compress_config = """
    SET hive.exec.compress.output=true;
    SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
    SET mapred.output.compression.type=BLOCK;
    """

    merge_config = """
    SET hive.merge.mapredfiles=true;
    SET hive.merge.mapfiles=true;
    """

    data_schema_jar = """
    add jar file:///usr/local/adnxs/agg/current/hadoop-jars/data-schemas.jar;
    """

    query = '\n'.join([data_schema_jar,
                       merge_config,
                       compress_config,
                       insert_click,
                       hive_agg_click_q])

    beeline.beeline_execute(query,
                            cluster_name=CLUSTER_NAME,
                            debug=False)


def delete_old_data(dt_str):
    dy = dt_str[0:4]
    dm = dt_str[0:7]
    dd = dt_str[0:10]
    dh = dt_str

    delete_old_data = """
    ALTER TABLE
        %s
    DROP IF EXISTS PARTITION(dh<= '%s')
    """ % (DESTINATION_AGG_TABLE, dt_str)

    beeline.beeline_execute(delete_old_data,
                            cluster_name=CLUSTER_NAME,
                            debug=False)


def main():
    setup_logging(filename_log=''.join([expanduser("~"), '/agg_logs/', DESTINATION_AGG_TABLE, '.log']),
                  log_name=''.join([DESTINATION_AGG_TABLE, '.log']), is_debug=True)

    logging.info("Phase 1: Appending to custom agg table")
    current_time = datetime.datetime.now()
    update_time = (current_time - datetime.timedelta(days=2)).strftime("%Y-%m-%d %H")
    update_custom_agg(update_time)
    logging.info('Finished agg %s', update_time)

    logging.info("Phase 2: Deleting data from custom agg table")
    lookback_day = LOOKBACK_DAY
    delete_time = (datetime.datetime.now() - datetime.timedelta(days=1 + lookback_day)).strftime("%Y-%m-%d %H")
    delete_old_data(delete_time)
    logging.info('Finished delete of data older than %s', delete_time)

    logging.info("Finished processing custom agg table.")


if __name__ == "__main__":
    main()