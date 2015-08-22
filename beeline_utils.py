import os
import subprocess
import pandas as pd
import random


beeline_conn_strings =  {
    "hadoop-c":"jdbc:hive2://02.ebee.prod.lax1.adnexus.net:10001/dmf;principal=hive/02.ebee.prod.lax1.adnexus.net@CORP.APPNEXUS.COM",
    "hadoop-f":"jdbc:hive2://01.ebee.prod.nym2.adnexus.net:10001/dmf;principal=hive/01.ebee.prod.nym2.adnexus.net@CORP.APPNEXUS.COM"
    }


def get_beeline_conn_string(cluster_name):

    if cluster_name not in beeline_conn_strings.keys():
        raise ValueError('cluster name not supported. Allowed values:'
                         +str(beeline_conn_strings.keys()))

    return beeline_conn_strings[cluster_name]

def beeline_execute(query, cluster_name, debug=False):

    beeline_conn_string = get_beeline_conn_string(cluster_name)

    #Building the beeline command
    beeline_cmd = "/usr/bin/beeline -u \"%s\"" % beeline_conn_string

    beeline_command = beeline_cmd \
                   + " -e \"" + \
                   query.replace('\n', ' ').replace('\t', ' ') + '\"\n'

    if debug:
        print(beeline_command)

    return subprocess.call(beeline_command, shell=True)


def pull_beeline(query, cluster_name, debug=False):
    """
    pull hive tables with a query and save output into a pandas dataframe
    """
    local_dir = os.getcwd()

    #format query into a hive command
    query = query.replace('\n', ' ') + '\n'

    #create a temporary file to store the hive output
    tmpfile = str(random.random())[2:]
    tmpfile_out = ''.join([local_dir, '/', tmpfile, '.out'])


    beeline_conn_string = get_beeline_conn_string(cluster_name)

    # Beeline options for csv output and file input
    beeline_opts = "  --outputformat=\"csv2\" --silent=true --showHeader=true -e "

    #Building the beeline command
    beeline_cmd = "/usr/bin/beeline -u \"%s\" %s" % (beeline_conn_string, beeline_opts)

    #write beeline shell command:
    command_for_beeline = ' '.join([beeline_cmd,
                                    '"',
                                    query,
                                    '"', '1>', tmpfile_out])

    if debug:
        print(command_for_beeline)

    #run the shell command from bash
    subprocess.call(command_for_beeline, shell=True)

    #read output into dataframe
    data_out = pd.read_csv(tmpfile_out, sep=',', header=0)

    #clean up
    os.remove(tmpfile_out)
    return data_out
