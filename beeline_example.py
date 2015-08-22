import beeline_utils as beeline


#Setting the cluster name: hadoop-c or hadoop-f
CLUSTER_NAME = "hadoop-c"


# Beeline execute query without collecting the output
beeline.beeline_execute(
    """
    select *
    from dmf.view_agg_dw_clicks
    where dh = '2015-07-10 00' limit 10
    """,
    cluster_name=CLUSTER_NAME,
    debug=True)




# Pull Beeline into a dataframe
df = beeline.pull_beeline(
    """
    select *
    from dmf.view_agg_dw_clicks
    where dh = '2015-07-10 00' limit 10
    """,
    cluster_name=CLUSTER_NAME,
    debug=True)

print(df.columns)

print(df['view_agg_dw_clicks.auction_id_64'])
