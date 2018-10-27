import boto3
import psycopg2
import sqlalchemy
import sqlalchemy_redshift
import datetime
from sqlalchemy import *


def get_matching_s3_objects(bucket, prefix='', suffix=''):
    """
    Generate objects in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch objects whose key starts with
        this prefix (optional).
    :param suffix: Only fetch objects whose keys end with
        this suffix (optional).
    """
    kwargs = {'Bucket': bucket}

    # If the prefix is a single string (not a tuple of strings), we can
    # do the filtering directly in the S3 API.
    if isinstance(prefix, str):
        kwargs['Prefix'] = prefix

    while True:

        # The S3 API response is a large blob of metadata.
        # 'Contents' contains information about the listed objects.
        resp = s3.list_objects_v2(**kwargs)

        try:
            contents = resp['Contents']
        except KeyError:
            return

        for obj in contents:
            key = obj['Key']
            if key.startswith(prefix) and key.endswith(suffix):
                yield obj

        # The S3 API is paginated, returning up to 1000 keys at a time.
        # Pass the continuation token into the next response, until we
        # reach the final page (when this field is missing).
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break


def get_matching_s3_keys(bucket, prefix='', suffix=''):
    """
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    """
    for obj in get_matching_s3_objects(bucket, prefix, suffix):
        yield obj['Key']


s3=boto3.client('s3', aws_access_key_id='AKIAIWYPLVOOM2HZ5PWA',
                aws_secret_access_key='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')
rs_iamrole='arn:aws:iam::902396392333:role/s3accessforredshift'
connection_string = "redshift+psycopg2://%s:%s@%s:%s/%s" % ('hingeadmin', 'mO0ZZqeV8z8h', 'hinge-demo-cluster.codrsvjs7qnu.us-east-1.redshift.amazonaws.com', str (5439),'hinge_demo')
sengine = sqlalchemy.create_engine (connection_string)
conn=sengine.connect()

##Quality Check Verification that we aren't loading redundant facts
quality=text(
"select count(*)"
"from hinge_load.fact_ratings_new_template fl inner join hinge_reporting.fact_ratings_template fr "
"on fr.time_stamp=fl.time_stamp AND fr.player_id=fl.player_id AND fr.pair_id=fl.pair_id AND fr.rating_type=fl.rating_type "
"where fl.fact_pending_status='Ready' "
"order by random()"
"limit 100000;"
)
if conn.execute(quality.execution_options(autocommit=True)).first()[0]==0:
    ## Creating a reporting file naming convention
    report_file='hinge_report_facts_'+datetime.datetime.now().isoformat().replace('-', '').replace('.', '').replace(':','')
    ##Creating SQL to unload into multiple batches of S3 files sized around 100MB to take advantage of Redshift parallel loading and also compressing to gzip format
    ##Also changed the delimiter to comma because some of the descriptions were not being processed with the \t
    runload=text("UNLOAD ("+r"'"+"select player_id,subject_id,pair_id,rating_type,rating_activity,time_stamp,source_file from hinge_load.fact_ratings_new_template where fact_pending_status="+r"\'"+"Ready"+r"\'"+" ORDER BY time_stamp asc"+r"'"+") to "+r"'"+"s3://andershingedemo/Transformations/"+report_file+r"'"+'iam_role '+r"'"+ rs_iamrole +r"'"+" HEADER DELIMITER "+r"'|' "+"GZIP MAXFILESIZE AS 100 MB")
    ##Creating SQL to load files into final reporting schema, choosing this over doing an insert because of the size of the data and the fact that we want it store optimized in the final schema
    rload=text("COPY hinge_reporting.fact_ratings_template(player_id,subject_id,pair_id,rating_type,rating_activity,time_stamp,source_file) from "+r"'"+"s3://andershingedemo/Transformations/hinge_report_facts_"+r"' "+'iam_role ' +r"'"+ rs_iamrole +r"'"+' COMPUPDATE OFF IGNOREHEADER AS 1 DELIMITER '+r"'|' "+'GZIP FILLRECORD MAXERROR 10000')
    #conn.execute(runload.execution_options(autocommit=True))
    #conn.execute(rload.execution_options(autocommit=True))
    ##Renaming Loaded S3 Files so they aren't processed on subsequent run
    loadtables = []
    lbucket = 'andershingedemo'
    import_bucket_prefix='Transformations'
    lprefix = 'Transformations'
    for key in get_matching_s3_keys(bucket=lbucket, prefix=lprefix):
        loadtables.append(key)
        loadtables.pop(0)
    ##Clean Up the List Before Processing
    cleantables = ' '.join(loadtables).replace(import_bucket_prefix + '/', '').split()

    for table in cleantables:
        new_table = table + '_loaded'
        s3.copy_object(ACL='bucket-owner-full-control',
                       Bucket=lbucket,
                       Key=import_bucket_prefix + '/loaded_' + table,
                       CopySource=lbucket + '/' + import_bucket_prefix + '/' + table
                       )
        s3.delete_object(
            Bucket=lbucket,
            Key=import_bucket_prefix + '/' + table
    )


##Looking at a means of reducing the number of ratings in the final fact table
##There are a lot of ratings for skipped occurring in a short amount of time maybe we could roll those up to the 15 minute mark for each pair_id
#Reducing Fact Table to the 15 minute mark for Ready Facts
#Cleaning Up Redundant Facts Particularly for Rating Zero
#No need to have every record stored
sql=text(
"""
INSERT INTO hinge_reporting.fact_ratings_template_reduced (player_id, subject_id, pair_id, rating_type, rating_activity, time_stamp, source_file)
(SELECT player_id, subject_id, pair_id, rating_type, rating_activity, date_trunc('hour', time_stamp) + INTERVAL '15 min' * round(date_part('minute', time_stamp)/15) as time_stamp, source_file
from hinge_reporting.fact_ratings_template
where rating_type=0
group by player_id, subject_id, pair_id, rating_type, rating_activity, time_stamp, source_file
UNION ALL
SELECT player_id, subject_id, pair_id, rating_type, rating_activity, time_stamp, source_file
from hinge_reporting.fact_ratings_template
where rating_type<>0
order by time_stamp desc)
""")
# conn.execute(sql.execution_options(autocommit=True))

print(rload)
