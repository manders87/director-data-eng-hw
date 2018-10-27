#Borrowed function from https://alexwlchan.net/2018/01/listing-s3-keys-redux/ for s3 ##iteration
import boto3
import psycopg2
import sqlalchemy
import sqlalchemy_redshift
from sqlalchemy import *


import_bucket='andershingedemo'
import_bucket_prefix='fake'
unprocessed_file_prefix='load'
rs_iamrole='arn:aws:iam::902396392333:role/s3accessforredshift'


s3=boto3.client('s3', aws_access_key_id='AKIAIWYPLVOOM2HZ5PWA',
    aws_secret_access_key='OA1QCY4u7o5tivISKYxWzws0LBRHsruhtDkGRYyO')


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

####Begin processing import
rawtables = []
lbucket = import_bucket
lprefix = import_bucket_prefix+'/'+unprocessed_file_prefix
for key in get_matching_s3_keys(bucket='andershingedemo',prefix=lprefix):
    rawtables.append(key)
rawtables.pop(0)
##Clean Up the List Before Processing
cleantables = ' '.join(rawtables).replace(import_bucket_prefix+'/', '').split()

##Begins Loading Operation
for table in cleantables:
    try:
        connection_string = "redshift+psycopg2://%s:%s@%s:%s/%s" % ('hingeadmin', 'mO0ZZqeV8z8h', 'hinge-demo-cluster.codrsvjs7qnu.us-east-1.redshift.amazonaws.com', str (5439),'hinge_demo')
        sengine = sqlalchemy.create_engine (connection_string)
        conn=sengine.connect()
        ##Creating Tables for Each File
        qloadcreate = text('CREATE TABLE IF NOT EXISTS hinge_load."'+table+'" (like hinge_load.fact_raw_template)')
        conn.execute(qloadcreate.execution_options(autocommit=True))
        ##A column for source file name is added with a default value of the source file being processed to avoid updating all these records later
        qsourcefile = text('ALTER TABLE hinge_load."'+table+'" ADD COLUMN source_file varchar(50) DEFAULT "'+table+'" ENCODE ZSTD')
        conn.execute (qsourcefile.execution_options (autocommit=True))
        ##Performing Redshift COPY
         qloadcopy = text(
            'COPY hinge_load."' + table +'" from '+r"'s3://" + import_bucket + "/" + import_bucket_prefix + '/' + table +r"' "+'iam_role ' +r"'"+ rs_iamrole +r"'"+' COMPUPDATE OFF DELIMITER '+r"'\t' "+' FILLRECORD MAXERROR 10000')
        conn.execute(qloadcopy.execution_options(autocommit=True))
        conn.close()
        # ##Renaming Operation to Set Files to Prefix Loaded
        # ##Must do copy/delete as there is no boto3 rename
            new_table=table+'_loaded'
            s3.copy_object(ACL='bucket-owner-full-control',
                       Bucket=import_bucket,
                       Key=import_bucket_prefix+'/loaded_'+table,
                       CopySource=import_bucket+'/'+import_bucket_prefix+'/'+table
                       )
         s3.delete_object (
             Bucket=import_bucket,
             Key=import_bucket_prefix+'/'+table
                      )
    except:
    #     ##Long Term Should Have Exception Handling to Log Issues and/or Send Email Notifications
             pass

