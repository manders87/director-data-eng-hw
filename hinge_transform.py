import psycopg2
import sqlalchemy
import sqlalchemy_redshift
from sqlalchemy import *

connection_string = "redshift+psycopg2://%s:%s@%s:%s/%s" % ('hingeadmin', 'mO0ZZqeV8z8h', 'hinge-demo-cluster.codrsvjs7qnu.us-east-1.redshift.amazonaws.com', str (5439),'hinge_demo')
sengine = sqlalchemy.create_engine (connection_string)
conn=sengine.connect()



#### Beginning Base ETL Transforms######
#Good
Get Raw Tables from Schema
sql=text("SELECT table_name FROM information_schema.tables where table_schema='hinge_load' and table_name not in ('fact_ratings_new_template','fact_ratings_derived_template', 'fact_raw_template') and table_name not like '%loaded%'")
transform = conn.execute(sql.execution_options(autocommit=True)).fetchall()
transform = map(list,transform)
# Example and template table for the final loading tables, structure is similiar to final tables
#Good
#Using an super fast alter table/append statment that remaps the data blocks instead of rewriting them from the stage tables
for i in transform:
        print(i[0])
        sql='ALTER TABLE hinge_load.fact_ratings_new_template APPEND FROM hinge_load."'+i[0]+'" FILLTARGET'
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(text(sql))
#Renaming raw tables after load but not necessary as the data has been moved out of them by appends
for i in transform:
        rename=i[0]+'_loaded'
        sql='ALTER TABLE hinge_load."'+i[0]+'" RENAME TO "'+rename+'"'
        conn.execute(text(sql).execution_options(autocommit=True))
#Beginning Heavy Data Transforms#-

# Good
# Generating the pair_id to be able to match the two-flow of actions between any two players
# Created a Python UDF that will output the same md5 encoded has for each pair of players and subjects
# The 32 lenght hex space, same as the player/subject ids, is enough to accommodate even if every person on earth paired with every other person on Earth
# Function is somewhat slow during the transformation and process could be optimized perhaps using a temp table

sql="UPDATE  hinge_load.fact_ratings_new_template SET pair_id = f_create_pairid(player_id,subject_id) WHERE pair_id='00000000000000000000000000000000'"
conn.execute(text(sql).execution_options(autocommit=True))


# Populating Derived Facts for Likes Viewed
# The player_id and subject_id are reversed here.  This captures event of a players logging in and seeing likes!
sql=text(
"""
INSERT INTO hinge_load.fact_ratings_derived_template (fact_pending_status,time_stamp, player_id, subject_id, pair_id, rating_type, rating_activity, source_file)
(SELECT 'Pending' as fact_pending_status, time_stamp, subject_id as player_id, player_id as subject_id, pair_id, 1.5, 'Received Like' as rating_activity, source_file from hinge_load.fact_ratings_new_template where rating_type=1
UNION
SELECT 'Pending' as fact_pending_status, time_stamp, subject_id, player_id, pair_id, 2.5, 'Received Like with Comment' as rating_activity, source_file
from hinge_load.fact_ratings_new_template
where rating_type=2
)
""")
conn.execute(sql.execution_options(autocommit=True))


#Good
#Get the timestamp and update records for when person receives their likes
sql=text("""
UPDATE hinge_load.fact_ratings_derived_template
SET time_stamp = rating_n_adjusted.adjusted_timestamp, fact_pending_status = 'Adjusted'
FROM
(SELECT rating_d.time_stamp, rating_d.player_id, rating_d.pair_id, min(rating_n.time_stamp) as adjusted_timestamp
FROM
hinge_load.fact_ratings_derived_template rating_d
inner join hinge_load.fact_ratings_new_template rating_n
on rating_d.player_id = rating_n.player_id
where rating_n.time_stamp >= rating_d.time_stamp
group by rating_d.time_stamp, rating_d.player_id, rating_d.subject_id, rating_d.pair_id, rating_d.rating_type, rating_d.rating_activity) rating_n_adjusted
where fact_ratings_derived_template.time_stamp = rating_n_adjusted.time_stamp AND fact_ratings_derived_template.player_id = rating_n_adjusted.player_id AND fact_ratings_derived_template.pair_id = rating_n_adjusted.pair_id and fact_ratings_derived_template.fact_pending_status<>'Adjusted'
""")
conn.execute(sql.execution_options(autocommit=True))

# GOOD
# Move Adjusted Facts to Loading New Facts Table & Deleting from Derived
# Processing Insert and Delete as Single Transaction

sql=text("""
INSERT INTO hinge_load.fact_ratings_new_template
(time_stamp, player_id, subject_id, pair_id, rating_type, rating_activity, source_file, fact_pending_status)
(SELECT time_stamp, player_id, subject_id, pair_id, rating_type, rating_activity, source_file, 'Ready' as fact_pending_status
FROM hinge_load.fact_ratings_derived_template
WHERE fact_pending_status='Adjusted'
)
""")
conn.execute(sql.execution_options(autocommit=True))

sql=text("DELETE FROM hinge_load.fact_ratings_derived_template WHERE fact_pending_status='Adjusted'")
conn.execute(sql.execution_options(autocommit=True))
#GOOD
#Adjust Rating 3 for Granularity and Provide Descriptions
# Reads from pending facts in the load table and also facts that have been loaded into reporting based on previous rating type from both the new ratings and existing ratings in the reporting schema
sql=text(
    """
UPDATE hinge_load.fact_ratings_new_template
SET rating_activity =
CASE
proceeding_rating
when 0 then 'Player blocked after skipping'
when 1 then 'Player blocked after liking'
when 2 then 'Player blocked after liking and sending a comment'
when 1.5 then 'Player blocked after receiving a like from subject'
when 2.5 then 'Player blocked after receiving a like and comment from subject'
when 5 then 'Player blocked after matching'
ELSE 'Player blocked before interaction'
end,
fact_pending_status='Ready'
FROM
(
SELECT rating_n.time_stamp, rating_n.player_id, rating_n.subject_id, rating_n.pair_id, rating_n.rating_type, lag(rating_n.rating_type,1) OVER (PARTITION BY pair_id order by time_stamp asc) AS proceeding_rating
FROM
hinge_load.fact_ratings_new_template rating_n where rating_activity is null and rating_type=3
UNION
SELECT rating_n.time_stamp, rating_n.player_id, rating_n.subject_id, rating_n.pair_id, rating_n.rating_type, lag(rating_n.rating_type,1) OVER (PARTITION BY pair_id order by time_stamp asc) AS proceeding_rating
FROM
hinge_reporting.fact_ratings_template rating_n where rating_activity is null
and rating_type=3) rating_three_calc
where rating_three_calc.rating_type=3 and rating_three_calc.time_stamp = hinge_load.fact_ratings_new_template.time_stamp and hinge_load.fact_ratings_new_template.player_id=rating_three_calc.player_id and hinge_load.fact_ratings_new_template.pair_id = rating_three_calc.pair_id
""")
conn.execute(sql.execution_options(autocommit=True))
#GOOD
#Adjust Rating 4 for Granularity and Provide Descriptions
# Reads from pending facts in the load table and also facts that have been loaded into reporting based on previous rating type
sql=text("""
UPDATE hinge_load.fact_ratings_new_template
SET rating_activity =
CASE
proceeding_rating
when 0 then 'Player reported after skipping'
when  1 then 'Player reported after liking'
when 2 then 'Player reported after liking and sending a comment'
when  1.5 then 'Player reported after receiving a like from subject'
when 2.5 then 'Player reported after receiving a like and comment from subject'
when 5 then 'Player reported after matching'
ELSE 'Player reported before interaction'
end,
fact_pending_status='Ready'
FROM
(
SELECT rating_n.time_stamp, rating_n.player_id, rating_n.subject_id, rating_n.pair_id, rating_n.rating_type, lag(rating_n.rating_type,1) OVER (PARTITION BY pair_id order by time_stamp asc) AS proceeding_rating
FROM
hinge_load.fact_ratings_new_template rating_n
WHERE rating_type=4
UNION
SELECT rating_n.time_stamp, rating_n.player_id, rating_n.subject_id, rating_n.pair_id, rating_n.rating_type, lag(rating_n.rating_type,1) OVER (PARTITION BY pair_id order by time_stamp asc) AS proceeding_rating
FROM
hinge_reporting.fact_ratings_template rating_n
WHERE rating_type=4) rating_four_calc
where rating_four_calc.time_stamp = hinge_load.fact_ratings_new_template.time_stamp and hinge_load.fact_ratings_new_template.player_id=rating_four_calc.player_id and hinge_load.fact_ratings_new_template.pair_id = rating_four_calc.pair_id
""")
conn.execute(sql.execution_options(autocommit=True))
# Adding Additional Activity Descriptions for Remaining Ratings
sql=text(
"""
UPDATE hinge_load.fact_ratings_new_template
SET rating_activity =
CASE
when rating_type = 0 then 'Player skipped'
when rating_type = 1 then 'Player liked'
when rating_type = 2 then 'Player liked with a comment'
when rating_type = 5 then 'Player matched'
end,
fact_pending_status='Ready'
where rating_activity is null
""")
conn.execute(sql.execution_options(autocommit=True))


##Vacuuming the transformation tables with deletes processed
vac1=text("VACUUM hinge_load.fact_ratings_derived_template")
vac2=text("VACUUM hinge_load.fact_ratings_new_template")
conn.execution_options(isolation_level="AUTOCOMMIT").execute(vac1)
conn.execution_options(isolation_level="AUTOCOMMIT").execute(vac2)


