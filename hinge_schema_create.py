import psycopg2
import sqlalchemy
import sqlalchemy_redshift

from sqlalchemy import text, create_engine

##Hinge Create Table Schema
connection_string = "redshift+psycopg2://%s:%s@%s:%s/%s" % ('hingeadmin', 'mO0ZZqeV8z8h', 'hinge-demo-cluster.codrsvjs7qnu.us-east-1.redshift.amazonaws.com', str (5439),'hinge_demo')
sengine = create_engine (connection_string)
conn=sengine.connect()

## Example and template table for the final time-series fact tables for ratings
create_table1= text(
    "CREATE TABLE IF NOT EXISTS hinge_reporting.fact_ratings_template"
"(  player_id           char(32)       NOT NULL	ENCODE ZSTD,"
   "subject_id          char(32)       NOT NULL	ENCODE ZSTD,"
  "pair_id             char(32)       NOT NULL	ENCODE ZSTD,"
   "rating_type         decimal(4,2)   NOT NULL	ENCODE RAW,"
   "rating_activity     varchar (25)				      ENCODE ZSTD,"
   "time_stamp            timestamp    NOT NULL	,"
   "source_file         varchar(50)              ENCODE ZSTD)"
   "DISTSTYLE EVEN SORTKEY(time_stamp, rating_type, pair_id, player_id);"
   )
conn.execute(create_table1.execution_options(autocommit=True))

## Template for Raw Staging Tables Mirroring the Exact Format of the Raw Files
## Sort Keys are Choosen Here to be optimized for ELT processes of joining player_ids that will occur subsequently
create_table2=text(
    "CREATE TABLE IF NOT EXISTS hinge_load.fact_raw_template"
"(  time_stamp          timestamp      NOT NULL	ENCODE RAW,"
   "player_id           char(32)       NOT NULL	ENCODE RAW,"
   "subject_id          char(32)       NOT NULL	ENCODE ZSTD,"
   "rating_type         decimal(4,2)   NOT NULL	ENCODE ZSTD)"
   "DISTKEY(player_id) SORTKEY(player_id, time_stamp, rating_type);"
)
conn.execute(create_table2.execution_options(autocommit=True))

## Example and template table for the final loading tables, structure is similiar to final tables
## The sort keys are chosen for optimization of the ELT processes that will join player_ids and timestamps, the additional columns will not affect the alter table append operation
create_table3=text("CREATE TABLE IF NOT EXISTS hinge_load.fact_ratings_new_template"
"(  fact_pending_status varchar (10)   NOT NULL ENCODE RAW  DEFAULT 'Not Ready'," ## Not in raw table
   "time_stamp		   timestamp      NOT NULL ENCODE RAW,"
   "player_id           char(32)       NOT NULL ENCODE RAW,"
   "subject_id          char(32)       NOT NULL ENCODE ZSTD,"
   "pair_id             char(32)       NOT NULL ENCODE ZSTD DEFAULT '0000000000000000000000000000000000000000',"   ## Not in raw table
   "rating_type         decimal(4,2)   NOT NULL ENCODE ZSTD,"
   "rating_activity     varchar (25)            ENCODE ZSTD,"             ## Not in raw table
   "source_file         varchar(50)             ENCODE ZSTD)"
	"   DISTKEY(player_id) "
     "  SORTKEY(player_id, time_stamp, rating_type);"
)
conn.execute(create_table3.execution_options(autocommit=True))

##Creating a supplemental loading fact table to store likes received
create_table4=text("""

CREATE TABLE IF NOT EXISTS hinge_load.fact_ratings_derived_template
(
   fact_pending_status varchar (10)             ENCODE RAW,  ## Not in raw table
   time_stamp		   timestamp      NOT NULL	    ENCODE RAW,
   player_id           char(32)       NOT NULL	ENCODE RAW,
   subject_id          char(32)       NOT NULL  ENCODE ZSTD,
   pair_id             char(32)       NOT NULL	ENCODE ZSTD,    ## Not in raw table
   rating_type         decimal(4,2)   NOT NULL	ENCODE RAW,
   rating_activity     varchar (25)		          ENCODE ZSTD,    ## Not in raw table
   source_file         varchar(50)              ENCODE ZSTD
   DISTKEY(player_id)
   SORTKEY(fact_pending_status, player_id, time_stamp, rating_type)
   """)
conn.execute(create_table4.execution_options(autocommit=True))
conn.close()


## Table for reduced facts
create_table5= text(
    "CREATE TABLE IF NOT EXISTS hinge_reporting.fact_ratings_template_reduced"
"(  player_id           char(32)       NOT NULL	ENCODE ZSTD,"
   "subject_id          char(32)       NOT NULL	ENCODE ZSTD,"
  "pair_id             char(32)       NOT NULL	ENCODE ZSTD,"
   "rating_type         decimal(4,2)   NOT NULL	ENCODE RAW,"
   "rating_activity     varchar (25)				      ENCODE ZSTD,"
   "time_stamp            timestamp    NOT NULL	,"
   "source_file         varchar(50)              ENCODE ZSTD)"
   "DISTSTYLE EVEN SORTKEY(time_stamp, rating_type, pair_id, player_id);"
   )
conn.execute(create_table5.execution_options(autocommit=True))