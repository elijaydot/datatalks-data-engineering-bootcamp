"""
Pass-through Flink job: reads rides from Kafka and writes them to PostgreSQL.
This is the Flink equivalent of consumer_postgres.py — same result,
but Flink handles offset tracking, checkpointing, and DB writes automatically.

Submit with (from week7/ folder):
    docker compose exec jobmanager ./bin/flink run `
        -py /opt/src/job/pass_through_job.py `
        --pyFiles /opt/src -d

NOTE: backtick ` is the PowerShell line-continuation character.
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def create_events_source_kafka(t_env):
    """
    Declare the Kafka topic as a Flink SQL table.
    Flink will deserialize JSON automatically and map fields to columns.

    'redpanda:29092' is the INTERNAL Docker address — Flink runs inside
    Docker, so it can't use localhost:9092 (that's for scripts on your PC).
    """
    table_name = "events"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            PULocationID    INTEGER,
            DOLocationID    INTEGER,
            trip_distance   DOUBLE,
            total_amount    DOUBLE,
            tpep_pickup_datetime BIGINT
        ) WITH (
            'connector'                     = 'kafka',
            'properties.bootstrap.servers'  = 'redpanda:29092',
            'topic'                         = 'rides',
            'scan.startup.mode'             = 'latest-offset',
            'properties.auto.offset.reset'  = 'latest',
            'format'                        = 'json'
        );
    """
    t_env.execute_sql(source_ddl)
    return table_name


def create_processed_events_sink_postgres(t_env):
    """
    Declare the PostgreSQL table as a Flink SQL sink.
    Flink uses JDBC to insert rows — no psycopg2 needed.

    'postgres:5432' is the internal Docker service name.
    """
    table_name = "processed_events"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            PULocationID    INTEGER,
            DOLocationID    INTEGER,
            trip_distance   DOUBLE,
            total_amount    DOUBLE,
            pickup_datetime TIMESTAMP
        ) WITH (
            'connector'  = 'jdbc',
            'url'        = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username'   = 'postgres',
            'password'   = 'postgres',
            'driver'     = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def log_processing():
    # Set up the Flink streaming environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)   # checkpoint every 10 seconds

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    source_table  = create_events_source_kafka(t_env)
    sink_table    = create_processed_events_sink_postgres(t_env)

    # The pipeline: read from Kafka, convert timestamp, write to PostgreSQL
    t_env.execute_sql(f"""
        INSERT INTO {sink_table}
        SELECT
            PULocationID,
            DOLocationID,
            trip_distance,
            total_amount,
            TO_TIMESTAMP_LTZ(tpep_pickup_datetime, 3) AS pickup_datetime
        FROM {source_table}
    """).wait()


if __name__ == '__main__':
    log_processing()
