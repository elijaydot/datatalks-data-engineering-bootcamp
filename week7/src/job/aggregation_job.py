"""
Aggregation Flink job: groups taxi rides into 1-hour tumbling windows,
counts trips and sums revenue per pickup location per hour.

Key concepts used here vs the pass-through job:
  - event_timestamp computed column (epoch ms -> TIMESTAMP)
  - WATERMARK: tells Flink when to close a window (patience for late events)
  - TUMBLE(): creates fixed 1-hour non-overlapping buckets
  - PRIMARY KEY with NOT ENFORCED: enables upsert in the JDBC sink
    so late events can correct already-published window results

Requires these tables in PostgreSQL:
    CREATE TABLE processed_events_aggregated (
        window_start    TIMESTAMP,
        PULocationID    INTEGER,
        num_trips       BIGINT,
        total_revenue   DOUBLE PRECISION,
        PRIMARY KEY (window_start, PULocationID)
    );

Submit with (from week7/ folder):
    docker compose exec jobmanager ./bin/flink run `
        -py /opt/src/job/aggregation_job.py `
        --pyFiles /opt/src -d
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def create_events_source_kafka(t_env):
    """
    Kafka source table — two new lines vs pass_through_job:

    1. event_timestamp AS TO_TIMESTAMP_LTZ(tpep_pickup_datetime, 3)
       A computed column that converts epoch milliseconds to a proper TIMESTAMP.
       '3' = milliseconds precision.

    2. WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
       Tells Flink: "wait up to 5 seconds for late events, then close the window."
       The watermark trails the latest event by 5 seconds. When it passes
       the end of a window, Flink publishes that window's results.
    """
    table_name = "events"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            PULocationID    INTEGER,
            DOLocationID    INTEGER,
            trip_distance   DOUBLE,
            total_amount    DOUBLE,
            tpep_pickup_datetime BIGINT,
            event_timestamp AS TO_TIMESTAMP_LTZ(tpep_pickup_datetime, 3),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector'                     = 'kafka',
            'properties.bootstrap.servers'  = 'redpanda:29092',
            'topic'                         = 'rides',
            'scan.startup.mode'             = 'earliest-offset',
            'properties.auto.offset.reset'  = 'earliest',
            'format'                        = 'json'
        );
    """
    t_env.execute_sql(source_ddl)
    return table_name


def create_aggregated_sink_postgres(t_env):
    """
    PostgreSQL sink with PRIMARY KEY + NOT ENFORCED.

    PRIMARY KEY enables upsert mode in the JDBC connector:
    - If Flink sends an UPDATE for a window (because a late event arrived),
      PostgreSQL updates the existing row instead of creating a duplicate.
    - NOT ENFORCED means Flink declares the key for its own routing logic
      but doesn't enforce uniqueness itself (PostgreSQL handles that).
    """
    table_name = "processed_events_aggregated"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            window_start    TIMESTAMP(3),
            PULocationID    INT,
            num_trips       BIGINT,
            total_revenue   DOUBLE,
            PRIMARY KEY (window_start, PULocationID) NOT ENFORCED
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


def log_aggregation():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(3)   # process 3 partitions in parallel

    # Set up the table environment with streaming mode settings
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        source_table     = create_events_source_kafka(t_env)
        aggregated_table = create_aggregated_sink_postgres(t_env)

        # TUMBLE() creates fixed, non-overlapping 1-hour windows.
        # DESCRIPTOR(event_timestamp) must reference the column that has
        # the WATERMARK defined on it.
        t_env.execute_sql(f"""
            INSERT INTO {aggregated_table}
            SELECT
                window_start,
                PULocationID,
                COUNT(*)            AS num_trips,
                SUM(total_amount)   AS total_revenue
            FROM TABLE(
                TUMBLE(
                    TABLE {source_table},
                    DESCRIPTOR(event_timestamp),
                    INTERVAL '1' HOUR
                )
            )
            GROUP BY window_start, PULocationID
        """).wait()

    except Exception as e:
        print(f"Aggregation job failed: {e}")


if __name__ == '__main__':
    log_aggregation()
