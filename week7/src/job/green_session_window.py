"""
Q5: Session window with 5-minute gap on PULocationID.
Find the session with the most trips.

Submit with:
    docker exec -it week7-jobmanager-1 flink run -py /opt/src/job/green_session_window.py
"""
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(10_000)

    t_env = StreamTableEnvironment.create(
        env,
        environment_settings=EnvironmentSettings.new_instance().in_streaming_mode().build()
    )

    t_env.execute_sql("""
        CREATE TABLE green_trips (
            lpep_pickup_datetime  VARCHAR,
            lpep_dropoff_datetime VARCHAR,
            PULocationID          INTEGER,
            DOLocationID          INTEGER,
            trip_distance         DOUBLE,
            tip_amount            DOUBLE,
            total_amount          DOUBLE,
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector'                    = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic'                        = 'green-trips',
            'scan.startup.mode'            = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format'                       = 'json'
        )
    """)

    t_env.execute_sql("""
        CREATE TABLE trips_by_session (
            window_start  TIMESTAMP(3),
            window_end    TIMESTAMP(3),
            PULocationID  INTEGER,
            num_trips     BIGINT,
            PRIMARY KEY (window_start, PULocationID) NOT ENFORCED
        ) WITH (
            'connector'  = 'jdbc',
            'url'        = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = 'trips_by_session',
            'username'   = 'postgres',
            'password'   = 'postgres',
            'driver'     = 'org.postgresql.Driver'
        )
    """)

    t_env.execute_sql("""
        INSERT INTO trips_by_session
        SELECT window_start, window_end, PULocationID, COUNT(*) AS num_trips
        FROM TABLE(
            SESSION(TABLE green_trips, DESCRIPTOR(event_timestamp), INTERVAL '5' MINUTE)
        )
        GROUP BY window_start, window_end, PULocationID
    """).wait()

if __name__ == '__main__':
    main()