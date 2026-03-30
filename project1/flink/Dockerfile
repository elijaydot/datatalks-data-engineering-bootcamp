FROM flink:1.18-scala_2.12-java11

WORKDIR /opt/flink

# Install Python + pip
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Symlink python3 to python
RUN ln -s /usr/bin/python3 /usr/bin/python

# Install PyFlink and dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir apache-flink==1.18.0 \
    cassandra-driver==3.29.0 \
    duckdb==0.10.0 \
    confluent-kafka==2.3.0 \
    prometheus-client==0.20.0 \
    google-cloud-storage==2.14.0

# Copy job scripts into the image
COPY jobs/ /opt/flink/jobs/

# Flink config will be bind-mounted from host