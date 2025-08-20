#!/bin/bash
# Workaround for incomplete Spark installation using Maven classpath

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Get the Maven classpath
SPARK_CLASSPATH=$(cat spark-classpath.txt)

# Find Java executable
JAVA_CMD="java"
if [ -n "$JAVA_HOME" ]; then
    JAVA_CMD="$JAVA_HOME/bin/java"
fi

# Spark configuration
SPARK_LOCAL_IP=${SPARK_LOCAL_IP:-127.0.0.1}
DRIVER_MEMORY=${DRIVER_MEMORY:-8G}
SPARK_MASTER=${SPARK_MASTER:-"local[*]"}

# Extract JAR file from arguments (last argument that ends with .jar)
JAR_FILE=""
for arg in "$@"; do
    if [[ "$arg" == *.jar ]]; then
        JAR_FILE="$arg"
    fi
done

# Set up Java options for Spark, including the JAR file in classpath
JAVA_OPTS="-cp $SPARK_CLASSPATH:$JAR_FILE"
JAVA_OPTS="$JAVA_OPTS -Dspark.driver.host=$SPARK_LOCAL_IP"
JAVA_OPTS="$JAVA_OPTS -Dspark.driver.memory=$DRIVER_MEMORY"
JAVA_OPTS="$JAVA_OPTS -Dspark.master=$SPARK_MASTER"
JAVA_OPTS="$JAVA_OPTS -Dspark.app.name=GLnext"

# Add JVM options for Java 17 compatibility with Spark 3.2.0
JAVA_OPTS="$JAVA_OPTS --add-opens=java.base/java.lang=ALL-UNNAMED"
JAVA_OPTS="$JAVA_OPTS --add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
JAVA_OPTS="$JAVA_OPTS --add-opens=java.base/java.lang.reflect=ALL-UNNAMED"
JAVA_OPTS="$JAVA_OPTS --add-opens=java.base/java.io=ALL-UNNAMED"
JAVA_OPTS="$JAVA_OPTS --add-opens=java.base/java.net=ALL-UNNAMED"
JAVA_OPTS="$JAVA_OPTS --add-opens=java.base/java.nio=ALL-UNNAMED"
JAVA_OPTS="$JAVA_OPTS --add-opens=java.base/java.util=ALL-UNNAMED"
JAVA_OPTS="$JAVA_OPTS --add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
JAVA_OPTS="$JAVA_OPTS --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED"
JAVA_OPTS="$JAVA_OPTS --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
JAVA_OPTS="$JAVA_OPTS --add-opens=java.base/sun.nio.cs=ALL-UNNAMED"
JAVA_OPTS="$JAVA_OPTS --add-opens=java.base/sun.security.action=ALL-UNNAMED"
JAVA_OPTS="$JAVA_OPTS --add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
JAVA_OPTS="$JAVA_OPTS --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"

# Add any additional Java options
if [ -n "$_JAVA_OPTIONS" ]; then
    JAVA_OPTS="$JAVA_OPTS $_JAVA_OPTIONS"
fi

# Run the Spark application
exec $JAVA_CMD $JAVA_OPTS org.apache.spark.deploy.SparkSubmit "$@"