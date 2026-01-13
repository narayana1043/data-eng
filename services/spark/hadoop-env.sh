export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_OPTS="
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED
--add-opens=java.base/java.io=ALL-UNNAMED
--add-opens=java.base/java.net=ALL-UNNAMED
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
--add-opens=java.base/java.util.concurrent=ALL-UNNAMED
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
"
export YARN_OPTS="$HADOOP_OPTS"
