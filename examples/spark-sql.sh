#!/bin/sh

# this prints an absurd amount of noise to the console

if ! [ $# = 1 ]; then
  echo "usage ./$0 DIR"
  exit 1
fi

parquet_dir=`realpath $1`

java_options=`xargs <<END
-Djava.security.manager=allow
END`

spark-sql \
  --driver-java-options "$java_options" \
  --conf spark.ui.enabled=false \
  -e "SELECT eventTime,eventName FROM parquet.\`$parquet_dir\`;"
