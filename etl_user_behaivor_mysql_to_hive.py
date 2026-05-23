#!/usr/bin/env python3
from pyspark.sql import SparkSession, functions as F
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["full", "append"], required=True)
    parser.add_argument("--start-date", help="yyyy-MM-dd (仅append)")
    parser.add_argument("--end-date", help="yyyy-MM-dd (仅append, 右开区间)")
    parser.add_argument("--mysql-url", required=True)
    parser.add_argument("--mysql-user", required=True)
    parser.add_argument("--mysql-password", required=True)
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("mysql_to_hive_ods_user_behavior_events")
        .enableHiveSupport()
        .getOrCreate()
    )

    mysql_table = "user_behavior_events"
    hive_table = "ods.user_behavior_events"

    # 从 MySQL 读取整个表（利用分区读取加速）
    df = (
        spark.read.format("jdbc")
        .option("url", args.mysql_url)
        .option("dbtable", mysql_table)
        .option("user", args.mysql_user)
        .option("password", args.mysql_password)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("fetchsize", "10000")
        .option("partitionColumn", "id")
        .option("lowerBound", "1")
        .option("upperBound", "1000000000")
        .option("numPartitions", "8")
        .load()
    )

    # 增量模式：按时间戳过滤
    if args.mode == "append":
        if not args.start_date or not args.end_date:
            raise ValueError("append 模式必须提供 --start-date 和 --end-date")
        start_ts = int(pd.Timestamp(args.start_date).timestamp() * 1000)  # 毫秒
        end_ts = int(pd.Timestamp(args.end_date).timestamp() * 1000)
        df = df.filter((F.col("event_ts_ms") >= start_ts) & (F.col("event_ts_ms") < end_ts))

    # 写入 Hive（全量覆盖，增量追加）
    if args.mode == "full":
        df.write.mode("overwrite").saveAsTable(hive_table)
    else:
        df.write.mode("append").saveAsTable(hive_table)

    spark.stop()

if __name__ == "__main__":
    import pandas as pd   # 放在这里避免全局导入冲突，也可以用 pyspark 内置方法
    main()