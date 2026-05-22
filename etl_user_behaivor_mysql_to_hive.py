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
        .appName("mysql_to_hive_ods_user_behavior_event")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sql("SET hive.exec.dynamic.partition=true")
    spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")

    base_query = """
        SELECT id, user_id, product_id, category_id, user_behavior,
               latitude, longitude, event_ts_ms
        FROM biz_db.user_behavior_event
    """

    if args.mode == "append":
        if not args.start_date or not args.end_date:
            raise ValueError("append 模式必须提供 --start-date 和 --end-date")
        # event_ts_ms 为毫秒时间戳
        base_query += f"""
        WHERE event_ts_ms >= UNIX_TIMESTAMP('{args.start_date}','yyyy-MM-dd')*1000
          AND event_ts_ms <  UNIX_TIMESTAMP('{args.end_date}','yyyy-MM-dd')*1000
        """

    df = (
        spark.read.format("jdbc")
        .option("url", args.mysql_url)
        .option("dbtable", f"({base_query}) t")
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

    df = (
        df.withColumn("latitude", F.col("latitude").cast("decimal(10,6)"))
          .withColumn("longitude", F.col("longitude").cast("decimal(10,6)"))
          .withColumn("dt", F.from_unixtime(F.col("event_ts_ms")/1000, "yyyy-MM-dd"))
    )

    target = "ods_db.ods_user_behacvior_event"

    if args.mode == "full":
        # 全量：首次全量导入可直接写入（会生成所有分区）
        df.write.mode("append").insertInto(target)
    else:
        # 增量：按时间分区追加写
        df.write.mode("append").insertInto(target)

    spark.stop()

if __name__ == "__main__":
    main()