from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, struct, to_json
from pyspark.sql.types import LongType, StringType, StructField, StructType

TOPIC_NAME_IN = "username_in"
TOPIC_NAME_OUT = "username_out"
POSTGRES_SETTINGS = {
    "url": "jdbc:postgresql://localhost:5432/de",
    "driver": "org.postgresql.Driver",
    "dbtable_restaurants": "public.subscribers_restaurants",
    "dbtable_feedback": "public.subscribers_feedback",
    "user": "jovyan",
    "password": "jovyan",
}
KAFKA_BOOTSTRAP_SERVERS = "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091"
KAFKA_SECURITY_PROTOCOL = "SASL_SSL"
KAFKA_SASL_JAAS_CFG = (
    "org.apache.kafka.common.security.scram.ScramLoginModule required "
    'username="de-student" password="ltcneltyn";',
)
KAFKA_SASL_MECHANISM = "SCRAM-SHA-512"


def foreach_batch_function(df, epoch_id):
    df.persist()
    df.write.format("parquet").mode("overwrite").save("/root/datas8/result_df")
    df.withColumn("feedback", lit(None)).write.format("jdbc").options(
        url=POSTGRES_SETTINGS["url"],
        driver=POSTGRES_SETTINGS["driver"],
        dbtable=POSTGRES_SETTINGS["dbtable_feedback"],
        user=POSTGRES_SETTINGS["user"],
        password=POSTGRES_SETTINGS["password"],
    ).mode("append").save()
    send_df = df.withColumn(
        "value",
        to_json(
            struct(
                col("restaurant_id"),
                col("adv_campaign_id"),
                col("adv_campaign_content"),
                col("adv_campaign_owner"),
                col("adv_campaign_owner_contact"),
                col("adv_campaign_datetime_start"),
                col("adv_campaign_datetime_end"),
                col("datetime_created"),
                col("client_id"),
                col("trigger_datetime_created"),
            )
        ),
    )
    send_df.write.mode("append").format("kafka").option(
        "kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS
    ).option("kafka.security.protocol", KAFKA_SECURITY_PROTOCOL).option(
        "kafka.sasl.jaas.config",
        KAFKA_SASL_JAAS_CFG,
    ).option(
        "kafka.sasl.mechanism", KAFKA_SASL_MECHANISM
    ).option(
        "topic", TOPIC_NAME_OUT
    ).option(
        "checkpointLocation", "query"
    ).save()
    df.unpersist()


# pylint: disable=invalid-name
spark_jars_packages = ",".join(
    [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        "org.postgresql:postgresql:42.4.0",
    ]
)

spark = (
    SparkSession.builder.appName("RestaurantSubscribeStreamingService")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.jars.packages", spark_jars_packages)
    .getOrCreate()
)
restaurant_read_stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("kafka.security.protocol", KAFKA_SECURITY_PROTOCOL)
    .option(
        "kafka.sasl.jaas.config",
        KAFKA_SASL_JAAS_CFG,
    )
    .option("kafka.sasl.mechanism", KAFKA_SASL_MECHANISM)
    .option("subscribe", TOPIC_NAME_IN)
    .load()
)

incomming_message_schema = StructType(
    [
        StructField("restaurant_id", StringType(), nullable=True),
        StructField("adv_campaign_id", StringType(), nullable=True),
        StructField("adv_campaign_content", StringType(), nullable=True),
        StructField("adv_campaign_owner", StringType(), nullable=True),
        StructField("adv_campaign_owner_contact", StringType(), nullable=True),
        StructField("adv_campaign_datetime_start", LongType(), nullable=True),
        StructField("adv_campaign_datetime_end", LongType(), nullable=True),
        StructField("datetime_created", LongType(), nullable=True),
    ]
)

current_timestamp_utc = int(round(datetime.now(timezone.utc).timestamp()))

filtered_read_stream_df = restaurant_read_stream_df.withColumn(
    "value", restaurant_read_stream_df.value.cast("string")
).withColumn("key", restaurant_read_stream_df.key.cast("string"))
filtered_read_stream_df = (
    filtered_read_stream_df.withColumn(
        "value_json",
        from_json(filtered_read_stream_df.value, incomming_message_schema),
    )
    .selectExpr("value_json.*", "*")
    .drop("value_json")
)
filtered_read_stream_df = filtered_read_stream_df.filter(
    col("adv_campaign_datetime_start") < current_timestamp_utc
).filter(col("adv_campaign_datetime_end") > current_timestamp_utc)


subscribers_restaurant_df = (
    spark.read.format("jdbc")
    .options(
        url=POSTGRES_SETTINGS["url"],
        driver=POSTGRES_SETTINGS["driver"],
        dbtable=POSTGRES_SETTINGS["dbtable_restaurants"],
        user=POSTGRES_SETTINGS["user"],
        password=POSTGRES_SETTINGS["password"],
    )
    .load()
    .dropDuplicates(["client_id", "restaurant_id"])
    .cache()
)

result_df = (
    filtered_read_stream_df.join(subscribers_restaurant_df, "restaurant_id", "inner")
    .select(
        [
            "restaurant_id",
            "adv_campaign_id",
            "adv_campaign_content",
            "adv_campaign_owner",
            "adv_campaign_owner_contact",
            "adv_campaign_datetime_start",
            "adv_campaign_datetime_end",
            "datetime_created",
            "client_id",
        ]
    )
    .withColumn("trigger_datetime_created", lit(current_timestamp_utc))
)

result_df.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()
