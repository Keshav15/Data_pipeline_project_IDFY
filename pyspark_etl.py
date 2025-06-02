from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, to_timestamp, current_timestamp,
    min, max, sum, count, avg,
    datediff, unix_timestamp, array_distinct, array_contains,
    when, concat, array_join, coalesce, from_json,
    array_agg, lag, lead,
    struct, row_number,
    expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    IntegerType, BooleanType, FloatType, ArrayType, MapType
)
from pyspark.sql.window import Window


spark = SparkSession.builder.appName("MCQDataPipeline").getOrCreate()

# File location and type
file_location = "/FileStore/tables/events-4.json"
file_type = "json"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","


df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)



BIGQUERY_PROJECT = "varta"
BIGQUERY_DATASET = "idfy"



raw_events_df = raw_events_df.withColumn(
    "event_timestamp",
    to_timestamp(col("event_timestamp"), "yyyy-MM-dd HH:mm:ss 'UTC'") # Adjust format if needed
)


print("Creating dim_tests...")
dim_tests_df = df.filter(col("event_type") == "TEST_STARTED") \
    .groupBy("test_id") \
    .agg(
        max(col("event_timestamp")).alias("creation_timestamp"), # Using max as a proxy for creation
        lit("teacher1_default").alias("teacher_id"), # Placeholder, replace with actual teacher_id
        lit("Default Test Name").alias("test_name"), # Placeholder, replace with actual test_name
        lit(None).cast(TimestampType()).alias("deadline_timestamp"), # Placeholder
        lit(5).cast(IntegerType()).alias("total_questions"), # Placeholder
        lit("General").alias("subject"), # Placeholder
        col("metadata").alias("other_test_metadata") # Raw metadata string
    ) \
    .withColumn("other_test_metadata", from_json(col("other_test_metadata"), MapType(StringType(), StringType()))) # Parse JSON string to map

# Write dim_tests to BigQuery
dim_tests_df.write \
    .format("bigquery") \
    .option("table", f"{BIGQUERY_PROJECT}:{BIGQUERY_DATASET}.dim_tests") \
    .mode("overwrite") \
    .save()
print("dim_tests created and written to BigQuery.")


print("Creating dim_users...")
dim_users_df = raw_events_df.select("user_id", "event_timestamp") \
    .groupBy("user_id") \
    .agg(min("event_timestamp").alias("enrollment_date")) \
    .withColumn("user_name", concat(lit("User_"), col("user_id"))) 

# Write dim_users to BigQuery
dim_users_df.write \
    .format("bigquery") \
    .option("table", f"{BIGQUERY_PROJECT}:{BIGQUERY_DATASET}.dim_users") \
    .mode("overwrite") \
    .save()

dim_questions_df = raw_events_df.filter(col("event_type").isin("QUESTION_VIEWED", "QUESTION_ANSWERED")) \
    .select("question_id", "test_id", "page_number") \
    .distinct() \
    .withColumn("question_text", lit("Sample Question Text")) \
    .withColumn("correct_option", lit("A")) \
    .withColumn("options", expr("array('A', 'B', 'C', 'D')")) \
    .withColumn("question_type", lit("MCQ_SINGLE_CORRECT")) \
    .withColumn("difficulty_level", lit("Medium"))

# Write dim_questions to BigQuery
dim_questions_df.write \
    .format("bigquery") \
    .option("table", f"{BIGQUERY_PROJECT}:{BIGQUERY_DATASET}.dim_questions") \
    .mode("overwrite") \
    .save()
print("dim_questions created and written to BigQuery.")


print("Creating fact_test_attempts...")
window_test_attempt = Window.partitionBy("user_id", "test_id").orderBy("event_timestamp")

test_attempts_base = raw_events_df.withColumn(
    "rn", row_number().over(window_test_attempt)
) \
.filter(col("event_type").isin("TEST_STARTED", "TEST_SUBMITTED", "QUESTION_ANSWERED"))


test_start_end = test_attempts_base.groupBy("user_id", "test_id") \
    .agg(
        min(when(col("event_type") == "TEST_STARTED", col("event_timestamp"))).alias("start_timestamp"),
        max(when(col("event_type") == "TEST_SUBMITTED", col("event_timestamp"))).alias("end_timestamp"),
        array_distinct(col("session_id")).alias("all_sessions")
    )

question_summary = raw_events_df.filter(col("event_type") == "QUESTION_ANSWERED") \
    .groupBy("user_id", "test_id") \
    .agg(
        count(col("question_id")).alias("questions_answered"),
        sum(when(col("is_correct") == True, 1).otherwise(0)).alias("questions_correct"),
        max(col("page_number")).alias("last_page_visited") # Assuming page_number is max for last visited
    )

fact_test_attempts_df = test_start_end.join(question_summary, ["user_id", "test_id"], "left") \
    .withColumn("attempt_id", concat(col("user_id"), lit("_"), col("test_id"))) \
    .withColumn("status",
        when(col("end_timestamp").isNotNull(), "COMPLETED")
        .otherwise("IN_PROGRESS") 
    ) \
    .withColumn("total_score", coalesce(col("questions_correct") / col("questions_answered"), lit(0.0))) \
    .withColumn("completion_time_seconds",
        when(col("end_timestamp").isNotNull(), unix_timestamp(col("end_timestamp")) - unix_timestamp(col("start_timestamp")))
        .otherwise(None)
    ) \
    .withColumn("number_of_sessions", array_distinct(col("all_sessions")).size()) \
    .select(
        "attempt_id", "user_id", "test_id", "start_timestamp", "end_timestamp",
        "status", "total_score", "questions_answered", "questions_correct",
        "completion_time_seconds", "number_of_sessions", "last_page_visited"
    )

# Write fact_test_attempts to BigQuery
fact_test_attempts_df.write \
    .format("bigquery") \
    .option("table", f"{BIGQUERY_PROJECT}:{BIGQUERY_DATASET}.fact_test_attempts") \
    .mode("overwrite") \
    .save()
print("fact_test_attempts created and written to BigQuery.")


# fact_question_submissions
print("Creating fact_question_submissions...")
window_question_submission = Window.partitionBy("user_id", "test_id", "session_id").orderBy("event_timestamp")

fact_question_submissions_df = raw_events_df.filter(col("event_type").isin("QUESTION_ANSWERED", "QUESTION_RESUBMITTED")) \
    .withColumn("prev_event_timestamp", lag(col("event_timestamp"), 1).over(window_question_submission)) \
    .withColumn("prev_question_id", lag(col("question_id"), 1).over(window_question_submission)) \
    .withColumn("test_start_time", min(when(col("event_type") == "TEST_STARTED", col("event_timestamp"))).over(Window.partitionBy("user_id", "test_id"))) \
    .withColumn("is_resubmission", (col("event_type") == "QUESTION_RESUBMITTED")) \
    .withColumn("time_since_last_question_ms",
        when(col("prev_question_id").isNotNull() & (col("prev_question_id") != col("question_id")),
             (unix_timestamp(col("event_timestamp")) - unix_timestamp(col("prev_event_timestamp"))) * 1000)
        .otherwise(None)
    ) \
    .withColumn("time_since_test_start_ms",
        (unix_timestamp(col("event_timestamp")) - unix_timestamp(col("test_start_time"))) * 1000
    ) \
    .select(
        col("event_id").alias("submission_id"),
        concat(col("user_id"), lit("_"), col("test_id")).alias("attempt_id"),
        "user_id",
        "test_id",
        "session_id",
        "question_id",
        col("event_timestamp").alias("submission_timestamp"),
        "is_resubmission",
        "selected_option",
        "is_correct",
        "time_spent_on_question_ms", # Directly from raw event
        "time_since_last_question_ms",
        "time_since_test_start_ms"
    )

# Write fact_question_submissions to BigQuery
fact_question_submissions_df.write \
    .format("bigquery") \
    .option("table", f"{BIGQUERY_PROJECT}:{BIGQUERY_DATASET}.fact_question_submissions") \
    .mode("overwrite") \
    .save()
print("fact_question_submissions created and written to BigQuery.")


# fact_sessions
print("Creating fact_sessions...")
window_session = Window.partitionBy("user_id", "test_id", "session_id").orderBy("event_timestamp")

session_events = raw_events_df.filter(col("event_type").isin("SESSION_STARTED", "SESSION_ENDED", "QUESTION_ANSWERED", "QUESTION_VIEWED"))

fact_sessions_df = session_events.groupBy("user_id", "test_id", "session_id") \
    .agg(
        min(when(col("event_type") == "SESSION_STARTED", col("event_timestamp"))).alias("session_start_timestamp"),
        max(when(col("event_type") == "SESSION_ENDED", col("event_timestamp"))).alias("session_end_timestamp"),
        count(col("event_id")).alias("actions_in_session") # Count all events in session
    ) \
    .withColumn("attempt_id", concat(col("user_id"), lit("_"), col("test_id"))) \
    .withColumn("session_duration_seconds",
        when(col("session_end_timestamp").isNotNull(),
             unix_timestamp(col("session_end_timestamp")) - unix_timestamp(col("session_start_timestamp")))
        .otherwise(None) # Can be NULL if session is still active/not explicitly ended
    ) \
    .withColumn("ended_due_to_ttl", lit(False)) 

# Write fact_sessions to BigQuery
fact_sessions_df.write \
    .format("bigquery") \
    .option("table", f"{BIGQUERY_PROJECT}:{BIGQUERY_DATASET}.fact_sessions") \
    .mode("overwrite") \
    .save()
print("fact_sessions created and written to BigQuery.")

print("All fact and dimension tables processed and written to BigQuery.")

