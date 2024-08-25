from pyspark.sql.functions import udf, col, max, sum, round, concat_ws, datediff, count, lit
from pyspark.sql.types import IntegerType


def calculate_rfm_values(df):
    # Considering this date because we are analysing the data from 1st May 2022 to 20th July 2022
    max_datetime = "2022-07-21 00:00:00"

    agg_df = df.groupBy("account_id").agg(
        datediff(lit(max_datetime), max("round_datetime")).alias("recency"),
        count("round_id").alias("frequency"),
        round(sum("cash_spent"), 2).alias("monetary") # Considering only the cash_spent as the monetary value
    )

    return agg_df


def calculate_rfm_scores(df):
    recency_quantiles = df.approxQuantile("recency", [0.2, 0.4, 0.6, 0.8], 0.01)
    frequency_quantiles = df.approxQuantile("frequency", [0.2, 0.4, 0.6, 0.8], 0.01)
    monetary_quantiles = df.approxQuantile("monetary", [0.2, 0.4, 0.6, 0.8], 0.01)

    def score_recency(recency):
        if recency <= recency_quantiles[0]:
            return 5
        elif recency <= recency_quantiles[1]:
            return 4
        elif recency <= recency_quantiles[2]:
            return 3
        elif recency <= recency_quantiles[3]:
            return 2
        else:
            return 1

    def score_frequency(frequency):
        if frequency > frequency_quantiles[3]:
            return 5
        elif frequency > frequency_quantiles[2]:
            return 4
        elif frequency > frequency_quantiles[1]:
            return 3
        elif frequency > frequency_quantiles[0]:
            return 2
        else:
            return 1

    def score_monetary(monetary):
        if monetary > monetary_quantiles[3]:
            return 5
        elif monetary > monetary_quantiles[2]:
            return 4
        elif monetary > monetary_quantiles[1]:
            return 3
        elif monetary > monetary_quantiles[0]:
            return 2
        else:
            return 1

    udf_score_recency = udf(score_recency, IntegerType())
    udf_score_frequency = udf(score_frequency, IntegerType())
    udf_score_monetary = udf(score_monetary, IntegerType())

    rfm_df = df.withColumn("R_SCORE", udf_score_recency(col("recency"))) \
        .withColumn("F_SCORE", udf_score_frequency(col("frequency"))) \
        .withColumn("M_SCORE", udf_score_monetary(col("monetary"))) \
        .withColumn("FM_SCORE", concat_ws("", col("F_SCORE"), col("M_SCORE"))) \
        .withColumn("RFM_SCORE", concat_ws("", col("R_SCORE"), col("FM_SCORE")))

    return rfm_df