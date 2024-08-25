from pyspark.sql.functions import coalesce, when, lit


def combine_cash_and_bonus_data(cash_df, bonus_df):
    combined_df = cash_df.join(
        bonus_df,
        (cash_df["account_id"] == bonus_df["account_id"]) & (cash_df["round_id"] == bonus_df["round_id"]),
        "full_outer"
    ) \
        .select(
        coalesce(cash_df["account_id"], bonus_df["account_id"]).alias("account_id"),
        coalesce(cash_df["round_id"], bonus_df["round_id"]).alias("round_id"),
        coalesce(cash_df["cash"], lit("0")).alias("cash_spent"),
        coalesce(bonus_df["bonus"], lit("0")).alias("bonus_spent"),
        when(cash_df["cash"].isNotNull(), cash_df["date_time"])
            .otherwise(bonus_df["date_time"]).alias("round_datetime")
    )

    return combined_df
