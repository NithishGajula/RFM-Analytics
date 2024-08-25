from src.rfm_analysis import calculate_rfm_scores, calculate_rfm_values
from src.process_data import combine_cash_and_bonus_data
from src.segmentation import add_segmentation
from pyspark.sql import SparkSession


def main():
    cash_file_path = "src/resources/cash.csv"
    bonus_file_path = "src/resources/bonus.csv"
    output_path = "target/output"

    spark = SparkSession.builder.appName("RFM Analysis").getOrCreate()

    cash_df = spark.read.csv(cash_file_path, header=True, inferSchema=True)
    bonus_df = spark.read.csv(bonus_file_path, header=True, inferSchema=True)

    combined_df = combine_cash_and_bonus_data(cash_df, bonus_df)
    rfm_values_df = calculate_rfm_values(combined_df)
    rfm_scores_df = calculate_rfm_scores(rfm_values_df)
    segmented_df = add_segmentation(rfm_scores_df)

    segmented_df.write.csv(output_path, header=True, mode="overwrite")

    spark.stop()


if __name__ == "__main__":
    main()
