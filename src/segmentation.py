from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType


def add_segmentation(df):
    def segment_rfm(rfm_score):
        if rfm_score == '555':
            return 'Champions'
        elif rfm_score == '455':
            return 'Loyal Customers'
        elif rfm_score == '545':
            return 'Potential Loyalists'
        elif rfm_score == '444':
            return 'New Customers'
        elif rfm_score == '554':
            return 'Promising'
        elif rfm_score == '344':
            return 'Need Attention'
        elif rfm_score == '111':
            return 'At Risk'
        elif rfm_score == '211':
            return 'Hibernating'
        elif rfm_score == '122':
            return 'Lost'
        else:
            return 'Other'

    udf_segment_rfm = udf(segment_rfm, StringType())

    segmented_df = df.withColumn("segment", udf_segment_rfm(col("RFM_SCORE")))

    return segmented_df