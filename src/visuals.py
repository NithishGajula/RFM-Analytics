import pandas
from matplotlib import pyplot as plt
from seaborn import countplot, scatterplot, heatmap, color_palette

def plot_visuals(df):
    segmented_df_pd = df.toPandas()

    plt.figure(figsize=(10, 6))
    countplot(data=segmented_df_pd, x='segment', order=segmented_df_pd['segment'].value_counts().index, palette='viridis')
    plt.title('Customer Segmentation Count')
    plt.xticks(rotation=45)
    plt.show()

    plt.figure(figsize=(8, 8))
    segmentation_counts = segmented_df_pd['segment'].value_counts()
    plt.pie(segmentation_counts, labels=segmentation_counts.index, autopct='%1.1f%%', startangle=140, colors=color_palette('viridis', len(segmentation_counts)))
    plt.title('Customer Segmentation Distribution')
    plt.show()

    rfm_scores = segmented_df_pd.pivot_table(index='R_SCORE', columns='F_SCORE', values='M_SCORE', aggfunc='mean')
    plt.figure(figsize=(10, 8))
    heatmap(rfm_scores, annot=True, fmt=".1f", cmap='coolwarm')
    plt.title('Heatmap of RFM Scores by Segment')
    plt.show()

    plt.figure(figsize=(10, 6))
    scatterplot(data=segmented_df_pd, x='recency', y='frequency', size='monetary', hue='segment', palette='viridis', sizes=(20, 200))
    plt.title('Recency vs Frequency vs Monetary')
    plt.show()
