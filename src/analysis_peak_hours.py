import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, round

logger = logging.getLogger(__name__)


class PeakHourAnalyzer:

    def __init__(self, spark: SparkSession, data_path: str = "/opt/data/train_movements.parquet"):
        self.spark = spark
        self.data_path = data_path
        self.df = None

    def load_data(self):
        self.df = self.spark.read.parquet(self.data_path)

    def analyze_peak_hours(self, peak_hours: list = None, show_results: int = 200):

        if peak_hours is None:
            peak_hours = [7, 8, 17, 18]

        logger.info("Calculating peak hour statistics... (This might take a moment)")

        peak_condition = (col("planned_departure").isNotNull()) & (
            col("hour").isin(peak_hours)
        )

        daily_peak_counts = (
            self.df.filter(peak_condition)
            .groupBy("station_name", "date")
            .agg(count("*").alias("daily_departures"))
        )

        final_stats = (
            daily_peak_counts.groupBy("station_name")
            .agg(
                round(avg("daily_departures"), 2).alias("avg_peak_departures"),
                count("daily_departures").alias("days_observed"),
            )
            .orderBy(col("avg_peak_departures").desc())
        )

        return final_stats

    def run(self, peak_hours: list = None, show_results: int = 200):
        self.load_data()
        results = self.analyze_peak_hours(peak_hours, show_results)

        logger.info("\nAverage Peak Hour Departures per Station:")
        results.sort("avg_peak_departures", ascending=False).show(show_results, truncate=False)

        return results


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    spark = SparkSession.builder.appName("PeakHourAnalysis").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    analyzer = PeakHourAnalyzer(spark)
    analyzer.run()


if __name__ == "__main__":
    main()
