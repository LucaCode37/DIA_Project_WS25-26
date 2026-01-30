import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

logger = logging.getLogger(__name__)


class DelayAnalyzer:

    def __init__(self, spark: SparkSession, data_path: str = "/opt/data/train_movements.parquet"):
        self.spark = spark
        self.data_path = data_path
        self.df = None

    def load_data(self):
        self.df = self.spark.read.parquet(self.data_path)

    def analyze_station_delay(self, station_name: str) -> float:
        logger.info(f"Analyzing delay for: {station_name}...")

        df_station = self.df.filter(col("station_name") == station_name).filter(
            col("is_cancelled") == False
        )

        if df_station.count() == 0:
            return None

        daily_stats = df_station.groupBy("date").agg(
            avg("arrival_delay_minutes").alias("avg_delay_that_day")
        )

        daily_stats.sort("date").show(50)

        result = daily_stats.agg(avg("avg_delay_that_day").alias("final_avg")).collect()

        return result[0]["final_avg"]

    def run(self, station_name: str = "Berlin Hbf"):
        self.load_data()
        avg_delay = self.analyze_station_delay(station_name)

        if avg_delay is not None:
            logger.info(f"\nResults for {station_name}:")
            logger.info(f" Average Daily Delay: {avg_delay:.2f} minutes")
        else:
            logger.warning(f"\nNo data found for station: {station_name}")

        return avg_delay


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    spark = SparkSession.builder.appName("DelayAnalysis").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    analyzer = DelayAnalyzer(spark)
    analyzer.run("Berlin Hbf")


if __name__ == "__main__":
    main()
