import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, hour, unix_timestamp, row_number
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


class DataMerger:
    def __init__(
        self,
        spark: SparkSession,
        timetables_path: str = "data/timetables_raw.parquet",
        changes_path: str = "data/changes_raw.parquet",
        output_path: str = "output/train_movements.parquet",
    ):

        self.spark = spark
        self.timetables_path = timetables_path
        self.changes_path = changes_path
        self.output_path = output_path

    def process(self):
        logger.info("Reading Parquet files...")
        df_planned = self.spark.read.parquet(self.timetables_path)
        df_updates = self.spark.read.parquet(self.changes_path)

        logger.info(f"Planned trains: {df_planned.count()}")
        logger.info(f"Status updates: {df_updates.count()}")

        logger.info("Deduplicating updates...")
        df_updates_latest = self._deduplicate_updates(df_updates)

        logger.info("Merging datasets...")
        df_merged = self._merge_data(df_planned, df_updates_latest)

        logger.info("Calculating delays...")
        df_final = self._calculate_delays(df_merged)

        logger.info("Writing final dataset...")
        self._write_output(df_final)

        logger.info("COMPLETE! Final dataset is ready")

    def _deduplicate_updates(self, df_updates):
        window_spec = Window.partitionBy("stop_id").orderBy(
            col("update_timestamp").desc()
        )
        return (
            df_updates.withColumn("row_num", row_number().over(window_spec))
            .filter(col("row_num") == 1)
            .drop("row_num", "update_timestamp")
        )

    def _merge_data(self, df_planned, df_updates_latest):
        return df_planned.join(
            df_updates_latest,
            on="stop_id",
            how="left",  # Keep all planned trains, even if they have no updates
        )

    def _calculate_delays(self, df_merged):
        return (
            df_merged.withColumn(
                "arrival_delay_minutes",
                when(
                    col("actual_arrival").isNotNull(),
                    when(
                        (
                            unix_timestamp("actual_arrival")
                            - unix_timestamp("planned_arrival")
                        )
                        / 60
                        < 0,
                        0,
                    ).otherwise(
                        (
                            unix_timestamp("actual_arrival")
                            - unix_timestamp("planned_arrival")
                        )
                        / 60
                    ),
                ).otherwise(0),
            )
            .withColumn(
                "departure_delay_minutes",
                when(
                    col("actual_departure").isNotNull(),
                    when(
                        (
                            unix_timestamp("actual_departure")
                            - unix_timestamp("planned_departure")
                        )
                        / 60
                        < 0,
                        0,
                    ).otherwise(
                        (
                            unix_timestamp("actual_departure")
                            - unix_timestamp("planned_departure")
                        )
                        / 60
                    ),
                ).otherwise(0),
            )
            .withColumn(
                "is_cancelled",
                when(
                    (col("arrival_status").isNotNull() & (col("arrival_status") == "c"))
                    | (
                        col("departure_status").isNotNull()
                        & (col("departure_status") == "c")
                    ),
                    True,
                ).otherwise(False),
            )
            .withColumn(
                "date",
                when(
                    col("planned_departure").isNotNull(), to_date("planned_departure")
                ).otherwise(to_date("planned_arrival")),
            )
            .withColumn(
                "hour",
                when(
                    col("planned_departure").isNotNull(), hour("planned_departure")
                ).otherwise(hour("planned_arrival")),
            )
        )

    def _write_output(self, df_final):
        df_final.select(
            "stop_id",
            "station_name",
            "train_category",
            "train_number",
            "planned_arrival",
            "actual_arrival",
            "arrival_delay_minutes",
            "planned_departure",
            "actual_departure",
            "departure_delay_minutes",
            "is_cancelled",
            "date",
            "hour",
        ).write.mode("overwrite").partitionBy("date", "hour").parquet(self.output_path)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    spark = (
        SparkSession.builder.appName("Finalize_ETL")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    merger = DataMerger(spark)
    merger.process()


if __name__ == "__main__":
    main()
