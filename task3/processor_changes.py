import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    input_file_name,
    regexp_extract,
    to_timestamp,
    explode,
)
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

logger = logging.getLogger(__name__)


class TimetableChangesProcessor:
    def __init__(
        self,
        spark: SparkSession,
        base_path: str = "DBahn-berlin/timetable_changes",
        output_path: str = "data/changes_raw.parquet",
    ):
        self.spark = spark
        self.base_path = base_path
        self.output_path = output_path
        self.schema = self._create_schema()

    def _create_schema(self) -> StructType:
        return StructType(
            [
                StructField(
                    "s",
                    ArrayType(
                        StructType(
                            [
                                StructField("_id", StringType(), True),
                                StructField(
                                    "ar",
                                    StructType(
                                        [
                                            StructField(
                                                "_ct", StringType(), True
                                            ),  # changed time
                                            StructField(
                                                "_cp", StringType(), True
                                            ),  # changed platform
                                            StructField(
                                                "_cs", StringType(), True
                                            ),  # changed status
                                        ]
                                    ),
                                    True,
                                ),
                                StructField(
                                    "dp",
                                    StructType(
                                        [
                                            StructField("_ct", StringType(), True),
                                            StructField("_cp", StringType(), True),
                                            StructField("_cs", StringType(), True),
                                        ]
                                    ),
                                    True,
                                ),
                            ]
                        )
                    ),
                    True,
                )
            ]
        )

    def process(self):
        logger.info("Processing timetable changes...")

        input_pattern = f"{self.base_path}/*/*/*.xml"

        df_raw = (
            self.spark.read.format("xml")
            .option("rowTag", "timetable")
            .schema(self.schema)
            .load(input_pattern)
        )

        df_changes_stops = (
            df_raw.withColumn("s", explode("s"))
            .withColumn("file_path", input_file_name())
            .withColumn(
                "update_timestamp",
                regexp_extract("file_path", r"/(\d{10})/[^/]+\.xml$", 1),
            )
        )

        df_changes = df_changes_stops.select(
            col("s._id").alias("stop_id"),
            to_timestamp(col("s.ar._ct").cast("string"), "yyMMddHHmm").alias(
                "actual_arrival"
            ),
            col("s.ar").getField("_cp").alias("actual_platform_arrival"),
            col("s.ar._cs").alias("arrival_status"),
            to_timestamp(col("s.dp._ct").cast("string"), "yyMMddHHmm").alias(
                "actual_departure"
            ),
            col("s.dp").getField("_cp").alias("actual_platform_departure"),
            col("s.dp._cs").alias("departure_status"),
            col("file_path").alias("source_file"),
            col("update_timestamp"),
        )

        logger.info(f"Writing data to {self.output_path}...")
        df_changes.write.mode("overwrite").parquet(self.output_path)
        logger.info("Changes processing complete.")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    spark = (
        SparkSession.builder.appName("Changes_Processor")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    processor = TimetableChangesProcessor(spark)
    processor.process()


if __name__ == "__main__":
    main()
