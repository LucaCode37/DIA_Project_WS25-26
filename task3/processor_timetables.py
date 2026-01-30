import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, explode
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

logger = logging.getLogger(__name__)


class TimetableProcessor:
    def __init__(
        self,
        spark: SparkSession,
        base_path: str = "DBahn-berlin/timetables",
        output_path: str = "data/timetables_raw.parquet",
    ):
        self.spark = spark
        self.base_path = base_path
        self.output_path = output_path
        self.schema = self._create_schema()

    def _create_schema(self) -> StructType:
        return StructType(
            [
                StructField("_station", StringType(), True),
                StructField(
                    "s",
                    ArrayType(
                        StructType(
                            [
                                StructField("_id", StringType(), True),
                                StructField(
                                    "tl",
                                    StructType(
                                        [
                                            StructField(
                                                "_c", StringType(), True
                                            ),  # category
                                            StructField(
                                                "_n", StringType(), True
                                            ),  # number
                                        ]
                                    ),
                                    True,
                                ),
                                StructField(
                                    "ar",
                                    StructType(
                                        [
                                            StructField(
                                                "_pt", StringType(), True
                                            ),  # planned time
                                            StructField(
                                                "_pp", StringType(), True
                                            ),  # planned platform
                                        ]
                                    ),
                                    True,
                                ),
                                StructField(
                                    "dp",
                                    StructType(
                                        [
                                            StructField("_pt", StringType(), True),
                                            StructField("_pp", StringType(), True),
                                        ]
                                    ),
                                    True,
                                ),
                            ]
                        )
                    ),
                    True,
                ),
            ]
        )

    def process(self):
        logger.info("Processing timetables...")

        input_pattern = f"{self.base_path}/*/*/*.xml"

        df_raw = (
            self.spark.read.format("xml")
            .option("rowTag", "timetable")
            .schema(self.schema)
            .load(input_pattern)
        )

        df_timetables_stops = df_raw.withColumn("s", explode("s"))

        df_timetables = df_timetables_stops.select(
            col("_station").alias("station_name"),
            col("s._id").alias("stop_id"),
            col("s.tl._c").alias("train_category"),
            col("s.tl._n").alias("train_number"),
            to_timestamp(col("s.ar._pt").cast("string"), "yyMMddHHmm").alias(
                "planned_arrival"
            ),
            col("s.ar._pp").alias("platform_arrival"),
            to_timestamp(col("s.dp._pt").cast("string"), "yyMMddHHmm").alias(
                "planned_departure"
            ),
            col("s.dp._pp").alias("platform_departure"),
        )

        logger.info(f"Writing data to {self.output_path}...")
        df_timetables.write.mode("overwrite").parquet(self.output_path)
        logger.info("Timetable processing complete.")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    spark = (
        SparkSession.builder.appName("Timetable_Processor")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.files.maxPartitionBytes", "128m")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    processor = TimetableProcessor(spark)
    processor.process()


if __name__ == "__main__":
    main()
