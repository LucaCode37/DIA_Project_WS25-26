import os
import glob
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    explode,
)
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

logger = logging.getLogger(__name__)


class TimetableProcessor:
    def __init__(self, spark: SparkSession, base_path: str = "DBahn-berlin/timetables",
                 output_path: str = "data/timetables_raw.parquet", batch_size: int = 5000):

        self.spark = spark
        self.base_path = base_path
        self.output_path = output_path
        self.batch_size = batch_size
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
                                            StructField("_c", StringType(), True),  # category
                                            StructField("_n", StringType(), True),  # number
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
        logger.info("Searching for timetable files...")
        all_files = glob.glob(os.path.join(self.base_path, "**", "*.xml"), recursive=True)
        all_files.sort()
        total_files = len(all_files)
        logger.info(f"Found {total_files} total files")
        logger.info(f"First 10 timetable files: {all_files[:10]}")
        if total_files == 0:
            logger.warning(f"No timetable XML files found in {self.base_path}. Check path and file extensions.")
        logger.info(f"Batch size: {self.batch_size} files")

        for start_idx in range(0, total_files, self.batch_size):
            end_idx = min(start_idx + self.batch_size, total_files)
            current_batch = all_files[start_idx:end_idx]

            progress = (start_idx / total_files) * 100
            logger.info(f"[{progress:.1f}%] Processing batch {start_idx}-{end_idx}...")

            try:
                self._process_batch(current_batch)
            except Exception as e:
                logger.error(f"Error in batch {start_idx}: {str(e)[:100]}...")
                continue

        logger.info("All timetables processed successfully!")

    def _process_batch(self, file_list: list):
        if not file_list:
            logger.warning("Batch skipped: file_list is empty.")
            return
        # Lade alle XML-Dateien im Batch-Ordner per Wildcard
        batch_dir = os.path.dirname(file_list[0]) if file_list else self.base_path
        wildcard_path = os.path.join(batch_dir, "*.xml")
        df_timetables_root = (
            self.spark.read.format("xml")
            .option("rowTag", "timetable")
            .schema(self.schema)
            .load(wildcard_path)
        )
        df_timetables_stops = df_timetables_root.withColumn("s", explode("s"))
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

        df_timetables.write.mode("append").parquet(self.output_path)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
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