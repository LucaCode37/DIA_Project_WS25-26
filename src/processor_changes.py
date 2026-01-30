import glob
import os
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
    def __init__(self, spark: SparkSession, base_path: str = "DBahn-berlin/timetable_changes",
                 output_path: str = "data/changes_raw.parquet", batch_size: int = 5000):

        self.spark = spark
        self.base_path = base_path
        self.output_path = output_path
        self.batch_size = batch_size
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
        logger.info("Searching for change files...")
        all_files = glob.glob(os.path.join(self.base_path, "**", "*.xml"), recursive=True)
        all_files.sort()
        total_files = len(all_files)
        logger.info(f"Found {total_files} change files")
        logger.info(f"First 10 change files: {all_files[:10]}")
        if total_files == 0:
            logger.warning(f"No change XML files found in {self.base_path}. Check path and file extensions.")
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

        logger.info("All changes processed successfully!")

    def _process_batch(self, file_list: list):
        if not file_list:
            logger.warning("Batch skipped: file_list is empty.")
            return
        batch_dir = os.path.dirname(file_list[0]) if file_list else self.base_path
        wildcard_path = os.path.join(batch_dir, "*.xml")
        df_changes_root = (
            self.spark.read.format("xml")
            .option("rowTag", "timetable")
            .schema(self.schema)
            .load(wildcard_path)
        )
        df_changes_stops = (
            df_changes_root.withColumn("s", explode("s"))
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

        df_changes.write.mode("append").parquet(self.output_path)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
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