import os
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
import logging
from pyspark.sql import SparkSession
from processor_timetables import TimetableProcessor
from processor_changes import TimetableChangesProcessor
from processor_merge import DataMerger

logger = logging.getLogger(__name__)


class ETLPipeline:
    def __init__(self,
                 timetables_base_path: str = None,
                 changes_base_path: str = None,
                 timetables_output: str = None,
                 changes_output: str = None,
                 final_output: str = None,
                 batch_size: int = 10000):

        self.timetables_base_path = timetables_base_path or str(PROJECT_ROOT / "data" / "timetables")
        self.changes_base_path = changes_base_path or str(PROJECT_ROOT / "data" / "timetable_changes")
        self.timetables_output = timetables_output or str(PROJECT_ROOT / "data" / "timetables_raw.parquet")
        self.changes_output = changes_output or str(PROJECT_ROOT / "data" / "changes_raw.parquet")
        self.final_output = final_output or str(PROJECT_ROOT / "data" / "train_movements.parquet")
        self.batch_size = batch_size
        self.spark = None

    def _create_spark_session(self):
        self.spark = (
            SparkSession.builder.appName("DBahn_ETL_Pipeline")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .config("spark.sql.files.maxPartitionBytes", "128m")
            .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.15.0")
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("ERROR")

    def run(self):
        logger.info("=" * 80)
        logger.info("Starting ETL Pipeline")
        logger.info("=" * 80)

        self._create_spark_session()

        try:
            logger.info("[1/3] Processing timetables...")
            timetable_processor = TimetableProcessor(
                self.spark,
                base_path=self.timetables_base_path,
                output_path=self.timetables_output,
                batch_size=self.batch_size
            )
            timetable_processor.process()

            logger.info("[2/3] Processing timetable changes...")
            changes_processor = TimetableChangesProcessor(
                self.spark,
                base_path=self.changes_base_path,
                output_path=self.changes_output,
                batch_size=self.batch_size
            )
            changes_processor.process()

            logger.info("[3/3] Merging data and calculating delays...")
            merger = DataMerger(
                self.spark,
                timetables_path=self.timetables_output,
                changes_path=self.changes_output,
                output_path=self.final_output
            )
            merger.process()

            logger.info("=" * 80)
            logger.info("ETL Pipeline Complete!")
            logger.info("=" * 80)

        finally:
            if self.spark:
                self.spark.stop()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    pipeline = ETLPipeline()
    pipeline.run()


if __name__ == "__main__":
    main()
