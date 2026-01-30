import logging
from pyspark.sql import SparkSession
from processor_timetables import TimetableProcessor
from processor_changes import TimetableChangesProcessor
from processor_merge import DataMerger

logger = logging.getLogger(__name__)


class ETLPipeline:
    def __init__(
        self,
        timetables_base_path: str = "../data/timetables",
        changes_base_path: str = "../data/timetable_changes",
        timetables_output: str = "data/timetables_raw.parquet",
        changes_output: str = "data/changes_raw.parquet",
        final_output: str = "output/train_movements.parquet",
    ):

        self.timetables_base_path = timetables_base_path
        self.changes_base_path = changes_base_path
        self.timetables_output = timetables_output
        self.changes_output = changes_output
        self.final_output = final_output

    def _create_spark_session(self):
        self.spark = (
            SparkSession.builder.appName("DBahn_ETL_Pipeline")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .config("spark.sql.files.maxPartitionBytes", "128m")
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
            )
            timetable_processor.process()

            logger.info("[2/3] Processing timetable changes...")
            changes_processor = TimetableChangesProcessor(
                self.spark,
                base_path=self.changes_base_path,
                output_path=self.changes_output,
            )
            changes_processor.process()

            logger.info("[3/3] Merging data and calculating delays...")
            merger = DataMerger(
                self.spark,
                timetables_path=self.timetables_output,
                changes_path=self.changes_output,
                output_path=self.final_output,
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
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    pipeline = ETLPipeline()
    pipeline.run()


if __name__ == "__main__":
    main()
