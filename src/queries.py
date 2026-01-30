import logging
from pyspark.sql import SparkSession
from analysis_delays import DelayAnalyzer
from analysis_peak_hours import PeakHourAnalyzer

logger = logging.getLogger(__name__)


class AnalysisOrchestrator:
    def __init__(self,
                 data_path: str = "/opt/data/train_movements.parquet",
                 target_station: str = "Berlin Hbf",
                 peak_hours: list = None):
        self.data_path = data_path
        self.target_station = target_station
        self.peak_hours = peak_hours if peak_hours else [7, 8, 17, 18]
        self.spark = None

    def _create_spark_session(self):
        self.spark = (
            SparkSession.builder.appName("Train_Analysis")
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("ERROR")

    def run_all_analyses(self):
        logger.info("=" * 80)
        logger.info("Starting Train Movement Analysis")
        logger.info("=" * 80)

        self._create_spark_session()

        try:
            logger.info("[1/2] Analyzing Average Daily Delays...")
            delay_analyzer = DelayAnalyzer(self.spark, self.data_path)
            delay_analyzer.run(self.target_station)

            logger.info("[2/2] Analyzing Peak Hour Departures...")
            peak_analyzer = PeakHourAnalyzer(self.spark, self.data_path)
            peak_analyzer.run(self.peak_hours)

            logger.info("=" * 80)
            logger.info("All Analyses Complete!")
            logger.info("=" * 80)

        finally:
            if self.spark:
                self.spark.stop()

    def run_delay_analysis(self, station: str = None):
        station = station or self.target_station
        self._create_spark_session()

        try:
            analyzer = DelayAnalyzer(self.spark, self.data_path)
            return analyzer.run(station)
        finally:
            if self.spark:
                self.spark.stop()

    def run_peak_analysis(self, peak_hours: list = None):
        peak_hours = peak_hours or self.peak_hours
        self._create_spark_session()

        try:
            analyzer = PeakHourAnalyzer(self.spark, self.data_path)
            return analyzer.run(peak_hours)
        finally:
            if self.spark:
                self.spark.stop()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    orchestrator = AnalysisOrchestrator()
    orchestrator.run_all_analyses()


if __name__ == "__main__":
    main()
