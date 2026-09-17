import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, trim, desc, count

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input",
        required=True,
        help="Input CSV path",
    )

    parser.add_argument(
        "--output",
        required=True,
        help="Output parquet path",
    )

    parser.add_argument(
        "--release-year",
        type=int,
        default=2015,
        help="Minimum release year",
    )

    return parser.parse_args()

def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("NetflixGenreAggregation")
        .getOrCreate()
    )

    df = (
        spark.read
        .option("header", True)
        .option("multiLine", True)
        .option("escape", '"')
        .csv(args.input)
    )

    input_rows = df.count()
    print(f"input rows = {input_rows}")

    filtered = df.filter(
        col("release_year").cast("int") >= args.release_year
    )

    exploded = (
        filtered
        .select(
            col("type"),
            explode(split(col("listed_in"), ",")).alias("genre"),
        )
        .withColumn(
            "genre",
            trim(col("genre")),
        )
    )

    result = (
        exploded
        .groupBy("type", "genre")
        .agg(
            count("*").alias("title_count")
        )
        .orderBy(
            desc("title_count")
        )
    )

    aggregate_rows = result.count()

    print(f"release_year >= {args.release_year}")
    print(f"aggregate rows = {aggregate_rows}")

    result.show(50, truncate=False)

    (
        result.write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(args.output)
    )

    spark.stop()

if __name__ == "__main__":
    main()