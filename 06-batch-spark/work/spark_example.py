"""
Spark Example — Docker 환경에서 실행
spark-submit 또는 Jupyter Notebook에서 실행 가능
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    # SparkSession 생성 (Docker 내부에서는 master를 local로 사용해도 OK)
    spark = (
        SparkSession.builder
        .appName("ZoomcampSparkExample")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # ── 1. 간단한 DataFrame 생성 ─────────────────────────────
    data = [
        ("Alice", 34, "Engineering"),
        ("Bob", 45, "Marketing"),
        ("Charlie", 29, "Engineering"),
        ("Diana", 38, "Marketing"),
        ("Eve", 50, "Engineering"),
    ]
    columns = ["name", "age", "department"]

    df = spark.createDataFrame(data, columns)

    print("=== 원본 DataFrame ===")
    df.show()

    # ── 2. 부서별 평균 나이 ───────────────────────────────────
    agg_df = (
        df.groupBy("department")
        .agg(
            F.count("*").alias("count"),
            F.round(F.avg("age"), 1).alias("avg_age"),
            F.max("age").alias("max_age"),
        )
        .orderBy("department")
    )

    print("=== 부서별 통계 ===")
    agg_df.show()

    # ── 3. Parquet 파일로 저장 ────────────────────────────────
    output_path = "/opt/bitnami/spark/data/output/department_stats"
    agg_df.coalesce(1).write.mode("overwrite").parquet(output_path)
    print(f"Parquet 저장 완료: {output_path}")

    # ── 4. 다시 읽어서 확인 ───────────────────────────────────
    read_back = spark.read.parquet(output_path)
    print("=== Parquet에서 다시 읽기 ===")
    read_back.show()

    spark.stop()
    print("Spark 세션 종료. 예제 완료!")


if __name__ == "__main__":
    main()
