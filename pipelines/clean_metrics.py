from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, lit
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.utils import AnalysisException

def run_cleaner(input_path, output_path):
    spark = SparkSession.builder.appName("MonkeytypeDataCleaner").getOrCreate()

    df = spark.read.json(input_path)

    columns_to_drop = ['uid', 'charStats', 'mode', 'mode2', 'incompleteTestSeconds',
                       'keyConsistency', 'chartData', 'name', 'keySpacingStats',
                       'keyDurationStats', 'afkDuration', 'difficulty', 'punctuation']
    
    existing_columns_to_drop = [col for col in columns_to_drop if col in df.columns]
    
    rename_mappings = {'_id': 'test_id', 'wpm': 'wpm', 'rawWpm': 'raw_wpm',
                        'charStats': 'char_stats', 'acc': 'accuracy', 'timestamp': 'time',
                        'restartCount': 'restart_count', 'testDuration': 'duration',
                        'consistency': 'consistency', 'isPb': 'personal_best'
    }
    
    for old_name, new_name in rename_mappings.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
            
    if 'restart_count' in df.columns:
        df = df.withColumn('restart_count', coalesce(df['restart_count'], lit(0)))
    if 'personal_best' in df.columns:
        df = df.withColumn('personal_best', coalesce(df['personal_best'], lit(False)))
        
    def extract_oid(val):
        if isinstance(val, dict) and '$oid' in val:
            return val['$oid']
        return val

    extract_oid_udf = udf(extract_oid, StringType())

    if 'test_id' in df.columns:
        df = df.withColumn('test_id', extract_oid_udf(df['test_id']))
    
    cleaned_df = df.drop(*existing_columns_to_drop).na.drop()
    
    try:
        existing_df = spark.read.json(output_path, header=True, inferSchema=True)
        combined_df = existing_df.unionByName(cleaned_df, allowMissingColumns=True)
    except AnalysisException:
        combined_df = cleaned_df

    combined_df.write.mode("overwrite").option("header", True).json(output_path)

    spark.stop()