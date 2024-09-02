from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
import os
class DataLoader:
    def __init__(self, config):
        self.config = config
        self.spark = SparkSession.builder.appName("CrashAnalysis").config("spark.hadoop.fs.defaultFS", "file:///").master("local[*]").getOrCreate()
        self.data_dictionary = self.get_data_dictionary()
        self.table_cache = {}

    def load_table(self, table_name):
        if table_name in self.table_cache:
            return self.table_cache[table_name]

        file_name = self.config['tables_file_path'][table_name]
        # print(self.config)
        base_path = '\\'.join(self.config['project']['base_path'].split('/')) 
        input_path = '\\'.join(self.config['data']['input_path'].split('/'))
        file_path = os.path.join(base_path, input_path, file_name)
        # print(file_path)
        schema = self.get_schema(file_name)
        result = self.spark.read.csv(file_path, header=True, schema=schema)
        self.table_cache[table_name] = result
        return result
    
    def get_schema(self, table_name):
        # table_name = file_name in input path
        df = self.data_dictionary
        filtered_df = df.filter( df['file_name'] == table_name ).select('file_column_name', 'file_data_type')
        rows = filtered_df.collect()

        structfield_list = []
        for row in rows:
            raw_column_type_string = row['file_data_type'] 
            if raw_column_type_string == 'integer':
                field_type = IntegerType()
            elif raw_column_type_string == 'string':
                field_type = StringType()
            elif raw_column_type_string == 'double':
                field_type = DoubleType()

            structfield_list.append( StructField(row['file_column_name'],  field_type, True) )
        schema = StructType(structfield_list)
        # print(schema)
        return schema

    def get_data_dictionary(self):
        base_path = '\\'.join(self.config['project']['base_path'].split('/'))
        rel_path = '\\'.join(self.config['data']['data_dictionary_path'].split('/'))
        abs_path = os.path.join(base_path, rel_path)

        # read the data dictionary in excel
        df = self.spark.read.csv(abs_path, header=True, inferSchema=True)
        # df.show(10, truncate=True)

        return df

