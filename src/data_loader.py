from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
import os
class DataLoader:
    """
    A class to load and manage data tables using PySpark. It provides methods to load tables, 
    infer schemas from a data dictionary, and cache tables to avoid redundant loading.

    Attributes:
    config (dict): A dictionary containing configuration settings for the project, including paths to files.
    spark (SparkSession): The SparkSession object used for interacting with Spark.
    data_dictionary (DataFrame): A DataFrame containing the data dictionary for the project.
    table_cache (dict): A dictionary used to cache loaded tables, with table names as keys and DataFrames as values.
    """

    def __init__(self, config):
        """
        Initializes the DataLoader with the given configuration and creates a Spark session.

        Parameters:
        config (dict): Configuration settings, including paths to tables and the data dictionary.
        """

        self.config = config
        self.spark = SparkSession.builder.appName("CrashAnalysis").config("spark.hadoop.fs.defaultFS", "file:///").master("local[*]").getOrCreate()
        self.data_dictionary = self.get_data_dictionary()
        self.table_cache = {}

    def load_table(self, table_name):
        """
        Loads a table as a DataFrame. If the table has already been loaded, returns the cached version.

        Parameters:
        table_name (str): The name of the table to load.

        Returns:
        DataFrame: The loaded table as a PySpark DataFrame.
        """

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
        """
        Generates a schema for a table based on the data dictionary.

        Parameters:
        table_name (str): The name of the table (corresponding to the file name) to generate the schema for.

        Returns:
        StructType: The schema of the table as a PySpark StructType.
        """

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
        """
        Loads the data dictionary from a CSV file.

        Returns:
        DataFrame: The data dictionary as a PySpark DataFrame.
        """

        base_path = '\\'.join(self.config['project']['base_path'].split('/'))
        rel_path = '\\'.join(self.config['data']['data_dictionary_path'].split('/'))
        abs_path = os.path.join(base_path, rel_path)

        # read the data dictionary
        df = self.spark.read.csv(abs_path, header=True, inferSchema=True)
        # df.show(10, truncate=True)

        return df

