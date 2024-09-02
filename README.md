# Crash Data Analysis

## Project Overview

This project is designed to perform analysis on crash data using PySpark. The main objective is to process and analyze large datasets to derive insights related to crash incidents. The project leverages Apache Spark for distributed data processing and Python for scripting.

## Prerequisites

Before running the project, ensure that the following software is installed on your system if using Windows:

1. **Java JDK**: Required for running Spark.
2. **Apache Spark**: The primary engine for large-scale data processing.
3. **Hadoop**: Winutils is necessary for Spark to run on Windows.
4. **Python**: The scripting language used in this project.
5. **PySpark**: The Python API for Apache Spark.

## Environment Setup

After installing the prerequisites, follow these steps to set up the environment:

1. **Set Environment Variables**: Configure the following environment variables to point to your installations:
   - `JAVA_HOME`: Path to your Java JDK installation.
   - `SPARK_HOME`: Path to your Apache Spark installation.
   - `HADOOP_HOME`: Path to your Hadoop (winutils) installation.
   - `PYSPARK_HOME`: Path to your PySpark installation.

2. **Edit Configuration File**: Modify the `config/config.yaml` file to match your specific configurations, such as file paths and settings for your data analysis.

3. **Set PYTHONPATH**: In PowerShell, set the `PYTHONPATH` to gain access to the `src` module. Run the following command:

    ```powershell
        $env:PYTHONPATH = (Get-Location)
    ```

## Running the Analysis

To run the analysis, use the `spark-submit` command:

```powershell
spark-submit --master local[*] src/main.py .\config\config.yaml code=10
```

- **Config File Path**: Provide the path to your configuration file (`config.yaml`).
- **Analysis Code**: Specify the analysis code number (`code=10` in the example above) to run the corresponding analysis task.

This setup allows you to perform custom analyses on your crash data by simply updating the configuration and running the appropriate analysis code.

## Additional Notes

- Ensure that all the required environment variables are correctly set before running the analysis.
- The `config/config.yaml` file is crucial for configuring the analysis. Make sure it is properly edited according to your data and setup.
- If you encounter any issues with Hadoop on Windows, make sure that `winutils.exe` is correctly placed in the `HADOOP_HOME` directory.