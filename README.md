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

## PySpark Installation Guide

To install PySpark, follow these steps:

1. **Install Python**: Make sure you have Python installed. You can download it from [python.org](https://www.python.org/downloads/). It's recommended to use Python 3.6 or later.

2. **Install PySpark via pip**:
   Open a terminal or PowerShell and run the following command:

    ```powershell
        pip install pyspark
    ```

   This command installs PySpark and its dependencies.

3. **Verify Installation**:
   After installation, verify that PySpark is installed correctly by running:

   ```powershell
        pyspark
   ```

   This command should open a PySpark shell.

## Environment Setup

After installing the prerequisites, follow these steps to set up the environment:

1. **Set Environment Variables**: Configure the following environment variables to point to your installations:
   - `JAVA_HOME`: Path to your Java JDK installation.
   - `SPARK_HOME`: Path to your Apache Spark installation.
   - `HADOOP_HOME`: Path to your Hadoop (winutils) installation.
   - `PYSPARK_HOME`: Path to your Python installation.

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

## Analysis Details

The following details the code and corresponding analysis that it produces:

1. **code=1**: Find the number of crashes in which the number of males killed is greater than 2.
2. **code=2**: Determine how many two-wheelers are booked for crashes.
3. **code=3**: Determine the Top 5 Vehicle Makes of the cars present in crashes where the driver died and the airbags did not deploy.
4. **code=4**: Determine the number of vehicles with drivers having valid licenses involved in hit-and-run incidents.
5. **code=5**: Identify the state with the highest number of accidents where females are not involved.
6. **code=6**: Identify the 3rd to 5th Top VEH_MAKE_IDs contributing to the largest number of injuries, including deaths.
7. **code=7**: For all body styles involved in crashes, identify the top ethnic user group for each unique body style.
8. **code=8**: Identify the Top 5 Zip Codes with the highest number of crashes where alcohol was a contributing factor.
9. **code=9**: Count the distinct Crash IDs where no damaged property was observed, the Damage Level is above 4, and the car avails insurance.
10. **code=10**: Determine the Top 5 Vehicle Makes where drivers are charged with speeding-related offenses, have licensed drivers, use one of the top 10 most-used vehicle colors, and the car is licensed in one of the top 25 states with the highest number of offenses.

## Additional Notes

- Ensure that all the required environment variables are correctly set before running the analysis.
- The `config/config.yaml` file is crucial for configuring the analysis. Make sure it is properly edited according to your data and setup.
- If you encounter any issues with Hadoop on Windows, make sure that `winutils.exe` is correctly placed in the `HADOOP_HOME` directory.
