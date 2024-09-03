import os
from datetime import datetime

class ResultSaver:
    """
    A class to save analysis results to a file.

    Attributes:
    -----------
    base_path : str
        The base directory path for saving output files, derived from the provided configuration.
    output_path : str
        The specific directory path under the base path where output files will be saved, derived from the provided configuration.

    Methods:
    --------
    save_results(result, filename):
        Saves the provided analysis result to a file. Depending on the type of result (string or DataFrame), 
        the method saves it as a text file or a CSV file respectively, with a timestamp appended to the filename.
    """

    def __init__(self, config):
        """
        Initializes the ResultSaver with paths derived from the configuration.

        Parameters:
        -----------
        config : dict
            A configuration dictionary containing 'project' and 'data' keys, which hold paths for the base 
            directory and output directory, respectively. These paths are transformed to be OS-compatible.
        """
        self.base_path = '\\'.join(config['project']['base_path'].split('/'))
        self.output_path = '\\'.join(config['data']['output_path'].split('/'))

    def save_results(self, result, filename):
        """
        Saves the provided result to a file. 

        If the result is a string, it is saved as a .txt file. 
        If the result is a PySpark DataFrame, it is converted to a Pandas DataFrame and saved as a .csv file.
        A timestamp is appended to the filename for uniqueness.

        Parameters:
        -----------
        result : str or PySpark DataFrame
            The analysis result to be saved. It can either be a textual summary (string) or tabular data (PySpark DataFrame).
        filename : str
            The base filename to use when saving the result. A timestamp and appropriate file extension will be added automatically.

        Returns:
        --------
        None
        """
        if result == None:
            print('No result to save')
            return
        
        current_datetime = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

        if not isinstance(result, str):
            filename = filename + '_' + current_datetime + '.csv'
            output_file_path = os.path.join(self.base_path, self.output_path, filename)
            pd_df = result.toPandas()
            pd_df.to_csv(output_file_path, index=False)
        else:
            filename = filename + '_' + current_datetime + '.txt'
            output_file_path = os.path.join(self.base_path, self.output_path, filename)
            with open(output_file_path, "w") as file:
                file.write(result)
