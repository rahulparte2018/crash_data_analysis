import os
from datetime import datetime
class ResultSaver:
    def __init__(self, config):
        self.base_path = '\\'.join(config['project']['base_path'].split('/'))
        self.output_path = '\\'.join(config['data']['output_path'].split('/'))

    def save_results(self, result, filename):
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
