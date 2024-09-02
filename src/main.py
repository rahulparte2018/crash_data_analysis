import sys
import yaml
from src.data_loader import DataLoader
from src.result_saver import ResultSaver
from src.analyzer import Analyzer

def main(config_file_path, analytics_code):
    # load the config.yaml file
    with open(config_file_path, 'r') as file:
        config = yaml.safe_load(file)           # output is dictionary

    # Create all the required objects
    loader = DataLoader(config)
    saver = ResultSaver(config)
    analyzer = Analyzer(loader, saver)
    
    # Perform required Analysis
    result = analyzer.analyze(analytics_code)

    # Save the results
    saver.save_results(result, f"Analytics_report_{analytics_code}")

if __name__ == '__main__':
    # two command line arguments: config_file and analytics_code
    command_line_args = sys.argv
    if len(command_line_args) > 3:
        print('Incorrect number of command line arguments')
        exit()
    
    config_file_path = None
    analytics_code = 1

    for i in range(1, 3):
        value = str(command_line_args[i])
        if value.find('config') != -1:
            config_file_path = value
        if value.find('code') != -1:
            analytics_code = int(value.split('=')[1])

    if config_file_path == None:
        print("Provide proper config file path")
        exit()

    print('Hello BCG, Lets Analyse !!!!!')
    print('Lets provide you guys with the Analytics report', analytics_code)

    main(config_file_path, analytics_code)
