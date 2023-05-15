import os
import pandas as pd
import sys

sys.path.append("..")
import argparse
from colorama import Fore
import json
import stages as stage_classes
import time


def main(config, data_dir):
    """
    Handles the pipeline setup and execution (stage by stage)
    :param config: pipeline config
    :param data_dir: root folder
    """
    dataset_name = config["dataset_name"]
    skip_stage_if_exists = config["skip_stage_if_exists"]
    root_dir = os.path.join(data_dir, dataset_name)
    os.makedirs(root_dir, exist_ok=True)

    print("Pipeline summary:")
    print(pd.DataFrame(config["stages"]).to_string())

    # Go through each stage, check if it can be executed and if yes execute
    stages = {stage["name"]: stage for stage in config["stages"]}
    for name, stage in stages.items():
        implementation, stage_input, stage_output, enabled, params = stage["implementation"], stage["input"], stage["output"], stage[
            "enabled"], stage["params"]
        input_path = os.path.join(root_dir, stage_input) if stage_input else None
        output_path = os.path.join(root_dir, stage_output)
        if enabled:
            print("---{}---".format(name))

            stage_success = False
            # run the stage if the input to the stage exists or is not required
            if input_path is None or os.path.exists(input_path):
                # dynamic instantiation of the stage classes from Pipeline.stages
                if hasattr(stage_classes, implementation):
                    stage_instance = getattr(stage_classes, implementation)(root_dir, dataset_name, params)
                    # TODO: add handling for each stage to tell whether the stage execution succeeded (e.g. input file was not found, error during execution) and return None in run() if it didn't succeed
                    tic = time.perf_counter()
                    # run the stage
                    stage_success = stage_instance.run(input_path, output_path, skip_if_exists=skip_stage_if_exists)
                    toc = time.perf_counter()
                    stages[name]["execution time"] = toc - tic
                else:
                    print("Stage name {} has no corresponding implementation in {}".format(name, stage_classes))
                    stages[name]["execution time"] = None  # add it always so pandas recognizes it as a column
            else:
                print("Input expected at {} but none found.".format(input_path))
                stages[name]["execution time"] = None

            stages[name]["result"] = "Success" if stage_success else "Fail"
            print()

    # Put everything into a  dataframe for pretty printing
    print("Pipeline execution summary:")
    df_res = pd.DataFrame(stages.values())
    df_res["time %"] = df_res["execution time"] / df_res["execution time"].sum()
    df_res["time %"] = df_res["time %"].apply(lambda x: "{:.2%}".format(x))
    df_res["output"] = df_res["output"].apply(lambda x: os.path.basename(x))
    print(df_res[["name", "implementation", "enabled", "result", "output", "execution time", "time %"]].to_string())


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Run the scraping and preprocessing pipeline')

    parser.add_argument('--config', type=str, help='path to a pipeline config file', default="config/test.json")
    parser.add_argument('--root_dir', type=str, help='path to a directory where the pipeline output will be stored', default="../data/social_media_scraping")
    args = parser.parse_args()
    config_file = args.config
    data_dir = args.root_dir

    # read the pipeline config from file
    if os.path.exists(config_file):
        with open(config_file) as json_file:
            config = json.load(json_file)
            main(config, data_dir)
    else:
        print(Fore.RED + "The specified config file does not exist: {}".format(config_file))
