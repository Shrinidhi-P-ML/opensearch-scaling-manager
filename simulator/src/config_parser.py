import yaml
import sys
from cerberus import Validator
from pathlib import Path
import constants as const

class Config:
    def __init__(
        self,
        stats: dict,
        data_ingestion: dict,
        searches: dict,
        data_generation_interval_minutes: int,
    ):
        self.stats = stats
        self.data_generation_interval_minutes = data_generation_interval_minutes
        self.data_ingestion = data_ingestion
        self.searches = searches


def validate_config(all_configs):
    """
    Validate the config file against the defined schema
    :param current_dir: path to the src directory
    :param schema_path: path to the schema.py file
    :param schema: schema model that is used to validate config file
    :param v: validate object
    """
    # Fetching the dir path and appending the schema file path
    current_dir: Path = Path(__file__).parent.resolve()
    schema_path = str(current_dir) + const.SCHEMA_PATH 
    schema = eval(open(schema_path, "r").read())

    # validating config file against the schema
    v = Validator(schema)
    return v.validate(all_configs, schema), v.errors


def parse_config(config_file_path):
    """
    Read and parse the config file into objects,
    that can work with simulator
    :param config_file_path: path of the yaml file
    :param all_configs: config file parsed into a dictionary
    :param current_dir: path to the src directory
    :return: stats, events, data_ingestion, searches, data_generation_interval_minutes
    """
    # Fetching the config file from the specified path
    fp = open(config_file_path, "r")

    # Error handling mechanism for incompletely filled config file
    try:
        # Loading the config file content to dictionary to validate
        all_configs = yaml.safe_load(fp.read())

    except Exception as e:
        sys.stdout.write(e)
        sys.stdout.write("Could not read data, please check if all fields are filled")
        return

    fp.close()

    current_dir: Path = Path(__file__).parent.resolve()

    # Perform Validation of the config file
    is_valid, errors = validate_config(all_configs)

    # If it is a valid config file, Place the file in the simulator/src/main and return
    if is_valid:
        # cwd = os.getcwd()
        file = open(str(current_dir) + const.CONFIG_PATH, "w")
        yaml.dump(all_configs, file, allow_unicode=True)
        file.close()

    # If the required fields is not present in the config file then do not place it in src/
    else:
        sys.stdout.write(errors)
        sys.stdout.write("Please pass a Valid config file")
        exit()
    data_generation_interval_minutes = all_configs.pop(
        "data_generation_interval_minutes"
    )
    data_ingestion = all_configs.pop("data_ingestion")
    searches = all_configs.pop("searches")
    stats = all_configs

    return Config(stats, data_ingestion, searches, data_generation_interval_minutes)


if __name__ == "__main__":
    parse_config("config.yaml")
