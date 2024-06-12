import logging
import logging.config
import argparse

from typing import Any, Optional

import hydra
import yaml

from hydra.types import TaskFunction
from hydra import compose, initialize

from omegaconf import DictConfig, OmegaConf

from hydra import compose, initialize

from cybulde.config_schemas import data_processing_config_schema


def get_config(config_path: str, config_name: str) -> TaskFunction:
    setup_config()
    setup_logger()

    def main_decorator(task_function: TaskFunction) -> Any:
        @hydra.main(config_path=config_path, config_name=config_name, version_base=None)
        def decorated_main(dict_config: Optional[DictConfig] = None) -> Any:
            config = OmegaConf.to_object(dict_config)
            return task_function(config)

        return decorated_main

    return main_decorator


def setup_config() -> None:
    data_processing_config_schema.setup_config()


def setup_logger() -> None:
    with open("./cybulde/configs/hydra/job_logging/custom.yaml", "r") as stream:
        config = yaml.load(stream, Loader=yaml.FullLoader)
    logging.config.dictConfig(config)


def config_args_parser() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument("--config-path", type=str, default="../configs/", help="Directory of the config files")
    parser.add_argument("--config-name", type=str, required=True, help="Name of the config file")
    parser.add_argument("--overrides", nargs="*", help="Hydra config overrides", default=[])

    return parser.parse_args()


def compose_config(config_path: str, config_name: str, overrides: Optional[list[str]] = None) -> Any:
    setup_config()
    setup_logger()

    if overrides is None:
        overrides = []

    with initialize(version_base=None, config_path=config_path, job_name="config-compose"):
        dict_config = compose(config_name=config_name, overrides=overrides)
        config = OmegaConf.to_object(dict_config)
    return config