from hydra.core.config_store import config_store
from pydantic.dataclasses import dataclass
from omegaconf import MISSING

@dataclass
class DatasetReaderConfig:
    _target_: str = MISSING
    dataset_dir: str = MISSING
    dataset_name: str = MISSING

@dataclass
class GHCDatasetReaderConfig(DatasetReaderConfig):
    _target_: str = "data_processing.dataset_readers.GHCDatasetReader"
    dev_split_ratio: float = MISSING

    def setup_config() -> None:
        cs = ConfigStore.instance() 
        cs.store(name="ghc_dataset_reader", node=GHCDatasetReaderConfig, group="dataset_reader_manager/dataset_reader")
