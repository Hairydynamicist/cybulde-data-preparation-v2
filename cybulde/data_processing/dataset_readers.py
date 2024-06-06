from abc import ABC, abstractmethod
from cybulde.utils.utils import get_logger
import dask.dataframe as dd
import os

class DatasetReader(ABC):
    required_columns = {"text", "label", "split", "dataset_name"}
    split_names = {"train", "dev", "test"}

    def __init__(self, dataset_dir: str, dataset_name: str) -> None:
        self.logger = get_logger(self.__class__.__name__)
        self.dataset_dir = dataset_dir
        self.dataset_name = dataset_name

    def read_data(self) -> dd.core.dataframe:
        train_df, dev_df, test_df = self._read_data()
        df = self.assign_split_names_to_data_frames_and_merge(train_df, dev_df, test_df)
        df["dataset_name"] = self.dataset_name
        if any(required_column not in df.column.values for required_column in self.required_columns):
            raise ValueError(f"Dataset must contain all required columnsL {self.required_columns}")
        unique_split_names = set(df["split"].unique().compute().tolist())
        if unique_split_names != self.split_names:
            raise ValueError(f"Dataset must contain all required split names: {self.split_names}") 
        return df[list(self.required_columns)]

    @abstractmethod
    def _read_data(self) -> tuple:
        """
        Read and split dataset into 3 splits: train, dev, test.
        The return value must be a dd.core.DataFrame with required columns: self.required_columns
        """
    def assign_split_names_to_data_frames_and_merge(self, train_df, dev_df, test_df) -> dd.core.DataFrame:
        train_df["split"] = "train"
        dev_df["split"] = "dev"
        test_df["split"] = "test"
        return dd.concat([train_df, dev_df, test_df])
    
class GHCDatasetReader(DatasetReader):
    def __init__(self, dataset_dir: str, dataset_name: str, dev_split_ratio: float) -> None:
        super().__init__(dataset_dir, dataset_name)
        self.dev_split_ratio = dev_split_ratio

    def _read_data(self) -> tuple:
        self.logger,info("Reading GHC training dataset...")
        train_dev_path = os.path.join(self.dataset_dir, "ghc_train.tsv")
        train_df = dd.read_csv(train_tsv_path, sep="\t", header=0)

        test_dev_path = os.path.join(self.dataset_dir, "ghc_test.tsv")
        test_df = dd.read_csv(test_tsv_path, sep="\t", header=0)

        train_df["label"] = (train_df["hd"] + train_df["cv"] + train_df["vo"] > 0).astype(int)
        test_df["label"] = (test_df["hd"] + test_df["cv"] + test_df["vo"] > 0).astype(int)

        train_df, dev_df = self.split_dataset(train_df, self.dev_split_ratio, stratify_column="label")
