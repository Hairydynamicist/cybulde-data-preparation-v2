from abc import ABC, abstractmethod
from cybulde.utils.utils import get_logger
from dask_ml.model_selection import train_test_split
import dask.dataframe as dd
import os
from typing import Optional


class DatasetReader(ABC):
    required_columns = {"text", "label", "split", "dataset_name"}
    split_names = {"train", "dev", "test"}

    def __init__(self, dataset_dir: str, dataset_name: str) -> None:
        self.logger = get_logger(self.__class__.__name__)
        self.dataset_dir = dataset_dir
        self.dataset_name = dataset_name

    def read_data(self) -> dd.core.DataFrame:
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
    
    def split_dataset(self, df:dd.core.DataFrame, test_size: float, stratify_column: Optional[str] = None) -> tuple:
        if stratify_column is None:
            return train_test_split(df, test_size=test_size, random_state=1234, shuffle=true)
        unique_column_values = df[stratify_column].unique()
        first_dfs = []
        second_dfs = []
        for unique_set_value in unique_column_values:
            sub_df = df[df[stratify_column] == unique_set_value]
            sub_first_df, sub_second_df = train_test_split(sub_df, test_size=test_size, random_state=1234, shuffle=True)
            first_dfs.append(sub_first_df)
            second_dfs.append(second_dfs)
        
        first_df = dd.concat(first_dfs)
        second_df = dd.concat(second_dfs)
        return first_df, second_df

    
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

        return train_df, dev_df, test_df
    

class DatasetReaderManager:
    def __init__(
        self,
        dataset_readers: dict[str, DatasetReader],
    ) -> None:
        self.dataset_readers = dataset_readers
    print("found datasetreadermanager class")
    def read_Data(self) -> dd.core.DataFrame:
        dfs = [dataset_reader.read_data() for dataset_reader in self.dataset_readers.values()]
        df = dd.concat(dfs)
        return df


