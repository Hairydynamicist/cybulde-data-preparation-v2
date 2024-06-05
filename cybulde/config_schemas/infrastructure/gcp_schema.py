from pydantic.dataclasses import dataclass
from hydra.core.config_store import ConfigStore

@dataclass
class GCPConfig:
    project_id = "cybuldeproject"


def setup_config() -> None:
    cs = ConfigStore.instance()
    cs.store(name="gcp_config_schema", node="GCPConfig")