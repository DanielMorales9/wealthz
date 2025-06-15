from pydantic.v1 import BaseSettings


class PostgresCatalogSettings(BaseSettings):
    dbname: str
    host: str
    port: str
    user: str
    password: str

    class Config:
        env_prefix = "PG_"
        case_sensitive = False

    @property
    def connection(self) -> str:
        return " ".join(f"{key}={value}" for key, value in self.dict().items())


class StorageSettings(BaseSettings):
    type: str
    access_key_id: str
    secret_access_key: str
    data_path: str
    endpoint: str | None = None
    region: str | None = None
    url_style: str | None = None
    use_ssl: bool | None = None

    class Config:
        env_prefix = "STORAGE_"
        case_sensitive = False
