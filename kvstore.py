from contextlib import contextmanager
import json
import threading
from abc import ABC, abstractmethod
from typing import Optional, Union, List, ContextManager
from redis import Redis
from redis.exceptions import LockError as RedisLockError
from structlog import get_logger
from fastapi.encoders import jsonable_encoder
from gitential2.settings import GitentialSettings, KeyValueStoreType
from gitential2.exceptions import SettingsException, LockError

JsonableType = Union[dict, str, int, float, list]

logger = get_logger(__name__)


class KeyValueStore(ABC):
    def __init__(self, settings: GitentialSettings):
        self.settings = settings

    @abstractmethod
    def get_value(self, name: str) -> Optional[JsonableType]:
        pass

    @abstractmethod
    def set_value(self, name: str, value: JsonableType, ex: Optional[int] = None) -> JsonableType:
        pass

    @abstractmethod
    def delete_value(self, name: str):
        pass

    @abstractmethod
    def delete_values(self, pattern: str):
        pass

    @abstractmethod
    def delete_values_for_workspace(self, workspace_id: int):
        pass

    @abstractmethod
    def list_keys(self, pattern: str) -> List[str]:
        pass

    def get_or_set_default(self, name: str, default_value: JsonableType, ex: Optional[int] = None) -> JsonableType:
        value = self.get_value(name)
        if value:
            return value
        else:
            return self.set_value(name, default_value, ex)

    @abstractmethod
    def lock(self, name: str, timeout: Optional[int] = None, blocking_timeout: Optional[int] = None) -> ContextManager:
        pass


class InMemKeyValueStore(KeyValueStore):
    def __init__(self, settings: GitentialSettings):
        super().__init__(settings)
        self._storage: dict = {}
        self._locks: dict = {}

    def get_value(self, name: str) -> Optional[JsonableType]:
        return self._storage.get(name)

    def set_value(self, name: str, value: JsonableType, ex: Optional[int] = None) -> JsonableType:
        self._storage[name] = value
        return self._storage[name]

    def delete_value(self, name: str):
        if name in self._storage:
            del self._storage[name]

    def delete_values(self, pattern: str):
        keys_to_be_deleted = self.list_keys(pattern)
        for key in keys_to_be_deleted:
            self.delete_value(key)

    def delete_values_for_workspace(self, workspace_id: int):
        self.delete_values(pattern=f"ws-{workspace_id}*")
        self.delete_value(f"active-authors-{workspace_id}")
        self.delete_value(f"authors_ws_{workspace_id}")

    def list_keys(self, pattern: str) -> List[str]:
        def is_match_pattern(pattern, k):
            return k.startswith(pattern)

        return [is_match_pattern(pattern, k) for k in self._storage]

    def lock(
        self, name: str, timeout: Optional[int] = None, blocking_timeout: Optional[int] = None
    ) -> ContextManager:  # pylint: disable=unused-argument
        lock_ = self._locks.setdefault(name, threading.Lock())
        return lock_


class RedisKeyValueStore(KeyValueStore):
    def __init__(self, settings: GitentialSettings):
        super().__init__(settings)
        redis_url = settings.connections.redis_url
        if redis_url:
            self.redis = Redis.from_url(redis_url)
            logger.debug("Connected to redis", redis_url=redis_url)
        else:
            raise SettingsException("redis_url missing from settings.")

    def get_value(self, name: str) -> Optional[JsonableType]:
        encoded_value = self.redis.get(name)
        if encoded_value:
            return self._decode_value(encoded_value)
        return None

    def set_value(self, name: str, value: JsonableType, ex: Optional[int] = None) -> JsonableType:
        self.redis.set(name, self._encode_value(value), ex=ex)
        return value

    def delete_value(self, name: str):
        self.redis.delete(name)

    def delete_values(self, pattern: str):
        for key in self.redis.scan_iter(pattern):
            self.redis.delete(key)

    def delete_values_for_workspace(self, workspace_id: int):
        self.delete_values(pattern=f"ws-{workspace_id}*")
        self.delete_value(f"active-authors-{workspace_id}")
        self.delete_value(f"authors_ws_{workspace_id}")

    def list_keys(self, pattern: str) -> List[str]:
        return list(str(key) for key in self.redis.scan_iter(pattern))

    @staticmethod
    def _encode_value(value: JsonableType) -> str:
        return json.dumps(jsonable_encoder(value))

    @staticmethod
    def _decode_value(encoded_value: bytes) -> JsonableType:
        return json.loads(encoded_value)

    @contextmanager
    def lock(self, name: str, timeout: Optional[int] = None, blocking_timeout: Optional[int] = None):
        try:
            with self.redis.lock(name, timeout=timeout, blocking_timeout=blocking_timeout) as redis_lock:
                yield redis_lock
        except RedisLockError as e:
            logger.warning("Cannot acquire lock", name=name, timeout=timeout, blocking_timeout=blocking_timeout)
            raise LockError(f"Cannot acquire lock {name}") from e


def init_key_value_store(settings: GitentialSettings) -> KeyValueStore:
    if settings.kvstore == KeyValueStoreType.redis:
        return RedisKeyValueStore(settings)
    else:
        return InMemKeyValueStore(settings)
