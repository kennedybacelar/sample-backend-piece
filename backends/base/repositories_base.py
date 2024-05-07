from abc import ABC, abstractmethod
from typing import Iterable, Optional, TypeVar, Generic, Union, List

from gitential2.datatypes import CoreModel

IdType = TypeVar("IdType")
CreateType = TypeVar("CreateType", bound=CoreModel)
UpdateType = TypeVar("UpdateType", bound=CoreModel)
InDBType = TypeVar("InDBType", bound=CoreModel)


class NotFoundException(Exception):
    pass


class BaseRepository(ABC, Generic[IdType, CreateType, UpdateType, InDBType]):
    @abstractmethod
    def get(self, id_: IdType) -> Optional[InDBType]:
        pass

    def get_or_error(self, id_: IdType) -> InDBType:
        obj = self.get(id_)
        if obj:
            return obj
        else:
            raise NotFoundException("Object not found.")

    @abstractmethod
    def create(self, obj: CreateType) -> InDBType:
        pass

    @abstractmethod
    def create_or_update(self, obj: Union[CreateType, UpdateType, InDBType]) -> InDBType:
        pass

    @abstractmethod
    def insert(self, id_: IdType, obj: InDBType) -> InDBType:
        pass

    @abstractmethod
    def update(self, id_: IdType, obj: UpdateType) -> InDBType:
        pass

    @abstractmethod
    def delete(self, id_: IdType) -> int:
        pass

    @abstractmethod
    def all(self) -> Iterable[InDBType]:
        pass

    @abstractmethod
    def count_rows(self) -> int:
        pass

    @abstractmethod
    def truncate(self):
        pass

    @abstractmethod
    def reset_primary_key_id(self):
        pass


class BaseWorkspaceScopedRepository(ABC, Generic[IdType, CreateType, UpdateType, InDBType]):
    @abstractmethod
    def get(self, workspace_id: int, id_: IdType) -> Optional[InDBType]:
        pass

    def get_or_error(self, workspace_id: int, id_: IdType) -> InDBType:
        obj = self.get(workspace_id, id_)
        if obj:
            return obj
        else:
            raise NotFoundException("Object not found.")

    @abstractmethod
    def create(self, workspace_id: int, obj: CreateType) -> InDBType:
        pass

    @abstractmethod
    def create_or_update(self, workspace_id: int, obj: Union[CreateType, UpdateType, InDBType]) -> InDBType:
        pass

    @abstractmethod
    def insert(self, workspace_id: int, id_: IdType, obj: InDBType) -> InDBType:
        pass

    @abstractmethod
    def update(self, workspace_id: int, id_: IdType, obj: UpdateType) -> InDBType:
        pass

    @abstractmethod
    def delete(self, workspace_id: int, id_: IdType) -> int:
        pass

    @abstractmethod
    def all(self, workspace_id: int) -> Iterable[InDBType]:
        pass

    @abstractmethod
    def all_ids(self, workspace_id: int) -> List[int]:
        pass

    @abstractmethod
    def count_rows(self, workspace_id: int) -> int:
        pass

    @abstractmethod
    def iterate_all(self, workspace_id: int) -> Iterable[InDBType]:
        pass

    @abstractmethod
    def iterate_desc(self, workspace_id: int) -> Iterable[InDBType]:
        pass

    @abstractmethod
    def truncate(self, workspace_id: int):
        pass

    @abstractmethod
    def reset_primary_key_id(self, workspace_id: int):
        pass
