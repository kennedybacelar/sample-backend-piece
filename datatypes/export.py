from typing import List, Optional, Tuple, cast
from abc import abstractmethod
from fastapi.encoders import jsonable_encoder
from pydantic.main import BaseModel

dict_type = dict


class ExportableModel(BaseModel):
    @abstractmethod
    def export_names(self) -> Tuple[str, str]:
        pass

    def export_fields(self) -> List[str]:
        return cast(List[str], list(self.__fields_set__))

    def to_exportable(self, fields: Optional[List[str]] = None) -> dict_type:
        return jsonable_encoder(self.dict(include=set(fields or self.export_fields())))
