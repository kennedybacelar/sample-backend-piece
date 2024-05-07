from typing import Optional

from sqlalchemy import exc
from sqlalchemy.engine import Engine
from structlog import get_logger

from ...exceptions import SettingsException
from ...utils import get_schema_name, is_string_not_empty

logger = get_logger(__name__)


def __get_reset_workspace_query(workspace_id: int) -> Optional[str]:
    schema_name: str = get_schema_name(workspace_id)
    return (
        (
            "DO LANGUAGE PLPGSQL $$ "
            "  DECLARE "
            "    row RECORD; "
            "BEGIN "
            "  FOR row IN "
            "    SELECT tables.table_name "
            "    FROM information_schema.tables tables "
            f"   WHERE table_schema = '{schema_name}' "
            "    LOOP "
            f"     EXECUTE FORMAT('TRUNCATE TABLE {schema_name}.%%I RESTART IDENTITY CASCADE;', row.table_name); "
            "    END LOOP; "
            "  END; "
            "$$;"
        )
        if is_string_not_empty(schema_name)
        else None
    )


def reset_workspace(engine: Engine, workspace_id: int):
    reset_workspace_query = __get_reset_workspace_query(workspace_id)
    if reset_workspace_query:
        connection = engine.connect()
        trans = connection.begin()
        try:
            logger.info("Executing query for reset workspace.", query=reset_workspace_query)
            connection.execute(reset_workspace_query)
            trans.commit()
        except exc.SQLAlchemyError as se:
            trans.rollback()
            raise SettingsException("Exception while trying to run reset workspace query!") from se
    else:
        logger.exception(
            "Can not execute query for reset database!", query=reset_workspace_query, workspace_id=workspace_id
        )
