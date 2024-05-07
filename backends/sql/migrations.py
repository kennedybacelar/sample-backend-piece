from typing import List, Optional
from pydantic import BaseModel
from sqlalchemy.engine import Engine
from sqlalchemy import exc
from structlog import get_logger

from gitential2.datatypes.common import CoreModel
from ..base.repositories import BaseRepository
from .repositories import SQLRepository
from .tables import schema_revisions_table, get_workspace_metadata
from ...exceptions import SettingsException
from ...utils import get_schema_name

logger = get_logger(__name__)


class MigrationRevision(BaseModel):
    revision_id: str
    steps: List[str]


MigrationList = List[MigrationRevision]


class SchemaRevision(CoreModel):
    id: str
    revision_id: str


class SchemaRevisionRepository(BaseRepository[str, SchemaRevision, SchemaRevision, SchemaRevision]):
    pass


class SQLSchemaRevisionRepository(
    SchemaRevisionRepository, SQLRepository[str, SchemaRevision, SchemaRevision, SchemaRevision]
):
    pass


def public_schema_migrations() -> MigrationList:
    return [
        MigrationRevision(
            revision_id="000",
            steps=[
                # users
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS stripe_customer_id VARCHAR(256);",
                # subscriptions
                "ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS stripe_subscription_id VARCHAR(256);",
                "ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS features JSON;",
            ],
        ),
        MigrationRevision(
            revision_id="001",
            steps=[
                # Add columns to the existing table
                "ALTER TABLE public.auto_export ADD COLUMN IF NOT EXISTS emails json;",
                "ALTER TABLE public.auto_export ADD COLUMN IF NOT EXISTS extra json;",
                # Drop existing columns from the table
                "ALTER TABLE public.auto_export DROP COLUMN IF EXISTS cron_schedule_time;",
                "ALTER TABLE public.auto_export DROP COLUMN IF EXISTS tempo_access_token;",
                "ALTER TABLE public.auto_export DROP COLUMN IF EXISTS is_exported;",
                # Drop existing primary key constraint
                "ALTER TABLE public.auto_export DROP CONSTRAINT IF EXISTS auto_export_pkey;",
                # Add a new primary key constraint
                "ALTER TABLE public.auto_export ADD CONSTRAINT auto_export_pkey PRIMARY KEY (id);",
                # Drop existing foreign key constraint
                "ALTER TABLE public.auto_export DROP CONSTRAINT IF EXISTS auto_export_workspace_id_fkey;",
                # Add a new unique constraint
                "ALTER TABLE public.auto_export ADD CONSTRAINT auto_export_workspace_id_key UNIQUE (workspace_id);",
            ],
        ),
    ]


def workspace_schema_migrations(schema_name: str) -> MigrationList:
    return [
        MigrationRevision(
            revision_id="000",
            steps=[
                f"ALTER TABLE {schema_name}.pull_requests ADD COLUMN IF NOT EXISTS user_id_external VARCHAR(64);",
                f"ALTER TABLE {schema_name}.pull_requests ADD COLUMN IF NOT EXISTS user_name_external VARCHAR(128);",
                f"ALTER TABLE {schema_name}.pull_requests ADD COLUMN IF NOT EXISTS user_username_external VARCHAR(128);",
                f"ALTER TABLE {schema_name}.pull_requests ADD COLUMN IF NOT EXISTS user_aid INTEGER;",
                # merged_by who?
                f"ALTER TABLE {schema_name}.pull_requests ADD COLUMN IF NOT EXISTS merged_by_id_external VARCHAR(64);",
                f"ALTER TABLE {schema_name}.pull_requests ADD COLUMN IF NOT EXISTS merged_by_name_external VARCHAR(128);",
                f"ALTER TABLE {schema_name}.pull_requests ADD COLUMN IF NOT EXISTS merged_by_username_external VARCHAR(128);",
                f"ALTER TABLE {schema_name}.pull_requests ADD COLUMN IF NOT EXISTS merged_by_aid INTEGER;",
                # calculated_patches & pull_requests & calculated_commits
                f"ALTER TABLE {schema_name}.calculated_patches ADD COLUMN IF NOT EXISTS loc_effort_p INTEGER;",
                f"ALTER TABLE {schema_name}.pull_requests ADD COLUMN IF NOT EXISTS is_bugfix BOOLEAN;",
                f"ALTER TABLE {schema_name}.calculated_patches ADD COLUMN IF NOT EXISTS is_collaboration BOOLEAN;",
                f"ALTER TABLE {schema_name}.calculated_patches ADD COLUMN IF NOT EXISTS is_new_code BOOLEAN;",
                f"ALTER TABLE {schema_name}.calculated_patches ADD COLUMN IF NOT EXISTS is_bugfix BOOLEAN;",
                f"ALTER TABLE {schema_name}.calculated_commits ADD COLUMN IF NOT EXISTS is_pr_exists BOOLEAN;",
                f"ALTER TABLE {schema_name}.calculated_commits ADD COLUMN IF NOT EXISTS is_pr_open BOOLEAN;",
                f"ALTER TABLE {schema_name}.calculated_commits ADD COLUMN IF NOT EXISTS is_pr_closed BOOLEAN;",
                # add extra indexes to calculated_patches and calculated_commits
                f"CREATE INDEX IF NOT EXISTS calculated_patches_date_idx ON {schema_name}.calculated_patches USING btree (date);",
                f"CREATE INDEX IF NOT EXISTS calculated_commits_date_idx ON {schema_name}.calculated_commits USING btree (date);",
            ],
        ),
        MigrationRevision(
            revision_id="001",
            steps=[
                f"ALTER TABLE {schema_name}.its_issues ALTER COLUMN status_category_api TYPE VARCHAR(32);",
                f"ALTER TABLE {schema_name}.its_issues ALTER COLUMN issue_type_name TYPE VARCHAR(48);",
                f"ALTER TABLE {schema_name}.its_issues ALTER COLUMN issue_type_id TYPE VARCHAR(48);",
                f"ALTER TABLE {schema_name}.its_issues ALTER COLUMN resolution_id TYPE VARCHAR(48);",
                f"ALTER TABLE {schema_name}.its_issues ALTER COLUMN priority_name TYPE VARCHAR(32);",
                f"ALTER TABLE {schema_name}.its_issues ALTER COLUMN priority_id TYPE VARCHAR(48);",
            ],
        ),
        MigrationRevision(
            revision_id="002",
            steps=[
                f"ALTER TABLE {schema_name}.charts ADD COLUMN IF NOT EXISTS filters JSON;",
            ],
        ),
        MigrationRevision(
            revision_id="003",
            steps=[
                f"ALTER TABLE {schema_name}.dashboards DROP COLUMN IF EXISTS config;",
                f"ALTER TABLE {schema_name}.dashboards ADD COLUMN IF NOT EXISTS filters JSON;",
            ],
        ),
        MigrationRevision(
            revision_id="004",
            steps=[
                f"ALTER TABLE {schema_name}.projects ADD COLUMN IF NOT EXISTS sprints_enabled BOOLEAN DEFAULT FALSE;",
                f"ALTER TABLE {schema_name}.projects ADD COLUMN IF NOT EXISTS sprint JSON;",
            ],
        ),
        MigrationRevision(
            revision_id="005",
            steps=[
                # This might look like an overkill but noting else worked for me so far.
                # PostgreSQL won't accept the IF EXISTS in the ALTER TABLE statement.
                # Besides this DO block below, I had to set the isolation_level="AUTOCOMMIT" flag in the
                # create_engine function call in the SQLGitentialBackend class in the /backends/sql/__init__.py
                # otherwise the column name change is not working.
                "DO LANGUAGE PLPGSQL $$ "
                f'BEGIN ALTER TABLE {schema_name}."deploy_commits" RENAME COLUMN "repository_id" TO "repo_id"; '
                "EXCEPTION WHEN UNDEFINED_COLUMN THEN "
                "RAISE NOTICE 'caught UNDEFINED_COLUMN exception for revision_id=005'; "
                "END $$;",
            ],
        ),
        MigrationRevision(
            revision_id="006",
            steps=[
                f"ALTER TABLE {schema_name}.its_projects ALTER COLUMN integration_type SET NOT NULL;",
                f"ALTER TABLE {schema_name}.its_projects ALTER COLUMN integration_type TYPE varchar(128);",
                f"ALTER TABLE {schema_name}.its_projects ALTER COLUMN integration_name SET NOT NULL;",
                f"ALTER TABLE {schema_name}.its_projects ALTER COLUMN integration_name TYPE varchar(128);",
                f"ALTER TABLE {schema_name}.its_projects ALTER COLUMN integration_id SET NOT NULL;",
            ],
        ),
    ]


def migrate_database(engine: Engine, workspace_ids: List[int]):
    schema_revisions = SQLSchemaRevisionRepository(
        table=schema_revisions_table, engine=engine, in_db_cls=SchemaRevision
    )
    _do_migration("public", public_schema_migrations(), schema_revisions, engine)
    for workspace_id in workspace_ids:
        migrate_workspace(engine, workspace_id, _schema_revisions=schema_revisions)


def migrate_workspace(engine: Engine, workspace_id: int, _schema_revisions: Optional[SchemaRevisionRepository] = None):
    schema_revisions = _schema_revisions or SQLSchemaRevisionRepository(
        table=schema_revisions_table, engine=engine, in_db_cls=SchemaRevision
    )
    schema_name = get_schema_name(workspace_id)
    create_missing_workspace_tables(engine, schema_name)
    _do_migration(schema_name, workspace_schema_migrations(schema_name), schema_revisions, engine)


def delete_schema_revision(engine: Engine, workspace_id: int) -> bool:
    result = False
    schema_revision_id = get_schema_name(workspace_id)

    schema_rev_repo: SchemaRevisionRepository = SQLSchemaRevisionRepository(
        table=schema_revisions_table, engine=engine, in_db_cls=SchemaRevision
    )
    revision_to_delete = schema_rev_repo.get(schema_revision_id)
    if revision_to_delete:
        logger.info(f"Deleting schema revision for workspace with id: {workspace_id}!")
        schema_rev_repo.delete(schema_revision_id)
        result = True
    else:
        logger.info(f"Can not delete schema revision for workspace with id: {workspace_id}!")
    return result


def create_missing_workspace_tables(engine: Engine, schema_name: str):
    workspace_metadata, _ = get_workspace_metadata(schema_name)
    workspace_metadata.create_all(engine)


def _do_migration(
    schema_name: str, migrations: MigrationList, schema_revisions: SchemaRevisionRepository, engine: Engine
):
    logger.info("Migration: Running database migration for schema", schema_name=schema_name)
    current_rev = schema_revisions.get(schema_name)
    if current_rev:
        revision_ids = [m.revision_id for m in migrations]
        rev_index = revision_ids.index(current_rev.revision_id)
        remaining_steps = migrations[rev_index + 1 :]
    else:
        remaining_steps = migrations

    if remaining_steps:
        for ms in remaining_steps:
            logger.info("Migration | applying step", schema_name=schema_name, revision_id=ms.revision_id)

            connection = engine.connect()
            transaction_handle = connection.begin()
            try:
                for query_ in ms.steps:
                    logger.info(
                        "Migrations | executing query",
                        query=query_,
                        schema_name=schema_name,
                        revision_id=ms.revision_id,
                    )
                    connection.execute(query_)
                transaction_handle.commit()
            except exc.SQLAlchemyError as se:
                transaction_handle.rollback()
                raise SettingsException("Exception in database migration!") from se

        new_rev = SchemaRevision(id=schema_name, revision_id=remaining_steps[-1].revision_id)
        if current_rev:
            schema_revisions.update(schema_name, new_rev)
        else:
            schema_revisions.create(new_rev)
        logger.info("Migrations: schema is updated to revision", schema_name=schema_name, new_rev=new_rev.revision_id)
    else:
        logger.info(
            "Migrations: schema is up-to-date",
            schema_name=schema_name,
            current_rev=current_rev.revision_id if current_rev else None,
        )


def get_latest_ws_revision():
    return [m.revision_id for m in workspace_schema_migrations("ws_1")][-1]


def set_schema_to_revision(schema_name: str, revision_id: str, engine: Engine):
    schema_revisions = SQLSchemaRevisionRepository(
        table=schema_revisions_table, engine=engine, in_db_cls=SchemaRevision
    )
    current_rev = schema_revisions.get(schema_name)
    new_rev = SchemaRevision(id=schema_name, revision_id=revision_id)
    if current_rev:
        schema_revisions.update(schema_name, new_rev)
    else:
        schema_revisions.create(new_rev)


def set_ws_migration_revision_after_create(workspace_id: int, engine: Engine):
    revision_id = get_latest_ws_revision()
    schema_name = get_schema_name(workspace_id)
    set_schema_to_revision(schema_name, revision_id, engine)
