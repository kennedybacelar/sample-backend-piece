import json
import os
import csv

import click

from structlog import get_logger
from gitential2.datatypes.credentials import CredentialType
from gitential2.extraction.repository import extract_incremental
from gitential2.extraction.output import DataCollector
from gitential2.datatypes.repositories import RepositoryInDB, GitProtocol, RepositoryUpdate
from gitential2.settings import load_settings
from gitential2.logging import initialize_logging
from gitential2.core.context import init_context_from_settings
from gitential2.license import check_license as check_license_
from gitential2.legacy_import import import_legacy_database
from gitential2.legacy_import import import_legacy_workspace

from gitential2.core.context import GitentialContext

logger = get_logger(__name__)


def protocol_from_clone_url(clone_url: str) -> GitProtocol:
    if clone_url.startswith(("git@", "ssh")):
        return GitProtocol.ssh
    else:
        return GitProtocol.https


@click.group()
@click.pass_context
def cli(ctx):
    settings = load_settings()
    initialize_logging(settings)

    ctx.ensure_object(dict)
    ctx.obj["settings"] = settings


@click.command()
@click.argument("repo_id", type=int)
@click.argument("clone_url")
@click.pass_context
def extract_git_metrics(ctx, repo_id, clone_url):
    repository = RepositoryInDB(id=repo_id, clone_url=clone_url, protocol=protocol_from_clone_url(clone_url))
    output = DataCollector()
    extract_incremental(repository, output=output, settings=ctx.obj["settings"])


@click.command()
@click.option("--license-file-path", "-l", "license_file_path", type=str)
def check_license(license_file_path):
    license_, is_valid = check_license_(license_file_path)
    if is_valid:
        print("License is valid", license_)
    else:
        print("License is invalid or expired", license_)


@click.command(name="import-legacy-db")
@click.option("--users", "-u", "users_file", type=str)
@click.option("--secrets", "-s", "secrets_file", type=str)
@click.option("--accounts", "-a", "accounts_file", type=str)
@click.option("--collaborators", "-c", "collaborators_file", type=str)
@click.pass_context
def import_legacy_db_(ctx, users_file, secrets_file, accounts_file, collaborators_file):
    legacy_users = _load_list(users_file)
    legacy_secrects = _load_list(secrets_file)
    legacy_accounts = _load_list(accounts_file)
    legacy_collaborators = _load_list(collaborators_file)

    g = init_context_from_settings(ctx.obj["settings"])
    import_legacy_database(g, legacy_users, legacy_secrects, legacy_accounts, legacy_collaborators)


@click.command(name="import-legacy-workspace-bulk")
@click.option("--folder", "-f", "folder", type=str)
@click.pass_context
def import_legacy_workspace_bulk(ctx, folder):  # pylint: disable=unused-argument,too-many-arguments,unused-variable
    dirs = os.listdir(os.getcwd() + "/" + folder)
    g = init_context_from_settings(ctx.obj["settings"])
    for directory in dirs:
        workspace_id = int(directory.split("_")[1])
        path = folder + "/" + directory + "/"
        aliases_ = _load_list(path + "alias.json")
        authors_ = _load_list(path + "author.json")
        # account_ = _load_list(account)
        projects_ = _load_list(path + "project.json")
        project_repos_ = _load_list(path + "project_repo.json")
        account_repos_ = _load_list(path + "repo.json")
        team_authors_ = _load_list(path + "teams_author.json")
        teams_ = _load_list(path + "teams.json")

        import_legacy_workspace(
            g,
            workspace_id,
            legacy_projects_repos=project_repos_,
            legacy_aliases=aliases_,
            legacy_teams=teams_,
            legacy_teams_authors=team_authors_,
            legacy_authors=authors_,
            legacy_account_repos=account_repos_,
            legacy_projects=projects_,
        )  # pylint: disable=too-many-arguments


@click.command(name="import-legacy-workspace")
@click.option("--workspace-id", "-w", "workspace_id", type=int)
@click.option("--projectrepos", "-pr", "projectrepos", type=str)
@click.option("--teamauthors", "-ta", "teamauthors", type=str)
@click.option("--accountrepos", "-ar", "accountrepos", type=str)
@click.option("--authors", "-au", "authors", type=str)
@click.option("--teams", "-t", "teams", type=str)
@click.option("--projects", "-p", "projects", type=str)
@click.option("--aliases", "-al", "aliases", type=str)
@click.option("--account", "-ac", "account", type=str)
@click.pass_context
def import_legacy_workspace_(
    ctx, workspace_id, projectrepos, teamauthors, account, aliases, teams, authors, accountrepos, projects
):  # pylint: disable=unused-argument,too-many-arguments,unused-variable
    teams_ = _load_list(teams)
    authors_ = _load_list(authors)
    project_repos_ = _load_list(projectrepos)
    # account_ = _load_list(account)
    account_repos_ = _load_list(accountrepos)
    team_authors_ = _load_list(teamauthors)
    aliases_ = _load_list(aliases)
    projects_ = _load_list(projects)
    g = init_context_from_settings(ctx.obj["settings"])
    import_legacy_workspace(
        g,
        workspace_id=1,
        legacy_projects_repos=project_repos_,
        legacy_aliases=aliases_,
        legacy_teams=teams_,
        legacy_teams_authors=team_authors_,
        legacy_authors=authors_,
        legacy_account_repos=account_repos_,
        legacy_projects=projects_,
    )  # pylint: disable=too-many-arguments


def _load_list(filename):  # pylint: disable=unused-variable
    try:
        # pylint: disable=unspecified-encoding
        return json.loads(open(os.getcwd() + "/" + filename, "r").read())
    except Exception as e:  # pylint: disable=broad-except
        print(e)
        return []


# @click.command(name="refresh-workspace")
# @click.option("--workspace-id", "-w", "workspace_id", type=int)
# @click.option("--force", "-f", "force_rebuild", type=bool, is_flag=True)
# @click.pass_context
# def refresh_workspace_(ctx, workspace_id, force_rebuild):
#     g = init_context_from_settings(ctx.obj["settings"])
#     configure_celery(g.settings)

#     _refresh_workspace(g, workspace_id, force_rebuild)


# def _refresh_workspace(g: GitentialContext, workspace_id: int, force_rebuild):
#     for project in g.backend.projects.all(workspace_id):
#         logger.info("refreshing project", workspace_id=workspace_id, project_id=project.id, project_name=project.name)
#         schedule_project_refresh(g, workspace_id, project.id, force_rebuild)


def _load_fix_file():
    ret = {}
    # pylint: disable=unspecified-encoding
    with open(
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "credential_fix.csv"), "r"
    ) as f:
        reader = csv.DictReader(f)
        for row in reader:
            ret[(row["account_id"], row["clone_url"])] = row
    return ret


def _all_credentials_by_owner_and_name(g: GitentialContext):
    ret = {}
    for credential in g.backend.credentials.all():
        if credential.type == CredentialType.keypair:
            if (credential.owner_id, credential.name) in ret:
                logger.warning("Credential with the same name", owner_id=credential.owner_id, name=credential.name)
            ret[(credential.owner_id, credential.name)] = credential
    return ret


@click.command(name="fix-ssh-repo-credentials")
@click.pass_context
def fix_ssh_repo_credentials(ctx):

    g = init_context_from_settings(ctx.obj["settings"])

    fixes = _load_fix_file()

    all_workspaces = list(g.backend.workspaces.all())
    all_keypairs = _all_credentials_by_owner_and_name(g)

    logger.info("all_fixes", fixes=fixes.keys())
    logger.info("all_keypairs", all_credentials=all_keypairs.keys())

    for workspace in all_workspaces:
        logger.info("Fixing ssh repositories in workspace", workspace_id=workspace.id)
        repositories = g.backend.repositories.all(workspace.id)
        for repository in repositories:
            if repository.protocol == GitProtocol.https:
                continue
            elif (str(workspace.id), repository.clone_url) in fixes and repository.credential_id is None:
                fix = fixes[(str(workspace.id), repository.clone_url)]

                try:
                    credential = all_keypairs[(workspace.created_by, fix["name"])]
                except KeyError:
                    logger.error("Missing credential ", repository=repository, fix=fix, workspace_id=workspace.id)
                    continue

                logger.info(
                    "Updating repository credential_id",
                    workspace_id=workspace.id,
                    clone_url=repository.clone_url,
                    repo_id=repository.id,
                    credential_id=credential.id,
                    credential_name=credential.name,
                )
                repo_update = RepositoryUpdate(**repository.dict())
                repo_update.credential_id = credential.id
                g.backend.repositories.update(workspace.id, repository.id, repo_update)
            else:
                logger.warning("Missing fix ", workspace_id=workspace.id, repository=repository)


cli.add_command(import_legacy_db_)
cli.add_command(import_legacy_workspace_)
cli.add_command(import_legacy_workspace_bulk)
cli.add_command(extract_git_metrics)


cli.add_command(fix_ssh_repo_credentials)
