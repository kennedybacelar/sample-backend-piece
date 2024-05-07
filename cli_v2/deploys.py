from pathlib import Path
import typer

from scripts.gather_internal_deployment_history import (
    gathering_internal_deployment_history,
    exporting_internal_deployment_history_into_json_file,
)
from gitential2.core.deploys import get_all_deploys, recalculate_deploy_commits
from .common import get_context, print_results, OutputFormat

app = typer.Typer()


@app.command("get-all-deploys")
def get_deploys(workspace_id: int):
    g = get_context()
    deploys = get_all_deploys(g=g, workspace_id=workspace_id)
    print_results([deploys], format_=OutputFormat.json)


@app.command("recalculate-deploy-commits")
def recalculate_deploys(workspace_id: int):
    g = get_context()
    recalculate_deploy_commits(g=g, workspace_id=workspace_id)


@app.command("gather-internal-deployment")
def gather_internal_deployment_history(path: Path):
    """
    In order to display gitential's deployment history run:

    $ g2 deploys gather-internal-deployment {local_path_to_environments_repo}

    """
    internal_deployment_history = gathering_internal_deployment_history(path)
    print_results(internal_deployment_history, format_=OutputFormat.json)


@app.command("exporting-internal-deployment-into-json-file")
def export_data_into_json_file(repo_source_path: Path, destination_path: Path = Path("")):
    """
    The goal of this function is to export a json file with the deployment history of the following repositories: gitential2,
    catwalk2, gitential-front-end, and helm-chart.

    In order to achieve that, there are two input parameters, <repo_source_path> and  <destination_path>.

    <repo_source_path>: it is a local path where the repository environments is cloned in your machine.

    <destination_path>: it is the destination path of json file, it's an optional parameter, if not informed the file is going
    to be generated at the root of gitential2 directory.

    example of command:
    $ g2 deploys exporting-internal-deployment-into-json-file /home/specific_user/Documents/Dev/Work/Testing/environments
    """
    exporting_internal_deployment_history_into_json_file(repo_source_path, destination_path)
