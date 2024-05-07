from datetime import datetime
from typing import Optional, Dict, Generator, Set, List, Union
from collections import defaultdict
from pathlib import Path
import subprocess
import math
import re
import os
import numpy as np
import pygit2
from structlog import get_logger

from gitential2.datatypes.repositories import RepositoryInDB, GitRepositoryState
from gitential2.datatypes.extraction import (
    LocalGitRepository,
    ExtractedCommit,
    ExtractedPatch,
    ExtractedPatchRewrite,
    ExtractedKind,
    ExtractedCommitBranch,
)
from gitential2.datatypes.credentials import UserPassCredential, KeypairCredential, RepositoryCredential
from gitential2.settings import GitentialSettings
from gitential2.extraction.output import OutputHandler
from gitential2.extraction.langdetection import detect_lang
from gitential2.utils import is_timestamp_within_days
from gitential2.utils.tempdir import TemporaryDirectory
from gitential2.utils.timer import Timer, time_it_log
from gitential2.utils.executors import create_executor
from gitential2.utils.ignorespec import IgnoreSpec, default_ignorespec

logger = get_logger(__name__)


# https://stackoverflow.com/questions/40883798/how-to-get-git-diff-of-the-first-commit
EMPTY_TREE_HASH = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"
COMMIT_ID_RE = re.compile(r"^\w{40}")

# pylint: disable=too-many-arguments
def extract_incremental(
    repository: RepositoryInDB,
    output: OutputHandler,
    settings: GitentialSettings,
    credentials: Optional[RepositoryCredential] = None,
    previous_state: Optional[GitRepositoryState] = None,
    ignore_spec: IgnoreSpec = default_ignorespec,
):
    with TemporaryDirectory() as workdir:
        local_repo = clone_repository_pygit2(repository, destination_path=workdir.path, credentials=credentials)
        return extract_incremental_local(local_repo, output, settings, previous_state, ignore_spec)


def extract_incremental_local(
    local_repo: LocalGitRepository,
    output: OutputHandler,
    settings: GitentialSettings,
    previous_state: Optional[GitRepositoryState] = None,
    ignore_spec: IgnoreSpec = default_ignorespec,
    commits_we_already_have: Optional[Set[str]] = None,
):
    current_state = get_repository_state(local_repo)
    logger.info("Getting commits from", local_repo=local_repo)
    commits = get_commits(
        local_repo,
        repo_analysis_limit_in_days=settings.extraction.repo_analysis_limit_in_days,
        previous_state=previous_state,
        current_state=current_state,
        commits_we_already_have=commits_we_already_have,
    )

    executor = create_executor(
        settings, local_repo=local_repo, output=output, description="Extracting commits", ignore_spec=ignore_spec
    )
    executor.map(fn=_extract_single_commit, items=commits)
    logger.info("Finished commits extraction from", local_repo=local_repo)
    return current_state


def _extract_single_commit(commit_id, local_repo: LocalGitRepository, output: OutputHandler, ignore_spec: IgnoreSpec):
    g2_repo = _git2_repo(local_repo)
    extract_commit(local_repo, commit_id, output, g2_repo=g2_repo)
    extract_commit_patches(local_repo, commit_id, output, ignore_spec, g2_repo=g2_repo)
    # extract_commit_branches(local_repo, commit_id, output)
    del g2_repo
    return output


# pylint: disable=too-complex
def clone_repository_gitcli(
    repository: RepositoryInDB, destination_path: Path, credentials: Optional[RepositoryCredential] = None
) -> LocalGitRepository:
    def subprocess_run(job: Union[List[str], str], shell=False):
        subprocess.run(
            job,
            check=True,
            capture_output=True,
            text=True,
            shell=shell,
            cwd=destination_path,
        )

    if not os.path.exists(destination_path):
        os.makedirs(destination_path)
    with TemporaryDirectory() as workdir:
        try:
            if isinstance(credentials, KeypairCredential):
                private_key = workdir.new_file(credentials.privkey)
                if credentials and credentials.passphrase:
                    subprocess_run(
                        "{ sleep .1; echo "
                        + credentials.passphrase
                        + "; } | script -q /dev/null -c 'ssh-add "
                        + private_key
                        + "' ; git clone "
                        + repository.clone_url
                        + " .",
                        shell=True,
                    )
                else:
                    subprocess_run(
                        "ssh-add " + private_key + "' ; git clone " + repository.clone_url + " .",
                        shell=True,
                    )
            elif isinstance(credentials, UserPassCredential):
                jobs = [
                    [
                        "git",
                        "init",
                    ],
                    ["git", "remote", "add", "origin", repository.clone_url],
                    [
                        "git",
                        "config",
                        "credential.username",
                        credentials.username,
                    ],
                    [
                        "git",
                        "config",
                        "credential.helper",
                        '!f() { sleep 0.3; printf "%s\n" "password=' + credentials.password + '"; };f',
                    ],
                    ["git", "fetch"],
                    ["git", "checkout", "master"],
                ]
                for command in jobs:
                    subprocess_run(command)
            else:
                subprocess_run(["git", "clone", repository.clone_url, "."])
        except subprocess.CalledProcessError as e:
            logger.warning(
                "Failed to run git clone.", clone_url=repository.clone_url, error=e, stdout=e.stdout, stderr=e.stderr
            )
            if "pathspec" in e.stderr:
                try:
                    subprocess_run(["git", "checkout", "main"])
                except subprocess.CalledProcessError as e:
                    logger.warning(
                        "Failed to checkout main branch",
                        clone_url=repository.clone_url,
                        error=e,
                        stdout=e.stdout,
                        stderr=e.stderr,
                    )
        # Needs some validation
        return LocalGitRepository(repo_id=repository.id, directory=destination_path)


def clone_repository_pygit2(
    repository: RepositoryInDB, destination_path: Path, credentials: Optional[RepositoryCredential] = None
) -> LocalGitRepository:
    with TemporaryDirectory() as workdir:

        def _construct_callbacks(c):
            if isinstance(c, UserPassCredential):
                userpass = pygit2.UserPass(username=c.username, password=c.password)
                logger.debug(
                    "Userpass credential",
                    url=repository.clone_url,
                    path=str(destination_path),
                    username=c.username,
                    password=c.password,
                )
                return pygit2.RemoteCallbacks(credentials=userpass)
            elif isinstance(c, KeypairCredential):
                logger.info(
                    "Soon begin to clone SSH repo.",
                    public_key=c.pubkey,
                    private_key=c.privkey,
                    passphrase=c.passphrase,
                    username=c.username,
                )
                keypair = pygit2.Keypair(
                    username=c.username,
                    pubkey=workdir.new_file(c.pubkey),
                    privkey=workdir.new_file(c.privkey),
                    passphrase=c.passphrase,
                )
                return pygit2.RemoteCallbacks(credentials=keypair)
            return None

        callbacks = _construct_callbacks(credentials)
        logger.info("Cloning repository", url=repository.clone_url, path=str(destination_path), callbacks=callbacks)
        pygit2.clone_repository(url=repository.clone_url, path=str(destination_path), callbacks=callbacks)
        return LocalGitRepository(repo_id=repository.id, directory=destination_path)


def clone_repository(
    repository: RepositoryInDB, destination_path: Path, credentials: Optional[RepositoryCredential] = None
) -> LocalGitRepository:
    try:
        if repository.clone_url.startswith("https"):
            return clone_repository_gitcli(repository, destination_path, credentials)
    except:  # pylint: disable=bare-except
        logger.exception("There was an error with git cli, falling back to pygit2")
    return clone_repository_pygit2(repository, destination_path, credentials)


def _git2_repo(repository: LocalGitRepository) -> pygit2.Repository:
    return pygit2.Repository(str(repository.directory))


def get_repository_state(repository: LocalGitRepository) -> GitRepositoryState:
    g2_repo = _git2_repo(repository)

    def _get_references(from_, prefix, skip=None) -> Dict[str, str]:
        ret = {}
        skip = skip or []
        for ref in from_:
            if ref.startswith(prefix) and (ref not in skip):
                try:
                    commit, _ = g2_repo.resolve_refish(ref)
                    name = ref.replace(prefix, "")
                    ret[name] = str(commit.id)
                except pygit2.InvalidSpecError:
                    logger.warning("pygit2 error, invalid ref", ref_id=ref)
        return ret

    return GitRepositoryState(
        branches=_get_references(g2_repo.branches.remote, prefix="origin/", skip=["origin/HEAD"]),
        tags=_get_references(g2_repo.references, prefix="refs/tags/"),
    )


def get_commits(
    repository: LocalGitRepository,
    previous_state: Optional[GitRepositoryState] = None,
    current_state: Optional[GitRepositoryState] = None,
    commits_we_already_have: Optional[Set[str]] = None,
    repo_analysis_limit_in_days: Optional[int] = None,
) -> Generator[str, None, None]:

    current_state = current_state or get_repository_state(repository)
    previous_state = previous_state or GitRepositoryState(branches={}, tags={})
    heads = current_state.commit_ids
    tails = previous_state.commit_ids
    g2_repo = _git2_repo(repository)
    commits_already_yielded = commits_we_already_have or set()

    for head in heads:
        walker = g2_repo.walk(head, pygit2.GIT_SORT_TOPOLOGICAL | pygit2.GIT_SORT_REVERSE)

        # Ignore the tails and ancestors
        for tail in tails:
            try:
                walker.hide(tail)
            except KeyError:
                logger.warning(
                    "A previously seen commit hash is missing from the repository",
                    repo_id=repository.repo_id,
                    commit_id=tail,
                )

        # Collect all commits
        for commit in walker:
            if not repo_analysis_limit_in_days or is_timestamp_within_days(
                commit.commit_time, repo_analysis_limit_in_days
            ):
                # If we saw the commit before, we already know it's ancestors
                if str(commit.id) in commits_already_yielded:
                    walker.hide(commit.id)
                else:
                    logger.debug(
                        "Including commit.",
                        commit={
                            "id": commit.hex,
                            "time": commit.commit_time,
                            "time_formatted": datetime.fromtimestamp(commit.commit_time).strftime("%Y-%m-%d, %H:%M:%S"),
                        },
                    )
                    commits_already_yielded.add(str(commit.id))
                    yield str(commit.id)
            else:
                logger.debug(
                    "Skipping commit. Out of time range.",
                    time_range=repo_analysis_limit_in_days,
                    commit={
                        "id": commit.hex,
                        "time": commit.commit_time,
                        "time_formatted": datetime.fromtimestamp(commit.commit_time).strftime("%Y-%m-%d, %H:%M:%S"),
                    },
                )


def extract_commit(repository: LocalGitRepository, commit_id: str, output: OutputHandler, **kwargs):
    g2_repo = kwargs.get("g2_repo") or _git2_repo(repository)
    commit = g2_repo.get(commit_id)
    atime = _utc_timestamp_for(commit.author)
    ctime = _utc_timestamp_for(commit.committer)

    output.write(
        ExtractedKind.EXTRACTED_COMMIT.value,
        ExtractedCommit(
            repo_id=repository.repo_id,
            commit_id=commit.id.hex,
            atime=atime,
            aemail=commit.author.email,
            aname=commit.author.name,
            ctime=ctime,
            cemail=commit.committer.email,
            cname=commit.committer.name,
            message=commit.message,
            nparents=len(commit.parent_ids),
            tree_id=str(commit.tree_id),
        ),
    )
    return output


def extract_commit_patches(
    repository: LocalGitRepository,
    commit_id: str,
    output: OutputHandler,
    ignore_spec: IgnoreSpec = default_ignorespec,
    **kwargs,
):
    g2_repo = kwargs.get("g2_repo") or _git2_repo(repository)
    commit = kwargs.get("commit") or g2_repo.get(commit_id)

    def _get_parents_and_diffs():
        parents = [g2_repo.get(parent_id) for parent_id in commit.parent_ids]
        diffs = [g2_repo.diff(parent, commit) for parent in parents]
        if parents:
            return parents, diffs
        else:
            # Initial commit, use the empty tree
            return [g2_repo.get(EMPTY_TREE_HASH)], [g2_repo.diff(EMPTY_TREE_HASH, commit)]

    parents, diffs = _get_parents_and_diffs()

    patch_count = 0
    for parent, diff in zip(parents, diffs):
        for patch in diff:
            if not ignore_spec.should_ignore(patch.delta.new_file.path):
                _extract_patch(commit, parent, patch, g2_repo, output, repo_id=repository.repo_id)
            patch_count += 1
    # logger.debug("patch count", patch_count=patch_count, repository=repository, commit_id=commit_id)
    return patch_count


@time_it_log(logger)
def extract_branches(settings: GitentialSettings, repository: LocalGitRepository, output: OutputHandler):
    logger.info("Getting commits from", local_repo=repository)
    commits = get_commits(repository, repo_analysis_limit_in_days=settings.extraction.repo_analysis_limit_in_days)

    logger.info("Getting commit branches from", local_repo=repository)

    executor = create_executor(settings, local_repo=repository, output=output, description="Extracting commit branches")

    executor.map(fn=_extract_single_commit_branches, items=commits)

    logger.info("Finished commit branches extraction from", local_repo=repository)


def _extract_single_commit_branches(commit_id: str, local_repo: LocalGitRepository, output: OutputHandler):
    extract_commit_branches(local_repo, commit_id, output)
    return output


def extract_commit_branches(repository: LocalGitRepository, commit_id: str, output: OutputHandler):
    g2_repo = _git2_repo(repository)
    commit = g2_repo.get(commit_id)
    atime = _utc_timestamp_for(commit.author)

    branches = list(g2_repo.branches.with_commit(commit=commit))
    for branch in branches:
        output.write(
            ExtractedKind.EXTRACTED_COMMIT_BRANCH.value,
            ExtractedCommitBranch(repo_id=repository.repo_id, commit_id=commit.id.hex, atime=atime, branch=branch),
        )
    return output


def _extract_patch(commit, parent, patch, g2_repo, output, repo_id):
    patch_stats = _get_patch_stats(patch)
    nrewrites, rewrites_loc = _extract_patch_rewrites(commit, parent, patch, g2_repo, output, repo_id=repo_id)
    lang, langtype = _get_patch_lang_and_langtype(commit, patch, g2_repo)

    # We cannot store a larger file size in postgres
    # However, it's rare to see large files commited to git,
    # and usually these are more likely some kind of data files
    MAX_FILE_SIZE = 2147483647

    output.write(
        ExtractedKind.EXTRACTED_PATCH.value,
        ExtractedPatch(
            repo_id=repo_id,
            commit_id=commit.id.hex,
            parent_commit_id=parent.id.hex,
            status=patch.delta.status_char(),
            newpath=patch.delta.new_file.path[:255],
            oldpath=patch.delta.old_file.path[:255],
            newsize=min(patch.delta.new_file.size, MAX_FILE_SIZE),
            oldsize=min(patch.delta.old_file.size, MAX_FILE_SIZE),
            is_binary=patch.delta.is_binary,
            lang=lang,
            langtype=langtype,
            loc_d=patch_stats[0],
            loc_i=patch_stats[1],
            comp_d=patch_stats[2],
            comp_i=patch_stats[3],
            loc_d_std=patch_stats[4],
            loc_i_std=patch_stats[5],
            comp_d_std=patch_stats[6],
            comp_i_std=patch_stats[7],
            nhunks=len(patch.hunks),
            nrewrites=nrewrites,
            rewrites_loc=rewrites_loc,
        ),
    )
    return output


def _get_patch_lang_and_langtype(commit, patch, g2_repo):
    filepath, filesize, is_binary, commit_id = (
        patch.delta.new_file.path,
        patch.delta.new_file.size,
        patch.delta.is_binary,
        str(commit.id.hex),
    )
    return detect_lang(filepath, filesize, is_binary, commit_id, g2_repo.path)


def _get_patch_stats(patch):
    with Timer("_get_patch_stats", threshold_ms=10000):
        if len(patch.hunks) == 0:  # pylint: disable=compare-to-zero
            return np.zeros(8)

        addition = "+"
        deletion = "-"
        stats = np.zeros((len(patch.hunks), 4), dtype=np.int32)
        view = stats[:, :]
        for i, hunk in enumerate(patch.hunks):
            for line in hunk.lines:
                if line.origin == deletion:
                    view[i, 0] = view[i, 0] + 1
                    view[i, 2] = view[i, 2] + _indentation(line.raw_content)
                elif line.origin == addition:
                    view[i, 1] = view[i, 1] + 1
                    view[i, 3] = view[i, 3] + _indentation(line.raw_content)
        return np.hstack((stats.sum(axis=0), stats.std(axis=0)))


def _indentation(s, tabsize=4):
    nspaces = 0
    ntabs = 0
    for i in range(len(s)):  # pylint: disable=consider-using-enumerate
        c = s[i]
        if c == 32:
            nspaces += 1
        elif c == 9:
            if nspaces > 0:
                ntabs += math.ceil(nspaces / tabsize)
                nspaces = 0
            ntabs += 1
        else:
            if nspaces > 0:
                ntabs += math.ceil(nspaces / tabsize)
            return ntabs
    return 0


def _extract_patch_rewrites(commit, parent, patch, g2_repo, output, repo_id):  # pylint: disable=too-complex
    def _is_merge():
        return len(commit.parent_ids) > 1

    def _is_addition():
        return patch.delta.status_char() == "A"

    def _is_initial_commit():
        return not isinstance(parent, pygit2.Commit)

    def _is_empty_patch():
        return len(patch.hunks) == 0  # pylint: disable=compare-to-zero

    def _is_binary():
        return patch.delta.is_binary

    if _is_merge() or _is_addition() or _is_initial_commit() or _is_empty_patch() or _is_binary():
        return 0, 0

    deletion = "-"
    filepath = patch.delta.old_file.path
    rewrites = defaultdict(int)
    with Timer("blame_porcelain", threshold_ms=10000):
        line_blame_dict = blame_porcelain(g2_repo.path, filepath, str(parent.id))

    if not line_blame_dict:
        return 0, 0

    for hunk in patch.hunks:
        for line in hunk.lines:
            if line.origin == deletion:
                blame_co_id = line_blame_dict[line.old_lineno]
                rewrites[blame_co_id] += 1

    for rewritten_commit_id, line_count in rewrites.items():
        rewritten = g2_repo.get(rewritten_commit_id)
        output.write(
            ExtractedKind.EXTRACTED_PATCH_REWRITE.value,
            ExtractedPatchRewrite(
                repo_id=repo_id,
                commit_id=commit.id.hex,
                atime=_utc_timestamp_for(commit.author),
                aemail=commit.author.email,
                newpath=patch.delta.new_file.path[:255],
                rewritten_commit_id=str(rewritten_commit_id),
                rewritten_atime=_utc_timestamp_for(rewritten.author),
                rewritten_aemail=rewritten.author.email,
                loc_d=line_count,
            ),
        )

    return len(rewrites), sum(rewrites.values())


def _utc_timestamp_for(signature):
    # signature.time is already an unix timestamp in utc (we don't need the offset!!!)
    return signature.time


def blame_porcelain(git_path, filepath, newest_commit) -> Dict[int, str]:
    """Returns with line-> commit_id mapping"""

    blame_porcelain_command = ["git", "blame", newest_commit, "--porcelain", "--", filepath]
    try:
        completed_process = subprocess.run(blame_porcelain_command, capture_output=True, cwd=git_path, check=True)
    except subprocess.CalledProcessError:
        logger.exception("Failed to run git blame.", newest_commit=newest_commit, git_path=git_path, filepath=filepath)
        return {}
    porcelain_output = completed_process.stdout.decode(errors="ignore")

    def is_not_header(line):
        return (
            line.startswith("\t")
            or line.startswith("author")
            or line.startswith("committer")
            or line.startswith("summary")
            or line.startswith("filename")
            or line.startswith("previous")
        )

    headers = {}
    for line in porcelain_output.splitlines():
        if is_not_header(line):
            continue
        words = line.split(" ")
        # THE PORCELAIN FORMAT
        # In this format, each line is output after a header;
        # the header at the minimum has the first line which has:
        # 40-byte SHA-1 of the commit the line is attributed to;
        # the line number of the line in the original file;
        # the line number of the line in the final file;
        try:
            if words and COMMIT_ID_RE.match(words[0]):
                headers[int(words[2])] = words[0]
        except (IndexError, ValueError):
            logger.exception(
                "git porcelain output format error",
                git_path=git_path,
                filepath=filepath,
                newest_commit=newest_commit,
                line=line,
            )

    return headers
