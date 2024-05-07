import gc
from typing import List, Tuple, cast
from itertools import product
from functools import partial
import datetime as dt
import pandas as pd
import numpy as np
from structlog import get_logger
from gitential2.datatypes.authors import AuthorAlias
from ..utils import split_timerange
from ..utils.is_bugfix import calculate_is_bugfix
from ..utils.timer import LogTimeIt, time_it_log

from .authors import get_or_create_author_for_alias
from .context import GitentialContext

logger = get_logger(__name__)


def _get_time_intervals() -> List[Tuple[dt.datetime, dt.datetime]]:
    current_time = dt.datetime.utcnow()
    quaters_to_analyze = 32
    ret: list = []
    for i in range(quaters_to_analyze):
        ret.append((current_time - dt.timedelta(days=100 * (i + 1)), current_time - dt.timedelta(days=100 * i)))

    return ret


def _log_large_dataframe(workspace_id, repository_id, name: str, df: pd.DataFrame, **ctx) -> pd.DataFrame:
    mem_usage = df.memory_usage(deep=True).sum()
    megabyte = 1024 * 1024
    if mem_usage > 100 * megabyte:
        log_ = logger.warning
    else:
        log_ = logger.info

    log_(
        "DF-SIZE-LOG",
        df_name=name,
        workspace_id=workspace_id,
        repository_id=repository_id,
        size=df.size,
        mem_usage=mem_usage,
        mem_usage_kb=mem_usage / 1024,
        shape=df.shape,
        **ctx,
    )
    return df


def recalculate_repository_values(
    g: GitentialContext, workspace_id: int, repository_id: int
):  # pylint: disable=unused-variable
    logger.info("Recalculating repository commit values", workspace_id=workspace_id, repository_id=repository_id)

    for intervals in _get_time_intervals():
        from_, to_ = intervals
        recalculate_repo_values_in_interval(g, workspace_id, repository_id, from_, to_)


def recalculate_repo_values_in_interval(
    g: GitentialContext, workspace_id: int, repository_id: int, from_: dt.datetime, to_: dt.datetime, commit_limit=300
):

    extracted_commit_count = g.backend.extracted_commits.count(
        workspace_id=workspace_id, repository_ids=[repository_id], from_=from_, to_=to_
    )

    if extracted_commit_count > commit_limit:
        logger.warning(
            "Extraced commit count is too large for calculation, splitting time interval to half",
            workspace_id=workspace_id,
            repository_id=repository_id,
            commit_count=extracted_commit_count,
            from_=from_,
            to_=to_,
        )

        intervals = split_timerange(from_, to_)
        for (from__, to__) in intervals:
            recalculate_repo_values_in_interval(g, workspace_id, repository_id, from__, to__, commit_limit=commit_limit)
    else:
        with LogTimeIt("get_extracted_dataframes", logger, threshold_ms=1000):
            (
                extracted_commits_df,
                extracted_patches_df,
                extracted_patch_rewrites_df,
                pull_request_commits_df,
            ) = g.backend.get_extracted_dataframes(
                workspace_id=workspace_id, repository_id=repository_id, from_=from_, to_=to_
            )

        _log_large_df = partial(
            _log_large_dataframe, workspace_id=workspace_id, repository_id=repository_id, from_=from_, to_=to_
        )

        with LogTimeIt("get_extracted_dataframes", logger, threshold_ms=1000):
            (
                extracted_commits_df,
                extracted_patches_df,
                extracted_patch_rewrites_df,
                pull_request_commits_df,
            ) = g.backend.get_extracted_dataframes(
                workspace_id=workspace_id, repository_id=repository_id, from_=from_, to_=to_
            )
        for name, df in [
            ("extracted_commits_df", extracted_commits_df),
            ("extracted_patches_df", extracted_patches_df),
            ("extracted_patch_rewrites_df", extracted_patch_rewrites_df),
            ("pull_request_commits_df", pull_request_commits_df),
        ]:
            _log_large_df(name=name, df=df)

        if extracted_patches_df.empty or extracted_commits_df.empty:
            return

        parents_df = _log_large_df(
            name="parents_df",
            df=extracted_patches_df.reset_index()[["commit_id", "parent_commit_id"]].drop_duplicates(),
        )

        prepared_commits_df = _log_large_df(
            name="prepared_commits_df",
            df=_prepare_extracted_commits_df(g, workspace_id, extracted_commits_df, parents_df),
        )
        prepared_patches_df = _log_large_df(
            name="prepared_patches_df", df=_prepare_extracted_patches_df(extracted_patches_df)
        )
        uploc_df = _log_large_df(
            name="uploc_df", df=_calculate_uploc_df(extracted_commits_df, extracted_patch_rewrites_df)
        )

        # We can remove the original dataframes here
        del extracted_commits_df
        del extracted_patches_df
        del extracted_patch_rewrites_df
        gc.collect()

        commits_patches_df = _log_large_df(
            name="commits_patches_df",
            df=_prepare_commits_patches_df(prepared_commits_df, prepared_patches_df, uploc_df),
        )
        outlier_df = _log_large_df(name="outlier_df", df=_calc_outlier_detection_df(prepared_patches_df))

        calculated_commits_df = _log_large_df(
            name="calculated_commits_df",
            df=_calculate_commit_level(prepared_commits_df, commits_patches_df, outlier_df, pull_request_commits_df),
        )
        calculated_patches_df = _log_large_df(
            name="calculated_patches_df", df=_calculate_patch_level(commits_patches_df)
        )

        logger.info(
            "Saving repository commit calculations",
            workspace_id=workspace_id,
            repository_id=repository_id,
            from_=from_,
            to_=to_,
        )

        with LogTimeIt("save_calculated_dataframes", logger, threshold_ms=1000):
            g.backend.save_calculated_dataframes(
                workspace_id=workspace_id,
                repository_id=repository_id,
                calculated_commits_df=calculated_commits_df,
                calculated_patches_df=calculated_patches_df,
                from_=from_,
                to_=to_,
            )

        del calculated_commits_df
        del calculated_patches_df
        del outlier_df
        del commits_patches_df
        del parents_df
        del prepared_commits_df
        del prepared_patches_df
        del uploc_df
        gc.collect()


@time_it_log(logger)
def _prepare_extracted_commits_df(
    g: GitentialContext, workspace_id: int, extracted_commits_df: pd.DataFrame, parents_df: pd.DataFrame
) -> pd.DataFrame:
    email_author_map = _get_or_create_authors_from_commits(g, workspace_id, extracted_commits_df)
    extracted_commits_df["aid"] = extracted_commits_df.apply(lambda row: email_author_map.get(row["aemail"]), axis=1)
    extracted_commits_df["cid"] = extracted_commits_df.apply(lambda row: email_author_map.get(row["cemail"]), axis=1)
    extracted_commits_df["date"] = extracted_commits_df["atime"]
    extracted_commits_df["is_merge"] = extracted_commits_df["nparents"] > 1
    extracted_commits_df["is_bugfix"] = extracted_commits_df.apply(
        lambda x: calculate_is_bugfix(labels=[], title=x["message"]), axis=1
    )
    age_df = _calculate_age_df(extracted_commits_df, parents_df)
    ret = extracted_commits_df.set_index(["commit_id"]).join(age_df)
    hourse_measured_df = _measure_hours(ret)
    return ret.join(hourse_measured_df)


@time_it_log(logger)
def _prepare_extracted_patches_df(extracted_patches_df: pd.DataFrame) -> pd.DataFrame:
    def _calc_is_test(row):
        return row["langtype"] == "PROGRAMMING" and "test" in row["newpath"]

    def _fix_lang_type(row):
        return row["langtype"].name

    def _zero_if_unknown(field_name):
        def _inner(row):
            return row[field_name] if row["langtype"] != "UNKNOWN" else 0

        return _inner

    extracted_patches_df["outlier"] = 0
    extracted_patches_df["anomaly"] = 0
    extracted_patches_df["langtype"] = extracted_patches_df.apply(_fix_lang_type, axis=1)
    extracted_patches_df["is_test"] = extracted_patches_df.apply(_calc_is_test, axis=1)
    extracted_patches_df["comp_i"] = extracted_patches_df.apply(_zero_if_unknown("comp_i"), axis=1)
    extracted_patches_df["comp_d"] = extracted_patches_df.apply(_zero_if_unknown("comp_d"), axis=1)
    extracted_patches_df["loc_i"] = extracted_patches_df.apply(_zero_if_unknown("loc_i"), axis=1)
    extracted_patches_df["loc_d"] = extracted_patches_df.apply(_zero_if_unknown("loc_d"), axis=1)

    return extracted_patches_df.set_index(["commit_id"])


@time_it_log(logger)
def _calculate_age_df(extracted_commit_df: pd.DataFrame, parents_df: pd.DataFrame) -> pd.DataFrame:
    author_times = cast(dict, extracted_commit_df.set_index(["commit_id"])[["atime"]].to_dict(orient="dict"))["atime"]

    def _calc_age(row):
        # print(row["commit_id"], row["parent_commit_id"], author_times.get(row["commit_id"]))
        if row["commit_id"] in author_times and row["parent_commit_id"] in author_times:
            delta = (author_times[row["commit_id"]] - author_times[row["parent_commit_id"]]).total_seconds()
            return delta
        else:
            return -1

    parents_df["age"] = parents_df.apply(_calc_age, axis=1)
    return parents_df.groupby("commit_id").min()["age"].to_frame()


@time_it_log(logger)
def _calculate_uploc_df(
    extracted_commits_df, extracted_patch_rewrites_df
):  # pylint: disable=compare-to-zero,singleton-comparison
    # prepare patch rewrites
    if not extracted_patch_rewrites_df.empty:
        merges_df = extracted_commits_df[["commit_id", "is_merge"]].set_index("commit_id")
        extracted_patch_rewrites_df = extracted_patch_rewrites_df.join(merges_df, on=["rewritten_commit_id"])
        extracted_patch_rewrites_df = extracted_patch_rewrites_df.join(
            merges_df, on=["commit_id"], rsuffix="__newer", lsuffix="__older"
        )
        # calculate uploc_df
        df = extracted_patch_rewrites_df[
            extracted_patch_rewrites_df["atime"] - extracted_patch_rewrites_df["rewritten_atime"]
            < dt.timedelta(days=21)
        ]
        df = df[df["is_merge__newer"] == False]
        df = df[df["is_merge__older"] == False]
        uploc_df = (
            pd.DataFrame({"uploc": df.groupby(["rewritten_commit_id", "newpath"])["loc_d"].agg("sum")})
            .reset_index()
            .rename(columns={"rewritten_commit_id": "commit_id"})
            .set_index(["commit_id", "newpath"])
        )
        return uploc_df
    else:
        return pd.DataFrame()


@time_it_log(logger)
def _prepare_commits_patches_df(
    prepared_commits_df: pd.DataFrame, prepared_patches_df: pd.DataFrame, uploc_df: pd.DataFrame
):
    df = (
        prepared_commits_df.drop(labels=["message"], axis=1)
        .join(prepared_patches_df, lsuffix="__commit", rsuffix="__patch")
        .reset_index()
        .set_index(["commit_id", "parent_commit_id", "newpath"])
    )
    if not uploc_df.empty:
        df_with_uploc = df.join(uploc_df, on=["commit_id", "newpath"])
    else:
        df_with_uploc = df
        df_with_uploc["uploc"] = 0

    def _finalize_uploc(row):
        if row["is_merge"]:
            return 0
        else:
            return min(row["uploc"], row["loc_i"])

    df_with_uploc["uploc"] = df_with_uploc.apply(_finalize_uploc, axis=1)
    return df_with_uploc


@time_it_log(logger)
def _get_or_create_authors_from_commits(g: GitentialContext, workspace_id, extracted_commits_df):
    authors_df = (
        extracted_commits_df[["aname", "aemail"]]
        .drop_duplicates()
        .rename(columns={"aname": "name", "aemail": "email"})
        .set_index("email")
    )

    commiters_df = (
        extracted_commits_df[["cname", "cemail"]]
        .drop_duplicates()
        .rename(columns={"cname": "name", "cemail": "email"})
        .set_index("email")
    )

    developers_df = pd.concat([authors_df, commiters_df])
    developers_df = developers_df[~developers_df.index.duplicated(keep="first")]

    authors = [
        get_or_create_author_for_alias(g, workspace_id, AuthorAlias(name=name, email=email))
        for email, name in developers_df["name"].to_dict().items()
    ]
    email_aid_map = {}
    for author in authors:
        for alias in author.aliases:
            email_aid_map[alias.email] = author.id

    return email_aid_map


@time_it_log(logger)
def _measure_hours(prepared_commits_df: pd.DataFrame) -> pd.DataFrame:
    atime = prepared_commits_df[["aid", "atime"]].sort_values(by=["aid", "atime"], ascending=True).atime

    deltasecs = (atime - atime.shift(1)).dt.total_seconds()
    measured = pd.concat([deltasecs, prepared_commits_df.age], axis=1)
    measured[measured < 0] = np.nan  # substract some sort of overhead
    return (measured.min(axis=1, skipna=True) / 3600).to_frame(name="hours_measured")


@time_it_log(logger)
def _calculate_commit_level(
    prepared_commits_df: pd.DataFrame,
    commits_patches_df: pd.DataFrame,
    outlier_df: pd.DataFrame,
    pull_request_commits_df: pd.DataFrame,
):
    calculated_commits = prepared_commits_df.join(
        commits_patches_df.groupby("commit_id")
        .agg({"loc_i": "sum", "loc_d": "sum", "comp_i": "sum", "comp_d": "sum", "uploc": "sum", "aid": "count"})
        .rename(
            columns={
                "aid": "nfiles",
                "loc_i": "loc_i_c",
                "loc_d": "loc_d_c",
                "comp_i": "comp_i_c",
                "comp_d": "comp_d_c",
                "uploc": "uploc_c",
            }
        )
        # commits_patches_df.groupby("commit_id")[["loc_i", "loc_d", "comp_i", "comp_d", "uploc"]].sum()
    )

    calculated_commits["loc_effort_c"] = 1.0 * calculated_commits["loc_i_c"] + 0.2 * calculated_commits["loc_d_c"]

    calculated_commits = calculated_commits.join(outlier_df)

    # In order to prevent infinity or null values in the calculated columns velocity_measured & velocity
    replacement_values = {float("inf"): 0, np.nan: 0}

    calculated_commits["velocity_measured"] = (
        (calculated_commits["loc_i_c"] + (0.2 * calculated_commits["loc_d_c"])) / calculated_commits["hours_measured"]
    ).replace(replacement_values)
    calculated_commits = _add_estimate_hours(_median_measured_velocity(calculated_commits))
    calculated_commits["velocity"] = (
        (calculated_commits["loc_i_c"] + (0.2 * calculated_commits["loc_d_c"])) / calculated_commits["hours"]
    ).replace(replacement_values)
    # calculated_commits["is_bugfix"] = calculated_commits.apply(
    #     lambda x: calculate_is_bugfix(labels=[], title=x["message"]), axis=1
    # )
    calculated_commits["is_pr_exists"] = calculated_commits.apply(
        lambda x: len(pull_request_commits_df[pull_request_commits_df["commit_id"] == x.name]) > 0, axis=1
    )
    calculated_commits["is_pr_open"] = calculated_commits.apply(
        lambda x: x["is_pr_exists"]
        and len(
            pull_request_commits_df[
                (pull_request_commits_df["commit_id"] == x.name) & (pull_request_commits_df["state"] == "open")
            ]
        )
        > 0,
        axis=1,
    )
    calculated_commits["is_pr_closed"] = calculated_commits.apply(
        lambda x: x["is_pr_exists"]
        and len(pull_request_commits_df[pull_request_commits_df["commit_id"] == x.name])
        == len(
            pull_request_commits_df[
                (pull_request_commits_df["commit_id"] == x.name) & (pull_request_commits_df["state"] == "closed")
            ]
        )
        > 0,
        axis=1,
    )
    return calculated_commits


@time_it_log(logger)
def _calculate_patch_level(calculated_patches_df: pd.DataFrame) -> pd.DataFrame:
    calculated_patches_columns = [
        "repo_id",
        "commit_id",
        "aid",
        "cid",
        "date",
        "parent_commit_id",
        "status",
        "newpath",
        "oldpath",
        "newsize",
        "oldsize",
        "is_binary",
        "lang",
        "langtype",
        "loc_i",
        "loc_d",
        "comp_i",
        "comp_d",
        "nhunks",
        "nrewrites",
        "rewrites_loc",
        "is_merge",
        "is_test",
        "uploc",
        "outlier",
        "anomaly",
        "is_bugfix",
    ]

    calculated_patches_df = calculated_patches_df.reset_index().rename(columns={"repo_id__commit": "repo_id"})
    calculated_patches_df = calculated_patches_df[calculated_patches_columns]
    calculated_patches_df = calculated_patches_df[calculated_patches_df["parent_commit_id"].notnull()]
    calculated_patches_df["loc_effort_p"] = 1.0 * calculated_patches_df["loc_i"] + 0.2 * calculated_patches_df["loc_d"]

    # is_new_code: True if the patch has at least 10 lines addition and the added lines are at least 2x the deleted lines.
    calculated_patches_df["is_new_code"] = (calculated_patches_df["loc_i"] >= 10) & (
        (calculated_patches_df["loc_i"] / calculated_patches_df["loc_d"]) >= 2
    )

    # is_collaboration: True if there is another patch for the same file in the same repository between +/- 3 weeks but with a different author.
    with LogTimeIt("calculating is_collaboration", logger):
        # pylint: disable=singleton-comparison,compare-to-zero
        collaboration_df = calculated_patches_df[calculated_patches_df["is_merge"] == False][
            ["repo_id", "newpath", "date", "aid"]
        ]

        patches_map = {}

        def _is_collaboration(collaboration_df: pd.DataFrame, x: pd.DataFrame) -> bool:
            if x["newpath"] not in patches_map:
                sub_df = collaboration_df[
                    (collaboration_df["repo_id"] == x["repo_id"]) & (collaboration_df["newpath"] == x["newpath"])
                ]
                sub_df.sort_values(["date"])
                patches_map[x["newpath"]] = sub_df

            sub_df = patches_map[x["newpath"]]

            result = False

            from_date = x["date"] - pd.Timedelta("21 days")
            to_date = x["date"] + pd.Timedelta("21 days")

            # pylint: disable=unused-variable
            for index, row in sub_df.iterrows():
                if row["date"] < from_date:
                    continue
                elif row["date"] > to_date:
                    break
                elif row["aid"] != x["aid"]:
                    result = True
                    break

            return result

        calculated_patches_df["is_collaboration"] = calculated_patches_df.apply(
            lambda x: _is_collaboration(collaboration_df, x),
            axis=1,
        )
    return calculated_patches_df.set_index(["repo_id", "commit_id", "parent_commit_id", "newpath"])


def _median_measured_velocity(calculated_commits: pd.DataFrame) -> pd.DataFrame:
    accurates = (
        calculated_commits["hours_measured"].between(0.001, 2.0)
        & (calculated_commits["velocity_measured"] > 0)
        & ~calculated_commits["is_merge"]
    )
    medians = calculated_commits[accurates].groupby("aid").agg({"velocity_measured": "median"})
    medians.columns = ["median_velocity_measured"]
    df = calculated_commits.merge(medians, left_on="aid", right_index=True, how="left")
    return df


def _add_estimate_hours(calculated_commits: pd.DataFrame) -> pd.DataFrame:
    df = calculated_commits.copy()
    df["hours_estimated"] = (df["loc_i_c"] / df["median_velocity_measured"]).fillna(1 / 12)
    df["hours_estimated"] = df["hours_estimated"].clip(lower=1 / 12)
    df["hours"] = df[["hours_measured", "hours_estimated"]].min(axis=1, skipna=True).clip(upper=4.0)
    return df


def _calc_outlier_detection_df(prepared_patches_df: pd.DataFrame) -> pd.DataFrame:
    pdf = prepared_patches_df.copy()
    columns = [
        "loc_i",
        "loc_d",
        "comp_i",
        "comp_d",
    ]  # 'blame_loc'

    stats = pdf.reset_index().groupby(["commit_id", "outlier"])[columns].sum().unstack(fill_value=0)

    # pylint: disable=consider-using-f-string
    stats.columns = [
        "{}_{}".format(metric, "outlier" if outlier else "inlier") for metric, outlier in stats.columns.values
    ]

    required_columns = map("_".join, product(columns, ["inlier", "outlier"]))
    for col in required_columns:
        if col not in stats:
            stats[col] = 0
    return stats
