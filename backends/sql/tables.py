from enum import Enum
from typing import Optional
import datetime as dt

import sqlalchemy as sa
from sqlalchemy.sql.sqltypes import String
from sqlalchemy_utils import IPAddressType

from gitential2.datatypes import WorkspaceRole
from gitential2.datatypes.subscriptions import SubscriptionType
from gitential2.datatypes.repositories import GitProtocol
from gitential2.datatypes.extraction import Langtype

metadata = sa.MetaData()


class MaterializedViewNames(str, Enum):
    commits_v = "commits_v"
    patches_v = "patches_v"
    pull_requests_v = "pull_requests_v"
    pull_request_comments_v = "pull_request_comments_v"


class WorkspaceTableNames(str, Enum):
    projects = "projects"
    repositories = "repositories"
    its_projects = "its_projects"
    project_repositories = "project_repositories"
    project_its_projects = "project_its_projects"
    extracted_commits = "extracted_commits"
    dashboards = "dashboards"
    charts = "charts"
    thumbnails = "thumbnails"
    calculated_commits = "calculated_commits"
    extracted_patches = "extracted_patches"
    calculated_patches = "calculated_patches"
    extracted_patch_rewrites = "extracted_patch_rewrites"
    authors = "authors"
    teams = "teams"
    team_members = "team_members"
    pull_requests = "pull_requests"
    pull_request_commits = "pull_request_commits"
    pull_request_comments = "pull_request_comments"
    pull_request_labels = "pull_request_labels"
    extracted_commit_branches = "extracted_commit_branches"
    its_issues = "its_issues"
    its_issue_changes = "its_issue_changes"
    its_issue_times_in_statuses = "its_issue_times_in_statuses"
    its_issue_comments = "its_issue_comments"
    its_issue_linked_issues = "its_issue_linked_issues"
    its_sprints = "its_sprints"
    its_issue_sprints = "its_issue_sprints"
    its_issue_worklogs = "its_issue_worklogs"
    deploys = "deploys"
    deploy_commits = "deploy_commits"


schema_revisions_table = sa.Table(
    "schema_revisions",
    metadata,
    sa.Column("id", sa.String(32), primary_key=True),
    sa.Column("revision_id", sa.String(32), nullable=False),
)

users_table = sa.Table(
    "users",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("login", sa.String(128), nullable=True),
    sa.Column("email", sa.String(256), nullable=True),
    sa.Column("is_admin", sa.Boolean, default=False, nullable=False),
    sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    sa.Column("marketing_consent_accepted", sa.Boolean, nullable=False, default=False),
    sa.Column("first_name", sa.String(256), nullable=True),
    sa.Column("last_name", sa.String(256), nullable=True),
    sa.Column("company_name", sa.String(256), nullable=True),
    sa.Column("position", sa.String(256), nullable=True),
    sa.Column("development_team_size", sa.String(256), nullable=True),
    sa.Column("registration_ready", sa.Boolean, default=False, nullable=False),
    sa.Column("login_ready", sa.Boolean, default=False, nullable=False),
    sa.Column("is_active", sa.Boolean, default=False, nullable=False),
    sa.Column("extra", sa.JSON, nullable=True),
    sa.Column("stripe_customer_id", sa.String(256), nullable=True),
)

personal_access_tokens_table = sa.Table(
    "personal_access_tokens",
    metadata,
    sa.Column("id", sa.String(128), primary_key=True),
    sa.Column("user_id", sa.Integer(), nullable=False),
    sa.Column("name", sa.String(256), nullable=False),
    sa.Column("expire_at", sa.DateTime, nullable=True),
    sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    sa.Column("extra", sa.JSON, nullable=True),
)

workspace_api_keys_table = sa.Table(
    "workspace_api_keys",
    metadata,
    sa.Column("id", sa.String(128), primary_key=True),
    sa.Column("workspace_id", sa.Integer(), nullable=False),
    sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    sa.Column("extra", sa.JSON, nullable=True),
)

reseller_codes_table = sa.Table(
    "reseller_codes",
    metadata,
    sa.Column("id", sa.String(32), primary_key=True),
    sa.Column("reseller_id", sa.String(64)),
    sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    sa.Column("expire_at", sa.DateTime, nullable=True),
    sa.Column("user_id", sa.Integer(), nullable=True),
)

access_log_table = sa.Table(
    "access_log",
    metadata,
    sa.Column("log_time", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    sa.Column("ip_address", IPAddressType, nullable=True),
    sa.Column("user_id", sa.Integer, nullable=False),
    sa.Column("path", sa.String(256), nullable=False),
    sa.Column("method", sa.String(16), nullable=False),
    sa.Column("extra", sa.JSON, nullable=True),
    sa.Index("idx_user_id", "user_id"),
    sa.Index("idx_log_time_user_id", "log_time", "user_id"),
)

access_approvals_table = sa.Table(
    "access_approvals",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    sa.Column("user_id", sa.Integer, nullable=False),
    sa.Column("is_approved", sa.Boolean, default=True, nullable=False),
    sa.Column("approved_by", sa.Integer, nullable=False),
    sa.Column("extra", sa.JSON, nullable=True),
    sa.Index("access_approvals_user_id_idx", "user_id"),
)

subscriptions_table = sa.Table(
    "subscriptions",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column(
        "user_id",
        sa.Integer,
        sa.ForeignKey("users.id"),
        nullable=False,
    ),
    sa.Column("subscription_start", sa.DateTime, nullable=False),
    sa.Column("subscription_end", sa.DateTime, nullable=True),
    sa.Column("subscription_type", sa.Enum(SubscriptionType), default=SubscriptionType.trial),
    sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    sa.Column("number_of_developers", sa.Integer(), nullable=False, default=5),
    sa.Column("stripe_subscription_id", sa.String(256), nullable=True),
    sa.Column("features", sa.JSON, nullable=True),
)

user_infos_table = sa.Table(
    "user_infos",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column(
        "user_id",
        sa.Integer,
        sa.ForeignKey("users.id"),
        nullable=False,
    ),
    sa.Column("integration_name", sa.String(128), nullable=False),
    sa.Column("integration_type", sa.String(128), nullable=False),
    sa.Column("sub", sa.String(128), nullable=False),
    sa.Column("name", sa.String(128), nullable=True),
    sa.Column("email", sa.String(128), nullable=True),
    sa.Column("preferred_username", sa.String(128), nullable=True),
    sa.Column("profile", sa.String(256), nullable=True),
    sa.Column("picture", sa.String(256), nullable=True),
    sa.Column("website", sa.String(256), nullable=True),
    sa.Column("extra", sa.JSON, nullable=True),
    sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
)

credentials_table = sa.Table(
    "credentials",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("owner_id", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
    sa.Column("type", sa.String(32), nullable=False),
    sa.Column("integration_name", sa.String(128), nullable=True),
    sa.Column("integration_type", sa.String(128), nullable=True),
    sa.Column("name", sa.String(128), nullable=True),
    sa.Column("token", sa.LargeBinary, nullable=True),
    sa.Column("refresh_token", sa.LargeBinary, nullable=True),
    sa.Column("public_key", sa.LargeBinary, nullable=True),
    sa.Column("private_key", sa.LargeBinary, nullable=True),
    sa.Column("passphrase", sa.LargeBinary, nullable=True),
    sa.Column("expires_at", sa.DateTime, nullable=True),
    sa.Column("extra", sa.JSON, nullable=True),
    sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
)

workspaces_table = sa.Table(
    "workspaces",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("name", sa.String(128), nullable=True),
    sa.Column("extra", sa.JSON, nullable=True),
    sa.Column("created_by", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
    sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
)

workspace_members_table = sa.Table(
    "workspace_members",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("user_id", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
    sa.Column("workspace_id", sa.Integer, sa.ForeignKey("workspaces.id"), nullable=False),
    sa.Column("role", sa.Enum(WorkspaceRole), default=WorkspaceRole.owner),
    sa.Column("primary", sa.Boolean, default=False),
    sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
)

workspace_invitations_table = sa.Table(
    "workspace_invitations",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("invitation_by", sa.Integer, nullable=True),
    sa.Column("workspace_id", sa.Integer, sa.ForeignKey("workspaces.id"), nullable=False),
    sa.Column("email", sa.String(128)),
    sa.Column("invitation_code", sa.String(128)),
    sa.Column("status", sa.String(128)),
    sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
)

email_log_table = sa.Table(
    "email_log",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("user_id", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
    sa.Column("template_name", sa.String, nullable=False),
    sa.Column("status", sa.String, default="scheduled", nullable=False),
    sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    sa.Column("scheduled_at", sa.DateTime, nullable=False),
    sa.Column("sent_at", sa.DateTime, nullable=True),
)

auto_export_table = sa.Table(
    "auto_export",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("workspace_id", sa.Integer, sa.ForeignKey("workspaces.id"), nullable=False, unique=True),
    sa.Column("emails", sa.JSON, nullable=True),
    sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    sa.Column("extra", sa.JSON, nullable=True),
)

user_repositories_cache_table = sa.Table(
    "user_repositories_cache",
    metadata,
    sa.Column("user_id", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
    sa.Column("repo_provider_id", sa.String(256), nullable=False, unique=False),
    sa.Column("clone_url", sa.String(256), nullable=False, unique=False),
    sa.Column("protocol", sa.Enum(GitProtocol), default=GitProtocol.https),
    sa.Column("name", sa.String(128)),
    sa.Column("namespace", sa.String(128)),
    sa.Column("private", sa.Boolean, nullable=False, default=True),
    sa.Column("integration_type", sa.String(64), nullable=True),
    sa.Column("integration_name", sa.String(64), nullable=True),
    sa.Column("credential_id", sa.Integer, nullable=True),
    sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    sa.Column("extra", sa.JSON, nullable=True),
    sa.PrimaryKeyConstraint("user_id", "repo_provider_id", "integration_type"),
)


user_its_projects_cache_table = sa.Table(
    "user_its_projects_cache",
    metadata,
    sa.Column("user_id", sa.Integer, sa.ForeignKey("users.id"), nullable=False),
    sa.Column("api_url", sa.String(256), nullable=False, unique=False),
    sa.Column("name", sa.String(128)),
    sa.Column("namespace", sa.String(128)),
    sa.Column("private", sa.Boolean, nullable=False, default=True),
    sa.Column("key", sa.String(128), nullable=True),
    sa.Column("integration_type", sa.String(128), nullable=False),
    sa.Column("integration_name", sa.String(128), nullable=False),
    sa.Column("integration_id", sa.String(128), nullable=False),
    sa.Column("credential_id", sa.Integer, nullable=True),
    sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    sa.Column("extra", sa.JSON, nullable=True),
    sa.PrimaryKeyConstraint("user_id", "integration_id", "integration_type"),
)


# pylint: disable=unused-variable,too-many-locals
def get_workspace_metadata(schema: Optional[str] = None):
    metadata = sa.MetaData(schema=schema)

    projects = sa.Table(
        "projects",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String(128), nullable=True),
        sa.Column("pattern", sa.String(256), nullable=True),
        sa.Column("shareable", sa.Boolean, default=False, nullable=False),
        sa.Column("sprints_enabled", sa.Boolean, default=False),
        sa.Column("sprint", sa.JSON, nullable=True),
        sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.Column("extra", sa.JSON, nullable=True),
    )

    repositories = sa.Table(
        "repositories",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("clone_url", sa.String(256), nullable=False, unique=True),
        sa.Column("protocol", sa.Enum(GitProtocol), default=GitProtocol.https),
        sa.Column("name", sa.String(128)),
        sa.Column("namespace", sa.String(128)),
        sa.Column("private", sa.Boolean, nullable=False, default=True),
        sa.Column("integration_type", sa.String(64), nullable=True),
        sa.Column("integration_name", sa.String(64), nullable=True),
        sa.Column("credential_id", sa.Integer, nullable=True),
        sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.Column("extra", sa.JSON, nullable=True),
    )

    its_projects = sa.Table(
        "its_projects",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("api_url", sa.String(256), nullable=False, unique=True),
        sa.Column("name", sa.String(128)),
        sa.Column("namespace", sa.String(128)),
        sa.Column("private", sa.Boolean, nullable=False, default=True),
        sa.Column("key", sa.String(128), nullable=True),
        sa.Column("integration_type", sa.String(128), nullable=False),
        sa.Column("integration_name", sa.String(128), nullable=False),
        sa.Column("integration_id", sa.String(128), nullable=False),
        sa.Column("credential_id", sa.Integer, nullable=True),
        sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.Column("extra", sa.JSON, nullable=True),
    )

    project_repositories = sa.Table(
        "project_repositories",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("project_id", sa.Integer, sa.ForeignKey("projects.id"), nullable=False),
        sa.Column("repo_id", sa.Integer, sa.ForeignKey("repositories.id"), nullable=False),
    )

    project_its_projects = sa.Table(
        "project_its_projects",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("project_id", sa.Integer, sa.ForeignKey("projects.id"), nullable=False),
        sa.Column("itsp_id", sa.Integer, sa.ForeignKey("its_projects.id"), nullable=False),
    )

    dashboards = sa.Table(
        "dashboards",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("title", sa.String(128), nullable=False),
        sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.Column("filters", sa.JSON, nullable=False),
        sa.Column("charts", sa.JSON, nullable=False),
        sa.Column("extra", sa.JSON, nullable=True),
    )

    charts = sa.Table(
        "charts",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.Column("extra", sa.JSON, nullable=True),
        sa.Column("is_custom", sa.Boolean, default=True, nullable=False),
        sa.Column("title", sa.String(128), nullable=False),
        sa.Column("chart_type", sa.String(128), nullable=False),
        sa.Column("layout", sa.JSON, nullable=False),
        sa.Column("metrics", sa.JSON, nullable=False),
        sa.Column("dimensions", sa.JSON, nullable=False),
        sa.Column("filters", sa.JSON, nullable=True),
    )

    thumbnails = sa.Table(
        "thumbnails",
        metadata,
        sa.Column("id", sa.String(128), primary_key=True),
        sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.Column("extra", sa.JSON, nullable=True),
        sa.Column("image", sa.Text),
    )

    # Extracted Commits
    extracted_commits = sa.Table(
        "extracted_commits",
        metadata,
        sa.Column("repo_id", sa.Integer()),
        sa.Column("commit_id", sa.String(40)),
        sa.Column("atime", sa.DateTime()),
        sa.Column("aemail", sa.String(128)),
        sa.Column("aname", sa.String(128)),
        sa.Column("ctime", sa.DateTime()),
        sa.Column("cemail", sa.String(128)),
        sa.Column("cname", sa.String(128)),
        sa.Column("message", sa.Text()),
        sa.Column("nparents", sa.Integer()),
        sa.Column("tree_id", sa.String(40)),
        sa.PrimaryKeyConstraint("repo_id", "commit_id"),
    )

    calculated_commits = sa.Table(
        "calculated_commits",
        metadata,
        # same columns from extracted_commits
        sa.Column("repo_id", sa.Integer()),
        sa.Column("commit_id", sa.String(40)),
        sa.Column("atime", sa.DateTime()),
        sa.Column("aemail", sa.String(128)),
        sa.Column("aname", sa.String(128)),
        sa.Column("ctime", sa.DateTime()),
        sa.Column("cemail", sa.String(128)),
        sa.Column("cname", sa.String(128)),
        sa.Column("message", sa.Text()),
        sa.Column("nparents", sa.Integer()),
        sa.Column("tree_id", sa.String(40)),
        # additional column - date
        sa.Column("date", sa.DateTime()),
        sa.Column("age", sa.Integer(), nullable=True),
        # author ids
        sa.Column("aid", sa.Integer()),
        sa.Column("cid", sa.Integer()),
        # is_merge, is_test
        sa.Column("is_merge", sa.Boolean),
        # sa.Column("is_test", sa.Boolean),
        # number of patches
        sa.Column("nfiles", sa.Integer(), nullable=True),
        # calculated from patch, outlier
        sa.Column("loc_i_c", sa.Integer(), nullable=True),
        sa.Column("loc_i_inlier", sa.Integer(), nullable=True),
        sa.Column("loc_i_outlier", sa.Integer(), nullable=True),
        sa.Column("loc_d_c", sa.Integer(), nullable=True),
        sa.Column("loc_d_inlier", sa.Integer(), nullable=True),
        sa.Column("loc_d_outlier", sa.Integer(), nullable=True),
        sa.Column("comp_i_c", sa.Integer(), nullable=True),
        sa.Column("comp_i_inlier", sa.Integer(), nullable=True),
        sa.Column("comp_i_outlier", sa.Integer(), nullable=True),
        sa.Column("comp_d_c", sa.Integer(), nullable=True),
        sa.Column("comp_d_inlier", sa.Integer(), nullable=True),
        sa.Column("comp_d_outlier", sa.Integer(), nullable=True),
        sa.Column("loc_effort_c", sa.Integer(), nullable=True),
        sa.Column("uploc_c", sa.Integer(), default=0, comment="unproductive line of code"),
        sa.Column("is_bugfix", sa.Boolean(), default=None),
        sa.Column("is_pr_exists", sa.Boolean(), default=None),
        sa.Column("is_pr_open", sa.Boolean(), default=None),
        sa.Column("is_pr_closed", sa.Boolean(), default=None),
        # work hour estimation
        sa.Column("hours_measured", sa.Float(), nullable=True),
        sa.Column("hours_estimated", sa.Float(), nullable=True),
        sa.Column("hours", sa.Float(), nullable=True),
        sa.Column("velocity_measured", sa.Float(), nullable=True),
        sa.Column("velocity", sa.Float(), nullable=True),
        # primary key
        sa.PrimaryKeyConstraint("repo_id", "commit_id"),
        sa.Index("idx_repo_id", "repo_id"),
        sa.Index("idx_is_merge", "is_merge"),
        sa.Index("calculated_commits_date_idx", "date"),
    )

    # Extracted Patches
    extracted_patches = sa.Table(
        "extracted_patches",
        metadata,
        sa.Column("repo_id", sa.Integer()),
        sa.Column("commit_id", sa.String(40)),
        sa.Column("parent_commit_id", sa.String(40)),
        sa.Column("status", sa.String(128)),
        sa.Column("newpath", sa.String(256)),
        sa.Column("oldpath", sa.String(256)),
        sa.Column("newsize", sa.Integer()),
        sa.Column("oldsize", sa.Integer()),
        sa.Column("is_binary", sa.Boolean()),
        sa.Column("lang", sa.String(32)),
        sa.Column("langtype", sa.Enum(Langtype)),
        # Extracted plain metrics
        sa.Column("loc_i", sa.Integer()),
        sa.Column("loc_d", sa.Integer()),
        sa.Column("comp_i", sa.Integer()),
        sa.Column("comp_d", sa.Integer()),
        sa.Column("loc_i_std", sa.Integer()),
        sa.Column("loc_d_std", sa.Integer()),
        sa.Column("comp_i_std", sa.Integer()),
        sa.Column("comp_d_std", sa.Integer()),
        sa.Column("nhunks", sa.Integer()),
        sa.Column("nrewrites", sa.Integer()),
        sa.Column("rewrites_loc", sa.Integer()),
        sa.PrimaryKeyConstraint("repo_id", "commit_id", "parent_commit_id", "newpath"),
    )

    calculated_patches = sa.Table(
        "calculated_patches",
        metadata,
        sa.Column("repo_id", sa.Integer()),
        sa.Column("commit_id", sa.String(40)),
        sa.Column("parent_commit_id", sa.String(40), nullable=True),
        # author ids
        sa.Column("aid", sa.Integer()),
        sa.Column("cid", sa.Integer()),
        # atime -> date
        sa.Column("date", sa.DateTime()),
        sa.Column("status", sa.String(128)),
        sa.Column("newpath", sa.String(256)),
        sa.Column("oldpath", sa.String(256)),
        sa.Column("newsize", sa.Integer()),
        sa.Column("oldsize", sa.Integer()),
        sa.Column("is_binary", sa.Boolean()),
        sa.Column("lang", sa.String(32)),
        sa.Column("langtype", sa.Enum(Langtype)),
        sa.Column("loc_i", sa.Integer()),
        sa.Column("loc_d", sa.Integer()),
        sa.Column("comp_i", sa.Integer()),
        sa.Column("comp_d", sa.Integer()),
        sa.Column("nhunks", sa.Integer()),
        sa.Column("nrewrites", sa.Integer()),
        sa.Column("rewrites_loc", sa.Integer()),
        # calculated
        sa.Column("is_merge", sa.Boolean()),
        sa.Column("is_test", sa.Boolean()),
        sa.Column("uploc", sa.Integer()),
        sa.Column("outlier", sa.Integer()),
        sa.Column("anomaly", sa.Integer()),
        sa.Column("loc_effort_p", sa.Integer(), nullable=True),
        sa.Column("is_collaboration", sa.Boolean()),
        sa.Column("is_new_code", sa.Boolean()),
        sa.Column("is_bugfix", sa.Boolean(), default=None),
        sa.PrimaryKeyConstraint("repo_id", "commit_id", "parent_commit_id", "newpath"),
        sa.Index("idx_repo_id_commit_id", "repo_id", "commit_id"),
        sa.Index("idx_lang", "lang"),
        sa.Index("calculated_patches_date_idx", "date"),
    )

    # Extracted Patch Rewrites
    extracted_patch_rewrites = sa.Table(
        "extracted_patch_rewrites",
        metadata,
        sa.Column("repo_id", sa.Integer()),
        sa.Column("commit_id", sa.String(40)),
        sa.Column("atime", sa.DateTime()),
        sa.Column("aemail", sa.String(128)),
        sa.Column("newpath", sa.String(256)),
        sa.Column("rewritten_atime", sa.DateTime()),
        sa.Column("rewritten_aemail", sa.String(128)),
        sa.Column("rewritten_commit_id", sa.String(40)),
        sa.Column("loc_d", sa.Integer()),
        sa.PrimaryKeyConstraint("repo_id", "commit_id", "rewritten_commit_id", "newpath"),
    )

    authors = sa.Table(
        "authors",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("active", sa.Boolean),
        sa.Column("name", sa.String(256), nullable=True),
        sa.Column("email", sa.String(256), nullable=True),
        sa.Column("aliases", sa.JSON, nullable=True),
        sa.Column("extra", sa.JSON, nullable=True),
    )

    teams = sa.Table(
        "teams",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String(256), nullable=True),
        sa.Column("sprints_enabled", sa.Boolean, default=False),
        sa.Column("sprint", sa.JSON, nullable=True),
        sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    )

    team_members = sa.Table(
        "team_members",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("team_id", sa.Integer, sa.ForeignKey("teams.id"), nullable=False),
        sa.Column("author_id", sa.Integer, sa.ForeignKey("authors.id"), nullable=False),
    )

    # sprints = sa.Table(
    #     "sprints",
    #     metadata,
    #     sa.Column("id", sa.Integer, primary_key=True),
    #     sa.Column("team_id", sa.Integer, sa.ForeignKey("teams.id"), nullable=False),
    #     sa.Column("date", sa.DateTime, nullable=False),
    #     sa.Column("weeks", sa.Integer, default=1),
    #     sa.Column("pattern", sa.String(64)),
    #     sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    #     sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    # )

    # Pull Requests
    pull_requests = sa.Table(
        "pull_requests",
        metadata,
        sa.Column("repo_id", sa.Integer()),
        sa.Column("number", sa.Integer()),
        sa.Column("title", sa.String(256)),
        sa.Column("platform", sa.String(32)),
        sa.Column("id_platform", sa.Integer()),
        sa.Column("api_resource_uri", sa.String(256)),
        sa.Column("state_platform", sa.String(16)),
        sa.Column("state", sa.String(16)),
        sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.Column("closed_at", sa.DateTime, nullable=True),
        sa.Column("updated_at", sa.DateTime, nullable=True),
        sa.Column("merged_at", sa.DateTime, nullable=True),
        sa.Column("additions", sa.Integer(), nullable=True),
        sa.Column("deletions", sa.Integer(), nullable=True),
        sa.Column("changed_files", sa.Integer(), nullable=True),
        sa.Column("draft", sa.Boolean, default=False, nullable=False),
        sa.Column("user", sa.String(64)),  # NOT USED ANYMORE
        # user related fieds
        sa.Column("user_id_external", sa.String(64), nullable=True),
        sa.Column("user_name_external", sa.String(128), nullable=True),
        sa.Column("user_username_external", sa.String(128), nullable=True),
        sa.Column("user_aid", sa.Integer(), nullable=True),
        # number of commits
        sa.Column("commits", sa.Integer(), nullable=True),
        # merged_by who?
        sa.Column("merged_by", sa.String(64), nullable=True),  # NOT USED ANYMORE
        sa.Column("merged_by_id_external", sa.String(64), nullable=True),
        sa.Column("merged_by_name_external", sa.String(128), nullable=True),
        sa.Column("merged_by_username_external", sa.String(128), nullable=True),
        sa.Column("merged_by_aid", sa.Integer(), nullable=True),
        # calculated fields
        sa.Column("first_reaction_at", sa.DateTime, nullable=True),
        sa.Column("first_commit_authored_at", sa.DateTime, nullable=True),
        sa.Column("extra", sa.JSON, nullable=True),
        sa.Column("is_bugfix", sa.Boolean(), default=None),
        sa.PrimaryKeyConstraint("repo_id", "number"),
    )

    pull_request_commits = sa.Table(
        "pull_request_commits",
        metadata,
        sa.Column("repo_id", sa.Integer()),
        sa.Column("pr_number", sa.Integer()),
        sa.Column("commit_id", sa.String(40)),
        sa.Column("author_name", sa.String(128)),
        sa.Column("author_email", sa.String(128)),
        sa.Column("author_login", sa.String(128), nullable=True),
        sa.Column("author_date", sa.DateTime, nullable=False),
        sa.Column("committer_name", sa.String(128)),
        sa.Column("committer_email", sa.String(128)),
        sa.Column("committer_login", sa.String(128), nullable=True),
        sa.Column("committer_date", sa.DateTime, nullable=False),
        sa.Column("extra", sa.JSON, nullable=True),
        sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.PrimaryKeyConstraint("repo_id", "pr_number", "commit_id"),
    )

    pull_request_comments = sa.Table(
        "pull_request_comments",
        metadata,
        sa.Column("repo_id", sa.Integer()),
        sa.Column("pr_number", sa.Integer()),
        sa.Column("comment_type", sa.String(32)),
        sa.Column("comment_id", String(32)),
        sa.Column("author_id_external", sa.String(64), nullable=True),
        sa.Column("author_name_external", sa.String(128), nullable=True),
        sa.Column("author_username_external", sa.String(128), nullable=True),
        sa.Column("author_aid", sa.Integer(), nullable=True),
        sa.Column("published_at", sa.DateTime, nullable=True),
        sa.Column("content", sa.String()),
        sa.Column("parent_comment_id", String(32)),
        sa.Column("thread_id", String(32)),
        sa.Column("review_id", String(32)),
        sa.Column("extra", sa.JSON, nullable=True),
        sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.PrimaryKeyConstraint("repo_id", "pr_number", "comment_type", "comment_id"),
    )

    pull_request_labels = sa.Table(
        "pull_request_labels",
        metadata,
        sa.Column("repo_id", sa.Integer()),
        sa.Column("pr_number", sa.Integer()),
        sa.Column("name", sa.String(64)),
        sa.Column("color", sa.String(16), nullable=True),
        sa.Column("description", sa.String(128), nullable=True),
        sa.Column("active", sa.Boolean(), default=True),
        sa.Column("extra", sa.JSON, nullable=True),
        sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.PrimaryKeyConstraint("repo_id", "pr_number", "name"),
    )

    extracted_commit_branches = sa.Table(
        "extracted_commit_branches",
        metadata,
        sa.Column("repo_id", sa.Integer()),
        sa.Column("commit_id", sa.String(40)),
        sa.Column("atime", sa.DateTime()),
        sa.Column("branch", sa.String(1000)),
        sa.PrimaryKeyConstraint("repo_id", "commit_id", "branch"),
    )

    its_issues = sa.Table(
        "its_issues",
        metadata,
        sa.Column("id", sa.String(128), primary_key=True),
        sa.Column("itsp_id", sa.Integer(), nullable=False),
        sa.Column("api_url", sa.String(256)),
        sa.Column("api_id", sa.String(128)),
        sa.Column("key", sa.String(64)),
        # status fields
        sa.Column("status_name", sa.String(64)),
        sa.Column("status_id", sa.String(64)),
        sa.Column("status_category_api", sa.String(32)),
        sa.Column("status_category", sa.String(16)),
        # issue types and parent
        sa.Column("issue_type_name", sa.String(48)),
        sa.Column("issue_type_id", sa.String(48)),
        sa.Column("parent_id", sa.String(128)),
        # resolution fields
        sa.Column("resolution_name", sa.String(32)),
        sa.Column("resolution_id", sa.String(48)),
        sa.Column("resolution_date", sa.DateTime, default=dt.datetime.utcnow, nullable=True),
        # priority fields
        sa.Column("priority_name", sa.String(32)),
        sa.Column("priority_id", sa.String(48)),
        sa.Column("priority_order", sa.Integer(), nullable=True),
        # summary and description
        sa.Column("summary", sa.String(256)),
        sa.Column("description", sa.String()),
        # creator
        sa.Column("creator_api_id", sa.String(128)),
        sa.Column("creator_email", sa.String(128)),
        sa.Column("creator_name", sa.String(128)),
        sa.Column("creator_dev_id", sa.Integer()),
        # reporter
        sa.Column("reporter_api_id", sa.String(128)),
        sa.Column("reporter_email", sa.String(128)),
        sa.Column("reporter_name", sa.String(128)),
        sa.Column("reporter_dev_id", sa.Integer()),
        # assignee
        sa.Column("assignee_api_id", sa.String(128)),
        sa.Column("assignee_email", sa.String(128)),
        sa.Column("assignee_name", sa.String(128)),
        sa.Column("assignee_dev_id", sa.Integer()),
        # labels
        sa.Column("labels", sa.JSON, nullable=True),
        # calculated fields
        sa.Column("is_started", sa.Boolean()),
        sa.Column("started_at", sa.DateTime, nullable=True),
        sa.Column("is_closed", sa.Boolean()),
        sa.Column("closed_at", sa.DateTime, nullable=True),
        sa.Column("comment_count", sa.Integer()),
        sa.Column("last_comment_at", sa.DateTime, nullable=True),
        sa.Column("change_count", sa.Integer()),
        sa.Column("last_change_at", sa.DateTime, nullable=True),
        sa.Column("story_points", sa.Integer(), nullable=True),
        # extra, created_at, updated_at
        sa.Column("extra", sa.JSON, nullable=True),
        sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    )

    its_issue_changes = sa.Table(
        "its_issue_changes",
        metadata,
        sa.Column("id", sa.String(128), primary_key=True),
        # relations
        sa.Column("issue_id", sa.String(128), nullable=False),  # todo foreign key
        sa.Column("itsp_id", sa.Integer(), nullable=False),
        sa.Column("api_id", sa.String(128)),
        # author
        sa.Column("author_api_id", sa.String(128)),
        sa.Column("author_email", sa.String(128)),
        sa.Column("author_name", sa.String(128)),
        sa.Column("author_dev_id", sa.Integer()),
        # field & meta
        sa.Column("field_name", sa.String(64)),
        sa.Column("field_id", sa.String(32)),
        sa.Column("field_type", sa.String(32)),
        sa.Column("change_type", sa.String(16)),
        # values
        sa.Column("v_from", sa.String()),
        sa.Column("v_from_string", sa.String()),
        sa.Column("v_to", sa.String()),
        sa.Column("v_to_string", sa.String()),
        # extra, created_at, updated_at
        sa.Column("extra", sa.JSON, nullable=True),
        sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    )

    its_issue_times_in_statuses = sa.Table(
        "its_issue_times_in_statuses",
        metadata,
        sa.Column("id", sa.String(128), primary_key=True),
        # relations
        sa.Column("issue_id", sa.String(128), nullable=False),  # todo foreign key
        sa.Column("itsp_id", sa.Integer(), nullable=False),
        # status fields
        sa.Column("status_name", sa.String(64)),
        sa.Column("status_id", sa.String(64)),
        sa.Column("status_category_api", sa.String(16)),
        sa.Column("status_category", sa.String(16)),
        # started, ended
        sa.Column("started_at", sa.DateTime, nullable=False),
        sa.Column("started_issue_change_id", sa.String(128), nullable=True),
        sa.Column("ended_at", sa.DateTime, nullable=False),
        sa.Column("ended_issue_change_id", sa.String(128), nullable=True),
        sa.Column("ended_with_status_name", sa.String(64)),
        sa.Column("ended_with_status_id", sa.String(64)),
        sa.Column("seconds_in_status", sa.Integer()),
        # extra, created_at, updated_at
        sa.Column("extra", sa.JSON, nullable=True),
        sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    )

    its_issue_comments = sa.Table(
        "its_issue_comments",
        metadata,
        sa.Column("id", sa.String(128), primary_key=True),
        # relations
        sa.Column("issue_id", sa.String(128), nullable=False),  # todo foreign key
        sa.Column("itsp_id", sa.Integer(), nullable=False),
        # author
        sa.Column("author_api_id", sa.String(128)),
        sa.Column("author_email", sa.String(128)),
        sa.Column("author_name", sa.String(128)),
        sa.Column("author_dev_id", sa.Integer()),
        # comment
        sa.Column("comment", sa.String()),
        # extra, created_at, updated_at
        sa.Column("extra", sa.JSON, nullable=True),
        sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    )

    its_issue_linked_issues = sa.Table(
        "its_issue_linked_issues",
        metadata,
        sa.Column("id", sa.String(128), primary_key=True),
        sa.Column("itsp_id", sa.Integer(), nullable=False),
        sa.Column("issue_id", sa.String(128), nullable=False),
        sa.Column("issue_api_id", sa.String(128), nullable=False),
        sa.Column("issue_key", sa.String(128), nullable=True),
        sa.Column("linked_issue_api_id", sa.String(128), nullable=False),
        sa.Column("linked_issue_key", sa.String(128), nullable=True),
        sa.Column("link_type", sa.String(128), nullable=False),
        sa.Column("extra", sa.JSON, nullable=True),
    )

    its_sprints = sa.Table(
        "its_sprints",
        metadata,
        sa.Column("id", sa.String(128), primary_key=True),
        sa.Column("itsp_id", sa.Integer(), nullable=False),
        sa.Column("api_id", sa.String(128)),
        sa.Column("name", sa.String(128)),
        sa.Column("state", sa.String(64)),
        sa.Column("started_at", sa.DateTime, nullable=True),
        sa.Column("ended_at", sa.DateTime, nullable=True),
        sa.Column("completed_at", sa.DateTime, nullable=True),
        sa.Column("goal", sa.String()),
        sa.Column("extra", sa.JSON, nullable=True),
    )

    its_issue_sprints = sa.Table(
        "its_issue_sprints",
        metadata,
        sa.Column("id", sa.String(128), primary_key=True),
        sa.Column("itsp_id", sa.Integer(), nullable=False),
        sa.Column("issue_id", sa.String(128), nullable=False),
        sa.Column("sprint_id", sa.String(128), nullable=False),
    )

    its_issue_worklogs = sa.Table(
        "its_issue_worklogs",
        metadata,
        sa.Column("id", sa.String(128), primary_key=True),
        sa.Column("api_id", sa.String(128), nullable=False),
        sa.Column("issue_id", sa.String(128), nullable=False),
        sa.Column("itsp_id", sa.Integer(), nullable=False),
        # author
        sa.Column("author_api_id", sa.String(128)),
        sa.Column("author_email", sa.String(128)),
        sa.Column("author_name", sa.String(128)),
        sa.Column("author_dev_id", sa.Integer()),
        sa.Column("started_at", sa.DateTime, nullable=True),
        sa.Column("time_spent_seconds", sa.Integer()),
        sa.Column("time_spent_display_str", sa.String(32)),
        sa.Column("extra", sa.JSON, nullable=True),
        sa.Column("created_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
        sa.Column("updated_at", sa.DateTime, default=dt.datetime.utcnow, nullable=False),
    )

    deploys = sa.Table(
        "deploys",
        metadata,
        sa.Column("id", sa.String(128), primary_key=True),
        sa.Column("environments", sa.ARRAY(sa.String(length=256)), nullable=True),
        sa.Column("pull_requests", sa.JSON, nullable=True),
        sa.Column("commits", sa.JSON, nullable=True),
        sa.Column("issues", sa.JSON, nullable=True),
        sa.Column("deployed_at", sa.DateTime, nullable=False),
        sa.Column("extra", sa.JSON, nullable=True),
    )

    deploy_commits = sa.Table(
        "deploy_commits",
        metadata,
        sa.Column("id", sa.String(128), primary_key=True),
        sa.Column("deploy_id", sa.String(128), nullable=False),
        sa.Column("environment", sa.String(128), nullable=False),
        sa.Column("repo_id", sa.Integer(), nullable=False),
        sa.Column("repository_name", sa.String(128), nullable=False),
        sa.Column("commit_id", sa.String(128), nullable=False),
        sa.Column("deployed_at", sa.DateTime, nullable=False),
        sa.Column("authored_at", sa.DateTime, nullable=False),
        sa.Column("author_id", sa.Integer(), nullable=False),
    )

    tables = {
        WorkspaceTableNames.projects: projects,
        WorkspaceTableNames.repositories: repositories,
        WorkspaceTableNames.its_projects: its_projects,
        WorkspaceTableNames.project_repositories: project_repositories,
        WorkspaceTableNames.project_its_projects: project_its_projects,
        WorkspaceTableNames.extracted_commits: extracted_commits,
        WorkspaceTableNames.dashboards: dashboards,
        WorkspaceTableNames.charts: charts,
        WorkspaceTableNames.thumbnails: thumbnails,
        WorkspaceTableNames.calculated_commits: calculated_commits,
        WorkspaceTableNames.extracted_patches: extracted_patches,
        WorkspaceTableNames.calculated_patches: calculated_patches,
        WorkspaceTableNames.extracted_patch_rewrites: extracted_patch_rewrites,
        WorkspaceTableNames.authors: authors,
        WorkspaceTableNames.teams: teams,
        WorkspaceTableNames.team_members: team_members,
        WorkspaceTableNames.pull_requests: pull_requests,
        WorkspaceTableNames.pull_request_commits: pull_request_commits,
        WorkspaceTableNames.pull_request_comments: pull_request_comments,
        WorkspaceTableNames.pull_request_labels: pull_request_labels,
        WorkspaceTableNames.extracted_commit_branches: extracted_commit_branches,
        # its data tables
        WorkspaceTableNames.its_issues: its_issues,
        WorkspaceTableNames.its_issue_changes: its_issue_changes,
        WorkspaceTableNames.its_issue_times_in_statuses: its_issue_times_in_statuses,
        WorkspaceTableNames.its_issue_comments: its_issue_comments,
        WorkspaceTableNames.its_issue_linked_issues: its_issue_linked_issues,
        WorkspaceTableNames.its_sprints: its_sprints,
        WorkspaceTableNames.its_issue_sprints: its_issue_sprints,
        WorkspaceTableNames.its_issue_worklogs: its_issue_worklogs,
        WorkspaceTableNames.deploys: deploys,
        WorkspaceTableNames.deploy_commits: deploy_commits,
    }

    return metadata, tables
