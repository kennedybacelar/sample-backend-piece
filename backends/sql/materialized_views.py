def _drop_pull_requests_v(workspace_id):
    return f"""
DROP MATERIALIZED VIEW IF EXISTS ws_{workspace_id}.pull_requests_v;
"""


def _create_pull_requests_v(workspace_id):
    return f"""
CREATE MATERIALIZED VIEW IF NOT EXISTS ws_{workspace_id}.pull_requests_v
TABLESPACE pg_default
AS SELECT concat(pr.repo_id, '-', pr.number) AS pr_u_id,
    date(pr.created_at) AS created_at_date,
    date(pr.merged_at) AS merged_at_date,
    date(pr.updated_at) AS updated_at_date,
    date(pr.closed_at) AS closed_at_date,
    date(pr.first_reaction_at) AS first_reaction_at_date,
    date(pr.first_commit_authored_at) AS first_commit_authored_at_date,
    pr.created_at - pr.first_commit_authored_at AS development_time,
    date_part('epoch'::text, pr.created_at - pr.first_commit_authored_at) / 3600::double precision AS development_time_hours,
    date_part('epoch'::text, pr.created_at - pr.first_commit_authored_at) / 86400::double precision AS development_time_days,
    pr.first_reaction_at - pr.created_at AS pickup_time,
    date_part('epoch'::text, pr.first_reaction_at - pr.created_at) / 3600::double precision AS pickup_time_hours,
    date_part('epoch'::text, pr.first_reaction_at - pr.created_at) / 86400::double precision AS pickup_time_days,
    pr.merged_at - pr.first_reaction_at AS review_time,
    date_part('epoch'::text, pr.merged_at - pr.first_reaction_at) / 3600::double precision AS review_time_hours,
    date_part('epoch'::text, pr.merged_at - pr.first_reaction_at) / 86400::double precision AS review_time_days,
    pr.merged_at - pr.first_commit_authored_at AS cycle_time,
    date_part('epoch'::text, pr.merged_at - pr.first_commit_authored_at) / 3600::double precision AS cycle_time_hours,
    date_part('epoch'::text, pr.merged_at - pr.first_commit_authored_at) / 86400::double precision AS cycle_time_days,
    pr.repo_id,
    pr.number,
    pr.title,
    pr.platform,
    pr.id_platform,
    pr.api_resource_uri,
    pr.state_platform,
    pr.state,
    pr.created_at,
    pr.closed_at,
    pr.updated_at,
    pr.merged_at,
    pr.additions,
    pr.deletions,
    pr.changed_files,
    pr.draft,
    pr."user",
    pr.user_id_external,
    pr.user_name_external,
    pr.user_username_external,
    pr.user_aid,
    pr.commits,
    pr.merged_by,
    pr.merged_by_id_external,
    pr.merged_by_name_external,
    pr.merged_by_username_external,
    pr.merged_by_aid,
    pr.first_reaction_at,
    pr.first_commit_authored_at,
    pr.extra,
    pr.is_bugfix
   FROM ws_{workspace_id}.pull_requests pr
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX IF NOT EXISTS idx_pull_requests_v_id ON  ws_{workspace_id}.pull_requests_v USING btree (repo_id, number);
CREATE INDEX IF NOT EXISTS idx_pull_requests_v_author ON ws_{workspace_id}.pull_requests_v USING btree (user_aid);
CREATE INDEX IF NOT EXISTS idx_pull_requests_v_created_at_date ON ws_{workspace_id}.pull_requests_v USING btree (created_at_date);

"""


def _drop_pull_request_comments_v(workspace_id):
    return f"""
DROP MATERIALIZED VIEW IF EXISTS ws_{workspace_id}.pull_request_comments_v;
"""


def _create_pull_request_comments_v(workspace_id):
    return f"""
CREATE MATERIALIZED VIEW IF NOT EXISTS ws_{workspace_id}.pull_request_comments_v
TABLESPACE pg_default
AS SELECT concat(prc.repo_id, '-', prc.pr_number, '-', prc.comment_id) AS pr_comment_u_id,
    date(prc.created_at) AS created_at_date,
    date(prc.updated_at) AS updated_at_date,
    date(prc.published_at) AS published_at_date,
    prc.repo_id,
    prc.pr_number,
    prc.comment_type,
    prc.comment_id,
    prc.author_id_external,
    prc.author_name_external,
    prc.author_username_external,
    prc.author_aid,
    prc.published_at,
    prc.content,
    prc.parent_comment_id,
    prc.thread_id,
    prc.review_id,
    prc.extra,
    prc.created_at,
    prc.updated_at
   FROM ws_{workspace_id}.pull_request_comments prc
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_pull_request_comments_v_id ON  ws_{workspace_id}.pull_request_comments_v USING btree (repo_id, pr_number, comment_id);
"""


def _drop_patches_v(workspace_id):
    return f"""
DROP MATERIALIZED VIEW IF EXISTS ws_{workspace_id}.patches_v;
"""


def _create_patches_v(workspace_id):
    return f"""
CREATE MATERIALIZED VIEW IF NOT EXISTS ws_{workspace_id}.patches_v
TABLESPACE pg_default
AS SELECT concat(cp.repo_id, '-', cp.commit_id, '-', cp.newpath) AS patch_u_id,
    concat(cp.repo_id, '-', cp.commit_id) AS commit_u_id,
        CASE
            WHEN cp.is_test IS FALSE THEN cp.loc_i
            ELSE 0
        END AS loc_implemented,
        CASE
            WHEN cp.is_test IS TRUE THEN cp.loc_i
            ELSE 0
        END AS loc_test,
    date(cp.date) AS atime_date,
    cp.repo_id,
    cp.commit_id,
    cp.parent_commit_id,
    cp.aid,
    cp.cid,
    cp.date,
    cp.status,
    cp.newpath,
    cp.oldpath,
    cp.newsize,
    cp.oldsize,
    cp.is_binary,
    cp.lang,
    cp.langtype,
    cp.loc_i,
    cp.loc_d,
    cp.comp_i,
    cp.comp_d,
    cp.nhunks,
    cp.nrewrites,
    cp.rewrites_loc,
    cp.is_merge,
    cp.is_test,
    cp.uploc,
    cp.outlier,
    cp.anomaly,
    cp.loc_effort_p,
    cp.is_collaboration,
    cp.is_new_code
   FROM ws_{workspace_id}.calculated_patches cp
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX IF NOT EXISTS idx_patches_v_id ON ws_{workspace_id}.patches_v USING btree (repo_id, commit_id, parent_commit_id, newpath);
CREATE INDEX IF NOT EXISTS idx_patches_v_atime_date ON ws_{workspace_id}.patches_v USING btree (atime_date);
CREATE INDEX IF NOT EXISTS idx_patches_v_author ON ws_{workspace_id}.patches_v USING btree (aid);
"""


def _drop_commits_v(workspace_id):
    return f"""
DROP MATERIALIZED VIEW IF EXISTS ws_{workspace_id}.commits_v;
"""


def _create_commits_v(workspace_id):
    return f"""
CREATE MATERIALIZED VIEW IF NOT EXISTS ws_{workspace_id}.commits_v
TABLESPACE pg_default
AS SELECT concat(cc.repo_id, '-', cc.commit_id) AS commit_u_id,
    date(cc.atime) AS atime_date,
    date(cc.ctime) AS ctime_date,
        CASE
            WHEN cc.aid = cc.cid AND cc.aid IS NOT NULL THEN 1
            ELSE 0
        END AS is_author_commiter_same,
        CASE
            WHEN cc.loc_i_c IS NULL THEN 0
            WHEN cc.loc_i_c = 0 THEN 0
            WHEN cc.loc_i_c >= 100 AND (cc.loc_i_c / (cc.loc_i_c + cc.loc_d_c))::numeric > 0.3 THEN cc.loc_i_c
            WHEN cc.loc_i_c < 100 AND (cc.loc_i_c / (cc.loc_i_c + cc.loc_d_c))::numeric > 0.5 THEN cc.loc_i_c
            ELSE 0
        END AS loc_new_work,
    cc.repo_id,
    cc.commit_id,
    cc.atime,
    cc.aemail,
    cc.aname,
    cc.ctime,
    cc.cemail,
    cc.cname,
    cc.message,
    cc.nparents,
    cc.tree_id,
    cc.date,
    cc.age,
    cc.aid,
    cc.cid,
    cc.is_merge,
    cc.nfiles,
    cc.loc_i_c,
    cc.loc_i_inlier,
    cc.loc_i_outlier,
    cc.loc_d_c,
    cc.loc_d_inlier,
    cc.loc_d_outlier,
    cc.comp_i_c,
    cc.comp_i_inlier,
    cc.comp_i_outlier,
    cc.comp_d_c,
    cc.comp_d_inlier,
    cc.comp_d_outlier,
    cc.loc_effort_c,
    cc.uploc_c,
    cc.is_bugfix,
    cc.is_pr_exists,
    cc.is_pr_open,
    cc.is_pr_closed,
    cc.hours_measured,
    cc.hours_estimated,
    cc.hours,
    cc.velocity_measured,
    cc.velocity
   FROM ws_{workspace_id}.calculated_commits cc
WITH DATA;

-- View indexes:
CREATE UNIQUE INDEX IF NOT EXISTS idx_commits_v_id ON ws_{workspace_id}.commits_v USING btree (repo_id, commit_id);
CREATE INDEX IF NOT EXISTS idx_commits_v_atime_date ON ws_{workspace_id}.commits_v USING btree (atime_date);
CREATE INDEX IF NOT EXISTS idx_commits_v_author ON ws_{workspace_id}.commits_v USING btree (aid);

"""
