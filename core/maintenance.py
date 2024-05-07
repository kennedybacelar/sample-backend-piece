from datetime import datetime, timezone
from structlog import get_logger

from gitential2.datatypes.subscriptions import SubscriptionType
from gitential2.exceptions import InvalidStateException
from .context import GitentialContext
from .workspaces import get_workspace_subscription
from .tasks import schedule_task

logger = get_logger(__name__)


def maintenance(g: GitentialContext):
    def _dont_be_naive(dt: datetime):
        return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt

    def _is_pro_or_trial_subscription(workspace_id):
        subscription = get_workspace_subscription(g, workspace_id)
        logger.debug("Checking subscription for workspace", workspace_id=workspace_id, subscription=subscription)
        return (
            subscription.subscription_type in [SubscriptionType.professional, SubscriptionType.trial]
            and (_dont_be_naive(subscription.subscription_start) < g.current_time())
            and (
                (subscription.subscription_end is None)
                or (_dont_be_naive(subscription.subscription_end) > g.current_time())
            )
        )

    def _should_schedule_maintenance(workspace_id):
        return g.license.is_valid() and (g.license.is_on_premises or _is_pro_or_trial_subscription(workspace_id))

    for workspace in g.backend.workspaces.all():
        try:
            if _should_schedule_maintenance(workspace.id):
                schedule_task(
                    g,
                    task_name="maintain_workspace",
                    params={
                        "workspace_id": workspace.id,
                    },
                )
        except InvalidStateException:
            logger.warning("Skipping workspace, no owner?", workspace_id=workspace.id)
