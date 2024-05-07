from typing import cast, Optional
from structlog import get_logger
import stripe
from stripe.error import SignatureVerificationError
from gitential2.core.subscription import set_as_professional, set_as_free
from gitential2.datatypes.users import UserInDB, UserUpdate

from .context import GitentialContext

PAYMENT_METHOD_TYPES = ["card"]

logger = get_logger(__name__)


stripe.set_app_info("gitential", version="0.0.1", url="https://gitential.com")
stripe.api_version = "2020-08-27"


def get_user_by_customer(g: GitentialContext, customer_id: str) -> Optional[UserInDB]:
    stripe.api_key = g.settings.stripe.api_key
    customer = stripe.Customer.retrieve(customer_id)
    return g.backend.users.get_by_email(customer.email)


def create_checkout_session(
    g: GitentialContext,
    price_id: str,
    number_of_developers: int,
    user: UserInDB,
) -> dict:
    stripe.api_key = g.settings.stripe.api_key
    domain_url = g.backend.settings.web.frontend_url
    if not domain_url.endswith("/"):
        domain_url = domain_url + "/"

    stripe_customer = None
    if user.stripe_customer_id:
        stripe_customer = stripe.Customer.retrieve(user.stripe_customer_id)

    try:
        params = dict(
            success_url=domain_url + "?payment=true&session_id={CHECKOUT_SESSION_ID}",
            cancel_url=domain_url + "?payment=false",
            payment_method_types=PAYMENT_METHOD_TYPES,
            billing_address_collection="required",
            mode="subscription",
            customer_email=user.email,
            allow_promotion_codes=True,
            line_items=[
                {
                    "price": price_id,
                    "quantity": number_of_developers,
                }
            ],
            metadata={
                "user_id": user.id,
            },
        )
        if stripe_customer:
            params.pop("customer_email", None)
            params["customer"] = stripe_customer.stripe_id
        checkout_session = stripe.checkout.Session.create(**params)
        return {"session_id": checkout_session["id"]}
    except:  # pylint: disable=bare-except
        logger.exception("Failed to create checkout session", price_id=price_id, user_id=user.id)
        raise


def _get_stripe_subscription(g: GitentialContext, subscription_id) -> dict:
    stripe.api_key = g.settings.stripe.api_key
    subscription = stripe.Subscription.retrieve(subscription_id)
    return subscription


def change_subscription(g: GitentialContext, subscription_id: str, developer_num: int, pice_id: str):
    stripe.api_key = g.settings.stripe.api_key
    subscription = stripe.Subscription.retrieve(subscription_id)
    stripe.Subscription.modify(
        subscription.id,
        cancel_at_period_end=False,
        proration_behavior="create_prorations",
        items=[{"id": subscription["items"]["data"][0].id, "price": pice_id, "quantity": developer_num}],
    )


def delete_subscription(g: GitentialContext, subs_id) -> dict:
    stripe.api_key = g.settings.stripe.api_key
    stripe.Subscription.delete(subs_id)
    return {"status": "success"}


def process_webhook(g: GitentialContext, input_data: bytes, signature: str) -> None:  # pylint: disable=too-complex
    stripe.api_key = g.settings.stripe.api_key
    try:
        event = stripe.Webhook.construct_event(input_data, signature, g.settings.stripe.webhook_secret)
    except (ValueError, SignatureVerificationError):
        logger.info("Payload verification error")
        return None
    if event.type in ["customer.subscription.updated", "customer.subscription.created"]:
        logger.info("new subscription mod/update", type=event.type)
        customer_id = event["data"]["object"]["customer"]
        customer = stripe.Customer.retrieve(customer_id)
        user = g.backend.users.get_by_email(customer["email"])
        if user:
            if user.stripe_customer_id is None:
                user_copy = user.copy()
                user_copy.stripe_customer_id = customer.id
                user = g.backend.users.update(user.id, cast(UserUpdate, user_copy))
            if event.data.object["status"] == "active":
                developers = event.data.object["quantity"]
                set_as_professional(g, user.id, developers, event)
                stripe.Customer.modify(customer.id, metadata={"number_of_developers": developers, "user_id": user.id})
            elif (
                event.data.object["status"] == "incomplete"
                or event.data.object["status"] == "incomplete_expired"
                or event.data.object["status"] == "past_due"
                or event.data.object["status"] == "canceled"
                or event.data.object["status"] == "unpaid"
            ):
                logger.info("subscription obsoleted", status=event.data.object["status"])
                _set_as_free_everywhere(g, customer)
            else:
                pass
    elif event.type == "customer.subscription.deleted":
        customer_id = event["data"]["object"]["customer"]
        customer = stripe.Customer.retrieve(customer_id)
        _set_as_free_everywhere(g, customer)
    elif event.type == "customer.deleted":
        logger.info("customer deleted")
        email = event["data"]["object"]["email"]
        user = g.backend.users.get_by_email(email)
        if user:
            user.stripe_customer_id = None
            g.backend.users.update(user.id, cast(UserUpdate, user))
    else:
        logger.info("not handled stripe event", event_id=event.type)
    return None


def _set_as_free_everywhere(g: GitentialContext, customer) -> None:
    logger.info("subscription deleted")
    stripe.Customer.modify(customer.id, metadata={"number_of_developers": ""})
    set_as_free(g, customer["metadata"]["user_id"])


def get_customer_portal_session(g: GitentialContext, user: UserInDB) -> dict:
    stripe.api_key = g.settings.stripe.api_key

    domain_url = g.backend.settings.web.frontend_url
    if not domain_url.endswith("/"):
        domain_url = domain_url + "/"

    session = stripe.billing_portal.Session.create(customer=user.stripe_customer_id, return_url=domain_url)
    return {"url": session.url}


def get_checkout_session(g: GitentialContext, session_id: str):
    stripe.api_key = g.settings.stripe.api_key
    try:
        checkout_session = stripe.checkout.Session.retrieve(session_id)
    except stripe.error.InvalidRequestError:
        logger.error("error get checkout session", session_id=session_id)
        raise
    return checkout_session
