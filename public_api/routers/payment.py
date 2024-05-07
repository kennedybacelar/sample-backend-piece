from fastapi import APIRouter, Depends, Header, Request
from gitential2.core.context import GitentialContext
from gitential2.core.subscription_payment import (
    create_checkout_session,
    process_webhook,
    get_checkout_session,
    get_customer_portal_session,
)
from gitential2.exceptions import NotImplementedException
from gitential2.datatypes.subscriptions import CreateCheckoutSession
from ..dependencies import gitential_context, current_user

router = APIRouter(tags=["payment"])


@router.post("/payment/checkout-session")
def create_checkout(
    create_session: CreateCheckoutSession,
    g: GitentialContext = Depends(gitential_context),
    current_user=Depends(current_user),
):
    if g.license.is_on_premises:
        NotImplementedException("disabled.")
    if create_session.is_monthly:
        return create_checkout_session(
            g, g.settings.stripe.price_id_monthly, create_session.number_of_developers, current_user
        )
    else:
        return create_checkout_session(
            g, g.settings.stripe.price_id_yearly, create_session.number_of_developers, current_user
        )


@router.post("/payment/customer-portal")
def customer_portal(
    g: GitentialContext = Depends(gitential_context),
    current_user=Depends(current_user),
):
    if g.license.is_on_premises:
        NotImplementedException("disabled.")
    return get_customer_portal_session(g, current_user)


@router.get("/payment/checkout-session")
def get_checkout(
    session_id: str,
    g: GitentialContext = Depends(gitential_context),
):
    return get_checkout_session(g, session_id)


@router.post("/payment/webhook")
async def webhook_call(
    request: Request, g: GitentialContext = Depends(gitential_context), stripe_signature: str = Header(None)
):
    if g.license.is_on_premises:
        NotImplementedException("disabled.")
    else:
        payload = await request.body()
        process_webhook(
            g,
            payload,
            stripe_signature,
        )
        return {}
