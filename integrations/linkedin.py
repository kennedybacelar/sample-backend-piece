from typing import Callable, Tuple
from structlog import get_logger
from gitential2.datatypes import UserInfoCreate
from .base import BaseIntegration, OAuthLoginMixin

logger = get_logger(__name__)


def get_localized_value(name):
    key = "_".join([name["preferredLocale"]["language"], name["preferredLocale"]["country"]])
    return name["localized"].get(key, "")


class LinkedinIntegration(OAuthLoginMixin, BaseIntegration):
    def oauth_register(self):
        api_base_url = "https://api.linkedin.com/v2/"
        return {
            "api_base_url": api_base_url,
            "access_token_url": "https://www.linkedin.com/oauth/v2/accessToken",
            "authorize_url": "https://www.linkedin.com/oauth/v2/authorization",
            "client_kwargs": {
                "scope": "r_liteprofile r_emailaddress",
                "token_endpoint_auth_method": "client_secret_post",
            },
            "userinfo_endpoint": api_base_url + "me?projection=(id,firstName,lastName,profilePicture)",
            "client_id": self.settings.oauth.client_id,
            "client_secret": self.settings.oauth.client_secret,
        }

    def normalize_userinfo(self, data, token=None) -> UserInfoCreate:
        given_name = get_localized_value(data["firstName"])
        family_name = get_localized_value(data["lastName"])
        params = {
            "integration_name": self.name,
            "integration_type": "linkedin",
            "sub": str(data["id"]),
            "name": " ".join([given_name, family_name]),
            "preferred_username": " ".join([given_name, family_name]),
            "extra": data,
        }

        url = "emailAddress?q=members&projection=(elements*(handle~))"
        client = self.get_oauth2_client(token=token)
        api_base_url = self.oauth_register()["api_base_url"]
        resp = client.get(api_base_url + url)
        email_data = resp.json()

        logger.debug("linkedin email data", email_data=email_data)

        elements = email_data.get("elements")
        if elements:
            handle = elements[0].get("handle~")
            if handle:
                email = handle.get("emailAddress")
                if email:
                    params["email"] = email

        logger.debug("linkedin userinfo created", user_info_dict=params)
        return UserInfoCreate(**params)

    def refresh_token_if_expired(self, token, update_token: Callable) -> Tuple[bool, dict]:
        return False, token
