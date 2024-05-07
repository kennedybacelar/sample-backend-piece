from pathlib import Path
from typing import Optional
import smtplib
from email.utils import formataddr
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import yaml
from structlog import get_logger
from jinja2 import Template

from gitential2.datatypes.users import UserInDB
from gitential2.datatypes.email_templates import EmailTemplate, RenderedEmail
from gitential2.exceptions import InvalidStateException
from .context import GitentialContext

logger = get_logger(__name__)

EMAIL_TEMPLATES_DIR = Path(__file__).parents[2] / "email_templates"


def send_email_to_user(g: GitentialContext, user: UserInDB, template_name: str, **kwargs):
    template = get_email_template(template_name)
    if not template:
        logger.error(f"Email template not found: {template_name}, cannot send email to {user.email}")
        return
    kwargs["user"] = user
    rendered_email = _render_email_template(g, template, recipient=_user_to_recipient(user), **kwargs)
    smtp_send(g, rendered_email)


def send_email_to_address(g: GitentialContext, email: str, template_name: str, **kwargs):
    template = get_email_template(template_name)
    if not template:
        logger.error(f"Email template not found: {template_name}, cannot send email to {email}")
        return
    rendered_email = _render_email_template(g, template, recipient=email, **kwargs)
    smtp_send(g, rendered_email)


def send_system_notification_email(g: GitentialContext, user: UserInDB, template_name: str, **kwargs):
    template = get_email_template(template_name)
    kwargs["user"] = user
    if not template:
        logger.error(f"Email template not found: {template_name}, cannot send system notificaiton email.")
        return
    rendered_email = _render_email_template(
        g, template, recipient=g.settings.notifications.system_notification_recipient, **kwargs
    )
    smtp_send(g, rendered_email)


def get_email_template(template_name: str) -> Optional[EmailTemplate]:
    filename = EMAIL_TEMPLATES_DIR / f"{template_name}.yml"
    if filename.is_file():
        with open(filename, "r", encoding="utf-8") as template_file:
            template_dict = yaml.full_load(template_file)
            return EmailTemplate(**template_dict)
    return None


def smtp_send(g: GitentialContext, email: RenderedEmail):
    email_settings = g.settings.email
    if email_settings.smtp_host and email_settings.smtp_port:
        try:
            server = smtplib.SMTP(email_settings.smtp_host, email_settings.smtp_port)
            server.ehlo()
            server.starttls()
            # stmplib docs recommend calling ehlo() before & after starttls()
            server.ehlo()
            if email_settings.smtp_username and email_settings.smtp_password:
                server.login(email_settings.smtp_username, email_settings.smtp_password)
            server.sendmail(email.sender, email.recipient, _rendered_email_to_message(email).as_string())
            server.close()
        except Exception:  # pylint: disable=broad-except
            logger.exception("Failed to send email.")
        else:
            logger.info(f'Email sent to {email.recipient} with subject "{email.subject}"')
    else:
        logger.warning("SMTP not configured, cannot send emails.")


def _render_email_template(
    g: GitentialContext, template: EmailTemplate, recipient: Optional[str] = None, **kwargs
) -> RenderedEmail:
    def _render_template(s: str) -> str:
        t = Template(s)
        return t.render(settings=g.settings, **kwargs)

    return RenderedEmail(
        sender=g.settings.email.sender,
        recipient=recipient,
        subject=_render_template(template.subject),
        body_html=_render_template(template.body_html),
        body_plaintext=_render_template(template.body_plaintext),
    )


def _user_to_recipient(user: UserInDB) -> str:
    if user.email:
        if user.first_name and user.last_name:
            return formataddr((f"{user.first_name} {user.last_name}", user.email))
        elif user.login:
            return formataddr((user.login, user.email))
        else:
            return user.email
    else:
        raise InvalidStateException(f"Cannot send email to user without email address. user_id={user.id}")


def _rendered_email_to_message(email: RenderedEmail) -> MIMEMultipart:
    msg = MIMEMultipart("alternative")
    msg["Subject"] = email.subject
    msg["From"] = email.sender
    msg["To"] = email.recipient
    part1 = MIMEText(email.body_plaintext, "plain")
    part2 = MIMEText(email.body_html, "html")
    msg.attach(part1)
    msg.attach(part2)
    return msg
