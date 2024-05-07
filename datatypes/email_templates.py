from .common import CoreModel, ExtraFieldMixin


class EmailTemplate(ExtraFieldMixin, CoreModel):
    name: str
    subject: str
    body_html: str
    body_plaintext: str


class RenderedEmail(CoreModel):
    sender: str
    recipient: str
    subject: str
    body_html: str
    body_plaintext: str
