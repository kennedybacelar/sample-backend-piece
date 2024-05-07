from celery.signals import setup_logging
from gitential2.settings import load_settings
from gitential2.logging import initialize_logging
from gitential2.core.tasks import configure_celery


celery_app = configure_celery()


@setup_logging.connect
def config_loggers(*args, **kwags):  # pylint: disable=unused-argument
    initialize_logging(load_settings())
