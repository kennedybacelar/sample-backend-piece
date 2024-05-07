import logging.config

import structlog
import uvicorn


from gitential2.settings import GitentialSettings


shared_processors = (
    # structlog.contextvars.merge_contextvars,
    structlog.stdlib.add_logger_name,
    structlog.stdlib.add_log_level,
    structlog.processors.TimeStamper(fmt="iso"),
)


def logging_config_dict(settings: GitentialSettings):
    log_level = settings.log_level.value.upper()
    return {
        "version": 1,
        "disable_existing_loggers": True,
        "formatters": {
            "json": {
                "()": structlog.stdlib.ProcessorFormatter,
                "processor": structlog.processors.JSONRenderer(),
                "foreign_pre_chain": shared_processors,
            },
            "console": {
                "()": structlog.stdlib.ProcessorFormatter,
                "processor": structlog.dev.ConsoleRenderer(),
                "foreign_pre_chain": shared_processors,
            },
            **uvicorn.config.LOGGING_CONFIG["formatters"],
        },
        "handlers": {
            "default": {
                "level": "DEBUG",
                "class": "logging.StreamHandler",
                "formatter": "json",
            },
            # "uvicorn.access": {
            #     "level": "INFO",
            #     "class": "logging.StreamHandler",
            #     "formatter": "json",
            # },
            # "uvicorn.default": {
            #     "level": "INFO",
            #     "class": "logging.StreamHandler",
            #     "formatter": "default",
            # },
        },
        "loggers": {
            "": {"handlers": ["default"], "level": log_level},
            "uvicorn.error": {
                "handlers": ["default"],
                "level": "INFO",
                "propagate": False,
            },
            "uvicorn.access": {
                "handlers": ["default"],
                "level": "INFO",
                "propagate": False,
            },
            "celery.task": {
                "handlers": ["default"],
                "level": "INFO",
                "propagate": False,
            },
        },
    }


def initialize_logging(settings: GitentialSettings):
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            *shared_processors,
            _add_filename,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        # wrapper_class=structlog.stdlib.AsyncBoundLogger, # We're using a threadpool for the http requests (no async defs)
        cache_logger_on_first_use=False,
    )

    logging.config.dictConfig(logging_config_dict(settings))


#  pylint: disable=unused-argument
def _add_filename(logger, method_name, event_dict):
    # pylint: disable=protected-access
    frame, module_str = structlog._frames._find_first_app_frame_and_name(additional_ignores=[__name__])
    event_dict["filename"] = f"{module_str}:{frame.f_lineno}"
    return event_dict
