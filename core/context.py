from datetime import datetime, timezone
from gitential2.settings import GitentialSettings
from gitential2.backends import GitentialBackend, init_backend
from gitential2.secrets import Fernet
from gitential2.kvstore import KeyValueStore, init_key_value_store
from gitential2.license import License, check_license
from gitential2.integrations import init_integrations


class GitentialContext:
    def __init__(
        self,
        settings: GitentialSettings,
        integrations: dict,
        backend: GitentialBackend,
        kvstore: KeyValueStore,
        fernet: Fernet,
        license_: License,
    ):
        self._settings = settings
        self._integrations = integrations
        self._backend = backend
        self._fernet = fernet
        self._kvstore = kvstore
        self._license = license_

    def current_time(self) -> datetime:
        return datetime.utcnow().replace(tzinfo=timezone.utc)

    @property
    def settings(self) -> GitentialSettings:
        return self._settings

    @property
    def backend(self) -> GitentialBackend:
        return self._backend

    @property
    def integrations(self) -> dict:
        return self._integrations

    @property
    def fernet(self) -> Fernet:
        return self._fernet

    @property
    def kvstore(self) -> KeyValueStore:
        return self._kvstore

    @property
    def license(self) -> License:
        return self._license


def init_context_from_settings(settings: GitentialSettings) -> GitentialContext:
    kvstore = init_key_value_store(settings)
    integrations = init_integrations(settings, kvstore=kvstore)
    backend: GitentialBackend = init_backend(settings)
    fernet = Fernet(settings)

    license_ = check_license()
    return GitentialContext(
        settings=settings,
        integrations=integrations,
        backend=backend,
        kvstore=kvstore,
        fernet=fernet,
        license_=license_,
    )
