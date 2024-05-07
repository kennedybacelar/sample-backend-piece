import os
from base64 import b64encode
from typing import Optional, Dict, Union, List
from enum import Enum

import yaml
from pydantic import BaseModel, validator
from gitential2.utils import deep_merge_dicts


class LogLevel(str, Enum):
    debug = "debug"
    info = "info"
    warn = "warn"
    error = "error"
    critical = "critical"


class IntegrationType(str, Enum):
    dummy = "dummy"
    gitlab = "gitlab"
    github = "github"
    linkedin = "linkedin"
    bitbucket = "bitbucket"
    vsts = "vsts"
    jira = "jira"


class Executor(str, Enum):
    process_pool = "process_pool"
    single_tread = "single_thread"


class OAuthClientSettings(BaseModel):
    client_id: Optional[str] = None
    client_secret: Optional[str] = None


class IntegrationSettings(BaseModel):
    type: IntegrationType
    base_url: Optional[str] = None
    oauth: Optional[OAuthClientSettings] = None
    login: bool = False
    login_order: int = 0
    login_text: Optional[str] = None
    login_top_text: Optional[str] = None
    signup_text: Optional[str] = None
    display_name: Optional[str] = None

    options: Dict[str, Union[str, int, float, bool]] = {}

    @property
    def type_(self):
        return self.type


class BackendType(str, Enum):
    in_memory = "in_memory"
    sql = "sql"


class KeyValueStoreType(str, Enum):
    in_memory = "in_memory"
    redis = "redis"


class CelerySettings(BaseModel):
    broker_url: Optional[str] = None
    result_backend_url: Optional[str] = None
    worker_prefetch_multiplier: int = 1
    worker_max_tasks_per_child: int = 5
    worker_max_memory_per_child: int = 512 * 1024  # in kbytes


class S3Settings(BaseModel):
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    bucket_name: Optional[str] = None
    secret_key: Optional[str] = None


class ConnectionSettings(BaseModel):
    database_url: Optional[str] = None
    redis_url: Optional[str] = "redis://localhost:6379/0"
    s3: S3Settings = S3Settings()


class HTMLElementPosition(str, Enum):
    beforebegin = "beforebegin"
    afterbegin = "afterbegin"
    beforeend = "beforeend"
    afterend = "afterend"


class HTMLInjection(BaseModel):
    parent: str = "head"
    tag: str = ""
    content: str = ""
    position: HTMLElementPosition = HTMLElementPosition.beforeend
    attributes: Dict[str, Union[int, bool, str]] = {}


class RecaptchaSettings(BaseModel):
    site_key: str = ""
    secret_key: str = ""


class StripeIntegration(BaseModel):
    api_key: str = ""
    publishable_key: str = ""
    private_key: str = ""
    price_id_monthly: str = ""
    price_id_yearly: str = ""
    webhook_secret: str = ""


class FrontendSettings(BaseModel):
    inject_html: List[HTMLInjection] = []


class EmailSettings(BaseModel):
    sender: str = "gitential@gitential.com"
    smtp_username: Optional[str] = None
    smtp_password: Optional[str] = None
    smtp_host: Optional[str] = None
    smtp_port: Optional[int] = None


class WebSettings(BaseModel):
    base_url: str = "http://localhost:7998"
    frontend_url: str = "http://localhost:7999"
    enforce_base_url: bool = False
    session_cookie: str = "gitential"
    session_same_site: str = "None"
    session_https_only: bool = True
    session_max_age: int = 14 * 24 * 60 * 60  # 14 days, in seconds
    legacy_login: bool = False
    access_log: bool = True


class ExtractionSettings(BaseModel):
    executor: Executor = Executor.process_pool
    process_pool_size: int = 4
    show_progress: bool = False
    repo_analysis_limit_in_days: Optional[int] = None
    its_project_analysis_limit_in_days: Optional[int] = None


class CacheSettings(BaseModel):
    repo_cache_life_hours: int = 6
    scheduled_repo_cache_refresh_enabled: bool = False
    scheduled_repo_cache_refresh_hour_of_day: str = "*/3"
    scheduled_repo_cache_refresh_is_force_refresh: bool = True
    its_projects_cache_life_hours: int = 6
    scheduled_its_projects_cache_refresh_enabled: bool = False
    scheduled_its_projects_cache_refresh_hour_of_day: str = "*/3"
    scheduled_its_projects_cache_refresh_is_force_refresh: bool = True


class RefreshSettings(BaseModel):
    scheduled_maintenance_enabled: bool = True
    scheduled_maintenance_days_of_week: str = "5"  # https://docs.celeryq.dev/en/stable/reference/celery.schedules.html
    scheduled_maintenance_hour_of_day: int = 23
    interval_minutes: int = 60 * 24


class CleanupSettings(BaseModel):
    enable_scheduled_data_cleanup: bool = False
    scheduled_data_cleanup_days_of_week: str = "5"  # https://docs.celeryq.dev/en/stable/reference/celery.schedules.html
    scheduled_data_cleanup_hour_of_day: int = 23
    exp_days_after_user_deactivation: int = 3
    exp_days_since_user_last_login: int = 365


class AutoExportSettings(BaseModel):
    start_auto_export: bool = False


class MaintenanceSettings(BaseModel):
    enabled: bool = False
    message: str = ""


class ContactSettings(BaseModel):
    support_email: str = "support@gitential.com"
    info_email: str = "gitential@gitential.com"


class NotificationSettings(BaseModel):
    system_notification_recipient = "info@gitential.com"
    request_free_trial: bool = True


class AccessApprovalSettings(BaseModel):
    pending_message: str = (
        "Your user is currently not approved to use Gitential. <br />Please, contact your administrator."
    )


class FeaturesSettings(BaseModel):
    enable_additional_materialized_views: bool = False
    enable_access_approval: bool = False
    enable_its_analytics: bool = False
    enable_resellers: bool = False
    access_approval: AccessApprovalSettings = AccessApprovalSettings()


class ResellerSettings(BaseModel):
    reseller_id: str
    short_name: str
    redemption_route: str


class GitentialSettings(BaseModel):
    maintenance: MaintenanceSettings = MaintenanceSettings()
    secret: str
    log_level: LogLevel = LogLevel.info
    connections: ConnectionSettings = ConnectionSettings()
    email: EmailSettings = EmailSettings()
    notifications: NotificationSettings = NotificationSettings()
    web: WebSettings = WebSettings()
    extraction: ExtractionSettings = ExtractionSettings()
    cache: CacheSettings = CacheSettings()
    refresh: RefreshSettings = RefreshSettings()
    cleanup: CleanupSettings = CleanupSettings()
    auto_export: AutoExportSettings = AutoExportSettings()
    recaptcha: RecaptchaSettings = RecaptchaSettings()
    integrations: Dict[str, IntegrationSettings]
    backend: BackendType = BackendType.in_memory
    kvstore: KeyValueStoreType = KeyValueStoreType.redis
    celery: CelerySettings = CelerySettings()
    frontend: FrontendSettings = FrontendSettings()
    contacts: ContactSettings = ContactSettings()
    stripe: StripeIntegration = StripeIntegration()
    features: FeaturesSettings = FeaturesSettings()
    resellers: Optional[List[ResellerSettings]] = None

    @validator("secret")
    def secret_validation(cls, v):
        if len(v) < 32:
            raise ValueError("Secret must be at least 32 bytes long")
        return v

    @property
    def fernet_key(self) -> bytes:
        s: str = self.secret[:32]
        return b64encode(s.encode())


def _environtment_overrides(config_dict):
    def _override_config(env_name, dict_key):
        if os.environ.get(env_name):
            config_dict[dict_key] = os.environ.get(env_name)

    _override_config("LOG_LEVEL", "log_level")
    return config_dict


def load_settings(settings_file=None, override_file=None):
    # Load settings.yml as a dict
    settings_file = settings_file or os.environ.get("GITENTIAL_SETTINGS", "settings.yml")
    with open(settings_file, "r", encoding="utf-8") as f:
        config_dict = yaml.safe_load(f)

    # If override configured, load and merge it to the config
    override_file = override_file or os.environ.get("GITENTIAL_SETTINGS_OVERRIDE")
    if override_file:
        with open(override_file, "r", encoding="utf-8") as f:
            override_dict = yaml.safe_load(f)
            print("!!!", override_dict)
        config_dict = deep_merge_dicts(config_dict, override_dict)

    # Apply environment variable overrides
    config_dict = _environtment_overrides(config_dict)
    return GitentialSettings.parse_obj(config_dict)
