class GitentialException(Exception):
    pass


class AuthenticationException(GitentialException):
    pass


class PermissionException(GitentialException):
    pass


class NotImplementedException(GitentialException):
    pass


class NotFoundException(GitentialException):
    pass


class SettingsException(GitentialException):
    pass


class InvalidStateException(GitentialException):
    pass


class LockError(GitentialException):
    pass
