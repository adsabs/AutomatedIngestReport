class GoogleCredentialsError(Exception):
    pass


class GoogleServiceError(Exception):
    pass


class GoogleUploadError(Exception):
    pass


class MissingFileError(Exception):
    pass


class SlackPushError(Exception):
    pass


class SlackBadReturnError(Exception):
    pass


class GraylogException(Exception):
    pass


class GraylogQueryException(Exception):
    pass
