class PipelineCancelledError(Exception):
    """Raised when pipeline/step execution is cancelled."""


class DatabaseError(Exception):
    """Raised for database-related failures."""


class ApiError(Exception):
    """Raised for API failures with optional response metadata."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        response_body: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.response_body = response_body
