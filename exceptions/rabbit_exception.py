from typing import Optional
from dataclasses import dataclass

@dataclass
class RabbitMqError(Exception):
    """Base exception for RabbitMQ related errors"""
    message: str
    error_code: Optional[str] = None
    details: Optional[dict] = None

    def __str__(self) -> str:
        error_info = f"rabbitMQ error: {self.message}"
        if self.error_code:
            error_info += f" (code: {self.error_code})"
        if self.details:
            error_info += f" - details: {self.details}"
        return error_info