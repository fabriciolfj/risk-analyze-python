from dataclasses import dataclass


@dataclass
class Payment:
    customer: str
    identifier: str