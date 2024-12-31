from dataclasses import dataclass


@dataclass
class CustomerRisk:
    status: str
    transactionId: str

