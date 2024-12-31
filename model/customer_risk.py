from dataclasses import dataclass


@dataclass
class CustomerRisk:
    status: str
    transactionId: str

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "transactionId": self.transactionId
        }