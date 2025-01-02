from dataclasses import dataclass


@dataclass
class CustomerRisk:
    status: str
    transactionId: str
    customerId: str = None

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "transactionId": self.transactionId,
            "customerId": self.customerId
        }