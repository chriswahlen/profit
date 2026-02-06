from __future__ import annotations

METRIC_CONCEPT_ALIASES: dict[str, tuple[str, ...]] = {
    "capex": (
        "CapitalExpenditures",
        "PaymentsToAcquirePropertyPlantAndEquipment",
        "PurchasesOfPropertyPlantAndEquipment",
        "CapitalizedAttributableToPropertyPlantAndEquipment",
    ),
    "revenue": (
        "Revenues",
        "SalesRevenues",
        "SalesRevenueNet",
        "RevenuesNetOfUnusualItems",
    ),
    "netincome": (
        "NetIncomeLoss",
        "ProfitLoss",
        "IncomeLossFromContinuingOperations",
    ),
}
