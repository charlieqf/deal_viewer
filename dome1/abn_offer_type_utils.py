from __future__ import annotations


class OfferTypeClassification:
    def __init__(self, *, collection_method: str, summary_bucket: str) -> None:
        self.collection_method = collection_method
        self.summary_bucket = summary_bucket


def classify_offer_type(raw_offer_type: str) -> OfferTypeClassification:
    offer_type = (raw_offer_type or "").strip()
    if "定向" in offer_type or "私募" in offer_type:
        return OfferTypeClassification(
            collection_method="PrivateEquity",
            summary_bucket="private",
        )

    return OfferTypeClassification(
        collection_method="PublicOffering",
        summary_bucket="public",
    )
