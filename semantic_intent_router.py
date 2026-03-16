from dataclasses import dataclass
from typing import Sequence

import numpy as np


@dataclass
class SemanticRule:
    intent: str
    text: str


class SemanticIntentRouter:
    """Semantic intent router backed by sentence-transformers + FAISS cosine search."""

    def __init__(self, model_name: str, rules: Sequence[SemanticRule]) -> None:
        try:
            import faiss
            from sentence_transformers import SentenceTransformer
        except Exception as exc:  # pragma: no cover - import environment dependent
            raise RuntimeError(
                "Semantic intent dependencies are missing. Install sentence-transformers and faiss-cpu."
            ) from exc

        self._faiss = faiss
        self._model = SentenceTransformer(model_name)
        self._rules = list(rules)
        self._rule_intents = [rule.intent for rule in self._rules]

        vectors = self._model.encode([rule.text for rule in self._rules], convert_to_numpy=True)
        vectors = vectors.astype("float32")
        faiss.normalize_L2(vectors)

        self._index = faiss.IndexFlatIP(vectors.shape[1])
        self._index.add(vectors)

    def detect_intent(self, query: str, k: int = 2) -> tuple[str, float]:
        if not query.strip():
            return "", 0.0

        query_vec = self._model.encode([query], convert_to_numpy=True).astype("float32")
        self._faiss.normalize_L2(query_vec)

        distances, indices = self._index.search(query_vec, k)
        top_idx = int(indices[0][0])
        top_score = float(distances[0][0])

        intent = self._rule_intents[top_idx]
        confidence = max(0.0, min(1.0, (top_score + 1.0) / 2.0))
        return intent, confidence
