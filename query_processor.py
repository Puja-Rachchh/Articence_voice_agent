import re
import os
from dataclasses import dataclass
from datetime import date
from typing import Callable, Optional

from business_rules import (
    applecare_details,
    can_create_new_order,
    return_status,
    to_date,
    warranty_status,
)
from database import SnowflakeClient
from semantic_intent_router import SemanticIntentRouter, SemanticRule


AMOUNT_RE = re.compile(r"\$?\s*([0-9]+(?:\.[0-9]{1,2})?)")

# Canonical device names and their common aliases in user speech/text.
DEVICE_ALIAS_MAP: dict[str, tuple[str, ...]] = {
    "iphone": ("iphone", "i phone", "apple phone", "ios phone"),
    "ipad": ("ipad", "i pad", "tablet", "apple tablet"),
    "macbook": ("macbook", "mac book", "mac", "laptop", "macbook pro", "macbook air"),
}

INTENT_PATTERN_TABLE: dict[str, tuple[str, ...]] = {
    "return": (
        r"\breturn\b",
        r"\brefund\b",
        r"\bexchange\b",
        r"\bsend\s*back\b",
        r"\bdon[\s']?t\s*want\b",
        r"\bchanged\s*mind\b",
        r"\bcancel\b",
    ),
    "warranty": (
        r"\bwarrant(?:y|ies)?\b",
        r"\bhardware\s*defect\b",
        r"\bbroken\b",
        r"\bnot\s*work(?:ing)?\b",
        r"\brepair\b",
        r"\bmalfunction\b",
        r"\bscreen\s*crack(?:ed)?\b",
        r"\bbattery\b",
    ),
    "applecare": (
        r"\bapple\s?care\b",
        r"\bphysical\s*damage\b",
        r"\bcrack(?:ed)?\b",
        r"\bdropped\b",
        r"\baccidental\b",
        r"\binsurance\b",
        r"\bprotection\s*plan\b",
        r"\bcare\s*plan\b",
    ),
    "replacement": (
        r"\breplace\b",
        r"\breplacement\b",
        r"\bswap\b",
    ),
    "new_order": (
        r"\bnew\s+order\b",
        r"\bcreate\s+order\b",
        r"\bplace\s+order\b",
        r"\bplace.*order\b",
        r"\border\s+value\b",
        r"\bbuy\b",
        r"\bpurchase\b",
        r"\bwant\s*to\s*order\b",
    ),
    "order_lookup": (
        r"\blatest\s+order\b",
        r"\brecent\s+orders?\b",
        r"\blast\s+orders?\b",
        r"\bmy\s+order\b",
        r"\bmy\s+orders\b",
        r"\border\s+status\b",
        r"\border\s+id\b",
        r"\bmy\s+purchase\b",
    ),
    "price": (
        r"\bprice\b",
        r"\bvalue\b",
        r"\bcost\b",
        r"\bhow\s*much\b",
        r"\bpaid\b",
        r"\bamount\b",
    ),
    "date": (
        r"\bpurchase\s*date\b",
        r"\bhow\s*old\b",
        r"\bhow\s*long\s*ago\b",
        r"\bbought\b",
        r"\bdid\s+i\s+buy\b",
        r"\bwhen\s+did\s+i\s+buy\b",
        r"\bdid\s+i\s+purchase\b",
        r"\bdate\s*of\s*purchase\b",
    ),
    "greeting": (r"\bhello\b", r"\bhi\b", r"\bhey\b"),
    "policy": (r"\brules?\b", r"\bpolic(?:y|ies)\b", r"\bhow\s+long\b", r"\blimit\b", r"\bexplain\b"),
}

PERSONAL_ORDER_HINTS: tuple[str, ...] = (
    "my order",
    "my orders",
    "latest order",
    "recent order",
    "recent orders",
    "last order",
    "last orders",
    "order status",
    "order id",
    "my purchase",
)

SUPPORT_DOMAIN_HINTS: tuple[str, ...] = (
    "policy",
    "policies",
    "rule",
    "rules",
    "support",
    "return",
    "refund",
    "exchange",
    "warranty",
    "applecare",
    "apple care",
    "replace",
    "replacement",
    "order",
    "purchase",
    "bought",
    "buy",
    "price",
    "cost",
    "amount",
    "value",
)

PERSONAL_DATE_HINTS: tuple[str, ...] = (
    "purchase date",
    "date of purchase",
    "how old",
    "how long ago",
    "bought",
    "purchased",
    "did i buy",
    "did i purchase",
    "my purchase",
    "my order",
)


@dataclass
class ConversationSession:
    phone: Optional[str] = None
    email: Optional[str] = None
    customer_id: Optional[int] = None
    customer_name: Optional[str] = None
    verified: bool = False


@dataclass
class IntentResult:
    intent: str
    response: str
    confidence: float = 0.0


@dataclass
class IntentContext:
    raw_query: str
    normalized_query: str
    amount: Optional[float]
    device: Optional[str]


class QueryProcessor:
    def __init__(self, db: SnowflakeClient) -> None:
        self.db = db
        self._use_semantic_router = os.getenv("USE_SEMANTIC_INTENT_ROUTER", "false").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        self._semantic_threshold = float(os.getenv("SEMANTIC_INTENT_THRESHOLD", "0.55"))
        self._semantic_router: Optional[SemanticIntentRouter] = None

        if self._use_semantic_router:
            try:
                self._semantic_router = SemanticIntentRouter(
                    model_name=os.getenv("SEMANTIC_MODEL_NAME", "all-MiniLM-L6-v2"),
                    rules=self._semantic_rules(),
                )
            except Exception:
                # Keep the service running with regex fallback if semantic components are unavailable.
                self._semantic_router = None

        self._compiled_patterns: dict[str, tuple[re.Pattern[str], ...]] = {
            intent: tuple(re.compile(pattern, re.IGNORECASE) for pattern in patterns)
            for intent, patterns in INTENT_PATTERN_TABLE.items()
        }
        self._intent_handlers: dict[str, Callable[[ConversationSession, IntentContext, float], IntentResult]] = {
            "policy": self._handle_policy,
            "return": self._handle_return,
            "warranty": self._handle_warranty,
            "applecare": self._handle_applecare,
            "replacement": self._handle_replacement,
            "order_lookup": self._handle_order_lookup,
            "new_order": self._handle_new_order,
            "price": self._handle_price,
            "date": self._handle_date,
            "greeting": self._handle_greeting,
            "unknown": self._handle_unknown,
        }
        self._verification_required_intents = {"return", "warranty", "applecare", "replacement", "order_lookup", "price", "date"}

    def verify_identity(
        self,
        phone: str,
        email: str,
        session: ConversationSession,
    ) -> tuple[bool, str, ConversationSession]:
        normalized_phone = "".join(ch for ch in str(phone or "") if ch.isdigit())
        session.phone = normalized_phone
        session.email = (email or "").strip().lower()
        session.verified = False
        session.customer_id = None
        session.customer_name = None

        if not session.phone or not session.email:
            return False, "Please provide both phone number and email.", session

        if len(session.phone) < 10:
            return False, "Please provide a valid phone number with at least 10 digits.", session

        customer = self.db.verify_customer(session.phone, session.email)
        if not customer:
            return False, "I could not verify your account using that phone number and email.", session

        session.customer_id = int(customer["customer_id"])
        session.customer_name = customer.get("customer_name")
        session.verified = True
        return True, f"Verified successfully for {session.customer_name}.", session

    def process_query_with_intent(self, query: str, session: ConversationSession) -> tuple[IntentResult, ConversationSession]:
        normalized = query.strip().lower()
        if not normalized:
            return IntentResult(intent="empty", response="I did not catch that. Please repeat your question.", confidence=1.0), session

        context = IntentContext(
            raw_query=query,
            normalized_query=normalized,
            amount=self._extract_amount(query),
            device=self._extract_device_from_text(normalized),
        )

        intent, confidence = self._detect_intent(context)

        if intent in self._verification_required_intents:
            verified, message = self._ensure_verified(session)
            if not verified:
                return IntentResult(intent="verification_required", response=message, confidence=confidence), session

        handler = self._intent_handlers.get(intent, self._handle_unknown)
        result = handler(session, context, confidence)
        return result, session

    def process_query(self, query: str, session: ConversationSession) -> tuple[str, ConversationSession]:
        result, session = self.process_query_with_intent(query, session)
        return result.response, session

    def _detect_intent(self, context: IntentContext) -> tuple[str, float]:
        normalized = context.normalized_query
        policy_topics = self._extract_policy_topics(normalized)
        is_policy_query = self._is_generic_policy_query(normalized, policy_topics)
        is_personal_order_query = self._is_personal_order_query(normalized)
        is_domain_query = self._is_support_domain_query(normalized, context.device)
        is_personal_date_query = self._is_personal_date_query(normalized, context.device)

        if not is_domain_query and not any(pattern.search(normalized) for pattern in self._compiled_patterns["greeting"]):
            return "unknown", 0.25

        if self._semantic_router is not None:
            semantic_intent, semantic_confidence = self._semantic_router.detect_intent(context.raw_query, k=2)
            # Guardrails to prevent semantic misrouting for generic questions.
            if semantic_intent == "order_lookup" and not is_personal_order_query:
                semantic_intent = ""
            if semantic_intent == "date" and not is_personal_date_query:
                semantic_intent = ""
            if semantic_intent in self._verification_required_intents and not is_domain_query:
                semantic_intent = ""
            if semantic_intent in self._verification_required_intents and is_policy_query:
                semantic_intent = "policy"

            if semantic_intent and semantic_confidence >= self._semantic_threshold:
                return semantic_intent, round(semantic_confidence, 2)

        scores: dict[str, float] = {}

        for intent, patterns in self._compiled_patterns.items():
            match_count = sum(1 for pattern in patterns if pattern.search(normalized))
            if match_count > 0:
                scores[intent] = float(match_count)

        if context.amount is not None and any(
            token in normalized for token in ["new order", "create order", "place order", "order value", "buy", "purchase"]
        ):
            scores["new_order"] = scores.get("new_order", 0.0) + 2.0

        if is_policy_query:
            policy_boost = 1.0 + (0.25 * len(policy_topics))
            scores["policy"] = scores.get("policy", 0.0) + policy_boost

        # Historical purchase-date questions can include "buy"; keep them out of new-order intent.
        explicit_new_order_request = any(
            token in normalized
            for token in ["new order", "create order", "place order", "want to order", "order value"]
        )
        if "new_order" in scores and is_personal_date_query and not explicit_new_order_request:
            scores.pop("new_order", None)

        # Never route to personal order lookup unless query explicitly asks for user's order details.
        if "order_lookup" in scores and not is_personal_order_query:
            scores.pop("order_lookup", None)

        # Never route to purchase-date flow unless query indicates personal purchase context.
        if "date" in scores and not is_personal_date_query:
            scores.pop("date", None)

        if not scores:
            return "unknown", 0.25

        intent = max(scores, key=scores.get)
        total = sum(scores.values())
        confidence = scores[intent] / total if total > 0 else 0.25
        confidence = max(0.25, min(1.0, confidence))
        return intent, round(confidence, 2)

    def _is_personal_order_query(self, normalized_query: str) -> bool:
        return any(hint in normalized_query for hint in PERSONAL_ORDER_HINTS)

    def _is_support_domain_query(self, normalized_query: str, canonical_device: Optional[str]) -> bool:
        if canonical_device is not None:
            return True
        return any(hint in normalized_query for hint in SUPPORT_DOMAIN_HINTS)

    def _is_personal_date_query(self, normalized_query: str, canonical_device: Optional[str]) -> bool:
        has_date_signal = any(hint in normalized_query for hint in PERSONAL_DATE_HINTS)
        if not has_date_signal:
            return False
        has_personal_anchor = any(
            token in normalized_query
            for token in ["my ", " i ", "order", "purchase", "bought", "did i"]
        )
        return has_personal_anchor or canonical_device is not None

    def _semantic_rules(self) -> list[SemanticRule]:
        return [
            SemanticRule(
                intent="return",
                text=(
                    "Customer asks if a device can be returned, refunded, exchanged, sent back, or canceled. "
                    "Return eligibility is based on a 30-day window from order date."
                ),
            ),
            SemanticRule(
                intent="warranty",
                text=(
                    "Customer asks about warranty coverage, hardware defect, broken device, not working, repair, "
                    "malfunction, cracked screen, or battery issues. Warranty is 12 months from order date."
                ),
            ),
            SemanticRule(
                intent="applecare",
                text=(
                    "Customer asks about AppleCare, protection plan, accidental damage, dropped or physically damaged device. "
                    "AppleCare coverage and charges vary by iPhone, iPad, and MacBook."
                ),
            ),
            SemanticRule(
                intent="new_order",
                text=(
                    "Customer wants to create or place a new order, buy or purchase a device, and asks about order value limit. "
                    "New orders are allowed only below ten thousand dollars."
                ),
            ),
            SemanticRule(
                intent="replacement",
                text=(
                    "Customer asks to replace or swap a device. Replacement depends on AppleCare replacement support "
                    "or return window when AppleCare is not active."
                ),
            ),
            SemanticRule(
                intent="order_lookup",
                text=(
                    "Customer asks for latest order, recent orders, order status, order id, or purchase history."
                ),
            ),
            SemanticRule(
                intent="price",
                text=(
                    "Customer asks the amount paid, order price, order value, cost, or how much a purchased device costs."
                ),
            ),
            SemanticRule(
                intent="date",
                text=(
                    "Customer asks when they bought a device, purchase date, how old an order is, or how long ago it was bought."
                ),
            ),
            SemanticRule(
                intent="policy",
                text=(
                    "Customer asks generic policy rules, business rules, limits, or explanation of return, warranty, "
                    "AppleCare, replacement, and new order rules without asking for a specific personal order."
                ),
            ),
        ]

    def _handle_policy(self, session: ConversationSession, context: IntentContext, confidence: float) -> IntentResult:
        policy_topics = self._extract_policy_topics(context.normalized_query)
        return IntentResult(
            intent="policy",
            response=self._build_policy_response(policy_topics),
            confidence=confidence,
        )

    def _handle_return(self, session: ConversationSession, context: IntentContext, confidence: float) -> IntentResult:
        target_order = self._get_target_order_for_customer(session, context)
        if target_order is None:
            return IntentResult(intent="return", response="I could not find any orders for your account.", confidence=confidence)

        eligible, deadline = return_status(target_order["order_date"])
        purchased = self._format_date(target_order["order_date"])
        deadline_text = deadline.strftime("%B %d, %Y")

        if eligible:
            response = (
                f"Yes, your {target_order['device']} purchased on {purchased} "
                f"is eligible for return until {deadline_text}."
            )
        else:
            response = (
                f"Your {target_order['device']} purchased on {purchased} "
                f"is no longer eligible for return. The return window ended on {deadline_text}."
            )

        return IntentResult(intent="return", response=response, confidence=confidence)

    def _handle_warranty(self, session: ConversationSession, context: IntentContext, confidence: float) -> IntentResult:
        target_order = self._get_target_order_for_customer(session, context)
        if target_order is None:
            return IntentResult(intent="warranty", response="I could not find any orders for your account.", confidence=confidence)

        covered, expiry = warranty_status(target_order["order_date"])
        expiry_text = expiry.strftime("%B %d, %Y")

        if covered:
            response = f"Your {target_order['device']} is under warranty until {expiry_text}."
        else:
            response = f"The warranty for your {target_order['device']} expired on {expiry_text}."

        return IntentResult(intent="warranty", response=response, confidence=confidence)

    def _handle_applecare(self, session: ConversationSession, context: IntentContext, confidence: float) -> IntentResult:
        target_order = self._get_target_order_for_customer(session, context)
        if target_order is None:
            return IntentResult(intent="applecare", response="I could not find any orders for your account.", confidence=confidence)

        response = applecare_details(target_order["device"], bool(target_order["apple_care"]))
        return IntentResult(intent="applecare", response=response, confidence=confidence)

    def _handle_replacement(self, session: ConversationSession, context: IntentContext, confidence: float) -> IntentResult:
        target_order = self._get_target_order_for_customer(session, context)
        if target_order is None:
            return IntentResult(intent="replacement", response="I could not find any orders for your account.", confidence=confidence)

        device = str(target_order["device"])
        device_lower = device.lower()
        purchased_date = self._format_date(target_order["order_date"])

        if bool(target_order["apple_care"]):
            if "iphone" in device_lower:
                response = (
                    f"Yes, your {device} purchased on {purchased_date} has AppleCare active with replacement coverage. "
                    "You can proceed with an AppleCare replacement request."
                )
            elif "ipad" in device_lower:
                response = (
                    f"Your {device} purchased on {purchased_date} has AppleCare active. "
                    "Replacement support is available under AppleCare terms, with iPad minimum AppleCare charge of $49."
                )
            elif "macbook" in device_lower:
                response = (
                    f"Your {device} purchased on {purchased_date} has AppleCare active. "
                    "Replacement or repair support is available under AppleCare terms, with MacBook minimum AppleCare charge of $99."
                )
            else:
                response = (
                    f"Your {device} purchased on {purchased_date} has AppleCare active. "
                    "Replacement support depends on AppleCare terms for this device type."
                )
            return IntentResult(intent="replacement", response=response, confidence=confidence)

        eligible, deadline = return_status(target_order["order_date"])
        deadline_text = deadline.strftime("%B %d, %Y")
        if eligible:
            response = (
                f"AppleCare is not active for your {device}, but it is still within return window. "
                f"You can return it until {deadline_text}."
            )
        else:
            response = (
                f"AppleCare is not active for your {device}, and the return window ended on {deadline_text}. "
                "A replacement is not available under current policy."
            )
        return IntentResult(intent="replacement", response=response, confidence=confidence)

    def _handle_order_lookup(self, session: ConversationSession, context: IntentContext, confidence: float) -> IntentResult:
        orders = self.db.fetch_orders(session.customer_id)
        if not orders:
            return IntentResult(intent="order_lookup", response="I could not find any orders for your account.", confidence=confidence)

        limit = self._extract_order_count(context.normalized_query)
        selected_orders = orders[:limit]
        order_word = "order" if len(selected_orders) == 1 else "orders"

        lines = [f"Here are your most recent {len(selected_orders)} {order_word}:"]
        for idx, order in enumerate(selected_orders, start=1):
            order_date = self._format_date(order["order_date"])
            order_value = float(order["order_value"])
            lines.append(
                f"{idx}. {order['device']} on {order_date}."
            )

        response = " ".join(lines)
        return IntentResult(intent="order_lookup", response=response, confidence=confidence)

    def _handle_new_order(self, session: ConversationSession, context: IntentContext, confidence: float) -> IntentResult:
        if context.amount is None:
            return IntentResult(
                intent="new_order",
                response="Please tell me the new order value so I can validate it.",
                confidence=confidence,
            )

        if can_create_new_order(context.amount):
            response = f"A new order valued at ${context.amount:,.2f} can be created because it is below $10,000."
        else:
            response = f"A new order valued at ${context.amount:,.2f} cannot be created because it exceeds the $10,000 limit."

        return IntentResult(intent="new_order", response=response, confidence=confidence)

    def _handle_price(self, session: ConversationSession, context: IntentContext, confidence: float) -> IntentResult:
        target_order = self._get_target_order_for_customer(session, context)
        if target_order is None:
            return IntentResult(intent="price", response="I could not find any orders for your account.", confidence=confidence)

        response = (
            f"Your {target_order['device']} order value is ${float(target_order['order_value']):,.2f}."
        )
        return IntentResult(intent="price", response=response, confidence=confidence)

    def _handle_date(self, session: ConversationSession, context: IntentContext, confidence: float) -> IntentResult:
        target_order = self._get_target_order_for_customer(session, context)
        if target_order is None:
            return IntentResult(intent="date", response="I could not find any orders for your account.", confidence=confidence)

        purchased_date = to_date(target_order["order_date"])
        days_ago = (date.today() - purchased_date).days
        response = (
            f"Your {target_order['device']} was purchased on {purchased_date.strftime('%B %d, %Y')}, "
            f"which is {days_ago} days ago."
        )
        return IntentResult(intent="date", response=response, confidence=confidence)

    def _handle_greeting(self, session: ConversationSession, context: IntentContext, confidence: float) -> IntentResult:
        return IntentResult(
            intent="greeting",
            response="Hello. Please share your 10-digit phone number and email, then ask about return, warranty, or AppleCare.",
            confidence=confidence,
        )

    def _handle_unknown(self, session: ConversationSession, context: IntentContext, confidence: float) -> IntentResult:
        return IntentResult(
            intent="unknown",
            response="I can help with order lookup, returns, warranty, AppleCare, and new order value checks.",
            confidence=confidence,
        )

    def _ensure_verified(self, session: ConversationSession) -> tuple[bool, str]:
        if not session.phone or not session.email:
            return False, "Please provide both your 10-digit phone number and email for verification."

        if session.verified:
            return True, ""

        customer = self.db.verify_customer(session.phone, session.email)
        if not customer:
            session.verified = False
            session.customer_id = None
            session.customer_name = None
            return False, "I could not verify your account using that phone number and email."

        session.customer_id = int(customer["customer_id"])
        session.customer_name = customer.get("customer_name")
        session.verified = True
        return True, ""

    def _extract_policy_topics(self, normalized_query: str) -> list[str]:
        topics: list[str] = []
        if any(token in normalized_query for token in ["return", "refund"]):
            topics.append("return")
        if any(token in normalized_query for token in ["replace", "replacement", "swap"]):
            topics.append("replacement")
        if "warranty" in normalized_query:
            topics.append("warranty")
        if "applecare" in normalized_query or "apple care" in normalized_query:
            topics.append("applecare")
        if any(token in normalized_query for token in ["10000", "10,000", "10k", "order value", "create order", "place order"]):
            topics.append("new_order")
        if any(token in normalized_query for token in ["price", "value", "cost", "how much", "paid", "amount"]):
            topics.append("price")
        if any(token in normalized_query for token in ["purchase date", "how old", "how long ago", "bought", "date of purchase"]):
            topics.append("date")
        return topics

    def _is_generic_policy_query(self, normalized_query: str, policy_topics: list[str]) -> bool:
        if not policy_topics:
            return False

        has_policy_term = any(pattern.search(normalized_query) for pattern in self._compiled_patterns["policy"])
        personal_order_hint = any(
            token in normalized_query
            for token in ["my order", "my iphone", "my ipad", "my macbook", "latest order", "order id"]
        )
        return has_policy_term and not personal_order_hint

    def _build_policy_response(self, policy_topics: list[str]) -> str:
        parts: list[str] = []

        if "return" in policy_topics:
            parts.append("Return rule: products can be returned within 30 days from the order date.")
        if "replacement" in policy_topics:
            parts.append(
                "Replacement rule: if AppleCare is active, replacement follows AppleCare coverage; otherwise return-window rules apply."
            )
        if "warranty" in policy_topics:
            parts.append("Warranty rule: products are under warranty for 12 months from the order date.")
        if "applecare" in policy_topics:
            parts.append(
                "AppleCare rule: MacBook minimum charge is $99, iPhone includes full replacement coverage, and iPad minimum charge is $49."
            )
        if "new_order" in policy_topics:
            parts.append("New order rule: a new order can only be created when order value is below $10,000.")
        if "price" in policy_topics:
            parts.append("Price lookup needs your verified account and returns the value from your matching order.")
        if "date" in policy_topics:
            parts.append("Date lookup needs your verified account and returns the purchase date of your matching order.")

        if not parts:
            return "I can explain return, warranty, AppleCare, and new-order value rules."
        return " ".join(parts)

    def _get_target_order_for_customer(self, session: ConversationSession, context: IntentContext) -> Optional[dict]:
        orders = self.db.fetch_orders(session.customer_id)
        if not orders:
            return None
        return self._pick_target_order(orders, context.device)

    def _pick_target_order(self, orders: list[dict], canonical_device: Optional[str]) -> dict:
        if canonical_device:
            aliases = DEVICE_ALIAS_MAP.get(canonical_device, (canonical_device,))
            for order in orders:
                order_device = str(order["device"]).lower()
                if canonical_device in order_device or any(alias in order_device for alias in aliases):
                    return order
        return orders[0]

    def _extract_device_from_text(self, text: str) -> Optional[str]:
        normalized = text.lower()
        for canonical, aliases in DEVICE_ALIAS_MAP.items():
            if any(alias in normalized for alias in aliases):
                return canonical
        return None

    def _extract_amount(self, query: str) -> Optional[float]:
        match = AMOUNT_RE.search(query)
        if not match:
            return None
        try:
            return float(match.group(1))
        except ValueError:
            return None

    def _extract_order_count(self, normalized_query: str) -> int:
        # Default to first 5 recent orders unless user asks for an explicit count.
        patterns = [
            r"(?:first|top|latest|last|recent)\s+(\d{1,2})\s+orders?",
            r"(\d{1,2})\s+(?:most\s+)?(?:recent|latest|last)\s+orders?",
            r"(\d{1,2})\s+orders?",
        ]

        for pattern in patterns:
            match = re.search(pattern, normalized_query)
            if match:
                value = int(match.group(1))
                return max(1, min(20, value))

        if re.search(r"\b(most\s+recent|latest|last)\s+order\b", normalized_query):
            return 1

        if re.search(r"\bmost\s+recent\s+orders\b", normalized_query):
            return 5

        return 5

    def _format_date(self, value) -> str:
        dt = to_date(value)
        return dt.strftime("%B %d, %Y")
