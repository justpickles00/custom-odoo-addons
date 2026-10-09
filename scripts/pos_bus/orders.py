"""Native POS records for the pinned Odoo master sync_from_ui API.

No Odoo import or elevated/preparation context is used. Each RPC response must
be applied before the next transition, so UPDATE commands use actual server IDs.
UUIDs remain stable for a given run/device/order, including retries.
"""
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from uuid import NAMESPACE_URL, uuid5


def money(value):
    return float(Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def timestamp():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


class Order:
    def __init__(self, fixture, worker, sequence, table_index=0, user_id=None):
        self.fixture = fixture
        self.worker = str(worker)
        self.sequence = sequence
        self.id = None
        self.uuid = self._uuid("order")
        self.date_order = timestamp()
        self.write_date = None
        self.user_id = user_id or fixture["users"][0]["id"]
        self.table_id = fixture["tables"][table_index]["id"]
        self.lines = [
            {"uuid": self._uuid(f"line-{i}"), "product": fixture["products"][product_index],
             "qty": qty, "id": None}
            for i, (product_index, qty) in enumerate([(0, 1), (2, 2)])
        ]
        self.prep_id = None
        self.prep_line_ids = {}
        self.payment_id = None
        self.state = "draft"
        self.prepared = False

    def _uuid(self, role):
        key = f"posbus/{self.fixture.get('db', '')}/{self.worker}/{self.sequence}/{role}"
        return str(uuid5(NAMESPACE_URL, key))

    def context(self, device_identifier):
        return {"config_id": self.fixture["config_id"],
                "device_identifier": device_identifier,
                "current_order_uuid": self.uuid,
                "allowed_company_ids": [self.fixture["company_id"]]}

    @property
    def totals(self):
        untaxed = sum(money(line["product"]["price"] * line["qty"]) for line in self.lines)
        tax = sum(money(line["product"]["price"] * line["qty"] * self.fixture.get("tax_rate", 0.1)) for line in self.lines)
        return {"untaxed": money(untaxed), "tax": money(tax), "total": money(untaxed + tax)}

    def _line_values(self, line):
        product = line["product"]
        base = money(product["price"] * line["qty"])
        return {"uuid": line["uuid"], "product_id": product["id"],
                "full_product_name": product["name"], "qty": line["qty"],
                "price_unit": product["price"], "price_subtotal": base,
                "price_subtotal_incl": money(base + money(base * self.fixture.get("tax_rate", 0.1))),
                "discount": 0, "tax_ids": list(product["tax_ids"]),
                "price_type": "original", "customer_note": line.get("customer_note", ""), "note": ""}

    def _payload(self, state="draft", dirty_lines=()):
        total = self.totals
        return {"uuid": self.uuid, "name": f"Bus Lab {self.worker}/{self.sequence}",
                "state": state, "session_id": self.fixture["session_id"],
                "company_id": self.fixture["company_id"], "user_id": self.user_id,
                "table_id": self.table_id, "customer_count": 2, "date_order": self.date_order,
                "pricelist_id": self.fixture.get("pricelist_id", False),
                "fiscal_position_id": False, "partner_id": False, "to_invoice": False,
                "amount_total": total["total"], "amount_tax": total["tax"],
                "amount_return": 0, "amount_paid": total["total"] if state == "paid" else 0,
                "lines": [[1 if line["id"] else 0, line["id"] or 0, self._line_values(line)]
                          for line in dirty_lines], "payment_ids": []}

    def create(self):
        if self.id:
            raise ValueError("Order is already persisted")
        return self._payload(dirty_lines=self.lines)

    def edit(self):
        if not self.id or self.state != "draft":
            raise ValueError("Draft edit requires a persisted draft")
        # One additional taxed soup and a realistic customer note.
        self.lines[0]["qty"] = 2
        self.lines[0]["customer_note"] = "Please serve together"
        return self._payload(dirty_lines=self.lines[:1])

    def prepare(self):
        if not self.id or self.state != "draft" or self.prepared:
            raise ValueError("Preparation requires a persisted, unprepared draft")
        payload = self._payload()
        prep_lines = []
        for index, line in enumerate(self.lines):
            if not line["id"]:
                raise ValueError("Preparation requires persisted order-line IDs")
            prep_uuid = self._uuid(f"prep-line-{index}")
            prep_lines.append([0, 0, {
                "uuid": prep_uuid, "product_id": line["product"]["id"],
                "pos_order_line_id": line["id"],
                "quantity": line["qty"], "cancelled": 0,
                "attribute_value_ids": [],
            }])
        payload["prep_order_ids"] = [[0, 0, {
            "uuid": self._uuid("prep-order"), "prep_line_ids": prep_lines,
        }]]
        return payload

    def pay(self):
        if not self.id or self.state != "draft":
            raise ValueError("Payment requires a persisted draft")
        self.date_order = timestamp()
        payload = self._payload("paid")
        payload["payment_ids"] = [[1 if self.payment_id else 0, self.payment_id or 0, {
            "uuid": self._uuid("payment"), "amount": self.totals["total"],
            "payment_method_id": self.fixture["payment_method_id"],
            "payment_date": timestamp(), "name": "Bus Lab cash tender",
        }]]
        return payload

    def cancel(self):
        """Alternative sync cancellation; normal UI uses cancel_order_from_pos.

        Client should normally call cancel_order_from_pos([[self.id]]) and apply
        its returned pos.order records. This payload exercises sync state cancel.
        """
        if not self.id or self.state != "draft":
            raise ValueError("Cancellation requires a persisted draft")
        return self._payload("cancel")

    def apply_response(self, response):
        orders = [record for record in response.get("pos.order", []) if record.get("uuid") == self.uuid]
        if len(orders) != 1:
            raise AssertionError(f"Expected own UUID {self.uuid}; received {response.get('pos.order')}")
        order = orders[0]
        if self.id and order["id"] != self.id:
            raise AssertionError("Server changed order identity")
        self.id, self.state = order["id"], order["state"]
        self.write_date = order.get("write_date")
        by_uuid = {record["uuid"]: record for record in response.get("pos.order.line", [])}
        for line in self.lines:
            if line["uuid"] in by_uuid:
                line["id"] = by_uuid[line["uuid"]]["id"]
        for record in response.get("pos.prep.order", []):
            if record.get("uuid") == self._uuid("prep-order"):
                self.prep_id = record["id"]
                self.prepared = True
        for record in response.get("pos.prep.line", []):
            self.prep_line_ids[record["uuid"]] = record["id"]
        for record in response.get("pos.payment", []):
            if record.get("uuid") == self._uuid("payment"):
                self.payment_id = record["id"]
        return order

    def assert_response(self, response, expected_state="draft"):
        order = self.apply_response(response)
        assert order["state"] == expected_state, (order["state"], expected_state)
        for key, expected in [("amount_total", self.totals["total"]),
                              ("amount_tax", self.totals["tax"])]:
            assert abs(order[key] - expected) < 0.005, (key, order[key], expected)
        if expected_state == "paid":
            assert abs(order["amount_paid"] - self.totals["total"]) < 0.005
            assert self.payment_id is not None, "Server did not return payment UUID"
        if response.get("pos.order.line"):
            own_lines = {line["uuid"]: line for line in response["pos.order.line"]}
            for line in self.lines:
                assert line["uuid"] in own_lines, "Server did not return order line UUID"
                assert own_lines[line["uuid"]]["qty"] == line["qty"]
        if self.prepared and response.get("pos.prep.line"):
            by_uuid = {line["uuid"]: line for line in response["pos.prep.line"]}
            for index, line in enumerate(self.lines):
                prep = by_uuid[self._uuid(f"prep-line-{index}")]
                relation = prep["pos_order_line_id"]
                relation = relation[0] if isinstance(relation, list) else relation
                assert relation == line["id"], "Preparation graph did not resolve server line IDs"
                assert prep["quantity"] == line["qty"]
        return order
