"""Run with exec(open(...).read()) in Odoo shell on the isolated template DB.

Creates one bounded restaurant, real non-manager POS users and an open cash
session. This is deliberately not an importable Odoo addon or a load-test hook.
"""
import json
import os
from pathlib import Path


def build_fixture(env):
    if env.cr.dbname != "posbus_template":
        raise RuntimeError("Fixture writes are restricted to NEW posbus_template")
    if env["pos.config"].search_count([("name", "=", "POS Bus Laboratory")]):
        raise RuntimeError("Fixture already exists; use a fresh template database")

    company = env.ref("base.main_company")
    admin = env.ref("base.user_admin")
    env = env(context={**env.context, "allowed_company_ids": [company.id]})
    if not company.chart_template:
        env["account.chart.template"].try_loading("generic_coa", company)

    categories = env["pos.category"].create([
        {"name": "Bus Lab Food", "sequence": 1},
        {"name": "Bus Lab Drinks", "sequence": 2},
    ])
    tax = env["account.tax"].create({
        "name": "Bus Lab Sales 10%", "amount": 10.0,
        "amount_type": "percent", "type_tax_use": "sale",
        "company_id": company.id, "price_include_override": "tax_excluded",
    })
    products = env["product.product"].create([
        {"name": name, "list_price": price, "type": "consu",
         "is_storable": False, "available_in_pos": True,
         "company_id": company.id, "pos_categ_ids": [(6, 0, [category.id])],
         "taxes_id": [(6, 0, tax.ids)]}
        for name, price, category in [
            ("Bus Lab Soup", 10.0, categories[0]),
            ("Bus Lab Sandwich", 15.0, categories[0]),
            ("Bus Lab Coffee", 5.0, categories[1]),
            ("Bus Lab Juice", 4.0, categories[1]),
        ]
    ])
    journal, payment_methods = env["pos.config"]._create_journal_and_payment_methods(
        cash_journal_vals={"name": "Bus Lab Cash", "show_on_dashboard": False}
    )
    # Native helper returns IDs, not a payment-method recordset.
    config = env["pos.config"].create({
        "name": "POS Bus Laboratory", "company_id": company.id,
        "journal_id": journal.id, "payment_method_ids": [(6, 0, payment_methods)],
        "module_pos_restaurant": True, "module_pos_hr": False,
        "limit_categories": True,
        "iface_available_categ_ids": [(6, 0, categories.ids)],
        "use_presets": False,
    })
    cash = config.payment_method_ids.filtered(lambda p: p.type == "cash")[:1]
    if not cash:
        raise RuntimeError("Native POS configuration did not create a cash method")
    config.payment_method_ids = cash
    # A logical configuration entry only. No browser or printer I/O is used.
    printer = env["pos.printer"].create({
        "name": "Bus Lab Logical Kitchen (no I/O)",
        "printer_type": "epson_epos", "use_type": "preparation",
        "printer_ip": "192.0.2.1", "use_cashdrawer": False,
        "product_categories_ids": [(6, 0, categories.ids)],
    })
    config.preparation_printer_ids = printer
    floor = config.floor_ids[:1]
    floor.name = "Bus Lab Main Floor"
    # The restaurant config creates table 1; fill a total of 32 disjoint tables.
    existing = {t.table_number: t for t in floor.table_ids}
    for number in range(1, 33):
        values = {
            "table_number": number, "floor_id": floor.id, "seats": 4,
            "floor_plan_layout": {"top": 30 + ((number - 1) // 8) * 100,
                                  "left": 30 + ((number - 1) % 8) * 100,
                                  "width": 70, "height": 70,
                                  "color": "green", "shape": "square"},
        }
        if number in existing:
            existing[number].write(values)
        else:
            env["restaurant.table"].create(values)

    group = env.ref("point_of_sale.group_pos_user")
    users = []
    for number in range(1, 9):
        login = f"posbus{number:02d}"
        password = f"posbus-lab-user-{number:02d}"
        if env["res.users"].search_count([("login", "=", login)]):
            raise RuntimeError(f"Unexpected existing fixture login: {login}")
        user = env["res.users"].with_context(no_reset_password=True).create({
            "name": f"POS Bus Operator {number:02d}", "login": login,
            "password": password, "company_id": company.id,
            "company_ids": [(6, 0, company.ids)],
            "group_ids": [(6, 0, [env.ref("base.group_user").id, group.id])],
            "lang": "en_US", "tz": "UTC",
        })
        if user.has_group("point_of_sale.group_pos_manager") or user.has_group("base.group_system"):
            raise RuntimeError("Fixture operator unexpectedly has elevated privileges")
        users.append({"id": user.id, "login": login, "password": password})

    # The admin opens the shared session; operators perform all simulated RPCs.
    config.with_user(admin).open_ui()
    session = config.current_session_id
    session.with_user(admin).set_opening_control(0, "Isolated bus investigation")
    if session.state != "opened":
        raise RuntimeError(f"Expected opened session, got {session.state}")
    tables = floor.table_ids.sorted("table_number")
    manifest = {
        "schema_version": 1, "db": env.cr.dbname,
        "config_id": config.id, "session_id": session.id,
        "access_token": config.access_token,
        "config_access_token": config.access_token,
        "company_id": company.id, "pricelist_id": config.pricelist_id.id or False,
        "currency": {"id": config.currency_id.id, "name": config.currency_id.name,
                     "rounding": config.currency_id.rounding,
                     "decimal_places": config.currency_id.decimal_places},
        "users": users, "tables": [{"id": t.id, "table_number": t.table_number} for t in tables],
        "products": [{"id": p.id, "name": p.name, "price": p.lst_price,
                      "tax_ids": p.taxes_id.ids, "category_id": p.pos_categ_ids[:1].id}
                     for p in products],
        "tax_id": tax.id, "tax_rate": 0.10,
        "payment_method_id": cash.id, "preparation_printer_id": printer.id,
        "floor_id": floor.id, "session_state": session.state,
        "operator_group": "point_of_sale.group_pos_user",
        "printer_io": False,
    }
    env.cr.commit()
    path = Path(os.environ.get("POSBUS_FIXTURE_PATH", "/home/coder/playground/pos-bus-investigation/fixture.json"))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest, indent=2) + "\n")
    path.chmod(0o600)
    print(json.dumps({"fixture_path": str(path), "db": env.cr.dbname,
                      "config_id": config.id, "session_id": session.id,
                      "operator_count": len(users), "table_count": len(tables)}))
    return manifest


if "env" in globals():
    fixture_manifest = build_fixture(env)
