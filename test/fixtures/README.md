# Orders Validator

`vendor-orders-validator.json` is an unmodified copy of vendor-api's
`db/validators/orders.json` at commit `303a01b425246b588f027145db04abe53049882e`.
The real schema is checked in so the integration tests do not depend on a
separate vendor-api checkout. In particular, `invoice_date: null` must be
rejected by the same strict validator used by that service.
