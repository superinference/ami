# fees.json

**Type**: JSON Structured Data

**Format**: Array of 1000 objects

## Schema (Fields)

| Field | Type | Sample Value |
|-------|------|-------------|
| ID | int | 1 |
| card_scheme | str | TransactPlus |
| account_type | list | [] |
| capture_delay | NoneType | null |
| monthly_fraud_level | NoneType | null |
| monthly_volume | NoneType | null |
| merchant_category_code | list | [8000, 8011, 8021, 8031, 8041, 7299, 9399, 8742] |
| is_credit | bool | False |
| aci | list | ['C', 'B'] |
| fixed_amount | float | 0.1 |
| rate | int | 19 |
| intracountry | NoneType | null |

## Sample Objects (First 5)

### Object 1

```json
{
  "ID": 1,
  "card_scheme": "TransactPlus",
  "account_type": [],
  "capture_delay": null,
  "monthly_fraud_level": null,
  "monthly_volume": null,
  "merchant_category_code": [
    8000,
    8011,
    8021,
    8031,
    8041,
    7299,
    9399,
    8742
  ],
  "is_credit": false,
  "aci": [
    "C",
    "B"
  ],
  "fixed_amount": 0.1,
  "rate": 19,
  "intracountry": null
}
```

### Object 2

```json
{
  "ID": 2,
  "card_scheme": "GlobalCard",
  "account_type": [],
  "capture_delay": null,
  "monthly_fraud_level": ">8.3%",
  "monthly_volume": null,
  "merchant_category_code": [
    3000,
    3001,
    3002,
    3003,
    7011,
    7032,
    7512,
    7513
  ],
  "is_credit": null,
  "aci": [
    "B"
  ],
  "fixed_amount": 0.13,
  "rate": 86,
  "intracountry": 0.0
}
```

### Object 3

```json
{
  "ID": 3,
  "card_scheme": "TransactPlus",
  "account_type": [],
  "capture_delay": ">5",
  "monthly_fraud_level": null,
  "monthly_volume": null,
  "merchant_category_code": [
    4111,
    4121,
    4131,
    4411,
    4511,
    4789,
    7513,
    7523
  ],
  "is_credit": true,
  "aci": [
    "C",
    "A"
  ],
  "fixed_amount": 0.09,
  "rate": 16,
  "intracountry": 0.0
}
```

### Object 4

```json
{
  "ID": 4,
  "card_scheme": "NexPay",
  "account_type": [],
  "capture_delay": null,
  "monthly_fraud_level": null,
  "monthly_volume": null,
  "merchant_category_code": [
    8062,
    8011,
    8021,
    7231,
    7298,
    7991,
    8049
  ],
  "is_credit": null,
  "aci": [
    "C",
    "A"
  ],
  "fixed_amount": 0.11,
  "rate": 25,
  "intracountry": 1.0
}
```

### Object 5

```json
{
  "ID": 5,
  "card_scheme": "GlobalCard",
  "account_type": [],
  "capture_delay": "<3",
  "monthly_fraud_level": null,
  "monthly_volume": ">5m",
  "merchant_category_code": [
    5411,
    5412,
    5499,
    5912,
    5812,
    5813,
    5911,
    5983
  ],
  "is_credit": false,
  "aci": [],
  "fixed_amount": 0.13,
  "rate": 69,
  "intracountry": null
}
```

## Sample Objects (Last 2)

### Object 999

```json
{
  "ID": 999,
  "card_scheme": "SwiftCharge",
  "account_type": [
    "H",
    "R"
  ],
  "capture_delay": null,
  "monthly_fraud_level": ">8.3%",
  "monthly_volume": null,
  "merchant_category_code": [
    5814,
    5815,
    5816,
    7832,
    7922,
    7995,
    7999,
    5813
  ],
  "is_credit": false,
  "aci": [],
  "fixed_amount": 0.1,
  "rate": 76,
  "intracountry": null
}
```

### Object 1000

```json
{
  "ID": 1000,
  "card_scheme": "TransactPlus",
  "account_type": [],
  "capture_delay": null,
  "monthly_fraud_level": null,
  "monthly_volume": null,
  "merchant_category_code": [
    4111,
    4121,
    4131,
    4411,
    4511,
    4789,
    7513,
    7523
  ],
  "is_credit": false,
  "aci": [],
  "fixed_amount": 0.11,
  "rate": 76,
  "intracountry": 0.0
}
```

