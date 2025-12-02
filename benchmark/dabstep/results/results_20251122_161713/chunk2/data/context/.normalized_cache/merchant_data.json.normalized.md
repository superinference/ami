# merchant_data.json

**Type**: JSON Structured Data

**Format**: Array of 30 objects

## Schema (Fields)

| Field | Type | Sample Value |
|-------|------|-------------|
| merchant | str | Crossfit_Hanna |
| capture_delay | str | manual |
| acquirer | list | ['gringotts', 'the_savings_and_loan_bank', 'bank_o |
| merchant_category_code | int | 7997 |
| account_type | str | F |

## Sample Objects (First 5)

### Object 1

```json
{
  "merchant": "Crossfit_Hanna",
  "capture_delay": "manual",
  "acquirer": [
    "gringotts",
    "the_savings_and_loan_bank",
    "bank_of_springfield",
    "dagoberts_vault"
  ],
  "merchant_category_code": 7997,
  "account_type": "F"
}
```

### Object 2

```json
{
  "merchant": "Martinis_Fine_Steakhouse",
  "capture_delay": "immediate",
  "acquirer": [
    "dagoberts_geldpakhuis",
    "bank_of_springfield"
  ],
  "merchant_category_code": 5812,
  "account_type": "H"
}
```

### Object 3

```json
{
  "merchant": "Belles_cookbook_store",
  "capture_delay": "1",
  "acquirer": [
    "lehman_brothers"
  ],
  "merchant_category_code": 5942,
  "account_type": "R"
}
```

### Object 4

```json
{
  "merchant": "Golfclub_Baron_Friso",
  "capture_delay": "2",
  "acquirer": [
    "medici"
  ],
  "merchant_category_code": 7993,
  "account_type": "F"
}
```

### Object 5

```json
{
  "merchant": "Rafa_AI",
  "capture_delay": "7",
  "acquirer": [
    "tellsons_bank"
  ],
  "merchant_category_code": 7372,
  "account_type": "D"
}
```

## Sample Objects (Last 2)

### Object 29

```json
{
  "merchant": "Crafty_Cuisine",
  "capture_delay": "7",
  "acquirer": [
    "tellsons_bank"
  ],
  "merchant_category_code": 5812,
  "account_type": "H"
}
```

### Object 30

```json
{
  "merchant": "Cafe_Centrale",
  "capture_delay": "manual",
  "acquirer": [
    "dagoberts_vault"
  ],
  "merchant_category_code": 7997,
  "account_type": "H"
}
```

