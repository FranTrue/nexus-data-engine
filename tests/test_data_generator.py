from ingestion.data_generator import BASE_CUSTOMERS, CITIES, TIERS, generate_customer_data


def test_generate_customer_data_shape():
    df = generate_customer_data()
    assert len(df) == len(BASE_CUSTOMERS)
    assert list(df.columns) == ["customer_id", "first_name", "last_name", "city", "tier", "updated_at"]


def test_generate_customer_data_values_are_valid():
    df = generate_customer_data()
    assert set(df["city"]).issubset(set(CITIES))
    assert set(df["tier"]).issubset(set(TIERS))
    assert df["customer_id"].is_unique
