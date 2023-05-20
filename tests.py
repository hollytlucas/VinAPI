import requests


def test_lookup_vin_not_cached():
    vin = '1XP5DB9X7XD487964'
    url = f"http://localhost:8000/lookup/{vin}"
    response = requests.get(url)
    print("Valid lookup, cached should be False: ", response.json())


def test_lookup_vin_cached():
    vin = '1XP5DB9X7XD487964'
    url = f"http://localhost:8000/lookup/{vin}"
    response = requests.get(url)

    print("Valid lookup, cached should be True: ", response.json())


def test_remove_entry_that_exists():
    vin = '1XP5DB9X7XD487964'
    url = f"http://localhost:8000/remove/{vin}"
    response = requests.get(url)

    print("Valid removal request with existent vin in cache: ", response.json())


def test_remove_entry_that_does_not_exist():
    vin = '1XP5DB9X7XD487964'
    url = f"http://localhost:8000/remove/{vin}"
    response = requests.get(url)

    print("Valid removal request but nonexistent vin in cache: ", response.json())


def test_export_table():
    url = "http://localhost:8000/export"
    response = requests.get(url)

    file_path = "exported_table.parquet"
    with open(file_path, "wb") as file:
        file.write(response.content)

    print("Valid Export: ", f"Exported table saved to: {file_path}")


def test_lookup_vin_with_invalid_length():
    vin = "123456789012345"  # VIN with 16 characters
    url = f"http://localhost:8000/lookup/{vin}"
    response = requests.get(url)

    print("Invalid length: ", response.text)


def test_lookup_vin_with_invalid_alphanum():
    vin = "1234567-011234555"  # VIN with 16 characters
    url = f"http://localhost:8000/lookup/{vin}"
    response = requests.get(url)

    print("Invalid alphanum: ", response.text)


def test_remove_vin_with_nonexitent_vin():
    vin = "yyyyyyyyyyyyyyyyy"  # VIN with 16 characters
    url = f"http://localhost:8000/lookup/{vin}"
    response = requests.get(url)

    print("Nonexistent VIN in VPIC database: ", response.text)


def test_remove_vin_with_invalid_length():
    vin = "123456789012345"  # VIN with 16 characters
    url = f"http://localhost:8000/remove/{vin}"
    response = requests.get(url)

    print("Invalid length: ", response.text)
    

def test_remove_vin_with_invalid_alphanum():
    vin = "1234567890--23457"  # VIN with 16 characters
    url = f"http://localhost:8000/lookup/{vin}"
    response = requests.get(url)

    print("Invalid alphanum: ", response.text)


if __name__ == "__main__":
    test_lookup_vin_not_cached()
    test_lookup_vin_cached()
    test_remove_entry_that_exists()
    test_remove_entry_that_does_not_exist()
    test_export_table()
    test_lookup_vin_with_invalid_length()
    test_lookup_vin_with_invalid_alphanum()
    test_remove_vin_with_nonexitent_vin()
    test_remove_vin_with_invalid_length()
    test_remove_vin_with_invalid_alphanum()



