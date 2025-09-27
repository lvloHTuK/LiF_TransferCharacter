# test_import.py
try:
    from schemas import EventResponse
    print("✓ EventResponse imported successfully")
except ImportError as e:
    print(f"✗ Error importing EventResponse: {e}")

try:
    import schemas
    print("✓ schemas module imported successfully")
except ImportError as e:
    print(f"✗ Error importing schemas module: {e}")