import requests

r = requests.post(
    "http://localhost:11434/api/embed",
    json={
        "model": "minilm-embed-local",   # ✅ YOUR WORKING MODEL
        "input": "UPI transaction failure due to timeout"
    },
    proxies={"http": None, "https": None},  # Windows corporate proxy bypass
    timeout=30
)

print("Status:", r.status_code)
print("Response:", r.json())