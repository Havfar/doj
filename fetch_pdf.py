import requests

url = "https://www.justice.gov/epstein/files/DataSet%209/EFTA00191396.pdf"

headers = {
    "Cookie": "justiceGovAgeVerified=true"
}

response = requests.get(url, headers=headers)

if response.status_code == 200:
    with open("EFTA00191396.pdf", "wb") as f:
        f.write(response.content)
    print(f"Downloaded successfully: EFTA00191396.pdf ({len(response.content)} bytes)")
else:
    print(f"Failed to download. Status code: {response.status_code}")
