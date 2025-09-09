import time
import requests


url_symbols = "https://fapi.binance.com/fapi/v1/exchangeInfo"
url_history = "https://fapi.binance.com/fapi/v1/fundingRate"


def extract_symbols():
    rp = requests.get(url_symbols)
    data = rp.json()

    with open("view_data.txt", "w") as f:
        f.write(str(data["symbols"]))

    _i = 0
    _data = []
    for i in data["symbols"]:
        if i["contractType"] == "PERPETUAL" and i["status"] == "TRADING":
            _data.append(i["symbol"])
    return _data


def extract_funding_rate_history():
    symbols = extract_symbols()
    for symbol in symbols:
        params = {
            "symbol": symbol,
            "limit": 1000,
            "startTime": 0,
            "endTime": int(time.time() * 1000),
        }
        r = requests.get(url=url_history, params=params)
        data = r.json()
        print(data)
        time.sleep(2)


extract_funding_rate_history()
