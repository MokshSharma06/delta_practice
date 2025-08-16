
import yfinance as yf

nifty = yf.Ticker("^NSEI").history(period="1d", interval="1m")
sensex = yf.Ticker("^BSESN").history(period="1d", interval="1m")

print(nifty.tail())
print(sensex.tail())
