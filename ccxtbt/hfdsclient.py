import datetime
import orjson
import websockets
import pandas as pd
import asyncio

def checkLastCandle(fcandle, exchange, symbol, tf, to):
    last = fcandle[exchange][symbol][tf][1]
    # If endtime is > last candle + tf 
    if to.timestamp() * 1000 > last + convert_to_seconds(tf):
        print(symbol, "end date too high, auto adjusted to", last)

def checkFirstCandle(exchange, symbol, tf, since):
    with open('first_candle.json', 'r') as f:
        fcandle = orjson.loads(f.read())
    fts = fcandle[exchange][symbol][tf][0]
    if since.timestamp() * 1000 < fts:
        print(symbol, "From date too low, auto adjusted to", fts)
        since = datetime.fromtimestamp(fts/1000) 
    return since


def runGetCandles(*args, **kwargs):
    loop = asyncio.get_event_loop()
    candles = loop.run_until_complete(getCandles(*args, **kwargs))
    return [ (candle['mts'], candle['open'], candle['high'], candle['low'], candle['close'], candle['volume']) for candle in candles ]

async def getCandles(exchange, symbol, tf, fromdate, todate=None, url='ws://127.0.0.1:45000'):
    if not todate:
        tsto = int(datetime.datetime.utcnow().timestamp()) * 1000

    async with websockets.connect(url, max_size=None, close_timeout=3600) as ws:
        req = ['get.candles', exchange, {"uiID":symbol} , tf, fromdate, tsto]
        #req = ['get.candles', exchange, {"restID":symbol.replace('/', '')} , tf, fromdate, tsto]
        await ws.send(orjson.dumps(req))
        while True:
            data = await ws.recv()
            candles = orjson.loads(data)
            if candles[0] == "data.candles":
                return candles[6]
            #else:
                #print(candles)
        await ws.close()





