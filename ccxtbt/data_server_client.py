
import asyncio
from datetime import datetime
from data_server_websocket import DataServerWebsocket

class DataServerClient:
    def __init__(self, cmd, exchange, pair, start, end, tf, host='ws://127.0.0.1:8899', ssl=None):
        self.wsclient = DataServerWebsocket(host=host, ssl=ssl)
        self.end = int(datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()) * 1000
        #start = end - 60 * 60 * 1000

        start = 1612987200000
        #end = 1615581795000

    def get_candles('get.candles', exchange, symbol, start, ):

        @self.wsclient.on('data_candles')
        async def data_candles(candles):
            self.candles = candles
            await self.wsclient.stop()

        self.wsclient.run(self.cmd, self.exchange, self.symbol, self.start, self.end, self.tf)

        return self.candles
