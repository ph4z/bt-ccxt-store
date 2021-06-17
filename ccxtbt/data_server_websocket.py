import orjson
import asyncio

from generic_websocket import GenericWebsocket

class DataServerWebsocket(GenericWebsocket):
  '''
  Basic websocket client that simply reads data from the DataServer. This instance
  of the websocket should only ever be used in backtest mode since it isnt capable
  of handling orders.

  Events:
    - connected: called when a connection is made
    - done: fires when the backtest has finished running
  '''
  DS_CONNECT = 'connected'
  DS_ERROR = 'error'
  BT_END = 'bt.end'
  BT_CANDLE = 'bt.candle'
  BT_TRADE = 'bt.trade'
  BT_START = 'bt.start'
  BT_SYNC_START = 'bt.sync.start'
  BT_SYNC_END = 'bt.sync.end'
  DT_SYNC_START = 'data.sync.start'
  DT_SYNC_END = 'data.sync.end'
  DT_CANDLE = 'data.candles'
  DT_TRADE = 'data.trades'


  def __init__(self, eventEmmitter=None, host='ws://localhost:8899', *args, **kwargs):
    super(DataServerWebsocket, self).__init__(host, *args, **kwargs)

  def run(self, cmd, exchange, symbol, fromDate, toDate, syncTrades=True, syncCandles=True, tf='30m', syncMissing=True):
    self.cmd = cmd
    self.exchange = exchange
    self.fromDate = fromDate
    self.toDate = toDate
    self.tf = tf
    self.sync = syncCandles
    self.syncTrades = syncTrades
    self.syncCandles = syncCandles
    self.syncMissing = syncMissing
    self.symbol = symbol
    loop = asyncio.get_event_loop()
    loop.run_until_complete(super(DataServerWebsocket, self)._run_socket())
  
  async def on_message(self, socketId, message):
    self.logger.debug(message)
    msg = orjson.loads(message)
    eType = msg[0]
    if eType == self.BT_SYNC_START or eType == self.DT_SYNC_START:
      self.logger.info("Syncing data with backtest server, please wait...")
    elif eType == self.BT_SYNC_END or eType == self.DT_SYNC_END:
      self.logger.info("Syncing complete.")
    elif eType == self.BT_START:
      self.logger.info("Backtest data stream starting...")
    elif eType == self.BT_END:
      self.logger.info("Backtest data stream complete.")
      await self.on_close()
    elif eType == 'data.markets':
      pass
    elif eType == 'error':
      await self.on_error(msg[1])
    elif eType == self.BT_CANDLE: 
      await self._on_candle(msg)
    elif eType == self.DT_CANDLE:
      await self._on_candles(msg)
    elif eType == self.BT_TRADE or eType == self.DT_TRADE:
      await self._on_trade(msg)
    elif eType == self.DS_CONNECT:
      await self.on_open(socketId)
    else:
      self.logger.warn('Unknown websocket command: {}'.format(msg[0]))

  def _get_markets_string(self):
    data = '["get.markets", {}, {}]'.format(self.exchange, self.filter)
    return data

  def _get_trades_string(self):
    data = '["get.trades", {}, "{}", {}, {}, {}]'.format(self.exchange,
        self.symbol, 'trades', self.fromDate, self.toDate)
    return data

  def _get_candles_string(self):
    data = ["get.candles", self.exchange, self.symbol, self.tf, 'trades', self.fromDate, self.toDate]
    return data
  
  def _exec_bt_string(self):
    data = '["exec.bt", ["{}", {}, {}, "{}", "{}", {}, {}, {}]]'.format(self.exhange,
        self.fromDate, self.toDate, self.symbol, self.tf, orjson.dumps(self.syncCandles),
        orjson.dumps(self.syncTrades), orjson.dumps(self.sync))
    return data
  
  async def on_open(self, socketId):
    self._emit('connected')
    if self.cmd == 'get.candles':
        data = self._get_candles_string()
    elif self.cmd == 'get.markets':
        data = self._get_markets_string()
    elif self.cmd == 'exec.bt':
        data = self._exec_bt_string()
    elif self.cmd == 'get.trades':
        data = self._get_trades_string()
    await self.get_socket(socketId).ws.send(orjson.dumps(data))
  
  async def _on_candle(self, data):
    candle = data[3]
    self._emit('new_candle', candle)

  async def _on_candles(self, data):
    candles = data[8]
    self._emit('data_candles', candles)
  
  async def _on_trade(self, data):
    trade = data[2]
    self._emit('new_trade', trade)
