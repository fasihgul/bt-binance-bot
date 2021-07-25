import ccxt

class DataScrape():
    def __init__(self,end,output,exchange_id, max_retries, symbol, timeframe, since, limit,**kwargs):
        self.exchange = getattr(ccxt, exchange_id)({
            'enableRateLimit': True,  # required by the Manual
        })
        if isinstance(since, str):
            self.since = self.exchange.parse8601(since)
        if isinstance(end,str):
            self.end =self.exchange.parse8601(end)
        self.exchange.load_markets()
        self.timeframe = timeframe
        self.limit = limit
        self.max_retries = max_retries
        self.symbol = symbol

    def retry_fetch_ohlcv(self,exchange, max_retries, symbol, timeframe, since, limit):
        num_retries = 0
        try:
            num_retries += 1
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since, limit)
            # print('Fetched', len(ohlcv), symbol, 'candles from', exchange.iso8601 (ohlcv[0][0]), 'to', exchange.iso8601 (ohlcv[-1][0]))
            return ohlcv
        except Exception:
            if num_retries > max_retries:
                raise Exception('Failed to fetch', timeframe, symbol, 'OHLCV in', max_retries, 'attempts')


    def __fetch_ohlcv(self):
        self.timeframe_duration_in_seconds = self.exchange.parse_self.timeframe(self.timeframe)
        self.timeframe_duration_in_ms = self.timeframe_duration_in_seconds * 1000
        timedelta = self.limit * self.timeframe_duration_in_ms
        now = self.exchange.milliseconds()
        all_ohlcv = []
        fetch_since = self.since
        while fetch_since < now:
            ohlcv = self.retry_fetch_ohlcv(self.exchange, self.max_retries, self.symbol, self.timeframe, fetch_since, self.limit)
            fetch_since = (ohlcv[-1][0] + 1) if len(ohlcv) else (fetch_since + timedelta)
            all_ohlcv = all_ohlcv + ohlcv
            if len(all_ohlcv):
                print(len(all_ohlcv), 'candles in total from', self.exchange.iso8601(all_ohlcv[0][0]), 'to',
                      self.exchange.iso8601(all_ohlcv[-1][0]))
            else:
                print(len(all_ohlcv), 'candles in total from', self.exchange.iso8601(fetch_since))
        return self.exchange.filter_by_since_self.limit(all_ohlcv, self.since, None, key=0)
