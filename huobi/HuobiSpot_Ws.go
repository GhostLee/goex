package huobi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	. "github.com/nntaoli-project/goex"
	"github.com/nntaoli-project/goex/internal/logger"
	"strings"
	"sync"
	"time"
)

type SpotTradeResponse struct {
	Id   int64
	Ts   int64
	Data []struct {
		TradeId   int64 `json:"tradeId"`
		Amount    float64
		Price     float64
		Direction string
		Ts        int64
	}
}

type SpotWs struct {
	*WsBuilder
	sync.Once
	wsConn *WsConn

	candleCallback func(*Candle)
	tickerCallback func(*Ticker)
	depthCallback  func(*Depth)
	tradeCallback  func(*Trade)
}

func NewSpotWs() *SpotWs {
	ws := &SpotWs{
		WsBuilder: NewWsBuilder(),
	}
	ws.WsBuilder = ws.WsBuilder.
		WsUrl("wss://api.huobi.pro/ws").
		AutoReconnect().
		DecompressFunc(GzipDecompress).
		ProtoHandleFunc(ws.handle)
	return ws
}

func (ws *SpotWs) DepthCallback(call func(depth *Depth)) {
	ws.depthCallback = call
}

func (ws *SpotWs) TickerCallback(call func(ticker *Ticker)) {
	ws.tickerCallback = call
}
func (ws *SpotWs) TradeCallback(call func(trade *Trade)) {
	ws.tradeCallback = call
}

func (ws *SpotWs) CandleCallback(call func(candle *Candle)) {
	ws.candleCallback = call
}

func (ws *SpotWs) connectWs() {
	ws.Do(func() {
		ws.wsConn = ws.WsBuilder.Build()
	})
}

func (ws *SpotWs) subscribe(sub map[string]interface{}) error {
	ws.connectWs()
	return ws.wsConn.Subscribe(sub)
}

func (ws *SpotWs) SubscribeDepth(pair CurrencyPair) error {
	if ws.depthCallback == nil {
		return errors.New("please set depth callback func")
	}
	return ws.subscribe(map[string]interface{}{
		"id":  "spot.depth",
		"sub": fmt.Sprintf("market.%s.mbp.refresh.20", pair.ToLower().ToSymbol(""))})
}

func (ws *SpotWs) SubscribeTicker(pair CurrencyPair) error {
	if ws.tickerCallback == nil {
		return errors.New("please set ticker call back func")
	}
	return ws.subscribe(map[string]interface{}{
		"id":  "spot.ticker",
		"sub": fmt.Sprintf("market.%s.detail", pair.ToLower().ToSymbol("")),
	})
}

func (ws *SpotWs) SubscribeTrade(pair CurrencyPair) error {
	if ws.tickerCallback == nil {
		return errors.New("please set ticker call back func")
	}
	return ws.subscribe(map[string]interface{}{
		"id":  "spot.trade",
		"sub": fmt.Sprintf("market.%s.trade.detail", pair.ToLower().ToSymbol(""))},
	)
}

func (ws *SpotWs) SubscribeCandle(pair CurrencyPair, period KlinePeriod) error {
	if ws.candleCallback == nil {
		return errors.New("please set candle call back func")
	}
	periodS, isOk := _INERNAL_KLINE_PERIOD_CONVERTER[int(period)]
	if isOk != true {
		periodS = "1min"
	}
	return ws.subscribe(map[string]interface{}{
		"id":  "spot.candle",
		"sub": fmt.Sprintf("market.%s.kline.%s", pair.ToLower().ToSymbol(""), periodS)},
	)
}

func (ws *SpotWs) parseTrade(r SpotTradeResponse) []Trade {
	var trades []Trade
	for _, v := range r.Data {
		trades = append(trades, Trade{
			Tid:    v.TradeId,
			Price:  v.Price,
			Amount: v.Amount,
			Type:   AdaptTradeSide(v.Direction),
			Date:   v.Ts})
	}
	return trades
}
func (ws *SpotWs) handle(msg []byte) error {
	if bytes.Contains(msg, []byte("ping")) {
		pong := bytes.ReplaceAll(msg, []byte("ping"), []byte("pong"))
		ws.wsConn.SendMessage(pong)
		return nil
	}

	var resp WsResponse
	err := json.Unmarshal(msg, &resp)
	if err != nil {
		return err
	}
	currencyPair := ParseCurrencyPairFromSpotWsCh(resp.Ch)
	if strings.Contains(resp.Ch, "mbp.refresh") {
		var (
			depthResp DepthResponse
		)

		err := json.Unmarshal(resp.Tick, &depthResp)
		if err != nil {
			return err
		}

		dep := ParseDepthFromResponse(depthResp)
		dep.Pair = currencyPair
		dep.UTime = time.Unix(0, resp.Ts*int64(time.Millisecond))
		ws.depthCallback(&dep)

		return nil
	}
	if strings.Contains(resp.Ch, "kline") {
		var (
			klineResp KlineResponse
		)
		periodS := strings.Split(resp.Ch, ".")[3]
		err := json.Unmarshal(resp.Tick, &klineResp)
		if err != nil {
			return err
		}
		period := KLINE_PERIOD_1MIN
		for idx, value := range _INERNAL_KLINE_PERIOD_CONVERTER{
			if periodS == value {
				period = idx
			}
		}
		ws.candleCallback(&Candle{
			Pair:      currencyPair,
			Period: KlinePeriod(period),
			Timestamp: klineResp.ID,
			Open:      klineResp.Open,
			Close:     klineResp.Close,
			High:      klineResp.High,
			Low:       klineResp.Low,
			Count: 		klineResp.Count,
			Vol:       klineResp.Vol,
		})

		return nil
	}
	//{
	//"ch":"market.ethusdt.trade.detail","ts":1611162033214,"tick":{"id":116292089599,"ts":1611162033202,"data":[{"id":116292089599194888730981151,"ts":1611162033202,"tradeId":102051183444,"amount":2.3227,"price":1309.43,"direction":"buy"}]}}
	if strings.HasSuffix(resp.Ch, ".trade.detail") {
		var tradeResp SpotTradeResponse
		err := json.Unmarshal(resp.Tick, &tradeResp)
		if err != nil {
			fmt.Println(err)
			return err
		}
		trades := ws.parseTrade(tradeResp)
		for _, v := range trades {
			v.Pair = currencyPair
			ws.tradeCallback(&v)
		}
		return nil
	}
	if strings.HasSuffix(resp.Ch, ".detail") {
		var tickerResp DetailResponse
		err := json.Unmarshal(resp.Tick, &tickerResp)
		if err != nil {
			return err
		}
		ws.tickerCallback(&Ticker{
			Pair: currencyPair,
			Last: tickerResp.Close,
			High: tickerResp.High,
			Low:  tickerResp.Low,
			Vol:  tickerResp.Amount,
			Date: uint64(resp.Ts),
		})
		return nil
	}

	logger.Errorf("[%s] unknown message ch , msg=%s", ws.wsConn.WsUrl, string(msg))

	return nil
}
