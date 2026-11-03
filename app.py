import os, json, time, threading, queue
from datetime import datetime, timezone
from collections import deque

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st
import websocket
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Quotex Remix AI V3", page_icon="⚡", layout="wide")
st.markdown("""
<style>
.stApp{background:#080d18;color:#eef2ff}
[data-testid="stSidebar"]{background:#101827}
.hero{padding:8px 0 10px}.hero h1{font-size:2.5rem;margin:0;color:#f5f7ff}
.hero p,.muted{color:#aab5c8}
div[data-testid="stMetric"]{background:#101827;border:1px solid #273449;padding:12px;border-radius:13px}
.panel{background:#101827;border:1px solid #273449;border-radius:14px;padding:14px}
</style>
""", unsafe_allow_html=True)

def api_key():
    try:
        k = st.secrets.get("TWELVE_DATA_API_KEY", "")
        if k: return str(k).strip()
    except Exception: pass
    return os.getenv("TWELVE_DATA_API_KEY", "").strip()

def historical(symbol, key):
    url = "https://api.twelvedata.com/time_series"
    r = requests.get(url, params={"symbol":symbol,"interval":"1min","outputsize":120,"apikey":key}, timeout=15)
    r.raise_for_status()
    payload = r.json()
    if "values" not in payload:
        raise RuntimeError(payload.get("message", "No historical candles returned"))
    df = pd.DataFrame(payload["values"])
    for c in ["open","high","low","close"]: df[c] = pd.to_numeric(df[c], errors="coerce")
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df = df.dropna(subset=["datetime","open","high","low","close"]).sort_values("datetime").reset_index(drop=True)
    return df

class PriceStream:
    def __init__(self, key, symbols):
        self.key, self.symbols = key, symbols
        self.q = queue.Queue()
        self.ws = None
        self.thread = None
        self.status = "Connecting"
        self.last_message = None
        self.lock = threading.Lock()

    def start(self):
        def on_open(ws):
            self.status = "Connected — subscribing"
            ws.send(json.dumps({"action":"subscribe","params":{"symbols":",".join(self.symbols)}}))
        def on_message(ws, message):
            try:
                item = json.loads(message)
                if item.get("event") == "price":
                    self.q.put(item)
                elif item.get("event") == "subscribe-status":
                    self.status = "Connected — subscription response received"
                    if item.get("fails"):
                        self.q.put({"_error":"Some symbols were rejected: " + str(item["fails"])})
                elif item.get("status") == "error" or item.get("event") == "error":
                    self.q.put({"_error":item.get("message", str(item))})
            except Exception as e:
                self.q.put({"_error":str(e)})
        def on_error(ws, error):
            self.status = "Connection error"
            self.q.put({"_error":str(error)})
        def on_close(ws, code, msg):
            self.status = "Disconnected"
        self.ws = websocket.WebSocketApp(
            f"wss://ws.twelvedata.com/v1/quotes/price?apikey={self.key}",
            on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
        self.thread = threading.Thread(target=lambda: self.ws.run_forever(ping_interval=25,ping_timeout=10), daemon=True)
        self.thread.start()

    def drain(self):
        items=[]
        while True:
            try: items.append(self.q.get_nowait())
            except queue.Empty: break
        return items

    def stop(self):
        try:
            if self.ws: self.ws.close()
        except Exception: pass

def candle_from_tick(state, symbol, price, ts):
    # UTC minute boundaries keep the candle clock consistent with the feed.
    dt = datetime.fromtimestamp(float(ts), tz=timezone.utc)
    minute = dt.replace(second=0, microsecond=0)
    candles = state.setdefault("candles", {}).setdefault(symbol, deque(maxlen=180))
    if not candles:
        candles.append({"datetime":minute,"open":price,"high":price,"low":price,"close":price})
    elif candles[-1]["datetime"] == minute:
        c=candles[-1]; c["high"]=max(c["high"],price); c["low"]=min(c["low"],price); c["close"]=price
    elif minute > candles[-1]["datetime"]:
        candles.append({"datetime":minute,"open":price,"high":price,"low":price,"close":price})
    else:
        # Ignore out-of-order ticks rather than corrupting a completed candle.
        return
    state.setdefault("last_prices", {})[symbol] = (price, dt)

def indicators(df):
    d=df.copy()
    c=d.close
    d["ema9"]=c.ewm(span=9,adjust=False).mean()
    d["ema21"]=c.ewm(span=21,adjust=False).mean()
    d["ema50"]=c.ewm(span=50,adjust=False).mean()
    delta=c.diff(); gain=delta.clip(lower=0); loss=-delta.clip(upper=0)
    ag=gain.ewm(alpha=1/14,adjust=False,min_periods=14).mean()
    al=loss.ewm(alpha=1/14,adjust=False,min_periods=14).mean()
    rs=ag/al.replace(0,np.nan); d["rsi"]=100-100/(1+rs)
    e12=c.ewm(span=12,adjust=False).mean(); e26=c.ewm(span=26,adjust=False).mean()
    d["macd"]=e12-e26; d["macd_signal"]=d.macd.ewm(span=9,adjust=False).mean()
    d["bb_mid"]=c.rolling(20).mean()
    return d

def engines(d):
    if len(d)<55: return [],"WAIT",0,0
    x=d.iloc[-1]; p=d.iloc[-2]; old3=d.iloc[-4]; old8=d.iloc[-9]
    states=[]
    states.append(("EMA TREND","BULLISH" if x.ema9>x.ema21>x.ema50 and x.close>x.ema9 else "BEARISH" if x.ema9<x.ema21<x.ema50 and x.close<x.ema9 else "NEUTRAL"))
    states.append(("RSI","BULLISH" if x.rsi>=55 else "BEARISH" if x.rsi<=45 else "NEUTRAL"))
    states.append(("MACD","BULLISH" if x.macd>x.macd_signal else "BEARISH" if x.macd<x.macd_signal else "NEUTRAL"))
    states.append(("MOMENTUM","BULLISH" if x.close>old3.close and x.close>old8.close else "BEARISH" if x.close<old3.close and x.close<old8.close else "NEUTRAL"))
    states.append(("BOLLINGER","BULLISH" if x.close>x.bb_mid and x.bb_mid>d.iloc[-4].bb_mid else "BEARISH" if x.close<x.bb_mid and x.bb_mid<d.iloc[-4].bb_mid else "NEUTRAL"))
    rng=max(float(x.high-x.low),1e-12); body=abs(float(x.close-x.open))
    up=float(x.high-max(x.open,x.close)); low=float(min(x.open,x.close)-x.low)
    candle="BULLISH" if (low/rng>=.45 and x.close>=x.open) or (body/rng>=.45 and x.close>x.open and x.close>=p.close) else "BEARISH" if (up/rng>=.45 and x.close<=x.open) or (body/rng>=.45 and x.close<x.open and x.close<=p.close) else "NEUTRAL"
    states.append(("CANDLE STRUCTURE",candle))
    bull=sum(s=="BULLISH" for _,s in states); bear=sum(s=="BEARISH" for _,s in states)
    signal="UP BIAS" if bull>=5 and bear==0 else "DOWN BIAS" if bear>=5 and bull==0 else "WAIT"
    return states,signal,bull,bear

st.markdown('<div class="hero"><h1>⚡ QUOTEX REMIX AI V3</h1><p>Live price stream · tick-built 1-minute candles · six-engine analysis</p></div>',unsafe_allow_html=True)
key=api_key()
if not key:
    st.error("API key missing. Add TWELVE_DATA_API_KEY in Streamlit → App settings → Secrets.")
    st.stop()

# Basic plan supports only 8 trial WebSocket symbols at once, not unrestricted all-market streaming.
default_symbols=["EUR/USD","GBP/USD","USD/JPY","BTC/USD"]
with st.sidebar:
    st.header("Live settings")
    symbols_text=st.text_input("Symbols (max 8 trial subscriptions)", ",".join(default_symbols))
    symbols=list(dict.fromkeys(s.strip().upper() for s in symbols_text.split(",") if s.strip()))[:8]
    selected=st.selectbox("Chart / prediction symbol", symbols or default_symbols)
    st.caption("Free Basic: trial WebSocket access only. Symbols must be on Twelve Data's permitted trial list.")
    if st.button("Restart live connection", use_container_width=True):
        old=st.session_state.pop("stream",None)
        if old: old.stop()
        st.session_state.pop("candles",None); st.session_state.pop("last_prices",None)
        st.rerun()

if "stream" not in st.session_state:
    stream=PriceStream(key,symbols)
    stream.start()
    st.session_state.stream=stream
    st.session_state.candles={}
    st.session_state.last_prices={}
    st.session_state.stream_errors=[]
    # Seed chart with REST candles for selected symbol; live ticks will update the forming candle.
    try:
        seed=historical(selected,key)
        st.session_state.candles[selected]=deque(seed.tail(120).to_dict("records"),maxlen=180)
    except Exception as e:
        st.session_state.stream_errors.append("History seed: "+str(e))

stream=st.session_state.stream
# Trigger a full script rerun every second so incoming ticks update the chart.
st_autorefresh(interval=1000, key="live_tick_refresh")

# Apply queued stream ticks.
for item in stream.drain():
    if "_error" in item:
        st.session_state.stream_errors.append(item["_error"])
    elif item.get("symbol") and item.get("price") is not None:
        candle_from_tick(st.session_state,item["symbol"],float(item["price"]),item.get("timestamp",time.time()))

st.caption(f"Stream status: **{stream.status}** · last UI update {datetime.now().strftime('%H:%M:%S')}")
if st.session_state.stream_errors:
    st.warning(" · ".join(st.session_state.stream_errors[-3:]))
if not symbols:
    st.error("Enter at least one symbol.")
    st.stop()

# Live price cards
cols=st.columns(min(4,len(symbols)))
for i,sym in enumerate(symbols):
    with cols[i%len(cols)]:
        lp=st.session_state.last_prices.get(sym)
        if lp:
            st.metric(sym, f"{lp[0]:.6g}", help=f"Tick timestamp: {lp[1].strftime('%H:%M:%S UTC')}")
        else:
            st.metric(sym, "Waiting for tick")

ticks=st.session_state.last_prices.get(selected)
if ticks:
    st.success(f"LIVE TICK · {selected}: {ticks[0]:.6g} · {ticks[1].strftime('%H:%M:%S UTC')}")
else:
    st.info("Waiting for first live tick. If the selected symbol is not on the Basic trial list, choose a permitted trial symbol or check Twelve Data subscription status.")

raw=list(st.session_state.candles.get(selected,[]))
if raw:
    df=pd.DataFrame(raw).drop_duplicates(subset=["datetime"],keep="last").sort_values("datetime").reset_index(drop=True)
    # Keep live forming candle visible; chart timestamps are UTC.
    if len(df)>=55:
        d=indicators(df)
        states,signal,bull,bear=engines(d)
        a,b,c=st.columns(3)
        a.metric("Live signal",signal)
        b.metric("Bullish engines",f"{bull}/6")
        c.metric("Bearish engines",f"{bear}/6")
        st.caption("Signal is recalculated from the latest tick-built candle. It is an indicator bias, not a guaranteed next-candle result.")
        fig=go.Figure(go.Candlestick(x=d.datetime,open=d.open,high=d.high,low=d.low,close=d.close,name="Live 1m"))
        fig.add_trace(go.Scatter(x=d.datetime,y=d.ema9,name="EMA 9",line=dict(width=1.3)))
        fig.add_trace(go.Scatter(x=d.datetime,y=d.ema21,name="EMA 21",line=dict(width=1.3)))
        fig.update_layout(template="plotly_dark",height=470,xaxis_rangeslider_visible=False,margin=dict(l=5,r=5,t=25,b=5),paper_bgcolor="#080d18",plot_bgcolor="#080d18")
        st.plotly_chart(fig,use_container_width=True)
        st.subheader("Six live analysis engines")
        st.dataframe(pd.DataFrame(states,columns=["Engine","Current reading"]),hide_index=True,use_container_width=True)
    else:
        st.info(f"Building candle history: {len(df)}/55 candles. Live ticks update the current candle; wait for history to load.")
else:
    st.info("No candle data yet. Check the symbol and Twelve Data trial subscription response above.")

st.markdown("---")
st.caption("Data source: Twelve Data. Quotes can differ from Quotex, especially OTC instruments. No trade is placed by this app. Market-data latency, feed permissions, and connection availability depend on your plan and symbol.")
# The page refreshes once per second; the WebSocket connection remains in session state.
