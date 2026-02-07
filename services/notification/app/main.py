import os
import json
import asyncio
import threading
import logging
from typing import Set

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from starlette.responses import StreamingResponse

from shared.messaging import consume_events
from shared.schemas import EventEnvelope, DecisionMadePayload
from shared.constants import EVENT_DECISION_MADE

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("notification-service")

AMQP_URL = os.getenv("AMQP_URL", "amqp://guest:guest@rabbitmq:5672/%2F")

app = FastAPI(title="Notification Service")

ws_clients: Set[WebSocket] = set()
sse_queues: Set[asyncio.Queue] = set()

loop: asyncio.AbstractEventLoop | None = None


@app.get("/health")
def health():
    return {"status": "notification service running"}


async def broadcast_update(message: dict) -> None:
    logger.info("broadcast_update | ws_clients=%d | sse_queues=%d", len(ws_clients), len(sse_queues))

    dead = []
    for ws in list(ws_clients):
        try:
            await ws.send_json(message)
        except Exception:
            dead.append(ws)
    for ws in dead:
        ws_clients.discard(ws)

    for q in list(sse_queues):
        try:
            q.put_nowait(message)
        except Exception:
            sse_queues.discard(q)


def push_from_thread(message: dict) -> None:
    if loop is None:
        logger.error("push_from_thread dropped | loop is None | msg_keys=%s", list(message.keys()))
        return
    logger.info("push_from_thread scheduled | msg_keys=%s", list(message.keys()))
    asyncio.run_coroutine_threadsafe(broadcast_update(message), loop)


def handle_decision_made(event: dict):
    logger.info("handle_decision_made called | keys=%s", list(event.keys()))

    envelope = EventEnvelope(**event)
    payload = DecisionMadePayload(**envelope.payload)

    update = {
        "event_type": envelope.event_type,
        "correlation_id": str(envelope.correlation_id),
        "loan_id": str(payload.loan_id),
        "decision": payload.decision,
        "reasons": payload.reasons,
    }

    logger.info(
        "Notification update | loan_id=%s | decision=%s | reasons=%s",
        update["loan_id"],
        update["decision"],
        update["reasons"],
    )

    push_from_thread(update)


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    ws_clients.add(ws)
    try:
        # The dashboard does not send messages, so just keep the connection alive.
        while True:
            await asyncio.sleep(60)
    except WebSocketDisconnect:
        ws_clients.discard(ws)
    except Exception:
        ws_clients.discard(ws)


@app.get("/sse")
async def sse_endpoint():
    q: asyncio.Queue = asyncio.Queue()
    sse_queues.add(q)

    async def gen():
        try:
            yield "event: connected\ndata: {}\n\n"
            while True:
                msg = await q.get()
                data = json.dumps(msg, default=str)
                yield f"event: update\ndata: {data}\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            sse_queues.discard(q)

    return StreamingResponse(gen(), media_type="text/event-stream")


@app.get("/", response_class=HTMLResponse)
def dashboard():
    html = """
<!doctype html>
<html>
  <head>
    <meta charset="utf-8">
    <title>Loan Status Dashboard</title>
    <style>
      body { font-family: Arial, sans-serif; margin: 20px; }
      .row { padding: 10px; border: 1px solid #ddd; border-radius: 8px; margin-bottom: 10px; }
      .meta { font-size: 12px; color: #666; }
      .decision { font-weight: bold; }
      .ok { color: green; }
      .bad { color: red; }
      .info { color: #c58f00; }
      .controls { margin: 10px 0; }
      button { margin-right: 8px; }
    </style>
  </head>
  <body>
    <h2>Realtime Loan Status</h2>

    <div class="controls">
      <button onclick="useSSE()">Use SSE</button>
      <button onclick="useWS()">Use WebSocket</button>
    </div>

    <div class="meta">
      WebSocket: <span id="wsStatus">off</span> |
      SSE: <span id="sseStatus">off</span>
    </div>
    <hr />
    <div id="feed"></div>

    <script>
      const feed = document.getElementById("feed");

      function addUpdate(u) {
        const div = document.createElement("div");
        div.className = "row";

        const decisionClass =
          (u.decision || "").toLowerCase().includes("approve") ? "ok" :
          (u.decision || "").toLowerCase().includes("reject") ? "bad" : "info";

        div.innerHTML = `
          <div class="meta">correlation_id: ${u.correlation_id}</div>
          <div>loan_id: ${u.loan_id}</div>
          <div class="decision ${decisionClass}">decision: ${u.decision}</div>
          <div>reasons: ${(u.reasons || []).join(", ")}</div>
          <div class="meta">${new Date().toLocaleString()}</div>
        `;

        feed.prepend(div);
      }

      let ws = null;
      let es = null;

      function useWS() {
        if (es) { es.close(); es = null; }
        document.getElementById("sseStatus").textContent = "off";

        if (ws) { ws.close(); ws = null; }
        const scheme = (location.protocol === "https:") ? "wss" : "ws";
        ws = new WebSocket(`${scheme}://${location.host}/ws`);

        const wsStatus = document.getElementById("wsStatus");
        wsStatus.textContent = "connecting";
        ws.onopen = () => wsStatus.textContent = "connected";
        ws.onclose = () => wsStatus.textContent = "closed";
        ws.onerror = () => wsStatus.textContent = "error";
        ws.onmessage = (evt) => {
          try { addUpdate(JSON.parse(evt.data)); } catch(e) {}
        };
      }

      function useSSE() {
        if (ws) { ws.close(); ws = null; }
        document.getElementById("wsStatus").textContent = "off";

        if (es) { es.close(); es = null; }
        const sseStatus = document.getElementById("sseStatus");
        sseStatus.textContent = "connecting";

        es = new EventSource(`/sse`);
        es.onopen = () => sseStatus.textContent = "connected";
        es.onerror = () => sseStatus.textContent = "error/reconnecting";
        es.addEventListener("update", (evt) => {
          try { addUpdate(JSON.parse(evt.data)); } catch(e) {}
        });
      }

      // Default: SSE (simple and auto-reconnect)
      useSSE();
    </script>
  </body>
</html>
    """
    return HTMLResponse(html)


@app.on_event("startup")
async def startup_event():
    global loop
    loop = asyncio.get_running_loop()
    logger.info("Startup | loop_set=%s | loop=%s", loop is not None, loop)

    thread = threading.Thread(
        target=consume_events,
        args=(AMQP_URL, "q.notification", [EVENT_DECISION_MADE], handle_decision_made),
        daemon=True,
    )
    thread.start()
