import { createServer } from 'http';
import { WebSocketServer, WebSocket } from 'ws';

const PORT = parseInt(process.env.PORT || '10000', 10);
const API_KEY = process.env.ALIBABA_API_KEY || '';
const WORKSPACE_ID = process.env.ALIBABA_WORKSPACE_ID || 'ws-o3v6mh0i8y9tqdfx';
const QWEN_REALTIME_MODEL = 'qwen3.8-omni-flash-realtime';

function getDashScopeUrl(targetModel) {
  const model = (targetModel || process.env.QWEN_REALTIME_MODEL || QWEN_REALTIME_MODEL).trim();
  return (
    `wss://${WORKSPACE_ID}.ap-southeast-1.maas.aliyuncs.com` +
    `/api-ws/v1/realtime?model=${model}`
  );
}

if (!API_KEY) {
  console.error('[proxy] ❌ ALIBABA_API_KEY is not set.');
  process.exit(1);
}

// ── HTTP Health Check for Render ──────────────────────────────────────────
const httpServer = createServer((req, res) => {
  if (req.url === '/health' || req.url === '/') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', service: 'avelut-realtime-proxy' }));
  } else {
    res.writeHead(404);
    res.end();
  }
});

// ── WebSocket Server ──────────────────────────────────────────────────────
const wss = new WebSocketServer({ server: httpServer, path: '/qwen-realtime' });

wss.on('connection', (clientSocket, req) => {
  const parsedUrl = new URL(req.url || '', 'http://localhost');
  const clientRequestedModel = parsedUrl.searchParams.get('model');
  const upstreamUrl = getDashScopeUrl(clientRequestedModel);

  const upstreamSocket = new WebSocket(upstreamUrl, {
    headers: {
      Authorization: `Bearer ${API_KEY}`,
      'X-DashScope-WorkSpace': WORKSPACE_ID,
      'User-Agent': 'Avelut-LiveTeacher/1.0',
    },
  });

  let clientClosed = false;
  let upstreamClosed = false;
  const clientQueue = [];

  // Active Keepalive Ping every 20 seconds
  const pingInterval = setInterval(() => {
    if (clientSocket.readyState === WebSocket.OPEN) {
      try { clientSocket.ping(); } catch {}
    }
    if (upstreamSocket.readyState === WebSocket.OPEN) {
      try { upstreamSocket.ping(); } catch {}
    }
  }, 20000);

  const cleanup = () => {
    clearInterval(pingInterval);
  };

  clientSocket.on('pong', () => {});
  upstreamSocket.on('pong', () => {});

  // ── Upstream → Client pipe ──────────────────────────────────────────────
  upstreamSocket.on('open', () => {
    while (clientQueue.length > 0) {
      const item = clientQueue.shift();
      if (item && upstreamSocket.readyState === WebSocket.OPEN) {
        upstreamSocket.send(item.data, { binary: item.isBinary });
      }
    }
  });

  upstreamSocket.on('message', (data, isBinary) => {
    if (clientSocket.readyState === WebSocket.OPEN) {
      clientSocket.send(data, { binary: isBinary });
    }
  });

  upstreamSocket.on('error', (err) => {
    console.error('[proxy] Upstream error:', err.message);
    cleanup();
    if (clientSocket.readyState === WebSocket.OPEN) {
      clientSocket.close(1011, 'Upstream error');
    }
  });

  upstreamSocket.on('close', (code, reason) => {
    upstreamClosed = true;
    cleanup();
    if (!clientClosed && clientSocket.readyState === WebSocket.OPEN) {
      clientSocket.close(code, reason);
    }
  });

  // ── Client → Upstream pipe ──────────────────────────────────────────────
  clientSocket.on('message', (data, isBinary) => {
    if (!isBinary) {
      try {
        const str = typeof data === 'string' ? data : data.toString();
        if (str.includes('"type":"ping"') || str.trim() === 'ping') {
          if (clientSocket.readyState === WebSocket.OPEN) {
            clientSocket.send(JSON.stringify({ type: 'pong' }));
          }
          return;
        }
      } catch {}
    }

    if (upstreamSocket.readyState === WebSocket.OPEN) {
      upstreamSocket.send(data, { binary: isBinary });
    } else if (upstreamSocket.readyState === WebSocket.CONNECTING) {
      clientQueue.push({ data, isBinary });
    }
  });

  clientSocket.on('error', (err) => {
    console.error('[proxy] Client error:', err.message);
    cleanup();
    if (!upstreamClosed && upstreamSocket.readyState === WebSocket.OPEN) {
      upstreamSocket.close(1011, 'Client error');
    }
  });

  clientSocket.on('close', (code, reason) => {
    clientClosed = true;
    cleanup();
    if (!upstreamClosed && upstreamSocket.readyState === WebSocket.OPEN) {
      upstreamSocket.close(code, reason);
    }
  });
});

httpServer.listen(PORT, () => {
  console.log(`[proxy] 🚀 WebSocket Proxy running on port ${PORT}/qwen-realtime`);
});
