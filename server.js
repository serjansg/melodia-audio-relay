/**
 * Melodía Audio Share - Servidor Relay
 * =====================================
 *
 * Este servidor hace de "intermediario":
 *   1. Recibe el audio en vivo desde tu PC (el "host")
 *   2. Lo reparte a todos los amigos conectados como "oyentes"
 *
 * No procesa ni guarda el audio, solo lo reenvía en tiempo real
 * (por eso es liviano y puede correr gratis en Render/Railway).
 *
 * Dos tipos de conexión WebSocket:
 *   - ws://.../host    -> solo tú te conectas aquí, mandando el audio
 *   - ws://.../listen  -> tus amigos se conectan aquí, para escuchar
 *
 * Además sirve la página web que usan tus amigos para escuchar
 * (carpeta /public).
 */

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const path = require("path");

const app = express();
const server = http.createServer(app);

// Sirve la página del oyente (public/index.html)
app.use(express.static(path.join(__dirname, "public")));

// Dos "salas" de WebSocket: una para el host, otra para los oyentes
const wssHost = new WebSocket.Server({ noServer: true });
const wssListen = new WebSocket.Server({ noServer: true });

let hostSocket = null;
const listeners = new Set();

// --- Ruteo manual de las conexiones WebSocket según la URL ---
server.on("upgrade", (req, socket, head) => {
  if (req.url === "/host") {
    wssHost.handleUpgrade(req, socket, head, (ws) => {
      wssHost.emit("connection", ws, req);
    });
  } else if (req.url === "/listen") {
    wssListen.handleUpgrade(req, socket, head, (ws) => {
      wssListen.emit("connection", ws, req);
    });
  } else {
    socket.destroy();
  }
});

// --- Conexión del HOST (tu PC transmitiendo) ---
wssHost.on("connection", (ws) => {
  console.log("[HOST] conectado");

  if (hostSocket) {
    console.log("[HOST] ya había uno conectado, se reemplaza");
    hostSocket.close();
  }
  hostSocket = ws;

  ws.on("message", (data) => {
    // Reenvía cada chunk de audio a todos los oyentes conectados
    for (const listener of listeners) {
      if (listener.readyState === WebSocket.OPEN) {
        listener.send(data);
      }
    }
  });

  ws.on("close", () => {
    console.log("[HOST] desconectado");
    if (hostSocket === ws) hostSocket = null;
  });
});

// --- Conexión de un OYENTE (un amigo escuchando) ---
wssListen.on("connection", (ws) => {
  console.log(`[LISTENER] conectado (total: ${listeners.size + 1})`);
  listeners.add(ws);

  ws.on("close", () => {
    listeners.delete(ws);
    console.log(`[LISTENER] desconectado (total: ${listeners.size})`);
  });
});

// Endpoint simple para verificar que el servidor está vivo
app.get("/status", (req, res) => {
  res.json({
    host_conectado: hostSocket !== null,
    oyentes: listeners.size,
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Servidor relay corriendo en puerto ${PORT}`);
});
