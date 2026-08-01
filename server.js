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

// Los primeros bytes que manda el host contienen el "encabezado" del
// stream (info necesaria para que el navegador sepa decodificar el audio).
// Si alguien se conecta DESPUÉS de que ese encabezado ya se mandó, nunca
// lo recibiría y el audio no sonaría. Por eso lo guardamos en caché acá,
// y se lo mandamos a cada oyente nuevo apenas se conecta.
let initChunks = [];
let initBytes = 0;
let initCapturado = false;
const INIT_MAX_BYTES = 32 * 1024; // 32 KB es de sobra para el encabezado

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

  // Nueva transmisión: reiniciamos la caché del encabezado
  initChunks = [];
  initBytes = 0;
  initCapturado = false;

  ws.on("message", (data) => {
    // Guardamos los primeros bytes como "encabezado" para poder
    // repetírselo a oyentes que se conecten más tarde.
    if (!initCapturado) {
      initChunks.push(data);
      initBytes += data.length;
      if (initBytes >= INIT_MAX_BYTES) {
        initCapturado = true;
        console.log(`[HOST] encabezado capturado (${initBytes} bytes)`);
      }
    }

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

  // Si ya hay una transmisión en curso, le mandamos primero el encabezado
  // guardado, para que su navegador pueda decodificar el audio que sigue.
  if (initChunks.length > 0) {
    for (const chunk of initChunks) {
      ws.send(chunk);
    }
  }

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
