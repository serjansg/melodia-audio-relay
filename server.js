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

// Solo necesitamos WebSocket para el HOST (mandar el audio hacia el
// servidor). Los oyentes reciben el audio por HTTP normal, como una
// radio por internet clásica -- el navegador lo maneja solo, sin que
// tengamos que programar la reproducción a mano.
const wssHost = new WebSocket.Server({ noServer: true });

let hostSocket = null;
const oyentesHttp = new Set(); // objetos "response" de cada oyente conectado

// Los primeros bytes que manda el host contienen el "encabezado" del
// stream (info necesaria para que el navegador sepa decodificar el audio).
// Lo guardamos para mandárselo a cada oyente nuevo apenas se conecta,
// sin importar cuándo se una a la transmisión.
let initChunks = [];
let initBytes = 0;
let initCapturado = false;
const INIT_MAX_BYTES = 32 * 1024; // 32 KB es de sobra para el encabezado

// --- Ruteo manual de la conexión WebSocket del host ---
server.on("upgrade", (req, socket, head) => {
  if (req.url === "/host") {
    wssHost.handleUpgrade(req, socket, head, (ws) => {
      wssHost.emit("connection", ws, req);
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

  // Si ya había oyentes conectados de una transmisión anterior, hay que
  // cortarles la conexión a propósito -- técnicamente esto es un stream
  // nuevo (con su propio encabezado nuevo), y si seguimos empujándoles
  // datos sin que reinicien su conexión, su navegador no va a poder
  // decodificarlo bien. Al cerrar su conexión, la página (con
  // reconexión automática) los vuelve a conectar solos y reciben el
  // encabezado correcto de este nuevo stream.
  if (oyentesHttp.size > 0) {
    console.log(`[HOST] reconectado, reiniciando ${oyentesHttp.size} oyente(s)`);
    for (const res of oyentesHttp) {
      res.end();
    }
    oyentesHttp.clear();
  }

  ws.on("message", (data) => {
    if (!initCapturado) {
      initChunks.push(data);
      initBytes += data.length;
      if (initBytes >= INIT_MAX_BYTES) {
        initCapturado = true;
        console.log(`[HOST] encabezado capturado (${initBytes} bytes)`);
      }
    }

    // Manda el audio a cada oyente conectado por HTTP, en vivo
    for (const res of oyentesHttp) {
      res.write(data);
    }
  });

  ws.on("close", () => {
    console.log("[HOST] desconectado");
    if (hostSocket === ws) hostSocket = null;
  });
});

// --- Endpoint de streaming para los OYENTES (radio por internet clásica) ---
app.get("/stream", (req, res) => {
  console.log(`[LISTENER] conectado (total: ${oyentesHttp.size + 1})`);

  res.writeHead(200, {
    "Content-Type": "audio/webm",
    "Cache-Control": "no-cache, no-store",
    "Connection": "keep-alive",
  });

  // Le mandamos primero el encabezado guardado, así puede decodificar
  // el audio que sigue, sin importar cuándo se conectó.
  for (const chunk of initChunks) {
    res.write(chunk);
  }

  oyentesHttp.add(res);

  req.on("close", () => {
    oyentesHttp.delete(res);
    console.log(`[LISTENER] desconectado (total: ${oyentesHttp.size})`);
  });
});

// Endpoint simple para verificar que el servidor está vivo
app.get("/status", (req, res) => {
  res.json({
    host_conectado: hostSocket !== null,
    oyentes: oyentesHttp.size,
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Servidor relay corriendo en puerto ${PORT}`);
});
