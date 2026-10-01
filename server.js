const express = require("express");
const http = require("http");
const path = require("path");
const fs = require("fs");
const { Server } = require("socket.io");

const PORT = process.env.PORT || 3000;
const DB_PATH = path.join(__dirname, "db.json");
const TICK_RATE = 1000 / 60;
const PADDLE_SPEED = 420;
// Toleransi kecepatan paddle dari client (paket bisa datang bergerombol karena jitter).
const PADDLE_SPEED_TOLERANCE = 1.5;
const DB_WRITE_DELAY = 500;
const INITIAL_BALL_SPEED = 240;
const MAX_BALL_SPEED = 700;
const BALL_ACCELERATION = 28;
const BOUNCE_SPEED_BONUS = 10;

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static(path.join(__dirname, "public")));

const rooms = new Map();
let dbWriteTimer = null;
let dbWriting = false;
let dbDirty = false;

function readDb() {
  try {
    return JSON.parse(fs.readFileSync(DB_PATH, "utf8"));
  } catch {
    return { rooms: [] };
  }
}

// Tulis db.json secara async + debounce supaya tidak memblok game loop.
function writeDb() {
  dbDirty = true;
  if (dbWriteTimer || dbWriting) return;
  dbWriteTimer = setTimeout(flushDb, DB_WRITE_DELAY);
}

async function flushDb() {
  dbWriteTimer = null;
  dbDirty = false;
  dbWriting = true;
  const data = {
    rooms: [...rooms.values()].map((room) => ({
      id: room.id,
      name: room.name,
      status: room.status,
      playerCount: room.players.size,
      score: room.state.score,
      readyCount: room.ready.size,
      winner: room.winner,
      createdAt: room.createdAt,
      updatedAt: new Date().toISOString(),
    })),
  };

  try {
    await fs.promises.writeFile(DB_PATH, JSON.stringify(data, null, 2));
  } catch (error) {
    console.error("Gagal menulis db.json:", error);
  } finally {
    dbWriting = false;
    if (dbDirty) writeDb();
  }
}

function loadDbRooms() {
  const db = readDb();
  for (const item of db.rooms || []) {
    if (item.status !== "finished") continue;
    rooms.set(item.id, createRoom(item.id, item.name, item.createdAt, "finished", item.score, item.winner));
  }
}

function createRoom(id = makeRoomId(), name, createdAt = new Date().toISOString(), status = "waiting", score, winner) {
  return {
    id,
    name: name || `Room ${id}`,
    status,
    winner: winner || null,
    createdAt,
    players: new Map(),
    paddleTargets: new Map(),
    ready: new Set(),
    loop: null,
    lastTick: Date.now(),
    round: 0,
    pendingServeDirection: Math.random() > 0.5 ? 1 : -1,
    state: {
      width: 900,
      height: 520,
      paddleWidth: 14,
      paddleHeight: 100,
      ballSize: 14,
      leftY: 210,
      rightY: 210,
      ballX: 443,
      ballY: 253,
      ballVX: 0,
      ballVY: 0,
      score: score || { left: 0, right: 0 },
    },
  };
}

function makeRoomId() {
  return Math.random().toString(36).slice(2, 7).toUpperCase();
}

function publicRooms() {
  return [...rooms.values()]
    .filter((room) => room.status !== "finished")
    .map((room) => ({
      id: room.id,
      name: room.name,
      status: room.status,
      playerCount: room.players.size,
      readyCount: room.ready.size,
      maxPlayers: 2,
      createdAt: room.createdAt,
    }));
}

function emitRooms() {
  io.emit("rooms", publicRooms());
  writeDb();
}

function send(socket, event, payload) {
  socket.emit(event, payload);
}

function joinRoom(socket, roomId) {
  const room = rooms.get(roomId);
  if (!room || room.status === "finished") {
    send(socket, "errorMessage", "Room tidak ditemukan.");
    return;
  }

  if (room.players.size >= 2 && !room.players.has(socket.id)) {
    send(socket, "errorMessage", "Room sudah penuh.");
    return;
  }

  leaveCurrentRoom(socket);

  const side = room.players.size === 0 ? "left" : "right";
  room.players.set(socket.id, { id: socket.id, side });
  room.ready.delete(socket.id);
  socket.data.roomId = room.id;
  socket.data.side = side;
  socket.join(room.id);

  if (room.players.size === 2) {
    room.status = "ready";
    centerBall(room);
  } else {
    room.status = "waiting";
    centerBall(room);
  }

  send(socket, "joined", { roomId: room.id, side });
  broadcastRoomState(room);
  emitRooms();
}

function leaveCurrentRoom(socket) {
  const roomId = socket.data.roomId;
  if (!roomId) return;

  const room = rooms.get(roomId);
  if (room) {
    room.players.delete(socket.id);
    room.paddleTargets.delete(socket.id);
    room.ready.delete(socket.id);
    socket.leave(roomId);

    if (room.players.size === 0 && room.status !== "finished") {
      stopGameLoop(room);
      rooms.delete(roomId);
    } else if (room.status !== "finished") {
      room.status = "waiting";
      stopGameLoop(room);
      centerBall(room);
      broadcastRoomState(room);
    }
  }

  socket.data.roomId = null;
  socket.data.side = null;
}

function broadcastRoomState(room) {
  io.to(room.id).emit("roomState", {
    id: room.id,
    name: room.name,
    status: room.status,
    playerCount: room.players.size,
    readyCount: room.ready.size,
    players: [...room.players.values()].map((player) => ({
      ...player,
      ready: room.ready.has(player.id),
    })),
    state: room.state,
    round: room.round,
    t: Date.now(),
    winner: room.winner,
  });
}

// Snapshot ringan untuk posisi saja. Volatile: kalau koneksi tersendat,
// snapshot basi dibuang alih-alih menumpuk lalu datang bergerombol.
function broadcastSnapshot(room) {
  const s = room.state;
  io.to(room.id).volatile.emit("snapshot", [
    Date.now(),
    room.round,
    round1(s.leftY),
    round1(s.rightY),
    round1(s.ballX),
    round1(s.ballY),
    Math.round(s.ballVX),
    Math.round(s.ballVY),
  ]);
}

function round1(value) {
  return Math.round(value * 10) / 10;
}

function startGameLoop(room) {
  if (room.loop) return;
  room.lastTick = Date.now();
  room.loop = setInterval(() => tick(room), TICK_RATE);
}

function stopGameLoop(room) {
  if (!room.loop) return;
  clearInterval(room.loop);
  room.loop = null;
}

function resetBall(room, direction) {
  const s = room.state;
  centerPaddles(room);
  room.round += 1;
  room.paddleTargets.clear();
  s.ballX = s.width / 2 - s.ballSize / 2;
  s.ballY = s.height / 2 - s.ballSize / 2;
  const angle = (Math.random() * 0.7 - 0.35);
  s.ballVX = direction * Math.cos(angle) * INITIAL_BALL_SPEED;
  s.ballVY = Math.sin(angle) * INITIAL_BALL_SPEED;
}

function centerBall(room) {
  const s = room.state;
  s.ballX = s.width / 2 - s.ballSize / 2;
  s.ballY = s.height / 2 - s.ballSize / 2;
  s.ballVX = 0;
  s.ballVY = 0;
}

function centerPaddles(room) {
  const s = room.state;
  s.leftY = (s.height - s.paddleHeight) / 2;
  s.rightY = (s.height - s.paddleHeight) / 2;
}

function markReady(socket) {
  const room = rooms.get(socket.data.roomId);
  if (!room || !room.players.has(socket.id)) return;
  if (!["ready", "pointPause"].includes(room.status)) return;

  room.ready.add(socket.id);

  if (room.players.size === 2 && room.ready.size === 2) {
    room.ready.clear();
    room.status = "playing";
    resetBall(room, room.pendingServeDirection);
    startGameLoop(room);
  }

  broadcastRoomState(room);
  emitRooms();
}

function tick(room) {
  if (room.players.size < 2 || room.status !== "playing") return;

  const now = Date.now();
  const dt = Math.min((now - room.lastTick) / 1000, 0.04);
  room.lastTick = now;

  const s = room.state;
  const maxStep = PADDLE_SPEED * PADDLE_SPEED_TOLERANCE * dt;

  // Posisi paddle dikirim client (prediksi lokal), server hanya membatasi kecepatannya.
  for (const [socketId, player] of room.players) {
    const key = player.side === "left" ? "leftY" : "rightY";
    const target = room.paddleTargets.get(socketId);
    if (target === undefined) continue;
    s[key] = clamp(moveToward(s[key], target, maxStep), 0, s.height - s.paddleHeight);
  }

  accelerateBall(s, dt);
  s.ballX += s.ballVX * dt;
  s.ballY += s.ballVY * dt;

  if (s.ballY <= 0 || s.ballY + s.ballSize >= s.height) {
    s.ballY = clamp(s.ballY, 0, s.height - s.ballSize);
    s.ballVY *= -1;
  }

  const leftPaddle = { x: 24, y: s.leftY };
  const rightPaddle = { x: s.width - 24 - s.paddleWidth, y: s.rightY };

  if (hitsPaddle(s, leftPaddle) && s.ballVX < 0) {
    bounceBall(room, leftPaddle, 1);
  }

  if (hitsPaddle(s, rightPaddle) && s.ballVX > 0) {
    bounceBall(room, rightPaddle, -1);
  }

  if (s.ballX + s.ballSize < 0) {
    s.score.right += 1;
    pauseAfterPoint(room, -1);
  }

  if (s.ballX > s.width) {
    s.score.left += 1;
    pauseAfterPoint(room, 1);
  }

  if (s.score.left >= 5 || s.score.right >= 5) {
    room.status = "finished";
    room.winner = s.score.left > s.score.right ? "Player 1" : "Player 2";
    stopGameLoop(room);
  }

  if (room.status === "playing") {
    broadcastSnapshot(room);
  } else {
    broadcastRoomState(room);
    if (room.status === "finished") emitRooms();
  }
}

function moveToward(value, target, maxStep) {
  if (Math.abs(target - value) <= maxStep) return target;
  return value + Math.sign(target - value) * maxStep;
}

function pauseAfterPoint(room, nextServeDirection) {
  if (room.state.score.left >= 5 || room.state.score.right >= 5) return;

  room.status = "pointPause";
  room.pendingServeDirection = nextServeDirection;
  room.ready.clear();
  stopGameLoop(room);
  centerBall(room);
}

function hitsPaddle(s, paddle) {
  return (
    s.ballX < paddle.x + s.paddleWidth &&
    s.ballX + s.ballSize > paddle.x &&
    s.ballY < paddle.y + s.paddleHeight &&
    s.ballY + s.ballSize > paddle.y
  );
}

function bounceBall(room, paddle, direction) {
  const s = room.state;
  const paddleCenter = paddle.y + s.paddleHeight / 2;
  const ballCenter = s.ballY + s.ballSize / 2;
  const impact = (ballCenter - paddleCenter) / (s.paddleHeight / 2);
  const nextSpeed = Math.min(getBallSpeed(s) + BOUNCE_SPEED_BONUS, MAX_BALL_SPEED);

  s.ballVX = direction * nextSpeed * 0.86;
  s.ballVY = impact * nextSpeed * 0.72;
  normalizeBallSpeed(s, nextSpeed);
  s.ballX = direction > 0 ? paddle.x + s.paddleWidth : paddle.x - s.ballSize;
}

function accelerateBall(s, dt) {
  const speed = getBallSpeed(s);
  if (!speed || speed >= MAX_BALL_SPEED) return;

  const nextSpeed = Math.min(speed + BALL_ACCELERATION * dt, MAX_BALL_SPEED);
  const scale = nextSpeed / speed;
  s.ballVX *= scale;
  s.ballVY *= scale;
}

function getBallSpeed(s) {
  return Math.hypot(s.ballVX, s.ballVY);
}

function normalizeBallSpeed(s, targetSpeed) {
  const speed = getBallSpeed(s);
  if (!speed) return;

  const scale = targetSpeed / speed;
  s.ballVX *= scale;
  s.ballVY *= scale;
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

io.on("connection", (socket) => {
  send(socket, "rooms", publicRooms());

  socket.on("createRoom", (name) => {
    const room = createRoom(undefined, String(name || "").trim().slice(0, 24));
    rooms.set(room.id, room);
    joinRoom(socket, room.id);
  });

  socket.on("joinRoom", (roomId) => {
    joinRoom(socket, String(roomId || "").trim().toUpperCase());
  });

  socket.on("getRooms", () => {
    send(socket, "rooms", publicRooms());
  });

  socket.on("ready", () => {
    markReady(socket);
  });

  socket.on("leaveRoom", () => {
    leaveCurrentRoom(socket);
    emitRooms();
  });

  socket.on("paddle", (payload) => {
    const room = rooms.get(socket.data.roomId);
    if (!room || !room.players.has(socket.id) || room.status !== "playing") return;
    const y = Number(payload && payload.y);
    // Abaikan posisi dari ronde sebelumnya (sebelum paddle di-reset ke tengah).
    if (!Number.isFinite(y) || payload.round !== room.round) return;
    room.paddleTargets.set(socket.id, clamp(y, 0, room.state.height - room.state.paddleHeight));
  });

  socket.on("disconnect", () => {
    leaveCurrentRoom(socket);
    emitRooms();
  });
});

loadDbRooms();

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Pong server berjalan di http://localhost:${PORT}`);
});
