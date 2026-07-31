// ==== AYARLAR ====
// Render backend adresinizi buraya yazın:
const BACKEND_URL = "https://sevrercde.onrender.com";

let myName = "";
let pendingImage = null; // base64 data URL
const renderedMsgIds = new Set(); // Mesajların tekrar yazdırılmasını önler

const nameScreen = document.getElementById("nameScreen");
const chatScreen = document.getElementById("chatScreen");
const nameInput = document.getElementById("nameInput");
const joinBtn = document.getElementById("joinBtn");
const whoLabel = document.getElementById("whoLabel");
const statusLabel = document.getElementById("statusLabel");
const messagesEl = document.getElementById("messages");
const textInput = document.getElementById("textInput");
const sendBtn = document.getElementById("sendBtn");
const attachBtn = document.getElementById("attachBtn");
const fileInput = document.getElementById("fileInput");
const imgPreviewWrap = document.getElementById("imgPreviewWrap");
const imgPreview = document.getElementById("imgPreview");
const removeImgBtn = document.getElementById("removeImgBtn");
const errBox = document.getElementById("errBox");
const loadingOverlay = document.getElementById("loadingOverlay");
const lightbox = document.getElementById("lightbox");
const lightboxImg = document.getElementById("lightboxImg");
const lightboxClose = document.getElementById("lightboxClose");
const zoomInBtn = document.getElementById("zoomInBtn");
const zoomOutBtn = document.getElementById("zoomOutBtn");
const zoomResetBtn = document.getElementById("zoomResetBtn");

let zoomLevel = 1;
let panX = 0, panY = 0;
let isDragging = false;
let dragStartX = 0, dragStartY = 0;
let panStartX = 0, panStartY = 0;
const MIN_ZOOM = 1, MAX_ZOOM = 6;

function showError(msg) {
  errBox.textContent = msg;
  errBox.style.display = "block";
  setTimeout(() => { errBox.style.display = "none"; }, 4000);
}

function showLoading(show = true) {
  if (show) {
    loadingOverlay.classList.add("show");
  } else {
    loadingOverlay.classList.remove("show");
  }
}

// İsim girilmeden hiçbir ağ isteği BAŞLAMAZ
joinBtn.addEventListener("click", startChat);
nameInput.addEventListener("keydown", (e) => { if (e.key === "Enter") startChat(); });

function startChat() {
  const val = nameInput.value.trim();
  if (!val) { nameInput.focus(); return; }
  myName = val;
  whoLabel.textContent = myName + " olarak bağlandınız";
  nameScreen.style.display = "none";
  chatScreen.style.display = "flex";

  // Ağ istekleri SADECE isim seçildikten sonra başlar
  loadInitial();
  connectStream();
}

let eventSource = null;
function connectStream() {
  if (eventSource) eventSource.close();
  
  eventSource = new EventSource(BACKEND_URL + "/api/stream");

  eventSource.onopen = () => {
    statusLabel.textContent = "bağlı";
  };

  eventSource.onmessage = (event) => {
    try {
      const msg = JSON.parse(event.data);
      if (msg && msg.id && !renderedMsgIds.has(msg.id)) {
        renderMessage(msg);
        scrollToBottom();
      }
    } catch (e) {
      console.error("SSE parse hatası:", e);
    }
  };

  eventSource.onerror = () => {
    statusLabel.textContent = "yeniden bağlanıyor...";
  };
}

const MAX_IMAGE_BYTES = 1024 * 1024; // 1MB sıkıştırma hedefi

function loadImageFromFile(file) {
  return new Promise((resolve, reject) => {
    const img = new Image();
    img.onload = () => resolve(img);
    img.onerror = reject;
    img.src = URL.createObjectURL(file);
  });
}

function canvasToDataUrl(canvas, quality) {
  return canvas.toDataURL("image/jpeg", quality);
}

function dataUrlByteSize(dataUrl) {
  const commaIdx = dataUrl.indexOf(",");
  const base64 = commaIdx >= 0 ? dataUrl.slice(commaIdx + 1) : dataUrl;
  return Math.floor((base64.length * 3) / 4);
}

async function compressImageToTarget(file, maxBytes) {
  const img = await loadImageFromFile(file);
  URL.revokeObjectURL(img.src);

  let width = img.naturalWidth;
  let height = img.naturalHeight;
  const MAX_DIMENSION = 2000;
  if (width > MAX_DIMENSION || height > MAX_DIMENSION) {
    const scale = MAX_DIMENSION / Math.max(width, height);
    width = Math.round(width * scale);
    height = Math.round(height * scale);
  }

  const canvas = document.createElement("canvas");
  canvas.width = width;
  canvas.height = height;
  const ctx = canvas.getContext("2d");
  ctx.drawImage(img, 0, 0, width, height);

  let quality = 0.9;
  let dataUrl = canvasToDataUrl(canvas, quality);

  while (dataUrlByteSize(dataUrl) > maxBytes && quality > 0.1) {
    quality -= 0.1;
    dataUrl = canvasToDataUrl(canvas, quality);
  }

  while (dataUrlByteSize(dataUrl) > maxBytes && (canvas.width > 200 || canvas.height > 200)) {
    canvas.width = Math.round(canvas.width * 0.85);
    canvas.height = Math.round(canvas.height * 0.85);
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    ctx.drawImage(img, 0, 0, canvas.width, canvas.height);
    quality = 0.85;
    dataUrl = canvasToDataUrl(canvas, quality);
    while (dataUrlByteSize(dataUrl) > maxBytes && quality > 0.1) {
      quality -= 0.1;
      dataUrl = canvasToDataUrl(canvas, quality);
    }
  }

  return dataUrl;
}

attachBtn.addEventListener("click", () => fileInput.click());
fileInput.addEventListener("change", async () => {
  const file = fileInput.files[0];
  if (!file) return;
  showLoading(true);
  try {
    const compressed = await compressImageToTarget(file, MAX_IMAGE_BYTES);
    pendingImage = compressed;
    imgPreview.src = pendingImage;
    imgPreviewWrap.style.display = "block";
  } catch (err) {
    showError("Görsel işlenirken hata oluştu.");
  } finally {
    showLoading(false);
  }
});

removeImgBtn.addEventListener("click", () => {
  pendingImage = null;
  fileInput.value = "";
  imgPreviewWrap.style.display = "none";
});

textInput.addEventListener("input", () => {
  textInput.style.height = "auto";
  textInput.style.height = Math.min(textInput.scrollHeight, 120) + "px";
});

textInput.addEventListener("keydown", (e) => {
  if (e.key === "Enter" && !e.shiftKey) {
    e.preventDefault();
    sendMessage();
  }
});
sendBtn.addEventListener("click", sendMessage);

async function sendMessage() {
  const text = textInput.value.trim();
  if (!text && !pendingImage) return;
  sendBtn.disabled = true;
  showLoading(true);
  try {
    const res = await fetch(BACKEND_URL + "/api/send", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name: myName, message: text, image: pendingImage }),
    });
    const data = await res.json();
    if (!res.ok) {
      showError(data.error || "Gönderim başarısız.");
    } else {
      textInput.value = "";
      textInput.style.height = "auto";
      pendingImage = null;
      fileInput.value = "";
      imgPreviewWrap.style.display = "none";
    }
  } catch (err) {
    showError("Bağlantı hatası: " + err.message);
  } finally {
    sendBtn.disabled = false;
    showLoading(false);
  }
}

function parseMessageWithLinks(text) {
  const div = document.createElement("div");
  const urlRegex = /(https?:\/\/[^\s]+)/g;
  const parts = text.split(urlRegex);
  
  parts.forEach((part) => {
    if (urlRegex.test(part)) {
      const a = document.createElement("a");
      a.href = part;
      a.textContent = part;
      a.target = "_blank";
      a.rel = "noopener noreferrer";
      div.appendChild(a);
    } else {
      if (part) div.appendChild(document.createTextNode(part));
    }
  });
  
  return div;
}

function applyTransform() {
  lightboxImg.style.transform = `translate(${panX}px, ${panY}px) scale(${zoomLevel})`;
  lightboxImg.classList.toggle("zoomed", zoomLevel > 1);
}

function resetZoom() {
  zoomLevel = 1;
  panX = 0;
  panY = 0;
  applyTransform();
}

function setZoom(newZoom) {
  zoomLevel = Math.min(MAX_ZOOM, Math.max(MIN_ZOOM, newZoom));
  if (zoomLevel === 1) { panX = 0; panY = 0; }
  applyTransform();
}

function openLightbox(src) {
  lightboxImg.src = src;
  lightbox.classList.add("show");
  resetZoom();
}

function closeLightbox() {
  lightbox.classList.remove("show");
  lightboxImg.src = "";
  resetZoom();
}

lightboxClose.addEventListener("click", closeLightbox);
lightbox.addEventListener("click", (e) => {
  if (e.target === lightbox) closeLightbox();
});
document.addEventListener("keydown", (e) => {
  if (e.key === "Escape") closeLightbox();
});

zoomInBtn.addEventListener("click", () => setZoom(zoomLevel + 0.5));
zoomOutBtn.addEventListener("click", () => setZoom(zoomLevel - 0.5));
zoomResetBtn.addEventListener("click", resetZoom);

lightboxImg.addEventListener("wheel", (e) => {
  e.preventDefault();
  const delta = e.deltaY < 0 ? 0.25 : -0.25;
  setZoom(zoomLevel + delta);
}, { passive: false });

lightboxImg.addEventListener("dblclick", () => {
  setZoom(zoomLevel > 1 ? 1 : 2.5);
});

lightboxImg.addEventListener("mousedown", (e) => {
  if (zoomLevel <= 1) return;
  isDragging = true;
  lightboxImg.classList.add("dragging");
  dragStartX = e.clientX;
  dragStartY = e.clientY;
  panStartX = panX;
  panStartY = panY;
  e.preventDefault();
});

window.addEventListener("mousemove", (e) => {
  if (!isDragging) return;
  panX = panStartX + (e.clientX - dragStartX);
  panY = panStartY + (e.clientY - dragStartY);
  applyTransform();
});

window.addEventListener("mouseup", () => {
  isDragging = false;
  lightboxImg.classList.remove("dragging");
});

let touchStartDist = null;
let touchStartZoom = 1;
lightboxImg.addEventListener("touchstart", (e) => {
  if (e.touches.length === 2) {
    const dx = e.touches[0].clientX - e.touches[1].clientX;
    const dy = e.touches[0].clientY - e.touches[1].clientY;
    touchStartDist = Math.hypot(dx, dy);
    touchStartZoom = zoomLevel;
  } else if (e.touches.length === 1 && zoomLevel > 1) {
    isDragging = true;
    dragStartX = e.touches[0].clientX;
    dragStartY = e.touches[0].clientY;
    panStartX = panX;
    panStartY = panY;
  }
}, { passive: true });

lightboxImg.addEventListener("touchmove", (e) => {
  if (e.touches.length === 2 && touchStartDist !== null) {
    const dx = e.touches[0].clientX - e.touches[1].clientX;
    const dy = e.touches[0].clientY - e.touches[1].clientY;
    const dist = Math.hypot(dx, dy);
    setZoom(touchStartZoom * (dist / touchStartDist));
  } else if (e.touches.length === 1 && isDragging) {
    panX = panStartX + (e.touches[0].clientX - dragStartX);
    panY = panStartY + (e.touches[0].clientY - dragStartY);
    applyTransform();
  }
}, { passive: true });

lightboxImg.addEventListener("touchend", () => {
  isDragging = false;
  touchStartDist = null;
});

function downloadImage(dataUrl, filename = "image.png") {
  const a = document.createElement("a");
  a.href = dataUrl;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
}

function renderMessage(m) {
  if (m.id) renderedMsgIds.add(m.id);

  const wrap = document.createElement("div");
  wrap.className = "msg " + (m.name === myName ? "me" : "other");

  const bubble = document.createElement("div");
  bubble.className = "bubble";
  if (m.message) {
    const p = parseMessageWithLinks(m.message);
    bubble.appendChild(p);
  }
  if (m.image) {
    const imgContainer = document.createElement("div");
    imgContainer.className = "imgContainer";
    
    const img = document.createElement("img");
    img.src = m.image;
    img.addEventListener("click", (e) => {
      e.stopPropagation();
      openLightbox(m.image);
    });
    imgContainer.appendChild(img);
    
    const downloadBtn = document.createElement("button");
    downloadBtn.className = "downloadBtn";
    downloadBtn.textContent = "⬇️ İndir";
    downloadBtn.addEventListener("click", () => {
      downloadImage(m.image, `image_${m.id || Date.now()}.png`);
    });
    imgContainer.appendChild(downloadBtn);
    
    bubble.appendChild(imgContainer);
  }
  wrap.appendChild(bubble);

  const meta = document.createElement("div");
  meta.className = "meta";
  const d = new Date(m.created_at);
  meta.textContent = m.name + " • " + d.toLocaleTimeString("tr-TR", { hour: "2-digit", minute: "2-digit" });
  wrap.appendChild(meta);

  messagesEl.appendChild(wrap);
}

function scrollToBottom() {
  messagesEl.scrollTop = messagesEl.scrollHeight;
}

async function loadInitial() {
  try {
    const res = await fetch(BACKEND_URL + "/api/messages?limit=50");
    const data = await res.json();
    if (res.ok) {
      (data.messages || []).forEach((m) => {
        if (m.id && !renderedMsgIds.has(m.id)) {
          renderMessage(m);
        }
      });
      scrollToBottom();
      statusLabel.textContent = "bağlı";
    } else {
      statusLabel.textContent = "hata";
    }
  } catch (err) {
    statusLabel.textContent = "bağlantı yok";
  }
}
