const conversation = document.getElementById("conversation");
const verifyForm = document.getElementById("verifyForm");
const textForm = document.getElementById("textForm");
const phoneInput = document.getElementById("phoneInput");
const emailInput = document.getElementById("emailInput");
const sourceSelect = document.getElementById("sourceSelect");
const queryInput = document.getElementById("queryInput");
const queryButton = document.getElementById("queryButton");
const recordButton = document.getElementById("recordButton");
const statusBadge = document.getElementById("statusBadge");
const verificationBadge = document.getElementById("verificationBadge");
const liveTranscript = document.getElementById("liveTranscript");

let sessionId = null;
let recognition = null;
let isRecording = false;
let preferredVoice = null;

function initVoiceSelection() {
    const pickVoice = () => {
        const voices = window.speechSynthesis.getVoices() || [];
        preferredVoice =
            voices.find((v) => v.lang && v.lang.toLowerCase().startsWith("en") && /female|zira|aria/i.test(v.name)) ||
            voices.find((v) => v.lang && v.lang.toLowerCase().startsWith("en")) ||
            null;
    };

    pickVoice();
    if (typeof window.speechSynthesis.onvoiceschanged !== "undefined") {
        window.speechSynthesis.onvoiceschanged = pickVoice;
    }
}

function speakResponse(text) {
    if (!text || !("speechSynthesis" in window)) {
        return false;
    }

    window.speechSynthesis.cancel();
    const utterance = new SpeechSynthesisUtterance(text);
    utterance.lang = "en-US";
    utterance.rate = 1.0;
    utterance.pitch = 1.0;
    if (preferredVoice) {
        utterance.voice = preferredVoice;
    }
    window.speechSynthesis.speak(utterance);
    return true;
}

function formatLatency(latencyMs) {
    if (typeof latencyMs !== "number" || Number.isNaN(latencyMs)) {
        return "";
    }
    return `${Math.max(0, Math.round(latencyMs))} ms`;
}

function formatSource(sourceName) {
    if (!sourceName) {
        return "";
    }
    return String(sourceName).toUpperCase();
}

function addMessage(role, text, options = {}) {
    if (!text) {
        return;
    }

    const card = document.createElement("article");
    card.className = `message ${role}`;

    const title = document.createElement("h3");
    title.className = "message-title";
    const roleLabel = role === "user" ? "Customer" : "Agent";
    const titleParts = [roleLabel];
    if (role === "agent") {
        const sourceLabel = formatSource(options.dataSource);
        const latencyLabel = formatLatency(options.latencyMs);
        if (sourceLabel) {
            titleParts.push(sourceLabel);
        }
        if (latencyLabel) {
            titleParts.push(latencyLabel);
        }
    }
    title.textContent = titleParts.join(" • ");

    const body = document.createElement("p");
    body.className = "message-body";
    body.textContent = text;

    card.appendChild(title);
    card.appendChild(body);
    conversation.prepend(card);
}

function updateVerificationBadge(isVerified, customerName) {
    verificationBadge.classList.toggle("success", Boolean(isVerified));
    verificationBadge.classList.toggle("muted", !isVerified);
    verificationBadge.textContent = isVerified
        ? `Verified${customerName ? `: ${customerName}` : ""}`
        : "Not verified";
}

function setQueryControlsEnabled(enabled) {
    queryInput.disabled = !enabled;
    queryButton.disabled = !enabled;
    recordButton.disabled = !enabled;
}

async function ensureSession() {
    if (sessionId) {
        return sessionId;
    }

    const response = await fetch("/api/session", {
        method: "POST",
    });
    const payload = await response.json();
    sessionId = payload.session_id;
    if (Array.isArray(payload.available_sources) && sourceSelect) {
        sourceSelect.innerHTML = "";
        payload.available_sources.forEach((source) => {
            const option = document.createElement("option");
            option.value = source;
            option.textContent = source.toUpperCase();
            sourceSelect.appendChild(option);
        });
    }
    if (sourceSelect && payload.data_source) {
        sourceSelect.value = payload.data_source;
    }
    statusBadge.textContent = "Connected";
    updateVerificationBadge(payload.verified, payload.customer_name || "");
    setQueryControlsEnabled(Boolean(payload.verified));
    return sessionId;
}

async function verifyCustomer() {
    const activeSessionId = await ensureSession();
    const phone = phoneInput.value.trim();
    const email = emailInput.value.trim();

    if (!phone || !email) {
        addMessage("agent", "Please provide both phone number and email to verify.");
        return;
    }

    const formData = new FormData();
    formData.append("session_id", activeSessionId);
    formData.append("phone", phone);
    formData.append("email", email);
    formData.append("data_source", sourceSelect ? sourceSelect.value : "sf1");

    const response = await fetch("/api/verify", {
        method: "POST",
        body: formData,
    });

    if (!response.ok) {
        const errorPayload = await response.json().catch(() => ({}));
        addMessage("agent", errorPayload.detail || "Verification failed. Please check your details and selected source.");
        return;
    }

    const payload = await response.json();

    updateVerificationBadge(payload.verified, payload.customer_name);
    if (sourceSelect && payload.data_source) {
        sourceSelect.value = payload.data_source;
    }
    setQueryControlsEnabled(Boolean(payload.verified));
    addMessage("agent", payload.response || "Verification completed.");
}

async function sendTextQuery(query, source = "text") {
    const activeSessionId = await ensureSession();
    const formData = new FormData();
    formData.append("session_id", activeSessionId);
    formData.append("query", query);
    const requestStartTime = performance.now();

    const response = await fetch("/api/text-query", {
        method: "POST",
        body: formData,
    });

    if (response.status === 403) {
        addMessage("agent", "Please verify phone number and email first.");
        setQueryControlsEnabled(false);
        return;
    }

    const payload = await response.json();
    const requestLatencyMs = performance.now() - requestStartTime;
    const latencyMs = typeof payload.latency_ms === "number" ? payload.latency_ms : requestLatencyMs;

    addMessage("user", payload.transcript);
    if (source === "voice") {
        addMessage("agent", payload.response, { latencyMs, dataSource: payload.data_source });
        speakResponse(payload.response);
    } else {
        addMessage("agent", payload.response, { latencyMs, dataSource: payload.data_source });
    }
    updateVerificationBadge(payload.verified, payload.customer_name);
}

function buildRecognition() {
    const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
    if (!SpeechRecognition) {
        return null;
    }

    const rec = new SpeechRecognition();
    rec.lang = "en-US";
    rec.continuous = true;
    rec.interimResults = true;

    rec.onstart = () => {
        isRecording = true;
        recordButton.textContent = "Stop listening";
        statusBadge.textContent = "Listening";
        liveTranscript.textContent = "";
    };

    rec.onend = () => {
        if (isRecording) {
            // Restart automatically if user did not explicitly stop.
            rec.start();
        }
    };

    rec.onerror = (event) => {
        if (event.error === "no-speech") {
            return;
        }
        addMessage("agent", `Microphone error: ${event.error}`);
        stopRecording();
    };

    rec.onresult = async (event) => {
        let interim = "";
        let finalText = "";

        for (let i = event.resultIndex; i < event.results.length; i++) {
            const result = event.results[i];
            if (result.isFinal) {
                finalText += result[0].transcript.trim();
            } else {
                interim += result[0].transcript;
            }
        }

        liveTranscript.textContent = interim;

        if (finalText) {
            liveTranscript.textContent = "";
            await sendTextQuery(finalText, "voice");
        }
    };

    return rec;
}

function stopRecording() {
    isRecording = false;
    recordButton.textContent = "Start voice";
    statusBadge.textContent = "Connected";
    liveTranscript.textContent = "";
    if (recognition) {
        recognition.onend = null;
        recognition.stop();
        recognition = null;
    }
}

function toggleRecording() {
    if (isRecording) {
        stopRecording();
        return;
    }

    const SpeechRecognition = window.SpeechRecognition || window.webkitSpeechRecognition;
    if (!SpeechRecognition) {
        addMessage("agent", "Your browser does not support the Web Speech API. Please use Chrome or Edge.");
        return;
    }

    recognition = buildRecognition();
    recognition.start();
}

textForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const query = queryInput.value.trim();
    if (!query) {
        return;
    }

    queryInput.value = "";
    await sendTextQuery(query, "text");
});

recordButton.addEventListener("click", () => {
    toggleRecording();
});

verifyForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    await verifyCustomer();
});

ensureSession().catch(() => {
    statusBadge.textContent = "Backend unavailable";
});

initVoiceSelection();
