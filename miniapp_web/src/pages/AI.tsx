import { useState, useRef, useCallback, useEffect } from "react";
import { clsx } from "clsx";
import { getInitData } from "@/lib/telegram";
import { hapticImpact, hapticSuccess, hapticError } from "@/lib/haptics";
import type { VoiceServerMessage } from "@/api/types";

const WS_BASE = import.meta.env.VITE_WS_URL ?? `${location.protocol === "https:" ? "wss:" : "ws:"}//${location.host}`;

const LISTENING_BAR_HEIGHTS_PX = [14, 22, 16, 20, 12];

interface ChatMessage {
  role: "user" | "assistant" | "system";
  text: string;
  timestamp: number;
}

export default function AI() {
  const [connected, setConnected] = useState(false);
  const [listening, setListening] = useState(false);
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [textInput, setTextInput] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [, setUsername] = useState("");
  const wsRef = useRef<WebSocket | null>(null);
  const mediaRecorderRef = useRef<MediaRecorder | null>(null);
  const audioContextRef = useRef<AudioContext | null>(null);
  const audioNextStartRef = useRef(0);
  const pendingMicRef = useRef(false);
  const pendingTextRef = useRef<string | null>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  const addMessage = useCallback((msg: ChatMessage) => {
    setMessages((prev) => [...prev, msg]);
  }, []);

  const stopRecording = useCallback(() => {
    const recorder = mediaRecorderRef.current as unknown as {
      stream?: MediaStream;
      audioCtx?: AudioContext;
      processor?: ScriptProcessorNode;
      source?: MediaStreamAudioSourceNode;
    };
    if (recorder) {
      recorder.processor?.disconnect();
      recorder.source?.disconnect();
      recorder.audioCtx?.close();
      recorder.stream?.getTracks().forEach((t) => t.stop());
      mediaRecorderRef.current = null;
    }
    setListening(false);
  }, []);

  const startRecording = useCallback(async () => {
    try {
      const stream = await navigator.mediaDevices.getUserMedia({
        audio: {
          sampleRate: 16000,
          channelCount: 1,
          echoCancellation: true,
          noiseSuppression: true,
        },
      });

      const audioCtx = new AudioContext({ sampleRate: 16000 });
      const source = audioCtx.createMediaStreamSource(stream);
      const processor = audioCtx.createScriptProcessor(4096, 1, 1);

      processor.onaudioprocess = (e) => {
        if (!wsRef.current || wsRef.current.readyState !== WebSocket.OPEN) return;
        const float32 = e.inputBuffer.getChannelData(0);
        const int16 = new Int16Array(float32.length);
        for (let i = 0; i < float32.length; i++) {
          const s = Math.max(-1, Math.min(1, float32[i] ?? 0));
          int16[i] = s < 0 ? s * 0x8000 : s * 0x7fff;
        }
        const bytes = new Uint8Array(int16.buffer);
        let binary = "";
        for (let i = 0; i < bytes.length; i++) {
          binary += String.fromCharCode(bytes[i] ?? 0);
        }
        const base64 = btoa(binary);
        wsRef.current.send(JSON.stringify({ type: "audio", data: base64 }));
      };

      source.connect(processor);
      processor.connect(audioCtx.destination);

      mediaRecorderRef.current = { stream, audioCtx, processor, source } as unknown as MediaRecorder;
      setListening(true);
      hapticImpact("medium");
    } catch (err) {
      console.error("Mic access failed:", err);
      setError("Microphone access denied");
      hapticError();
    }
  }, []);

  const playAudio = useCallback(async (base64Data: string, mimeType: string) => {
    try {
      if (!audioContextRef.current) {
        audioContextRef.current = new AudioContext({ sampleRate: 24000 });
        audioNextStartRef.current = 0;
      }
      const ctx = audioContextRef.current;

      const binaryStr = atob(base64Data);
      const bytes = new Uint8Array(binaryStr.length);
      for (let i = 0; i < binaryStr.length; i++) {
        bytes[i] = binaryStr.charCodeAt(i);
      }

      const schedule = (buffer: AudioBuffer) => {
        const source = ctx.createBufferSource();
        source.buffer = buffer;
        source.connect(ctx.destination);
        const startAt = Math.max(ctx.currentTime, audioNextStartRef.current);
        source.start(startAt);
        audioNextStartRef.current = startAt + buffer.duration;
      };

      if (mimeType.includes("pcm")) {
        const samples = new Int16Array(bytes.buffer);
        const float32 = new Float32Array(samples.length);
        for (let i = 0; i < samples.length; i++) {
          float32[i] = (samples[i] ?? 0) / 32768;
        }
        const buffer = ctx.createBuffer(1, float32.length, 24000);
        buffer.copyToChannel(float32, 0);
        schedule(buffer);
      } else {
        const audioBuffer = await ctx.decodeAudioData(bytes.buffer.slice(0));
        schedule(audioBuffer);
      }
    } catch (err) {
      console.warn("Audio playback failed:", err);
    }
  }, []);

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return;
    if (wsRef.current?.readyState === WebSocket.CONNECTING) return;

    const ws = new WebSocket(`${WS_BASE}/ws/voice`);
    wsRef.current = ws;
    setError(null);

    ws.onopen = () => {
      const initData = getInitData();
      ws.send(JSON.stringify({ type: "auth", init_data: initData }));
    };

    ws.onmessage = (event) => {
      const msg: VoiceServerMessage = JSON.parse(event.data);

      switch (msg.type) {
        case "auth_ok":
          setConnected(true);
          setUsername(msg.username ?? "");
          addMessage({
            role: "assistant",
            text: `Hi ${msg.username ?? "Bro"}, how can I help you today?`,
            timestamp: Date.now(),
          });
          hapticSuccess();
          if (pendingMicRef.current) {
            pendingMicRef.current = false;
            void startRecording();
          }
          if (pendingTextRef.current != null) {
            const t = pendingTextRef.current;
            pendingTextRef.current = null;
            ws.send(JSON.stringify({ type: "text", text: t }));
          }
          break;

        case "text":
          if (msg.text) {
            addMessage({
              role: "assistant",
              text: msg.text,
              timestamp: Date.now(),
            });
          }
          break;

        case "audio":
          if (msg.data) {
            void playAudio(msg.data, msg.mime_type ?? "audio/pcm;rate=24000");
          }
          break;

        case "function_call":
          if (msg.name && msg.result) {
            const status = (msg.result as Record<string, unknown>).status;
            const resultText =
              status === "filled"
                ? `Trade executed: ${(msg.args as Record<string, unknown>)?.side} ${(msg.args as Record<string, unknown>)?.product} $${(msg.args as Record<string, unknown>)?.size_usd} @ ${(msg.result as Record<string, unknown>).fill_price}`
                : status === "closed"
                  ? `Position closed: ${(msg.args as Record<string, unknown>)?.product}`
                  : status === "error" || status === "failed"
                    ? `Error: ${(msg.result as Record<string, unknown>).error}`
                    : JSON.stringify(msg.result);
            addMessage({
              role: "system",
              text: resultText,
              timestamp: Date.now(),
            });
            if (status === "filled" || status === "closed") {
              hapticSuccess();
            } else {
              hapticError();
            }
          }
          break;

        case "turn_complete":
          break;

        case "error":
          setConnected(false);
          setError(msg.message ?? "Unknown error");
          hapticError();
          break;
      }
    };

    ws.onerror = () => {
      setError("Connection error");
      setConnected(false);
    };

    ws.onclose = () => {
      setConnected(false);
      setListening(false);
      stopRecording();
    };
  }, [addMessage, playAudio, startRecording, stopRecording]);

  const disconnect = useCallback(() => {
    pendingMicRef.current = false;
    pendingTextRef.current = null;
    if (wsRef.current) {
      try {
        if (wsRef.current.readyState === WebSocket.OPEN) {
          wsRef.current.send(JSON.stringify({ type: "end" }));
        }
      } catch {
        /* ignore */
      }
      wsRef.current.close();
      wsRef.current = null;
    }
    stopRecording();
    setConnected(false);
    setListening(false);
    void audioContextRef.current?.close();
    audioContextRef.current = null;
    audioNextStartRef.current = 0;
  }, [stopRecording]);

  const toggleListening = useCallback(() => {
    if (listening) {
      stopRecording();
    } else if (!connected) {
      pendingMicRef.current = true;
      connect();
    } else {
      void startRecording();
    }
  }, [listening, connected, connect, startRecording, stopRecording]);

  const sendText = useCallback(() => {
    if (!textInput.trim()) return;
    const text = textInput.trim();
    setTextInput("");
    addMessage({ role: "user", text, timestamp: Date.now() });

    if (wsRef.current?.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify({ type: "text", text }));
      return;
    }
    pendingTextRef.current = text;
    connect();
  }, [textInput, connect, addMessage]);

  useEffect(() => {
    return () => {
      disconnect();
    };
  }, [disconnect]);

  return (
    <div className="flex-1 flex flex-col bg-tg-bg">
      <div className="px-4 pt-4 pb-3">
        <h1 className="text-xl font-bold text-white">Speak with Bro</h1>
        <p className="text-xs text-tg-hint mt-0.5">Voice-powered trading assistant</p>
      </div>

      <div className="flex-1 overflow-y-auto hide-scrollbar px-4 pb-2">
        {messages.length === 0 && !connected && (
          <div className="flex flex-col items-center justify-center h-full text-center">
            <div className="w-20 h-20 rounded-full bg-white/5 flex items-center justify-center mb-4">
              <svg
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth={1.5}
                className="w-10 h-10 text-tg-hint"
              >
                <path d="M12 1a3 3 0 00-3 3v8a3 3 0 006 0V4a3 3 0 00-3-3z" />
                <path d="M19 10v2a7 7 0 01-14 0v-2" strokeLinecap="round" />
                <line x1="12" y1="19" x2="12" y2="23" strokeLinecap="round" />
                <line x1="8" y1="23" x2="16" y2="23" strokeLinecap="round" />
              </svg>
            </div>
            <p className="text-sm text-tg-hint mb-1">Say &quot;Yo Bro&quot; or tap the mic</p>
            <p className="text-xs text-tg-hint/60">Trade, check positions, get prices — all by voice</p>
          </div>
        )}

        {messages.map((msg, i) => (
          <div
            key={i}
            className={clsx("mb-3 max-w-[85%]", {
              "ml-auto": msg.role === "user",
              "mr-auto": msg.role === "assistant",
              "mx-auto": msg.role === "system",
            })}
          >
            <div
              className={clsx("rounded-2xl px-4 py-2.5 text-sm", {
                "bg-tg-button text-white": msg.role === "user",
                "bg-white/5 text-white": msg.role === "assistant",
                "bg-white/5 text-tg-hint text-xs text-center italic": msg.role === "system",
              })}
            >
              {msg.text}
            </div>
          </div>
        ))}
        <div ref={messagesEndRef} />
      </div>

      {error && (
        <div className="mx-4 mb-2 px-4 py-2 rounded-xl bg-short/10 text-short text-xs">{error}</div>
      )}

      <div className="px-4 pb-4 pt-2 border-t border-white/5">
        <div className="flex items-center justify-center mb-3">
          <button
            onClick={toggleListening}
            aria-label={listening ? "Stop listening" : "Start voice chat"}
            className={clsx(
              "w-16 h-16 rounded-full flex items-center justify-center transition-all",
              listening
                ? "bg-short scale-110 shadow-lg shadow-short/30"
                : connected
                  ? "bg-tg-button shadow-lg shadow-tg-button/20"
                  : "bg-white/10 hover:bg-white/15",
            )}
          >
            {listening ? (
              <svg viewBox="0 0 24 24" fill="currentColor" className="w-7 h-7 text-white">
                <rect x="6" y="6" width="12" height="12" rx="2" />
              </svg>
            ) : (
              <svg
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth={2}
                className={clsx("w-7 h-7", connected ? "text-white" : "text-tg-hint")}
              >
                <path d="M12 1a3 3 0 00-3 3v8a3 3 0 006 0V4a3 3 0 00-3-3z" />
                <path d="M19 10v2a7 7 0 01-14 0v-2" strokeLinecap="round" />
                <line x1="12" y1="19" x2="12" y2="23" strokeLinecap="round" />
                <line x1="8" y1="23" x2="16" y2="23" strokeLinecap="round" />
              </svg>
            )}
          </button>
        </div>

        {listening && (
          <div className="flex items-center justify-center gap-1 mb-3">
            {LISTENING_BAR_HEIGHTS_PX.map((h, i) => (
              <div
                key={i}
                className="w-1 bg-short rounded-full animate-pulse"
                style={{
                  height: `${h}px`,
                  animationDelay: `${i * 0.15}s`,
                }}
              />
            ))}
            <span className="ml-2 text-xs text-short">Listening...</span>
          </div>
        )}

        <div className="flex gap-2">
          <input
            type="text"
            placeholder="Ask Bro anything..."
            value={textInput}
            onChange={(e) => setTextInput(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && sendText()}
            className="flex-1 bg-white/5 rounded-xl px-4 py-3 text-sm text-white outline-none placeholder:text-white/20"
          />
          <button
            onClick={sendText}
            disabled={!textInput.trim()}
            className="px-4 py-3 rounded-xl bg-tg-button text-tg-button-text text-sm font-medium disabled:opacity-40"
          >
            Send
          </button>
        </div>

        <p className="text-[10px] text-tg-hint/40 text-center mt-2">Powered by Gemini Live API</p>
      </div>
    </div>
  );
}
