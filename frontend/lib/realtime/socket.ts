import { io, Socket } from "socket.io-client";

/**
 * The base URL for realtime connections.
 * Uses a dedicated environment variable for the socket server,
 * falling back to a relative path if unspecified.
 */
const SOCKET_URL = process.env.NEXT_PUBLIC_SOCKET_URL || "";

/**
 * Singleton instance of the Socket.IO client.
 * Kept module-private to enforce access through controlled methods
 * and ensure SSR safety.
 */
let socketInstance: Socket | null = null;

/**
 * Lazily initializes and returns the Socket.IO client instance.
 * Ensures that initialization only occurs in browser environments (SSR-safe).
 * 
 * @returns The initialized Socket instance.
 */
export function getSocket(): Socket {
  if (typeof window === "undefined") {
    throw new Error(
      "Socket.IO transport cannot be accessed during Server-Side Rendering."
    );
  }

  if (!socketInstance) {
    socketInstance = io(SOCKET_URL, {
      autoConnect: false, // Manual control over connection lifecycle
      reconnection: true, // Use default Socket.IO reconnection behavior
    });

    // Transport-level lifecycle logging
    socketInstance.on("connect_error", (error: Error) => {
      console.error("Realtime transport connection error:", error);
    });

    socketInstance.on("disconnect", (reason: string) => {
      console.warn("Realtime transport disconnected. Reason:", reason);
    });
  }

  return socketInstance;
}

/**
 * Initiates the realtime connection.
 * Safe to call multiple times (idempotent).
 */
export function connectRealtime(): void {
  if (typeof window === "undefined") return;
  const socket = getSocket();
  if (!socket.connected) {
    socket.connect();
  }
}

/**
 * Disconnects the realtime connection and performs cleanup.
 */
export function disconnectRealtime(): void {
  if (socketInstance) {
    socketInstance.disconnect();
    socketInstance = null;
  }
}

/**
 * Subscribes to a generic realtime event.
 *
 * @param event The event name
 * @param callback The callback invoked when the event is received
 */
export function subscribeToEvent<T = unknown>(
  event: string,
  callback: (payload: T) => void
): void {
  if (typeof window === "undefined") return;
  getSocket().on(event, callback as (...args: any[]) => void);
}

/**
 * Unsubscribes from a generic realtime event.
 *
 * @param event The event name
 * @param callback The specific callback to remove, or removes all if omitted
 */
export function unsubscribeFromEvent<T = unknown>(
  event: string,
  callback?: (payload: T) => void
): void {
  if (typeof window === "undefined") return;
  if (callback) {
    getSocket().off(event, callback as (...args: any[]) => void);
  } else {
    getSocket().off(event);
  }
}

/**
 * Emits a generic realtime event to the server.
 *
 * @param event The event name
 * @param payload The event data to send
 */
export function emitEvent<T = unknown>(event: string, payload: T): void {
  if (typeof window === "undefined") return;
  getSocket().emit(event, payload);
}
