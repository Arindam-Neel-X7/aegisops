import { ApiError } from "./errors";

/**
 * Base URL for the API backend.
 * Uses environment variable, falling back to a relative path for proxying if needed.
 * Avoids hardcoding production or localhost URLs directly.
 */
const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || "";

interface RequestOptions extends Omit<RequestInit, "body"> {
  body?: unknown;
}

/**
 * Minimal, generic HTTP request abstraction wrapping the native fetch API.
 * Ensures JSON serialization, standard headers, and predictable error normalization.
 *
 * @param endpoint The API endpoint (e.g., "/health" or "health")
 * @param options Standard RequestInit options, overriding body to accept unknown objects
 * @returns The parsed JSON response or an empty object for 204 No Content
 */
export async function apiClient<T>(
  endpoint: string,
  options: RequestOptions = {}
): Promise<T> {
  const { body, headers, ...customConfig } = options;

  const config: RequestInit = {
    ...customConfig,
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
      ...headers,
    },
  };

  if (body !== undefined) {
    config.body = JSON.stringify(body);
  }

  const baseUrl = API_BASE_URL.replace(/\/+$/, "");
  const normalizedEndpoint = endpoint.replace(/^\/+/, "");
  const url = `${baseUrl}/${normalizedEndpoint}`;
  let response: Response;

  try {
    response = await fetch(url, config);
  } catch (error) {
    // Handle network-level errors (e.g., CORS, DNS, offline)
    throw new ApiError(
      error instanceof Error ? error.message : "Network request failed",
      0
    );
  }

  if (!response.ok) {
    // Attempt to parse standard error payload from the backend
    let errorMessage = response.statusText;
    let errorCode: string | undefined;

    try {
      const errorData = await response.json();
      if (errorData && typeof errorData === "object") {
        const data = errorData as Record<string, unknown>;

        if (typeof data.message === "string") {
          errorMessage = data.message;
        }

        if (typeof data.code === "string") {
          errorCode = data.code;
        }
      }
    } catch {
      // Ignore JSON parse errors for non-JSON error responses
    }

    const correlationId = response.headers.get("x-correlation-id") || undefined;

    throw new ApiError(errorMessage, response.status, errorCode, correlationId);
  }

  // Handle 204 No Content
  if (response.status === 204) {
    return {} as T;
  }

  // Return the properly typed JSON response
  return response.json();
}
