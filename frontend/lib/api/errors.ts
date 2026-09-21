/**
 * Generic API Error representation.
 * Handles HTTP transport-level errors without coupling to feature-specific logic.
 */
export class ApiError extends Error {
  public readonly status: number;
  public readonly code?: string;
  public readonly correlationId?: string;

  constructor(
    message: string,
    status: number,
    code?: string,
    correlationId?: string
  ) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.code = code;
    this.correlationId = correlationId;

    // Maintain V8 stack trace context if available
    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, ApiError);
    }
  }
}
