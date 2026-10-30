// Runtime validation (§2.3, WP2): Ajv validators over the embedded protocol schemas, the rules the
// schemas cannot express, and the capability schemas compiled on demand (D-19). No schema is
// re-implemented by hand.
import type {
  ActioMeta,
  Announce,
  CancellationBody,
  CancellationFile,
  Capability,
  ConfirmationBody,
  ConfirmationFile,
  DiariumLine,
  ErrorResult,
  Header,
  RequestBody,
  RequestFile,
  ResponseBody,
  ResponseFile,
  Spec,
  Status,
} from "./generated/types.js";

/** Result of a runtime validator: `ok` with the narrowed value, or the Ajv error texts. */
export type Validated<T> = { ok: true; value: T } | { ok: false; errors: string[] };

/** Validates a team metadata file (actio.json). */
export function validateActio(x: unknown): Validated<ActioMeta> {
  throw new Error("not implemented");
}

/** Validates an agent spec (spec.json). */
export function validateSpec(x: unknown): Validated<Spec> {
  throw new Error("not implemented");
}

/** Validates an agent status (status.json). */
export function validateStatus(x: unknown): Validated<Status> {
  throw new Error("not implemented");
}

/** Validates an announce or broadcast file (nuntium). */
export function validateAnnounce(x: unknown): Validated<Announce> {
  throw new Error("not implemented");
}

/** Validates a request file (<rid>.json). */
export function validateRequest(x: unknown): Validated<RequestFile> {
  throw new Error("not implemented");
}

/** Validates a cancellation file (<rid>.can.json). */
export function validateCancellation(x: unknown): Validated<CancellationFile> {
  throw new Error("not implemented");
}

/** Validates a response file (<rid>.res.json). */
export function validateResponse(x: unknown): Validated<ResponseFile> {
  throw new Error("not implemented");
}

/** Validates a confirmation file (<rid>.con.json). */
export function validateConfirmation(x: unknown): Validated<ConfirmationFile> {
  throw new Error("not implemented");
}

/** Validates an error result (the result of an error response). */
export function validateError(x: unknown): Validated<ErrorResult> {
  throw new Error("not implemented");
}

/** Validates one diarium line. */
export function validateDiarium(x: unknown): Validated<DiariumLine> {
  throw new Error("not implemented");
}

/** Validates a decrypted request body (type "req" or "cmd"). */
export function validateRequestBody(x: unknown): Validated<RequestBody> {
  throw new Error("not implemented");
}

/** Validates a decrypted cancellation body. */
export function validateCancellationBody(x: unknown): Validated<CancellationBody> {
  throw new Error("not implemented");
}

/** Validates a decrypted response body. */
export function validateResponseBody(x: unknown): Validated<ResponseBody> {
  throw new Error("not implemented");
}

/** Validates a decrypted confirmation body. */
export function validateConfirmationBody(x: unknown): Validated<ConfirmationBody> {
  throw new Error("not implemented");
}

/** Kind of an envelope object by its body key; undefined when none is present. */
export function envelopeKind(obj: unknown): "request" | "cancellation" | "response" | "confirmation" | undefined {
  throw new Error("not implemented");
}

/** All header fields of two envelopes equal, except `datetime` (D-11). */
export function sameHeader(a: Header, b: Header): boolean {
  throw new Error("not implemented");
}

/** The cancellation's `name` equals the request's `name` (*Schemas* rules). */
export function nameMatches(req: RequestBody, can: CancellationBody): boolean {
  throw new Error("not implemented");
}

/** Capability names are unique within an agent (*Schemas* rules). */
export function uniqueCapabilityNames(spec: Spec): boolean {
  throw new Error("not implemented");
}

/** Compiled parameter/result validators of one capability (D-19). */
export interface CompiledCapability {
  validateParams(params: unknown): boolean;
  validateResult(result: unknown): boolean;
}

/**
 * Compiles a capability's `parameters` and `actio.result` schemas with the capability Ajv instance
 * (strict, D-19); throws when a schema does not compile (own registration error; peers are handled
 * by the caller, which logs once and skips the checks).
 */
export function compileCapability(cap: Capability): CompiledCapability {
  throw new Error("not implemented");
}
