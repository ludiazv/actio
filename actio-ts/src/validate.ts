// Runtime validation (§2.3): Ajv validators over the embedded protocol schemas, the rules the
// schemas cannot express, and the capability schemas compiled on demand (D-19). No schema is
// re-implemented by hand.
import { Ajv2020 } from "ajv/dist/2020.js";
import type { ValidateFunction } from "ajv";
import { schemas } from "./generated/schemas.js";
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

// The same options as the generator's strict compile gate (§2.3): no Ajv option is relaxed.
const AJV_OPTIONS = { strict: true, allErrors: false };

let protocolAjv: Ajv2020 | undefined; // created once per process, on first use
let capabilityAjv: Ajv2020 | undefined; // second instance: peer $ids never mix with protocol schemas

/** The protocol Ajv instance with every embedded schema added (lazy singleton). */
function protocol(): Ajv2020 {
  if (protocolAjv === undefined) {
    const ajv = new Ajv2020(AJV_OPTIONS);
    for (const id of Object.keys(schemas)) ajv.addSchema(schemas[id]);
    protocolAjv = ajv;
  }
  return protocolAjv;
}

/** The capability Ajv instance (D-19): same strict options, no protocol schemas added. */
function capabilities(): Ajv2020 {
  if (capabilityAjv === undefined) capabilityAjv = new Ajv2020(AJV_OPTIONS);
  return capabilityAjv;
}

/** The compiled validator of a schema pointer; a missing one is a generator/contract bug → throw. */
function validator(pointer: string): ValidateFunction {
  const validate = protocol().getSchema(pointer);
  if (validate === undefined) throw new Error(`validate.ts: no compiled schema for "${pointer}"`);
  return validate;
}

/** Run one validator; `errors` is one human-readable string per Ajv error, most specific first. */
function check<T>(pointer: string, x: unknown): Validated<T> {
  const ajv = protocol();
  const validate = validator(pointer);
  if (validate(x)) return { ok: true, value: x as T };
  return { ok: false, errors: (validate.errors ?? []).map((err) => ajv.errorsText([err])) };
}

/** Validates a team metadata file (actio.json). */
export function validateActio(x: unknown): Validated<ActioMeta> {
  return check<ActioMeta>("actio.schema.json", x);
}

/** Validates an agent spec (spec.json). */
export function validateSpec(x: unknown): Validated<Spec> {
  return check<Spec>("spec.schema.json", x);
}

/** Validates an agent status (status.json). */
export function validateStatus(x: unknown): Validated<Status> {
  return check<Status>("status.schema.json", x);
}

/** Validates an announce or broadcast file (nuntium). */
export function validateAnnounce(x: unknown): Validated<Announce> {
  return check<Announce>("announce.schema.json", x);
}

/** Validates a request file (<rid>.json). */
export function validateRequest(x: unknown): Validated<RequestFile> {
  return check<RequestFile>("request.schema.json", x);
}

/** Validates a cancellation file (<rid>.can.json). */
export function validateCancellation(x: unknown): Validated<CancellationFile> {
  return check<CancellationFile>("cancellation.schema.json", x);
}

/** Validates a response file (<rid>.res.json). */
export function validateResponse(x: unknown): Validated<ResponseFile> {
  return check<ResponseFile>("response.schema.json", x);
}

/** Validates a confirmation file (<rid>.con.json). */
export function validateConfirmation(x: unknown): Validated<ConfirmationFile> {
  return check<ConfirmationFile>("confirmation.schema.json", x);
}

/** Validates an error result (the result of an error response). */
export function validateError(x: unknown): Validated<ErrorResult> {
  return check<ErrorResult>("error.schema.json", x);
}

/** Validates one diarium line. */
export function validateDiarium(x: unknown): Validated<DiariumLine> {
  return check<DiariumLine>("diarium.schema.json", x);
}

/** Validates a decrypted request body (type "req" or "cmd"). */
export function validateRequestBody(x: unknown): Validated<RequestBody> {
  return check<RequestBody>("request.schema.json#/$defs/body", x);
}

/** Validates a decrypted cancellation body. */
export function validateCancellationBody(x: unknown): Validated<CancellationBody> {
  return check<CancellationBody>("cancellation.schema.json#/$defs/body", x);
}

/** Validates a decrypted response body. */
export function validateResponseBody(x: unknown): Validated<ResponseBody> {
  return check<ResponseBody>("response.schema.json#/$defs/body", x);
}

/** Validates a decrypted confirmation body. */
export function validateConfirmationBody(x: unknown): Validated<ConfirmationBody> {
  return check<ConfirmationBody>("confirmation.schema.json#/$defs/body", x);
}

/** The four envelope body keys, in the order the kinds are named. */
const BODY_KEYS = ["request", "cancellation", "response", "confirmation"] as const;

/** Kind of an envelope object by its body key; undefined when none is present. */
export function envelopeKind(obj: unknown): "request" | "cancellation" | "response" | "confirmation" | undefined {
  if (obj === null || typeof obj !== "object" || Array.isArray(obj)) return undefined;
  const present = BODY_KEYS.filter((key) => Object.hasOwn(obj, key));
  return present.length === 1 ? present[0] : undefined; // no body key, or more than one: not an envelope
}

/** The header fields that must match across an exchange (everything but `datetime`). */
const HEADER_FIELDS = ["rid", "src", "dst", "type", "actio_version"] as const;

/** All header fields of two envelopes equal, except `datetime` (D-11). */
export function sameHeader(a: Header, b: Header): boolean {
  return HEADER_FIELDS.every((field) => a[field] === b[field]);
}

/** The cancellation's `name` equals the request's `name` (*Schemas* rules). */
export function nameMatches(req: RequestBody, can: CancellationBody): boolean {
  return req.name === can.name;
}

/**
 * Capability names are unique within an agent (*Schemas*, "Rules that the schemas cannot express, checked
 * by the agents"). undefined when the names are unique; otherwise the first name the list repeats, so a
 * caller can say which one broke the rule (the schemas cannot: uniqueness is across array elements).
 */
export function uniqueCapabilityNames(spec: Spec): string | undefined {
  const seen = new Set<string>();
  for (const cap of spec.capabilities) {
    if (seen.has(cap.name)) return cap.name; // the first repeated name
    seen.add(cap.name);
  }
  return undefined;
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
  const ajv = capabilities();
  const params = ajv.compile(cap.parameters); // throws on a schema strict mode rejects
  const result = ajv.compile(cap.actio.result); // a JSON Schema may be a boolean
  return {
    validateParams: (paramsValue: unknown): boolean => params(paramsValue) === true,
    validateResult: (resultValue: unknown): boolean => result(resultValue) === true,
  };
}

/**
 * The first Ajv error of `capability.parameters` against `params` (D-19, capability Ajv instance),
 * as `"<instancePath or (root)> <message>"`; `undefined` when the parameters are valid, and when the
 * schema does not compile (the checks are skipped and the peer decides, as in D-19).
 */
export function checkParams(capability: Capability, params: unknown): string | undefined {
  let validate: ValidateFunction;
  try {
    validate = capabilities().compile(capability.parameters);
  } catch {
    return undefined; // D-19: an uncompilable schema skips the checks
  }
  if (validate(params) === true) return undefined;
  const err = (validate.errors ?? [])[0]; // allErrors: false → the first (most specific) error
  const where = err.instancePath === "" ? "(root)" : err.instancePath;
  return `${where} ${err.message}`;
}
