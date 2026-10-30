// Schema generator (master plan §2.3): embeds the 11 protocol schemas as JSON constants and
// generates TypeScript interfaces from them. Run by `npm run gen`. Never hand-edit src/generated/.
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import { Ajv2020 } from "ajv/dist/2020.js";
import { compile } from "json-schema-to-typescript";

const scriptDir = path.dirname(fileURLToPath(import.meta.url));
const projectDir = path.resolve(scriptDir, "..");
const schemasDir = path.resolve(projectDir, "..", "doc", "schemas");
const outDir = path.join(projectDir, "src", "generated");

// --- fixed name maps (§2.3: names are fixed by the generator, not by title heuristics) ---

const ROOT_NAMES: Record<string, string> = {
  "actio.schema.json": "ActioMeta",
  "spec.schema.json": "Spec",
  "status.schema.json": "Status",
  "announce.schema.json": "Announce",
  "request.schema.json": "RequestFile",
  "cancellation.schema.json": "CancellationFile",
  "response.schema.json": "ResponseFile",
  "confirmation.schema.json": "ConfirmationFile",
  "error.schema.json": "ErrorResult",
  "diarium.schema.json": "DiariumLine",
  // common.schema.json is a $defs container, not a document: no root interface.
};

/** capitalize-first: `agentId` -> `AgentId` (deterministic, no title heuristics). */
function defName(file: string, def: string): string {
  if (!(file in ROOT_NAMES) && file !== "common.schema.json") throw new Error(`unknown schema file ${file}`);
  // The four envelope-body defs would collide on the bare capitalized name; the plan fixes their names.
  const override: Record<string, string> = {
    "request.schema.json:body": "RequestBody",
    "cancellation.schema.json:body": "CancellationBody",
    "response.schema.json:body": "ResponseBody",
    "confirmation.schema.json:body": "ConfirmationBody",
  };
  const forced = override[`${file}:${def}`];
  if (forced !== undefined) return forced;
  return def.charAt(0).toUpperCase() + def.slice(1);
}

type Json = Record<string, unknown>;

function clone(value: unknown): unknown {
  return JSON.parse(JSON.stringify(value));
}

function defsOf(schema: Json): Json {
  return (schema["$defs"] as Json | undefined) ?? {};
}

// --- main ---

const files = fs
  .readdirSync(schemasDir)
  .filter((f) => f.endsWith(".schema.json"))
  .sort();
if (files.length !== 11) throw new Error(`expected 11 schemas in ${schemasDir}, found ${files.length}`);

const schemas = new Map<string, Json>();
for (const file of files) {
  const schema = JSON.parse(fs.readFileSync(path.join(schemasDir, file), "utf8")) as Json;
  if (schema["$id"] !== file) throw new Error(`${file}: $id is ${String(schema["$id"])}, expected the file name`);
  schemas.set(file, schema);
}

// 1. Strict compile gate: same Ajv options as the runtime (§2.3). Fails the build loudly.
const ajv = new Ajv2020({ strict: true, allErrors: false });
for (const file of files) ajv.addSchema(schemas.get(file) as Json);
let compiled = 0;
for (const file of files) {
  const schema = schemas.get(file) as Json;
  const pointers = [file, ...Object.keys(defsOf(schema)).map((d) => `${file}#/$defs/${d}`)];
  for (const pointer of pointers) {
    if (ajv.getSchema(pointer) === undefined) throw new Error(`cannot get compiled schema ${pointer}`);
    compiled++;
  }
}
console.error(`gen: strict compile gate passed (${compiled} schemas/$defs)`);

// 2. Shared bundle: every file root and every $defs entry under its fixed interface name.
const sourceFileOf = new Map<string, string>(); // interface name -> source schema file
const bundle: Json = {};
for (const file of files) {
  const schema = schemas.get(file) as Json;
  if (ROOT_NAMES[file] !== undefined) {
    bundle[ROOT_NAMES[file]] = clone(schema);
    sourceFileOf.set(ROOT_NAMES[file], file);
  }
  for (const [def, value] of Object.entries(defsOf(schema))) {
    const name = defName(file, def);
    if (bundle[name] !== undefined) throw new Error(`bundle name collision: ${name}`);
    bundle[name] = clone(value);
    sourceFileOf.set(name, file);
  }
}
for (const [name, def] of Object.entries(bundle)) {
  stripMeta(def);
  rewriteRefs(def, sourceFileOf.get(name) as string, name);
}

/** Strip metadata that would let json-schema-to-typescript pick its own names or complain.
 * Nested `$defs` are dead weight after ref rewriting (all refs point at `#/definitions/`). */
function stripMeta(node: unknown): void {
  if (Array.isArray(node)) {
    for (const item of node) stripMeta(item);
    return;
  }
  if (node === null || typeof node !== "object") return;
  const obj = node as Json;
  delete obj["title"];
  delete obj["$id"];
  delete obj["$schema"];
  delete obj["$defs"];
  for (const value of Object.values(obj)) stripMeta(value);
}

/** Rewrite every `$ref` to an internal bundle pointer; fail on an unresolvable ref. */
function rewriteRefs(node: unknown, currentFile: string, where: string): void {
  if (Array.isArray(node)) {
    for (const item of node) rewriteRefs(item, currentFile, `${where}[]`);
    return;
  }
  if (node === null || typeof node !== "object") return;
  const obj = node as Json;
  const ref = obj["$ref"];
  if (typeof ref === "string") {
    const [filePart, frag] = ref.split("#", 2);
    const file = filePart === "" ? currentFile : filePart;
    let target: string | undefined;
    if (frag === undefined || frag === "") target = ROOT_NAMES[file];
    else if (frag.startsWith("/$defs/") && !frag.slice(7).includes("/")) target = defName(file, frag.slice(7));
    if (target === undefined || bundle[target] === undefined) {
      throw new Error(`unresolvable $ref "${ref}" at bundle/${where}`);
    }
    obj["$ref"] = `#/definitions/${target}`;
    return; // 2020-12: $ref may have siblings; they are rewritten too
  }
  for (const [key, value] of Object.entries(obj)) rewriteRefs(value, currentFile, `${where}/${key}`);
}

// 3. One json-schema-to-typescript run per target; merge declarations by name.
// Targets are exactly the plan's list (§2.3); further defs (e.g. diarium Exchange) are emitted
// automatically when referenced. if/then/else-only defs (requestRules, commandRules) are not
// emitted: json-schema-to-typescript ignores if/then/else (known limit, §2.3).
const targets: string[] = [
  "ActioMeta", "Spec", "Status", "Announce", "RequestFile", "CancellationFile", "ResponseFile",
  "ConfirmationFile", "ErrorResult", "DiariumLine", "Exchange",
  "RequestBody", "CancellationBody", "ResponseBody", "ConfirmationBody",
  "Capability", "CapabilityActio",
  "AgentId", "CapabilityName", "Datetime", "Ts", "Duration", "ActioVersion", "Rid", "Nonce",
  "Signature", "Base64", "Attachments", "Header", "RequestHeader",
];
for (const name of targets) if (bundle[name] === undefined) throw new Error(`no bundle def for target ${name}`);

const declarations = new Map<string, string>();
const order: string[] = [];
for (const name of targets) {
  const doc = clone(bundle[name]) as Json;
  doc["definitions"] = clone(bundle);
  // jst 15.0.4 emits JSDoc comments even with commentStyle "none"; strip them so that every
  // remaining chunk is exactly one declaration (descriptions stay available in the schemas).
  const emitted = (await compile(doc as never, name, { bannerComment: "" })).replace(/\/\*[\s\S]*?\*\//g, "");
  const chunks = emitted.split(/(?=export (?:interface|type) )/);
  const normalize = (s: string): string => s.replace(/\s+/g, " ").trim();
  for (const chunk of chunks) {
    const match = /^export (?:interface|type) (\w+)/.exec(chunk.trim());
    if (match === null) {
      if (chunk.trim() === "") continue;
      throw new Error(`unparsable emitted declaration for ${name}`);
    }
    const declared = match[1];
    const text = chunk.trimEnd() + "\n";
    const known = declarations.get(declared);
    if (known === undefined) {
      declarations.set(declared, text);
      order.push(declared);
    } else if (normalize(known) !== normalize(text)) {
      throw new Error(`declaration "${declared}" emitted twice with different text`);
    }
  }
}

// 4. Emit, in the fixed target order.
function header(kind: string): string {
  const lines = [
    "// GENERATED from ../doc/schemas by scripts/gen-schemas.ts — do not edit.",
    `// ${kind}`,
    "// SHA-256 of each source schema:",
  ];
  for (const file of files) {
    const sha = crypto.createHash("sha256").update(fs.readFileSync(path.join(schemasDir, file))).digest("hex");
    lines.push(`//   ${file}: ${sha}`);
  }
  return lines.join("\n") + "\n\n";
}

const camel = (s: string): string => s.charAt(0).toLowerCase() + s.slice(1);

let schemasTs = header("Embedded protocol schemas, keyed by $id (content unmodified).");
schemasTs += "export interface GeneratedSchema {\n  [key: string]: unknown;\n}\n\n";
for (const file of files) {
  const constName = `${camel(ROOT_NAMES[file] ?? "common")}Schema`;
  schemasTs += `export const ${constName}: GeneratedSchema = ${JSON.stringify(schemas.get(file), null, 2)};\n\n`;
}
schemasTs += "export const schemas: { [id: string]: GeneratedSchema } = {\n";
for (const file of files) {
  schemasTs += `  "${file}": ${camel(ROOT_NAMES[file] ?? "common")}Schema,\n`;
}
schemasTs += "};\n";

let typesTs = header(
  "Protocol TypeScript interfaces (json-schema-to-typescript; if/then/else not reflected — the Ajv validators are authoritative).",
);
for (const name of order) typesTs += `${declarations.get(name)}\n`;

fs.mkdirSync(outDir, { recursive: true });
fs.writeFileSync(path.join(outDir, "schemas.ts"), schemasTs);
fs.writeFileSync(path.join(outDir, "types.ts"), typesTs);
console.error(`gen: wrote schemas.ts (${files.length} schemas) and types.ts (${order.length} declarations)`);
