// GENERATED from ../doc/schemas by scripts/gen-schemas.ts — do not edit.
// Protocol TypeScript interfaces (json-schema-to-typescript; if/then/else not reflected — the Ajv validators are authoritative).
// SHA-256 of each source schema:
//   actio.schema.json: 42878a1f7f68bca69c7ff3da87067dfd196f0a1470f265ab69d098c5dd0d31a1
//   announce.schema.json: 2691e326edf212dd311cd44e73fcdbb3e75eb611e31b344db78e94fec16beee8
//   cancellation.schema.json: cd52565378d6a804e440c1fb3d6ba274133acfbfb80bc6140068a397f57021f1
//   common.schema.json: d63b38915805d9c63572864c75b087830ee08742cce2937a6bd477f483c0d6ed
//   confirmation.schema.json: a4c32d859e58aa58d86a88b8a12a5e0550a01450babd25d9176c4996535a6701
//   diarium.schema.json: 6dec1d475331f223b08be0121200ade36697ed2098e30b5311e4a10810e76961
//   error.schema.json: 640695f68cb686634d962adfac31bffad319f1fc8dbccee4a984ce2574c92796
//   request.schema.json: 41b9fe26797c539d4c7c5a58dae466391b2d38c3dbeef5b485c75a7585c568cd
//   response.schema.json: 3dbb69404d038aa67ff25cc270e2e559ca1e801c747de56705d0ffb3f3ab313e
//   spec.schema.json: 51106bc814c30169be8062f331468ef70c1fbe82fa68ec4f2d0f733db96fe978
//   status.schema.json: cdc840b01e5bd8d37424d8d2adf11017cbcdcbd475a19b9bc5f98ffe5109cd91

export type ActioVersion = string;

export interface ActioMeta {
  actio_version?: ActioVersion;
  description?: string;
  
  metadata?: {
    [k: string]: unknown;
  };
}

export type AgentId = string;

export type CapabilityName = string;

export interface Spec {
  
  id: string;
  actio_version: ActioVersion;
  
  datetime: string;
  
  description: string;
  
  namespaces?: (string | null)[] | null;
  
  depends?: AgentId[];
  
  wants?: AgentId[];
  
  disabled?: boolean;
  
  accepts_commands_from: AgentId[];
  
  capabilities: Capability[];
  
  security_support: ("sign" | "encrypt" | "issuer_sign")[];
  security_posture: "trusted" | "untrusted" | "secret" | "zero-trust";
  
  security?: {
    key: string;
    value: string;
  }[];
  
  info?: {
    [k: string]: unknown;
  };
}

export interface Capability {
  name: CapabilityName;
  description: string;
  
  parameters: {
    [k: string]: unknown;
  };
  actio: CapabilityActio;
}

export interface CapabilityActio {
  
  result:
    | {
        [k: string]: unknown;
      }
    | boolean;
  
  skill?: string;
  
  tool?: boolean;
  
  tool_name?: string;
  
  sync?: boolean;
  
  accepts_from?: AgentId[];
  
  max_parallel?: number;
  
  recommended_timeout?: number;
  
  disabled?: boolean;
}

export interface Status {
  id: AgentId;
  status: "inactive" | "idle" | "busy" | "error";
  
  datetime: string;
  
  msg?: string;
  
  load?: number;
  
  capability_status: {
    name: CapabilityName;
    running: number;
    queue: number;
  }[];
  
  tasks?: {
    capability: CapabilityName;
    
    name?: string;
    status: "pending" | "running" | "waiting" | "completed" | "cancelled";
    progress?: number;
    
    eta?: string;
  }[];
  
  contention?: boolean;
}

export interface Announce {
  
  id: string;
  
  datetime: string;
  
  ts: number;
  
  nuntium: string;
}

export type RequestFile = Header & {
  [k: string]: unknown;
} & {
  request: RequestBody | Base64;
  [k: string]: unknown;
};

export type Rid = string;

export type Datetime = string;

export type Ts = number;

export type Nonce = string;

export type Signature = string | null;

export type Attachments = string[];

export type Base64 = string;

export interface Header {
  rid: Rid;
  src: AgentId;
  dst: AgentId;
  datetime: Datetime;
  type: "cmd" | "req";
  actio_version: ActioVersion;
  [k: string]: unknown;
}

export interface RequestBody {
  ts: Ts;
  nonce: Nonce;
  signature: Signature;
  
  name: string;
  
  parameters: {
    [k: string]: unknown;
  };
  
  timeout: number;
  attachments: Attachments;
}

export type CancellationFile = RequestHeader & {
  cancellation: CancellationBody | Base64;
  [k: string]: unknown;
};

export type RequestHeader = Header & {
  type?: "req";
  [k: string]: unknown;
};

export interface CancellationBody {
  ts: Ts;
  nonce: Nonce;
  signature: Signature;
  name: CapabilityName;
  
  force: boolean;
}

export type ResponseFile = RequestHeader & {
  response: ResponseBody | Base64;
  [k: string]: unknown;
};

export type ResponseBody = {
  [k: string]: unknown;
} & {
  ts: Ts;
  nonce: Nonce;
  signature: Signature;
  
  error: boolean;
  
  cancellation: boolean;
  
  result: {
    [k: string]: unknown;
  };
  attachments: Attachments;
  
  work_time?: number;
};

export type ConfirmationFile = RequestHeader & {
  confirmation: ConfirmationBody | Base64;
  [k: string]: unknown;
};

export interface ConfirmationBody {
  ts: Ts;
  nonce: Nonce;
  signature: Signature;
}

export interface ErrorResult {
  
  code?: number;
  
  msg: string;
  
  details?: string;
}

export type DiariumLine = {
  [k: string]: unknown;
} & {
  
  id: string;
  
  datetime: string;
  event: string;
  
  event_data: {
    [k: string]: unknown;
  };
};

export interface Exchange {
  final_status: string;
  req: RequestFile | null;
  can: CancellationFile | null;
  res: ResponseFile | null;
  con: ConfirmationFile | null;
}

export type Duration = number;

