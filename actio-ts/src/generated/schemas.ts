// GENERATED from ../doc/schemas by scripts/gen-schemas.ts — do not edit.
// Embedded protocol schemas, keyed by $id (content unmodified).
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

export interface GeneratedSchema {
  [key: string]: unknown;
}

export const actioMetaSchema: GeneratedSchema = {
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "actio.schema.json",
  "title": "Actio team metadata (.actio/actio.json)",
  "description": "Optional team-level metadata, read-only for agents. All fields are optional.",
  "type": "object",
  "properties": {
    "actio_version": {
      "$ref": "common.schema.json#/$defs/actioVersion"
    },
    "description": {
      "type": "string"
    },
    "metadata": {
      "description": "Free content, not subject to any schema.",
      "type": "object"
    }
  },
  "additionalProperties": false
};

export const announceSchema: GeneratedSchema = {
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "announce.schema.json",
  "title": "Actio announce file (nuntium/<agent_id>.n.json) and broadcast file (nuntium/nuntium.json)",
  "description": "Signal only: any creation or update triggers a complete scan. All fields are mandatory.",
  "type": "object",
  "properties": {
    "id": {
      "$ref": "common.schema.json#/$defs/agentId",
      "description": "ID of the announcing agent."
    },
    "datetime": {
      "$ref": "common.schema.json#/$defs/datetime",
      "description": "Time of the announce."
    },
    "ts": {
      "$ref": "common.schema.json#/$defs/ts",
      "description": "Timestamp of the announce."
    },
    "nuntium": {
      "type": "string",
      "description": "Text with the news, e.g. \"agent is active\"."
    }
  },
  "required": [
    "id",
    "datetime",
    "ts",
    "nuntium"
  ],
  "additionalProperties": false
};

export const cancellationFileSchema: GeneratedSchema = {
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "cancellation.schema.json",
  "title": "Actio cancellation file (<rid>.can.json)",
  "description": "Written once by the requestor in the inbox of dst, with the same header as the request (only datetime changes). Requests only: commands cannot be cancelled. The body is an object, or a base64 string when encryption is in use. The name must be the same as in the request (not expressible in JSON Schema).",
  "type": "object",
  "allOf": [
    {
      "$ref": "common.schema.json#/$defs/requestHeader"
    }
  ],
  "properties": {
    "cancellation": {
      "anyOf": [
        {
          "$ref": "#/$defs/body"
        },
        {
          "$ref": "common.schema.json#/$defs/base64"
        }
      ]
    }
  },
  "required": [
    "cancellation"
  ],
  "unevaluatedProperties": false,
  "$defs": {
    "body": {
      "type": "object",
      "properties": {
        "ts": {
          "$ref": "common.schema.json#/$defs/ts"
        },
        "nonce": {
          "$ref": "common.schema.json#/$defs/nonce"
        },
        "signature": {
          "$ref": "common.schema.json#/$defs/signature"
        },
        "name": {
          "$ref": "common.schema.json#/$defs/capabilityName"
        },
        "force": {
          "type": "boolean",
          "description": "Forceful cancellation, interrupting the ongoing work as fast as possible."
        }
      },
      "required": [
        "ts",
        "nonce",
        "signature",
        "name",
        "force"
      ],
      "additionalProperties": false
    }
  }
};

export const commonSchema: GeneratedSchema = {
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "common.schema.json",
  "title": "Actio common definitions",
  "description": "Shared definitions used by the other Actio schemas (protocol version 0.1).",
  "$defs": {
    "agentId": {
      "description": "Agent ID: up to 32 characters, [a-zA-Z0-9_-].",
      "type": "string",
      "minLength": 1,
      "maxLength": 32,
      "pattern": "^[a-zA-Z0-9_-]+$"
    },
    "capabilityName": {
      "description": "Capability ID, unique within an agent.",
      "type": "string",
      "minLength": 1,
      "pattern": "^[a-zA-Z0-9_-]+$"
    },
    "datetime": {
      "description": "ISO 8601 date and time in UTC: YYYY-MM-DDThh:mm:ssZ.",
      "type": "string",
      "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$"
    },
    "ts": {
      "description": "Unix epoch time in seconds (UTC), 64-bit integer.",
      "type": "integer",
      "minimum": 0,
      "maximum": 9223372036854776000
    },
    "duration": {
      "description": "Duration in seconds.",
      "type": "number",
      "minimum": 0
    },
    "actioVersion": {
      "description": "Version of the Actio protocol, \"<major>.<minor>\" (informative).",
      "type": "string",
      "pattern": "^[0-9]+\\.[0-9]+$"
    },
    "rid": {
      "description": "Unique request ID: ULID in Crockford base32 (canonical upper-case form).",
      "type": "string",
      "pattern": "^[0-7][0-9A-HJKMNP-TV-Z]{25}$"
    },
    "nonce": {
      "description": "128-bit random nonce as a hex string.",
      "type": "string",
      "pattern": "^[0-9a-fA-F]{32}$"
    },
    "signature": {
      "description": "Base64 signature of the file by its writer; null when not needed (e.g. posture trusted).",
      "type": [
        "string",
        "null"
      ]
    },
    "base64": {
      "description": "Base64 string; an encrypted body that decrypts to the JSON object of the body.",
      "type": "string",
      "pattern": "^[A-Za-z0-9+/]*={0,2}$"
    },
    "attachments": {
      "description": "Files/objects in the forum folder, as paths relative to forum. Can be empty, never null.",
      "type": "array",
      "items": {
        "type": "string",
        "minLength": 1,
        "pattern": "^[^/]"
      }
    },
    "header": {
      "description": "Envelope header: the top-level fields of an envelope file (there is no nested \"header\" key), shared by all files of an exchange (only datetime changes). Applied to the whole file object. Public, never encrypted.",
      "type": "object",
      "properties": {
        "rid": {
          "$ref": "#/$defs/rid"
        },
        "src": {
          "$ref": "#/$defs/agentId"
        },
        "dst": {
          "$ref": "#/$defs/agentId"
        },
        "datetime": {
          "$ref": "#/$defs/datetime"
        },
        "type": {
          "enum": [
            "cmd",
            "req"
          ]
        },
        "actio_version": {
          "$ref": "#/$defs/actioVersion"
        }
      },
      "required": [
        "rid",
        "src",
        "dst",
        "datetime",
        "type",
        "actio_version"
      ]
    },
    "requestHeader": {
      "description": "Header of the files that exist only for requests (cancellation, response, confirmation): type is always \"req\". Like header, it applies to the top-level fields of the whole file object; \"type\": \"object\" repeats the type of header because strict validators (Ajv strictTypes) do not infer it through $ref.",
      "type": "object",
      "allOf": [
        {
          "$ref": "#/$defs/header"
        }
      ],
      "properties": {
        "type": {
          "const": "req"
        }
      }
    }
  }
};

export const confirmationFileSchema: GeneratedSchema = {
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "confirmation.schema.json",
  "title": "Actio confirmation file (<rid>.con.json)",
  "description": "Written once by the requestor in the inbox of dst after it captured the response, with the same header as the request (only datetime changes). Requests only. The body is an object, or a base64 string when encryption is in use.",
  "type": "object",
  "allOf": [
    {
      "$ref": "common.schema.json#/$defs/requestHeader"
    }
  ],
  "properties": {
    "confirmation": {
      "anyOf": [
        {
          "$ref": "#/$defs/body"
        },
        {
          "$ref": "common.schema.json#/$defs/base64"
        }
      ]
    }
  },
  "required": [
    "confirmation"
  ],
  "unevaluatedProperties": false,
  "$defs": {
    "body": {
      "type": "object",
      "properties": {
        "ts": {
          "$ref": "common.schema.json#/$defs/ts"
        },
        "nonce": {
          "$ref": "common.schema.json#/$defs/nonce"
        },
        "signature": {
          "$ref": "common.schema.json#/$defs/signature"
        }
      },
      "required": [
        "ts",
        "nonce",
        "signature"
      ],
      "additionalProperties": false
    }
  }
};

export const diariumLineSchema: GeneratedSchema = {
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "diarium.schema.json",
  "title": "Actio diarium line (<agent folder>/diarium.jsonl)",
  "description": "One JSON object per line. Implementations may define their own events; the events defined by the protocol have the event_data below.",
  "type": "object",
  "properties": {
    "id": {
      "$ref": "common.schema.json#/$defs/agentId",
      "description": "ID of the agent that writes the diarium."
    },
    "datetime": {
      "$ref": "common.schema.json#/$defs/datetime",
      "description": "Time of the event."
    },
    "event": {
      "type": "string",
      "minLength": 1
    },
    "event_data": {
      "description": "Depends on the event; can be null."
    }
  },
  "required": [
    "id",
    "datetime",
    "event",
    "event_data"
  ],
  "additionalProperties": false,
  "allOf": [
    {
      "if": {
        "properties": {
          "event": {
            "const": "status"
          }
        }
      },
      "then": {
        "properties": {
          "event_data": {
            "enum": [
              "inactive",
              "idle",
              "busy",
              "error"
            ]
          }
        }
      }
    },
    {
      "if": {
        "properties": {
          "event": {
            "const": "announce"
          }
        }
      },
      "then": {
        "properties": {
          "event_data": {
            "const": null
          }
        }
      }
    },
    {
      "if": {
        "properties": {
          "event": {
            "const": "inreq"
          }
        }
      },
      "then": {
        "properties": {
          "event_data": {
            "type": "object",
            "$ref": "#/$defs/exchange",
            "properties": {
              "final_status": {
                "enum": [
                  "cancelled",
                  "completed",
                  "confirmed",
                  "dropped"
                ]
              }
            }
          }
        }
      }
    },
    {
      "if": {
        "properties": {
          "event": {
            "const": "outreq"
          }
        }
      },
      "then": {
        "properties": {
          "event_data": {
            "type": "object",
            "$ref": "#/$defs/exchange",
            "properties": {
              "final_status": {
                "enum": [
                  "confirmed",
                  "cancelled",
                  "abandoned",
                  "sent"
                ]
              }
            }
          }
        }
      }
    },
    {
      "if": {
        "properties": {
          "event": {
            "const": "log"
          }
        }
      },
      "then": {
        "properties": {
          "event_data": {
            "type": "string"
          }
        }
      }
    }
  ],
  "$defs": {
    "exchange": {
      "description": "Final status and the four envelopes of an exchange, stored as exchanged (encrypted bodies stay encrypted); null when absent.",
      "type": "object",
      "properties": {
        "final_status": {
          "type": "string"
        },
        "req": {
          "anyOf": [
            {
              "$ref": "request.schema.json"
            },
            {
              "type": "null"
            }
          ]
        },
        "can": {
          "anyOf": [
            {
              "$ref": "cancellation.schema.json"
            },
            {
              "type": "null"
            }
          ]
        },
        "res": {
          "anyOf": [
            {
              "$ref": "response.schema.json"
            },
            {
              "type": "null"
            }
          ]
        },
        "con": {
          "anyOf": [
            {
              "$ref": "confirmation.schema.json"
            },
            {
              "type": "null"
            }
          ]
        }
      },
      "required": [
        "final_status",
        "req",
        "can",
        "res",
        "con"
      ],
      "additionalProperties": false
    }
  }
};

export const errorResultSchema: GeneratedSchema = {
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "error.schema.json",
  "title": "ActioErrorResponse",
  "description": "The result of an error response (error=true). Requests rejected at A0 use the standardized codes (see Acceptance errors).",
  "type": "object",
  "properties": {
    "code": {
      "type": "integer",
      "description": "Optional error code. Requests rejected at A0 use the standardized codes (see Acceptance errors)."
    },
    "msg": {
      "type": "string",
      "description": "Error message"
    },
    "details": {
      "type": "string",
      "description": "Error details"
    }
  },
  "required": [
    "msg"
  ],
  "additionalProperties": false
};

export const requestFileSchema: GeneratedSchema = {
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "request.schema.json",
  "title": "Actio request file (<rid>.json)",
  "description": "Written once by the requestor in the inbox of dst. Carries a request (type \"req\") or a command (type \"cmd\"). The body is an object, or a base64 string when encryption is in use; the decrypted body is validated with #/$defs/body.",
  "type": "object",
  "allOf": [
    {
      "$ref": "common.schema.json#/$defs/header"
    },
    {
      "if": {
        "type": "object",
        "properties": {
          "type": {
            "const": "req"
          }
        }
      },
      "then": {
        "type": "object",
        "properties": {
          "request": {
            "$ref": "#/$defs/requestRules"
          }
        }
      },
      "else": {
        "type": "object",
        "properties": {
          "request": {
            "$ref": "#/$defs/commandRules"
          }
        }
      }
    }
  ],
  "properties": {
    "request": {
      "anyOf": [
        {
          "$ref": "#/$defs/body"
        },
        {
          "$ref": "common.schema.json#/$defs/base64"
        }
      ]
    }
  },
  "required": [
    "request"
  ],
  "unevaluatedProperties": false,
  "$defs": {
    "body": {
      "type": "object",
      "properties": {
        "ts": {
          "$ref": "common.schema.json#/$defs/ts"
        },
        "nonce": {
          "$ref": "common.schema.json#/$defs/nonce"
        },
        "signature": {
          "$ref": "common.schema.json#/$defs/signature"
        },
        "name": {
          "type": "string",
          "minLength": 1,
          "description": "Capability name (type \"req\") or command name (type \"cmd\")."
        },
        "parameters": {
          "description": "Parameters of the request or command; null if there are none."
        },
        "timeout": {
          "$ref": "common.schema.json#/$defs/duration",
          "description": "Requestor timeout for the task; 0 = no timeout."
        },
        "attachments": {
          "$ref": "common.schema.json#/$defs/attachments"
        }
      },
      "required": [
        "ts",
        "nonce",
        "signature",
        "name",
        "parameters",
        "timeout",
        "attachments"
      ],
      "additionalProperties": false
    },
    "requestRules": {
      "description": "Extra rules for type \"req\" (applied only when the body is not encrypted). Guarded by \"if\": {\"type\": \"object\"} so that an encrypted body (a base64 string) still passes; a plain \"type\": \"object\" here would reject it. The guard also satisfies strict validators (Ajv strictTypes), which require an explicit type next to \"properties\".",
      "if": {
        "type": "object"
      },
      "then": {
        "type": "object",
        "properties": {
          "name": {
            "$ref": "common.schema.json#/$defs/capabilityName"
          },
          "parameters": {
            "type": [
              "object",
              "null"
            ]
          }
        }
      }
    },
    "commandRules": {
      "description": "Parameters of the protocol-standard commands (type \"cmd\"); custom commands are free. Applied only when the body is not encrypted: every branch \"if\" has \"type\": \"object\", so an encrypted body (a base64 string) matches no branch and passes; the \"type\": \"object\" in each \"then\" is there for strict validators (Ajv strictTypes).",
      "allOf": [
        {
          "if": {
            "type": "object",
            "properties": {
              "name": {
                "const": "stop"
              }
            },
            "required": [
              "name"
            ]
          },
          "then": {
            "type": "object",
            "properties": {
              "parameters": {
                "type": "object",
                "properties": {
                  "force": {
                    "type": "boolean"
                  }
                },
                "additionalProperties": false
              }
            }
          }
        },
        {
          "if": {
            "type": "object",
            "properties": {
              "name": {
                "const": "enable"
              }
            },
            "required": [
              "name"
            ]
          },
          "then": {
            "type": "object",
            "properties": {
              "parameters": {
                "type": "object",
                "properties": {
                  "enabled": {
                    "type": "boolean"
                  }
                },
                "required": [
                  "enabled"
                ],
                "additionalProperties": false
              }
            }
          }
        },
        {
          "if": {
            "type": "object",
            "properties": {
              "name": {
                "const": "restart"
              }
            },
            "required": [
              "name"
            ]
          },
          "then": {
            "type": "object",
            "properties": {
              "parameters": {
                "type": "object",
                "properties": {
                  "force": {
                    "type": "boolean"
                  },
                  "delay": {
                    "$ref": "common.schema.json#/$defs/duration"
                  }
                },
                "additionalProperties": false
              }
            }
          }
        },
        {
          "if": {
            "type": "object",
            "properties": {
              "name": {
                "const": "ping"
              }
            },
            "required": [
              "name"
            ]
          },
          "then": {
            "type": "object",
            "properties": {
              "parameters": {
                "type": "null"
              }
            }
          }
        }
      ]
    }
  }
};

export const responseFileSchema: GeneratedSchema = {
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "response.schema.json",
  "title": "Actio response file (<rid>.res.json)",
  "description": "Written once by the receiver in its own inbox, with the same header as the request (only datetime changes). Requests only. The body is an object, or a base64 string when encryption is in use. A non-error result must follow the capability result schema published in spec.json (checked by the requestor, not by this schema).",
  "type": "object",
  "allOf": [
    {
      "$ref": "common.schema.json#/$defs/requestHeader"
    }
  ],
  "properties": {
    "response": {
      "anyOf": [
        {
          "$ref": "#/$defs/body"
        },
        {
          "$ref": "common.schema.json#/$defs/base64"
        }
      ]
    }
  },
  "required": [
    "response"
  ],
  "unevaluatedProperties": false,
  "$defs": {
    "body": {
      "type": "object",
      "properties": {
        "ts": {
          "$ref": "common.schema.json#/$defs/ts"
        },
        "nonce": {
          "$ref": "common.schema.json#/$defs/nonce"
        },
        "signature": {
          "$ref": "common.schema.json#/$defs/signature"
        },
        "error": {
          "type": "boolean",
          "description": "The response is an error; false for a cancellation response."
        },
        "cancellation": {
          "type": "boolean",
          "description": "The response acknowledges a cancellation."
        },
        "result": {
          "description": "Result of the work (capability result schema), error result (error schema), or null for a cancellation response."
        },
        "attachments": {
          "$ref": "common.schema.json#/$defs/attachments"
        },
        "work_time": {
          "$ref": "common.schema.json#/$defs/duration",
          "description": "Seconds of actual work, without waiting times."
        }
      },
      "required": [
        "ts",
        "nonce",
        "signature",
        "error",
        "cancellation",
        "result",
        "attachments"
      ],
      "additionalProperties": false,
      "allOf": [
        {
          "if": {
            "properties": {
              "cancellation": {
                "const": true
              }
            },
            "required": [
              "cancellation"
            ]
          },
          "then": {
            "properties": {
              "error": {
                "const": false
              },
              "result": {
                "const": null
              },
              "attachments": {
                "type": "array",
                "maxItems": 0
              }
            }
          }
        },
        {
          "if": {
            "properties": {
              "error": {
                "const": true
              }
            },
            "required": [
              "error"
            ]
          },
          "then": {
            "properties": {
              "result": {
                "$ref": "error.schema.json"
              }
            }
          }
        }
      ]
    }
  }
};

export const specSchema: GeneratedSchema = {
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "spec.schema.json",
  "title": "Actio agent specification (<agent folder>/spec.json)",
  "description": "Publication of the agent's identity, capabilities and security features. Capability names must be unique within the agent (not expressible in JSON Schema).",
  "type": "object",
  "properties": {
    "id": {
      "$ref": "common.schema.json#/$defs/agentId",
      "description": "Unique agent ID."
    },
    "actio_version": {
      "$ref": "common.schema.json#/$defs/actioVersion"
    },
    "datetime": {
      "$ref": "common.schema.json#/$defs/datetime",
      "description": "Last time the spec was updated."
    },
    "description": {
      "type": "string",
      "description": "What the agent can do."
    },
    "namespaces": {
      "description": "Absent, null or empty: the agent belongs to the null namespace. null can be listed explicitly; \"*\" means all namespaces.",
      "type": [
        "array",
        "null"
      ],
      "items": {
        "anyOf": [
          {
            "type": "string",
            "minLength": 1
          },
          {
            "type": "null"
          }
        ]
      }
    },
    "depends": {
      "description": "Hard dependencies (agent IDs). Defaults to an empty list.",
      "type": "array",
      "items": {
        "$ref": "common.schema.json#/$defs/agentId"
      }
    },
    "wants": {
      "description": "Optional dependencies (agent IDs). Defaults to an empty list.",
      "type": "array",
      "items": {
        "$ref": "common.schema.json#/$defs/agentId"
      }
    },
    "disabled": {
      "description": "The agent is disabled (changed with the enable command). Absent means false.",
      "type": "boolean"
    },
    "accepts_commands_from": {
      "description": "Agent IDs allowed to send commands; an empty list means nobody.",
      "type": "array",
      "items": {
        "$ref": "common.schema.json#/$defs/agentId"
      }
    },
    "capabilities": {
      "description": "Capabilities offered to the team; can be empty.",
      "type": "array",
      "items": {
        "$ref": "#/$defs/capability"
      }
    },
    "security_support": {
      "description": "Security features implemented; empty if none.",
      "type": "array",
      "items": {
        "enum": [
          "sign",
          "encrypt",
          "issuer_sign"
        ]
      },
      "uniqueItems": true
    },
    "security_posture": {
      "enum": [
        "trusted",
        "untrusted",
        "secret",
        "zero-trust"
      ]
    },
    "security": {
      "description": "Security attributes as key-value pairs, e.g. public keys, issuer ID, issuer certificate.",
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "key": {
            "type": "string",
            "minLength": 1
          },
          "value": {
            "type": "string"
          }
        },
        "required": [
          "key",
          "value"
        ],
        "additionalProperties": false
      }
    },
    "info": {
      "description": "Extra information; free content, not schema validated.",
      "type": "object"
    }
  },
  "required": [
    "id",
    "actio_version",
    "datetime",
    "description",
    "accepts_commands_from",
    "capabilities",
    "security_support",
    "security_posture"
  ],
  "additionalProperties": false,
  "$defs": {
    "capability": {
      "description": "An OpenAI tool definition (name, description, parameters) plus the actio object.",
      "type": "object",
      "properties": {
        "name": {
          "$ref": "common.schema.json#/$defs/capabilityName"
        },
        "description": {
          "type": "string"
        },
        "parameters": {
          "description": "JSON object with the OpenAI tool calling parameter format; can be empty.",
          "type": "object"
        },
        "actio": {
          "$ref": "#/$defs/capabilityActio"
        }
      },
      "required": [
        "name",
        "description",
        "parameters",
        "actio"
      ],
      "additionalProperties": false
    },
    "capabilityActio": {
      "type": "object",
      "properties": {
        "result": {
          "description": "JSON schema of the result.",
          "anyOf": [
            {
              "type": "object"
            },
            {
              "type": "boolean"
            }
          ]
        },
        "skill": {
          "type": "string",
          "description": "Harness hint, no protocol effect."
        },
        "tool": {
          "type": "boolean",
          "description": "Harness hint, no protocol effect."
        },
        "tool_name": {
          "type": "string",
          "description": "Harness hint, no protocol effect."
        },
        "sync": {
          "type": "boolean",
          "description": "Harness hint, no protocol effect."
        },
        "accepts_from": {
          "description": "Absent or empty: every agent can use the capability; otherwise only the listed agents.",
          "type": "array",
          "items": {
            "$ref": "common.schema.json#/$defs/agentId"
          }
        },
        "max_parallel": {
          "description": "Maximum parallel requests, managed by the receiver. 0: unbounded.",
          "type": "integer",
          "minimum": 0
        },
        "recommended_timeout": {
          "description": "Recommended timeout for the response, in seconds.",
          "$ref": "common.schema.json#/$defs/duration"
        },
        "disabled": {
          "type": "boolean",
          "description": "The capability is disabled or unavailable."
        }
      },
      "required": [
        "result"
      ],
      "additionalProperties": false
    }
  }
};

export const statusSchema: GeneratedSchema = {
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "status.schema.json",
  "title": "Actio agent status (<agent folder>/status.json)",
  "description": "Written by the agent on every status change and at least every 90 seconds.",
  "type": "object",
  "properties": {
    "id": {
      "$ref": "common.schema.json#/$defs/agentId"
    },
    "status": {
      "enum": [
        "inactive",
        "idle",
        "busy",
        "error"
      ]
    },
    "datetime": {
      "$ref": "common.schema.json#/$defs/datetime",
      "description": "Time of the last status update."
    },
    "msg": {
      "type": "string",
      "description": "Last human-readable status message."
    },
    "load": {
      "type": "number",
      "minimum": 0,
      "maximum": 100,
      "description": "Estimated load of the agent, in %."
    },
    "capability_status": {
      "description": "One object per capability with the number of requests running and in queue.",
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "name": {
            "$ref": "common.schema.json#/$defs/capabilityName"
          },
          "running": {
            "type": "integer",
            "minimum": 0
          },
          "queue": {
            "type": "integer",
            "minimum": 0
          }
        },
        "required": [
          "name",
          "running",
          "queue"
        ],
        "additionalProperties": false
      }
    },
    "tasks": {
      "description": "Internal tasks of the agent, disclosed at its discretion.",
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "capability": {
            "$ref": "common.schema.json#/$defs/capabilityName"
          },
          "name": {
            "type": "string",
            "description": "Agent's internal reference name or ID."
          },
          "title": {
            "type": "string",
            "description": "Short description."
          },
          "status": {
            "enum": [
              "pending",
              "running",
              "waiting",
              "completed",
              "cancelled"
            ]
          },
          "progress": {
            "type": "number",
            "minimum": 0,
            "maximum": 100
          },
          "eta": {
            "type": "string",
            "pattern": "^[0-9]{2,}:[0-5][0-9]:[0-5][0-9]$",
            "description": "Estimated time to completion, hh:mm:ss."
          }
        },
        "required": [
          "capability",
          "status"
        ],
        "additionalProperties": false
      }
    },
    "contention": {
      "type": "boolean",
      "description": "Signals consumers to wait before submitting requests."
    }
  },
  "required": [
    "id",
    "status",
    "datetime",
    "capability_status"
  ],
  "additionalProperties": false
};

export const schemas: { [id: string]: GeneratedSchema } = {
  "actio.schema.json": actioMetaSchema,
  "announce.schema.json": announceSchema,
  "cancellation.schema.json": cancellationFileSchema,
  "common.schema.json": commonSchema,
  "confirmation.schema.json": confirmationFileSchema,
  "diarium.schema.json": diariumLineSchema,
  "error.schema.json": errorResultSchema,
  "request.schema.json": requestFileSchema,
  "response.schema.json": responseFileSchema,
  "spec.schema.json": specSchema,
  "status.schema.json": statusSchema,
};
