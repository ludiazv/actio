// Package actio implements the Actio protocol v0.1, core conformance profile: agents exchange
// requests through files in one shared folder, and every file they read or write is checked against
// the protocol's JSON schemas.
//
// An agent has an id, publishes what it can do (spec.json) and how it is doing (status.json),
// receives requests in its inbox and appends what happened to its own log (diarium.jsonl). Nothing
// runs on a port and no process is a server: the shared folder is the transport, and a peer that
// cannot be reached is a folder that has not been updated recently.
//
// The shape of the API follows the reference implementation (actio-ts) in behaviour and in names,
// with the differences Go expects: errors are values, context bounds the waits, and Done lets a
// program select on the agent's life. The README lists the differences.
//
// Two agents, one folder, one exchange:
//
//	cfg := actio.Config{Root: "/shared/actio"}
//	summarizer, err := actio.New(actio.Options{
//		ID: "summarizer", Description: "summarizes text",
//		AcceptsCommandsFrom: []string{"ops"}, Config: cfg,
//	})
//	if err != nil {
//		return err
//	}
//	err = summarizer.Capability(actio.Capability{
//		Name:        "summarize",
//		Description: "summarize text",
//		Parameters:  json.RawMessage(`{"type":"object"}`),
//		Actio:       actio.CapabilityActio{Result: json.RawMessage(`{"type":"string"}`)},
//	}, func(ctx context.Context, params json.RawMessage, call *actio.CallContext) (any, error) {
//		var in struct {
//			Text string `json:"text"`
//		}
//		if err := json.Unmarshal(params, &in); err != nil {
//			return nil, err
//		}
//		return in.Text[:40], nil
//	})
//	if err != nil {
//		return err
//	}
//	if err := summarizer.Start(ctx); err != nil {
//		return err
//	}
//	defer summarizer.Stop(context.Background(), actio.StopOptions{})
//
// The reader is a second agent in the same root, with its own id:
//
//	outcome, err := reader.Request(ctx, "summarizer", "summarize",
//		map[string]any{"text": text}, actio.RequestOptions{Timeout: 30 * time.Second})
//
// outcome.FinalStatus says how the exchange ended; outcome.Response carries the answer, and
// Response.Error says whether the answer was an error. A peer's failure is not a Go error.
//
// The library installs no signal handlers: a program that wants SIGINT decides what stopping means.
package actio
