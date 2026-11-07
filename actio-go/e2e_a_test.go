// The end-to-end suite, part 1: scenarios E1–E13 (requests and commands) through the public API,
// on t.TempDir() roots, with the protocol's timers shrunk. The robustness scenarios E14–E21 are in
// e2e_b_test.go. The interop half (Go against the TypeScript reference) is in interop_test.go,
// behind the interop build tag.
//
// Every scenario asserts the observable protocol: the files in the inboxes, the diarium's final
// statuses, the stream events, and the answer codes. No test depends on the scheduler: a bound is a
// named constant, not an unbounded wait or a bare sleep.
package actio

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// e2eFast is the configuration every e2e agent runs with: the protocol's timers shrunk to
// milliseconds, so a scenario settles in well under a second.
var e2eFast = Config{
	HeartbeatInterval: 40 * time.Millisecond,
	AnnounceDelay:     2 * time.Millisecond,
	NuntiumDebounce:   5 * time.Millisecond,
	StopGrace:         1500 * time.Millisecond,
	CancelWaitTimeout: 1500 * time.Millisecond,
	ConfirmTimeout:    1500 * time.Millisecond,
}

// e2eBound is the named bound every e2e wait uses: long enough for a scenario to settle on a loaded
// machine, short enough that a real failure is a fast, named failure, not a hang.
const e2eBound = 5 * time.Second

// newE2EAgent creates one agent under root with the fast e2e configuration and stops it (forced)
// when the test ends.
func newE2EAgent(t *testing.T, root, id string, mutate func(*Options)) *Agent {
	t.Helper()
	cfg := e2eFast
	cfg.Root = root
	opts := Options{ID: AgentID(id), AcceptsCommandsFrom: []string{}, Config: cfg}
	if mutate != nil {
		mutate(&opts)
	}
	a, err := New(opts)
	if err != nil {
		t.Fatalf("New(%s): %v", id, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := a.Stop(ctx, StopOptions{Force: true}); err != nil {
			t.Errorf("cleanup Stop(%s): %v", id, err)
		}
	})
	return a
}

// e2eEchoCap is the capability the e2e agents register: it echoes its parameters.
func e2eEchoCap() Capability {
	return Capability{
		Name:        "echo",
		Description: "echoes its parameters",
		Parameters:  json.RawMessage(`{"type":"object"}`),
		Actio:       CapabilityActio{Result: json.RawMessage(`{"type":"object"}`)},
	}
}

// e2eEchoHandler answers an echo request with a marker.
func e2eEchoHandler(ctx context.Context, params json.RawMessage, call *CallContext) (any, error) {
	return map[string]any{"echoed": true}, nil
}

// e2eSee waits, with the named bound, for one agent to discover another.
func e2eSee(t *testing.T, a *Agent, id, other string) {
	t.Helper()
	waitFor(t, e2eBound, id+" to discover "+other, func() bool {
		_, _ = a.Refresh(startCtx(t))
		_, ok := a.Agmen()[AgentID(other)]
		return ok
	})
}

// e2ePair starts alpha (with an echo capability) and beta in a fresh root and waits for each to
// discover the other. It returns the agents and the root.
func e2ePair(t *testing.T) (alpha, beta *Agent, root string) {
	t.Helper()
	root = t.TempDir()
	alpha = newE2EAgent(t, root, "alpha", nil)
	if err := alpha.Capability(e2eEchoCap(), e2eEchoHandler); err != nil {
		t.Fatalf("Capability(echo): %v", err)
	}
	beta = newE2EAgent(t, root, "beta", nil)
	if err := beta.Capability(e2eEchoCap(), e2eEchoHandler); err != nil {
		t.Fatalf("Capability(echo): %v", err)
	}
	if err := alpha.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(alpha): %v", err)
	}
	if err := beta.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(beta): %v", err)
	}
	e2eSee(t, alpha, "alpha", "beta")
	e2eSee(t, beta, "beta", "alpha")
	return
}

// e2eInboxNames lists the files of one inbox.
func e2eInboxNames(t *testing.T, root, id string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(root, id, "inbox"))
	if err != nil {
		return nil
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() {
			names = append(names, e.Name())
		}
	}
	return names
}

// e2eWaitInboxDrains waits, with the named bound, for one inbox to become empty (the archive).
func e2eWaitInboxDrains(t *testing.T, root, id string) {
	t.Helper()
	waitFor(t, e2eBound, id+"'s inbox to drain after the archive", func() bool {
		return len(e2eInboxNames(t, root, id)) == 0
	})
}

// e2eDiariumFinals returns, for one diarium, the final statuses of its inreq and outreq events, in
// file order.
func e2eDiariumFinals(t *testing.T, root, id string) (inreq, outreq []string) {
	t.Helper()
	read, err := ReadDiarium(filepath.Join(root, id))
	if err != nil {
		t.Fatalf("diarium of %s: %v", id, err)
	}
	for _, line := range read.Lines {
		if line.Event != "inreq" && line.Event != "outreq" {
			continue
		}
		var env ExchangeEnvelopes
		if err := json.Unmarshal(line.EventData, &env); err != nil {
			t.Fatalf("%s %s event data: %v", id, line.Event, err)
		}
		if line.Event == "inreq" {
			inreq = append(inreq, env.FinalStatus)
		} else {
			outreq = append(outreq, env.FinalStatus)
		}
	}
	return
}

// e2eDiariumClean fails the test if one diarium has any line that does not validate.
func e2eDiariumClean(t *testing.T, root, id string) {
	t.Helper()
	read, err := ReadDiarium(filepath.Join(root, id))
	if err != nil {
		t.Fatalf("diarium of %s: %v", id, err)
	}
	if len(read.Ignored) != 0 {
		t.Errorf("%s's diarium has %d ignored line(s): %+v", id, len(read.Ignored), read.Ignored)
	}
}

// e2eVehicleCfg is the fast configuration for the actio.Call vehicle, rooted at root.
func e2eVehicleCfg(root string) Config {
	cfg := e2eFast
	cfg.Root = root
	return cfg
}

// e2eSpecDisabled reads the disabled flag of one spec.json.
func e2eSpecDisabled(t *testing.T, root, id string) bool {
	t.Helper()
	var s Spec
	readJSON(t, filepath.Join(root, id, "spec.json"), &s)
	return isTrue(s.Disabled)
}

// TestE1NormalRequest: a normal request ends confirmed on both sides; the destination's inbox is
// empty after the archive, and every diarium line of both agents validates.
func TestE1NormalRequest(t *testing.T) {
	_, beta, root := e2ePair(t)
	out, err := beta.Request(startCtx(t), "alpha", "echo", map[string]any{"text": "hi"}, RequestOptions{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	if out.FinalStatus != "confirmed" || out.Response == nil {
		t.Fatalf("outcome: %+v", out)
	}
	e2eWaitInboxDrains(t, root, "alpha")
	if names := e2eInboxNames(t, root, "beta"); len(names) != 0 {
		t.Errorf("beta's inbox is not empty: %v", names)
	}
	inA, _ := e2eDiariumFinals(t, root, "alpha")
	_, outB := e2eDiariumFinals(t, root, "beta")
	if len(inA) != 1 || inA[0] != "confirmed" {
		t.Errorf("alpha inreq finals: %v (want [confirmed])", inA)
	}
	if len(outB) != 1 || outB[0] != "confirmed" {
		t.Errorf("beta outreq finals: %v (want [confirmed])", outB)
	}
	e2eDiariumClean(t, root, "alpha")
	e2eDiariumClean(t, root, "beta")
}

// TestE2UnknownCapability: an unknown capability is answered with code 101 and the exchange is
// confirmed on both sides. This goes through the actio.Call vehicle, not Agent.Request: the local
// gate refuses an unusable capability before writing anything, while the reference sends the request
// and lets the peer answer 101. The protocol behaviour (the 101) is what is under test here.
func TestE2UnknownCapability(t *testing.T) {
	_, _, root := e2ePair(t)
	out, err := Call(startCtx(t), e2eVehicleCfg(root), "beta", "alpha", "nosuch", map[string]any{}, RequestOptions{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if out.FinalStatus != "confirmed" {
		t.Fatalf("final status: %q (want confirmed)", out.FinalStatus)
	}
	if out.Response == nil || !out.Response.Error {
		t.Fatalf("the response: %+v (want an error answer)", out.Response)
	}
	var er ErrorResult
	if err := json.Unmarshal(out.Response.Result, &er); err != nil {
		t.Fatalf("the error result: %v", err)
	}
	if er.Code == nil || *er.Code != 101 {
		t.Errorf("the code: %v (want 101), msg %q", *er.Code, er.Msg)
	}
}

// TestE3AcceptanceRejections: the seven A0 rejections, each with its code. They go through the
// actio.Call vehicle, because the local gate (Agent.Request) re-scans the root on every request and
// would refuse several of these before writing, while the reference sends the request and lets the
// peer answer with its code. The 108 row writes an expired request file directly, because the
// vehicle stamps a fresh ts and cannot backdate one.
func TestE3AcceptanceRejections(t *testing.T) {
	t.Run("100_agent_disabled", func(t *testing.T) {
		root := t.TempDir()
		alpha := newE2EAgent(t, root, "alpha", func(o *Options) { o.AcceptsCommandsFrom = []string{"beta"} })
		if err := alpha.Capability(e2eEchoCap(), e2eEchoHandler); err != nil {
			t.Fatalf("Capability: %v", err)
		}
		beta := newE2EAgent(t, root, "beta", nil)
		if err := beta.Capability(e2eEchoCap(), e2eEchoHandler); err != nil {
			t.Fatalf("Capability: %v", err)
		}
		mustStart(t, alpha)
		mustStart(t, beta)
		if _, err := beta.Command(startCtx(t), "alpha", "enable", map[string]any{"enabled": false}); err != nil {
			t.Fatalf("Command(enable false): %v", err)
		}
		waitFor(t, e2eBound, "alpha's spec to report disabled", func() bool { return e2eSpecDisabled(t, root, "alpha") })
		e2eExpectCode(t, root, "beta", "alpha", "echo", map[string]any{}, 100)
	})
	t.Run("102_capability_disabled", func(t *testing.T) {
		root := t.TempDir()
		disabled := true
		alpha := newE2EAgent(t, root, "alpha", nil)
		if err := alpha.Capability(e2eEchoCap(), e2eEchoHandler); err != nil {
			t.Fatalf("Capability(echo): %v", err)
		}
		off := e2eEchoCap()
		off.Name = "off"
		off.Actio.Disabled = &disabled
		if err := alpha.Capability(off, e2eEchoHandler); err != nil {
			t.Fatalf("Capability(off): %v", err)
		}
		beta := newE2EAgent(t, root, "beta", nil)
		if err := beta.Capability(e2eEchoCap(), e2eEchoHandler); err != nil {
			t.Fatalf("Capability: %v", err)
		}
		mustStart(t, alpha)
		mustStart(t, beta)
		e2eSee(t, beta, "beta", "alpha")
		e2eExpectCode(t, root, "beta", "alpha", "off", map[string]any{}, 102)
	})
	t.Run("103_requestor_not_allowed", func(t *testing.T) {
		root := t.TempDir()
		alpha := newE2EAgent(t, root, "alpha", nil)
		cap := e2eEchoCap()
		cap.Actio.AcceptsFrom = []string{"nobody"}
		if err := alpha.Capability(cap, e2eEchoHandler); err != nil {
			t.Fatalf("Capability: %v", err)
		}
		beta := newE2EAgent(t, root, "beta", nil)
		mustStart(t, alpha)
		mustStart(t, beta)
		e2eExpectCode(t, root, "beta", "alpha", "echo", map[string]any{}, 103)
	})
	t.Run("105_invalid_parameters", func(t *testing.T) {
		root := t.TempDir()
		alpha := newE2EAgent(t, root, "alpha", nil)
		cap := e2eEchoCap()
		cap.Parameters = json.RawMessage(`{"type":"object","required":["text"]}`)
		if err := alpha.Capability(cap, e2eEchoHandler); err != nil {
			t.Fatalf("Capability: %v", err)
		}
		beta := newE2EAgent(t, root, "beta", nil)
		mustStart(t, alpha)
		mustStart(t, beta)
		e2eExpectCode(t, root, "beta", "alpha", "echo", map[string]any{}, 105) // no "text"
	})
	t.Run("106_namespace_mismatch", func(t *testing.T) {
		root := t.TempDir()
		aNS := "a"
		bNS := "b"
		alpha := newE2EAgent(t, root, "alpha", func(o *Options) { o.Namespaces = []*string{&aNS} })
		if err := alpha.Capability(e2eEchoCap(), e2eEchoHandler); err != nil {
			t.Fatalf("Capability: %v", err)
		}
		beta := newE2EAgent(t, root, "beta", func(o *Options) { o.Namespaces = []*string{&bNS} })
		mustStart(t, alpha)
		mustStart(t, beta)
		// The 106 check reads the requestor's spec as DISCOVERED (unfiltered), so alpha must have
		// scanned and found beta's folder first, even though the two share no namespace.
		_, _ = alpha.Refresh(startCtx(t))
		e2eExpectCode(t, root, "beta", "alpha", "echo", map[string]any{}, 106)
	})
	t.Run("107_capacity_exceeded", func(t *testing.T) {
		root := t.TempDir()
		alpha := newE2EAgent(t, root, "alpha", nil)
		started := make(chan struct{})
		cap := e2eEchoCap()
		cap.Name = "slow"
		cap.Actio.MaxParallel = 1
		if err := alpha.Capability(cap, func(ctx context.Context, p json.RawMessage, c *CallContext) (any, error) {
			close(started) // the handler is running: it now holds the one slot
			<-ctx.Done()
			return nil, ctx.Err()
		}); err != nil {
			t.Fatalf("Capability(slow): %v", err)
		}
		beta := newE2EAgent(t, root, "beta", nil)
		mustStart(t, alpha)
		mustStart(t, beta)
		e2eSee(t, beta, "beta", "alpha")
		// Hold the one slot with a slow request, then a second one is refused with 107.
		go func() {
			_, _ = beta.Request(startCtx(t), "alpha", "slow", map[string]any{}, RequestOptions{Timeout: 30 * time.Second})
		}()
		waitFor(t, e2eBound, "the slow handler to start and hold the slot", func() bool {
			select {
			case <-started:
				return true
			default:
				return false
			}
		})
		e2eExpectCode(t, root, "beta", "alpha", "slow", map[string]any{}, 107)
	})
	t.Run("108_request_expired", func(t *testing.T) {
		_, _, root := e2ePair(t)
		// Write an expired request straight into alpha's inbox: ts + timeout is in the past, so the
		// receiver answers 108 without running the handler.
		writeRequestFile(t, root, "alpha", "beta", "echo", 1000*time.Second, 1)
		e2eExpectInreqCode(t, root, "alpha", 108)
	})
}

// mustStart starts one agent and fails the test on an error.
func mustStart(t *testing.T, a *Agent) {
	t.Helper()
	if err := a.Start(startCtx(t)); err != nil {
		t.Fatalf("Start: %v", err)
	}
}

// e2eExpectCode drives one request through the vehicle and asserts the answer is an error with the
// given code.
func e2eExpectCode(t *testing.T, root, src, dst, name string, params any, want int) {
	t.Helper()
	out, err := Call(startCtx(t), e2eVehicleCfg(root), AgentID(src), AgentID(dst), name, params, RequestOptions{Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if out.FinalStatus != "confirmed" {
		t.Fatalf("final status: %q (want confirmed)", out.FinalStatus)
	}
	if out.Response == nil || !out.Response.Error {
		t.Fatalf("the response: %+v (want an error answer)", out.Response)
	}
	var er ErrorResult
	if err := json.Unmarshal(out.Response.Result, &er); err != nil {
		t.Fatalf("the error result: %v", err)
	}
	if er.Code == nil || *er.Code != want {
		t.Errorf("the code: %v (want %d), msg %q", *er.Code, want, er.Msg)
	}
}

// e2eExpectInreqCode waits for the receiver to archive a directly-written request and asserts the
// answer's error code, read from the response envelope the receiver stored in the inreq event.
func e2eExpectInreqCode(t *testing.T, root, id string, wantCode int) {
	t.Helper()
	waitFor(t, e2eBound, id+"'s inreq to be archived", func() bool {
		in, _ := e2eDiariumFinals(t, root, id)
		return len(in) >= 1
	})
	read, err := ReadDiarium(filepath.Join(root, id))
	if err != nil {
		t.Fatalf("diarium of %s: %v", id, err)
	}
	for _, line := range read.Lines {
		if line.Event != "inreq" {
			continue
		}
		var env ExchangeEnvelopes
		if err := json.Unmarshal(line.EventData, &env); err != nil || string(env.Res) == "null" {
			continue
		}
		var resFile ResponseFile
		if err := json.Unmarshal(env.Res, &resFile); err != nil {
			t.Fatalf("the inreq response envelope: %v", err)
		}
		var body ResponseBody
		if err := json.Unmarshal(resFile.Response, &body); err != nil {
			t.Fatalf("the response body: %v", err)
		}
		var er ErrorResult
		if err := json.Unmarshal(body.Result, &er); err != nil {
			t.Fatalf("the error result: %v", err)
		}
		if er.Code == nil || *er.Code != wantCode {
			t.Errorf("the code: %v (want %d), msg %q", *er.Code, wantCode, er.Msg)
		}
		return
	}
	t.Errorf("%s's diarium has no inreq with a response envelope", id)
}

// writeRequestFile writes a schema-valid request file into dst's inbox and returns its rid. tsAge is
// how far in the past the body's ts is (0 = now, so a large tsAge with a small timeout makes an
// expired request); timeoutSec is the body's timeout in seconds. Tests are exempt from the
// os-file-function rule, so this writes directly.
func writeRequestFile(t *testing.T, root, dst, src, name string, tsAge time.Duration, timeoutSec float64) RID {
	t.Helper()
	now := time.Now()
	body := RequestBody{
		TS:          now.Add(-tsAge).Unix(),
		Nonce:       newNonce(),
		Name:        name,
		Parameters:  json.RawMessage(`{}`),
		Timeout:     timeoutSec,
		Attachments: []string{},
	}
	rawBody, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal the request body: %v", err)
	}
	file := RequestFile{
		Header: Header{
			RID:          newULID(now),
			Src:          AgentID(src),
			Dst:          AgentID(dst),
			Datetime:     formatISO(now),
			Type:         "req",
			ActioVersion: ProtocolVersion,
		},
		Request: rawBody,
	}
	raw, err := json.Marshal(file)
	if err != nil {
		t.Fatalf("marshal the request file: %v", err)
	}
	path := filepath.Join(root, dst, "inbox", string(file.RID)+".json")
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("write the request: %v", err)
	}
	return file.RID
}

// writeCancellationFile writes a schema-valid cancellation file for one request into dst's inbox.
func writeCancellationFile(t *testing.T, root, dst, src, name string, rid RID) {
	t.Helper()
	now := time.Now()
	body := CancellationBody{
		TS:    now.Unix(),
		Nonce: newNonce(),
		Name:  name,
		Force: false,
	}
	rawBody, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal the cancellation body: %v", err)
	}
	file := CancellationFile{
		Header: Header{
			RID:          rid,
			Src:          AgentID(src),
			Dst:          AgentID(dst),
			Datetime:     formatISO(now),
			Type:         "can",
			ActioVersion: ProtocolVersion,
		},
		Cancellation: rawBody,
	}
	raw, err := json.Marshal(file)
	if err != nil {
		t.Fatalf("marshal the cancellation file: %v", err)
	}
	path := filepath.Join(root, dst, "inbox", string(rid)+".can.json")
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("write the cancellation: %v", err)
	}
}

// TestE4ConfirmationTimeout: the requestor never confirms, so the receiver's confirmation timeout
// fires and it archives the exchange as completed. The request is written directly so no requestor
// (and no cancellation) is involved: only the receiver's timeout is under test.
func TestE4ConfirmationTimeout(t *testing.T) {
	root := t.TempDir()
	alpha := newE2EAgent(t, root, "alpha", func(o *Options) { o.Config.ConfirmTimeout = 300 * time.Millisecond })
	if err := alpha.Capability(e2eEchoCap(), e2eEchoHandler); err != nil {
		t.Fatalf("Capability: %v", err)
	}
	mustStart(t, alpha)
	writeRequestFile(t, root, "alpha", "beta", "echo", 0, 60)
	// No confirmation is written: the receiver's confirmation timeout archives it as completed.
	waitFor(t, e2eBound, "alpha to archive as completed on the confirmation timeout", func() bool {
		in, _ := e2eDiariumFinals(t, root, "alpha")
		return len(in) >= 1 && in[len(in)-1] == "completed"
	})
}

// TestE5CancelledBeforeAcceptance: the receiver is stopped before it accepts; the requestor's
// timeout writes a cancellation; the receiver restarts and the D-4 initial scan processes the
// pending can+req, answering a cancellation and archiving as cancelled on both sides.
func TestE5CancelledBeforeAcceptance(t *testing.T) {
	root := t.TempDir()
	alpha := newE2EAgent(t, root, "alpha", nil)
	if err := alpha.Capability(e2eEchoCap(), e2eEchoHandler); err != nil {
		t.Fatalf("Capability: %v", err)
	}
	beta := newE2EAgent(t, root, "beta", nil)
	if err := beta.Capability(e2eEchoCap(), e2eEchoHandler); err != nil {
		t.Fatalf("Capability: %v", err)
	}
	mustStart(t, alpha)
	mustStart(t, beta)
	// Stop the receiver before it accepts anything.
	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := beta.Stop(stopCtx, StopOptions{}); err != nil {
		t.Fatalf("Stop(beta): %v", err)
	}
	// alpha's request times out (the receiver is down) and writes a cancellation. The local gate
	// refuses an inactive peer, so this goes through the vehicle, which skips the gate.
	out, err := Call(startCtx(t), e2eVehicleCfg(root), "alpha", "beta", "echo", map[string]any{}, RequestOptions{Timeout: 500 * time.Millisecond})
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if out.FinalStatus != "cancelled" && out.FinalStatus != "abandoned" {
		t.Fatalf("the requestor's final status: %q (want cancelled or abandoned)", out.FinalStatus)
	}
	// The cancellation is in beta's inbox; restart beta and the D-4 scan processes can+req.
	waitFor(t, e2eBound, "the cancellation to land in beta's inbox", func() bool {
		return len(e2eInboxNames(t, root, "beta")) >= 1
	})
	if err := beta.Start(startCtx(t)); err != nil {
		t.Fatalf("Start(beta) again: %v", err)
	}
	waitFor(t, e2eBound, "beta to archive the pending exchange as cancelled", func() bool {
		in, _ := e2eDiariumFinals(t, root, "beta")
		return len(in) >= 1 && in[len(in)-1] == "cancelled"
	})
}

// TestE6CancelledDuringWork: the sleep handler is running when the cancellation arrives; it observes
// the abort through its context and the exchange ends cancelled.
func TestE6CancelledDuringWork(t *testing.T) {
	root := t.TempDir()
	alpha := newE2EAgent(t, root, "alpha", nil)
	if err := alpha.Capability(e2eEchoCap(), e2eEchoHandler); err != nil {
		t.Fatalf("Capability: %v", err)
	}
	aborted := make(chan struct{})
	slow := e2eEchoCap()
	slow.Name = "sleep"
	beta := newE2EAgent(t, root, "beta", nil)
	if err := beta.Capability(slow, func(ctx context.Context, p json.RawMessage, c *CallContext) (any, error) {
		<-ctx.Done()
		close(aborted)
		return nil, ctx.Err()
	}); err != nil {
		t.Fatalf("Capability(sleep): %v", err)
	}
	mustStart(t, alpha)
	mustStart(t, beta)
	e2eSee(t, alpha, "alpha", "beta")
	out, err := alpha.Request(startCtx(t), "beta", "sleep", map[string]any{}, RequestOptions{Timeout: 500 * time.Millisecond})
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	if out.FinalStatus != "cancelled" {
		t.Fatalf("final status: %q (want cancelled)", out.FinalStatus)
	}
	select {
	case <-aborted:
	default:
		t.Error("the sleep handler did not observe the cancellation")
	}
}

// TestE7CancelledAfterResponse: the response is written, then a cancellation arrives; the receiver
// writes no second response and the exchange ends cancelled. The request and the cancellation are
// written directly so the receiver's "no second response" rule is what is under test.
func TestE7CancelledAfterResponse(t *testing.T) {
	root := t.TempDir()
	alpha := newE2EAgent(t, root, "alpha", func(o *Options) { o.Config.ConfirmTimeout = 300 * time.Millisecond })
	if err := alpha.Capability(e2eEchoCap(), e2eEchoHandler); err != nil {
		t.Fatalf("Capability: %v", err)
	}
	mustStart(t, alpha)
	rid := writeRequestFile(t, root, "alpha", "beta", "echo", 0, 60)
	// Wait for the response, then send the cancellation after it.
	waitFor(t, e2eBound, "alpha to write the response", func() bool {
		_, err := os.Stat(filepath.Join(root, "alpha", "inbox", string(rid)+".res.json"))
		return err == nil
	})
	writeCancellationFile(t, root, "alpha", "beta", "echo", rid)
	// No confirmation follows, so the confirmation timeout archives the exchange; the stored
	// response is the original (cancellation=false), proving no second response was written.
	waitFor(t, e2eBound, "alpha to archive the exchange", func() bool {
		in, _ := e2eDiariumFinals(t, root, "alpha")
		return len(in) >= 1
	})
	read, err := ReadDiarium(filepath.Join(root, "alpha"))
	if err != nil {
		t.Fatalf("diarium: %v", err)
	}
	var sawRes bool
	for _, line := range read.Lines {
		if line.Event != "inreq" {
			continue
		}
		var env ExchangeEnvelopes
		if err := json.Unmarshal(line.EventData, &env); err != nil || string(env.Res) == "null" {
			t.Fatalf("the inreq has no stored response: %+v", env)
		}
		var resFile ResponseFile
		if err := json.Unmarshal(env.Res, &resFile); err != nil {
			t.Fatalf("the response envelope: %v", err)
		}
		var body ResponseBody
		if err := json.Unmarshal(resFile.Response, &body); err != nil {
			t.Fatalf("the response body: %v", err)
		}
		if body.Cancellation {
			t.Errorf("the stored response is a cancellation response: a second response was written")
		}
		sawRes = true
	}
	if !sawRes {
		t.Error("the diarium has no inreq with a stored response")
	}
}

// TestE8RequestorTimeoutCancels: the requestor's own timeout fires, it writes a cancellation, the
// peer answers the cancellation, and the exchange ends cancelled.
func TestE8RequestorTimeoutCancels(t *testing.T) {
	root := t.TempDir()
	alpha := newE2EAgent(t, root, "alpha", nil)
	if err := alpha.Capability(e2eEchoCap(), e2eEchoHandler); err != nil {
		t.Fatalf("Capability: %v", err)
	}
	slow := e2eEchoCap()
	slow.Name = "sleep"
	beta := newE2EAgent(t, root, "beta", nil)
	if err := beta.Capability(slow, func(ctx context.Context, p json.RawMessage, c *CallContext) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}); err != nil {
		t.Fatalf("Capability(sleep): %v", err)
	}
	mustStart(t, alpha)
	mustStart(t, beta)
	e2eSee(t, alpha, "alpha", "beta")
	out, err := alpha.Request(startCtx(t), "beta", "sleep", map[string]any{}, RequestOptions{Timeout: 500 * time.Millisecond})
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	if out.FinalStatus != "cancelled" {
		t.Fatalf("final status: %q (want cancelled)", out.FinalStatus)
	}
	// beta answered the cancellation and archived as cancelled.
	waitFor(t, e2eBound, "beta to archive as cancelled", func() bool {
		in, _ := e2eDiariumFinals(t, root, "beta")
		return len(in) >= 1 && in[len(in)-1] == "cancelled"
	})
}

// TestE9ReceiverInactiveAbandons: the receiver is inactive, so the request is never answered and the
// requestor abandons; the request and its cancellation stay in the (inactive) receiver's inbox and
// the forum is left empty. This goes through the actio.Call vehicle, because the local gate refuses
// an inactive peer before writing, while the reference sends the request and abandons it.
func TestE9ReceiverInactiveAbandons(t *testing.T) {
	root := t.TempDir()
	alpha := newE2EAgent(t, root, "alpha", nil)
	if err := alpha.Capability(e2eEchoCap(), e2eEchoHandler); err != nil {
		t.Fatalf("Capability: %v", err)
	}
	beta := newE2EAgent(t, root, "beta", nil)
	if err := beta.Capability(e2eEchoCap(), e2eEchoHandler); err != nil {
		t.Fatalf("Capability: %v", err)
	}
	mustStart(t, alpha)
	mustStart(t, beta)
	e2eSee(t, alpha, "alpha", "beta")
	// Stop the receiver so it is inactive.
	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := beta.Stop(stopCtx, StopOptions{}); err != nil {
		t.Fatalf("Stop(beta): %v", err)
	}
	out, err := Call(startCtx(t), e2eVehicleCfg(root), "alpha", "beta", "echo", map[string]any{}, RequestOptions{Timeout: 500 * time.Millisecond})
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if out.FinalStatus != "abandoned" {
		t.Fatalf("final status: %q (want abandoned)", out.FinalStatus)
	}
	// The request and its cancellation stay in beta's inbox; the forum is empty.
	waitFor(t, e2eBound, "beta's inbox to hold the request and its cancellation", func() bool {
		names := e2eInboxNames(t, root, "beta")
		return len(names) >= 2
	})
	entries, err := os.ReadDir(filepath.Join(root, "forum"))
	if err != nil {
		t.Fatalf("list the forum: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("the forum is not empty after the abandon: %d entries", len(entries))
	}
}

// writeCommandFile writes a schema-valid command envelope (type "cmd") into dst's inbox and returns
// its rid. Used for the command rows where the sender is not a started agent (the dropped-command
// rows, where the source is unlisted).
func writeCommandFile(t *testing.T, root, dst, src, name string, params json.RawMessage) RID {
	t.Helper()
	if params == nil {
		params = json.RawMessage(`null`)
	}
	now := time.Now()
	body := RequestBody{
		TS:          now.Unix(),
		Nonce:       newNonce(),
		Name:        name,
		Parameters:  params,
		Timeout:     60,
		Attachments: []string{},
	}
	rawBody, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal the command body: %v", err)
	}
	file := RequestFile{
		Header: Header{
			RID:          newULID(now),
			Src:          AgentID(src),
			Dst:          AgentID(dst),
			Datetime:     formatISO(now),
			Type:         "cmd",
			ActioVersion: ProtocolVersion,
		},
		Request: rawBody,
	}
	raw, err := json.Marshal(file)
	if err != nil {
		t.Fatalf("marshal the command file: %v", err)
	}
	path := filepath.Join(root, dst, "inbox", string(file.RID)+".json")
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("write the command file: %v", err)
	}
	return file.RID
}

// e2eInreqCompleted returns true once one diarium has an inreq event with the given final status.
func e2eInreqFinal(t *testing.T, root, id, want string) bool {
	in, _ := e2eDiariumFinals(t, root, id)
	return len(in) >= 1 && in[len(in)-1] == want
}

// TestE10CommandsAccepted: ping, enable false/true and a custom noop are all accepted; each is
// archived as completed, the ping and the enable re-announce, and the enable toggles the spec's
// disabled flag.
func TestE10CommandsAccepted(t *testing.T) {
	root := t.TempDir()
	noopRan := make(chan struct{}, 1)
	alpha := newE2EAgent(t, root, "alpha", func(o *Options) { o.AcceptsCommandsFrom = []string{"beta"} })
	if err := alpha.Capability(e2eEchoCap(), e2eEchoHandler); err != nil {
		t.Fatalf("Capability: %v", err)
	}
	if err := alpha.OnCommand("noop", func(ctx context.Context, p json.RawMessage, c *CommandContext) error {
		noopRan <- struct{}{}
		return nil
	}); err != nil {
		t.Fatalf("OnCommand(noop): %v", err)
	}
	beta := newE2EAgent(t, root, "beta", nil)
	if err := beta.Capability(e2eEchoCap(), e2eEchoHandler); err != nil {
		t.Fatalf("Capability: %v", err)
	}
	mustStart(t, alpha)
	mustStart(t, beta)
	e2eSee(t, beta, "beta", "alpha")

	// ping: accepted, re-announces, archived completed.
	if _, err := beta.Command(startCtx(t), "alpha", "ping", nil); err != nil {
		t.Fatalf("Command(ping): %v", err)
	}
	waitFor(t, e2eBound, "alpha to archive the ping as completed", func() bool { return e2eInreqFinal(t, root, "alpha", "completed") })

	// enable false: the spec's disabled flag is set and a re-announce is written.
	if _, err := beta.Command(startCtx(t), "alpha", "enable", map[string]any{"enabled": false}); err != nil {
		t.Fatalf("Command(enable false): %v", err)
	}
	waitFor(t, e2eBound, "alpha's spec to report disabled", func() bool { return e2eSpecDisabled(t, root, "alpha") })
	// enable true: the flag is cleared again.
	if _, err := beta.Command(startCtx(t), "alpha", "enable", map[string]any{"enabled": true}); err != nil {
		t.Fatalf("Command(enable true): %v", err)
	}
	waitFor(t, e2eBound, "alpha's spec to report enabled", func() bool { return !e2eSpecDisabled(t, root, "alpha") })

	// noop: the custom command runs in the peer.
	if _, err := beta.Command(startCtx(t), "alpha", "noop", nil); err != nil {
		t.Fatalf("Command(noop): %v", err)
	}
	select {
	case <-noopRan:
	case <-time.After(e2eBound):
		t.Error("the noop command did not run")
	}
}

// TestE11CommandsDropped: a command from an unlisted source and an unknown command are both dropped
// — the file is deleted and status.json, spec.json and the announce file are byte-identical.
func TestE11CommandsDropped(t *testing.T) {
	root := t.TempDir()
	alpha := newE2EAgent(t, root, "alpha", func(o *Options) { o.AcceptsCommandsFrom = []string{"beta"} })
	if err := alpha.Capability(e2eEchoCap(), e2eEchoHandler); err != nil {
		t.Fatalf("Capability: %v", err)
	}
	beta := newE2EAgent(t, root, "beta", nil)
	if err := beta.Capability(e2eEchoCap(), e2eEchoHandler); err != nil {
		t.Fatalf("Capability: %v", err)
	}
	mustStart(t, alpha)
	mustStart(t, beta)
	e2eSee(t, beta, "beta", "alpha")
	// Let the publications settle, then capture the bytes a dropped command must not change.
	time.Sleep(50 * time.Millisecond) // bounded: the start publications are already written
	statusBefore := mustRead(t, filepath.Join(root, "alpha", "status.json"))
	specBefore := mustRead(t, filepath.Join(root, "alpha", "spec.json"))
	announceBefore := mustList(t, filepath.Join(root, "nuntium"), ".n.json")

	// A command from an unlisted source (gamma) and an unknown command (from the listed beta).
	writeCommandFile(t, root, "alpha", "gamma", "ping", nil)
	writeCommandFile(t, root, "alpha", "beta", "nosuchcmd", nil)
	waitFor(t, e2eBound, "the dropped command files to be deleted", func() bool {
		return len(e2eInboxNames(t, root, "alpha")) == 0
	})
	if got := mustRead(t, filepath.Join(root, "alpha", "status.json")); !bytesEqual(got, statusBefore) {
		t.Error("a dropped command changed status.json")
	}
	if got := mustRead(t, filepath.Join(root, "alpha", "spec.json")); !bytesEqual(got, specBefore) {
		t.Error("a dropped command changed spec.json")
	}
	if got := mustList(t, filepath.Join(root, "nuntium"), ".n.json"); !sameNames(got, announceBefore) {
		t.Errorf("a dropped command changed the announce files: %v -> %v", announceBefore, got)
	}
}

// TestE12StopWithRequestInFlight: a stop command with a request in flight answers the work with
// code 300, archives it before the status turns inactive, and ends inactive.
func TestE12StopWithRequestInFlight(t *testing.T) {
	root := t.TempDir()
	alpha := newE2EAgent(t, root, "alpha", func(o *Options) { o.AcceptsCommandsFrom = []string{"beta"} })
	slow := e2eEchoCap()
	slow.Name = "sleep"
	if err := alpha.Capability(slow, func(ctx context.Context, p json.RawMessage, c *CallContext) (any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}); err != nil {
		t.Fatalf("Capability(sleep): %v", err)
	}
	beta := newE2EAgent(t, root, "beta", nil)
	if err := beta.Capability(e2eEchoCap(), e2eEchoHandler); err != nil {
		t.Fatalf("Capability: %v", err)
	}
	mustStart(t, alpha)
	mustStart(t, beta)
	e2eSee(t, beta, "beta", "alpha")
	// Hold a request in flight, then stop alpha from beta.
	inflight := make(chan struct{})
	go func() {
		_, _ = beta.Request(startCtx(t), "alpha", "sleep", map[string]any{}, RequestOptions{Timeout: 30 * time.Second})
		close(inflight)
	}()
	waitFor(t, e2eBound, "the in-flight request to start", func() bool {
		return statusState(t, root, "alpha") == "busy"
	})
	if _, err := beta.Command(startCtx(t), "alpha", "stop", map[string]any{}); err != nil {
		t.Fatalf("Command(stop): %v", err)
	}
	waitFor(t, e2eBound, "alpha to turn inactive", func() bool { return statusState(t, root, "alpha") == "inactive" })
	// The in-flight work was answered with 300 and archived.
	waitFor(t, e2eBound, "the in-flight request to be archived", func() bool {
		in, _ := e2eDiariumFinals(t, root, "alpha")
		return len(in) >= 1
	})
	e2eExpectInreqCode(t, root, "alpha", 300)
}

// TestE13RestartWithDelay: a restart command archives, turns the agent inactive, and brings it back
// active no earlier than the delay.
func TestE13RestartWithDelay(t *testing.T) {
	root := t.TempDir()
	alpha := newE2EAgent(t, root, "alpha", func(o *Options) { o.AcceptsCommandsFrom = []string{"beta"} })
	if err := alpha.Capability(e2eEchoCap(), e2eEchoHandler); err != nil {
		t.Fatalf("Capability: %v", err)
	}
	beta := newE2EAgent(t, root, "beta", nil)
	if err := beta.Capability(e2eEchoCap(), e2eEchoHandler); err != nil {
		t.Fatalf("Capability: %v", err)
	}
	mustStart(t, alpha)
	mustStart(t, beta)
	e2eSee(t, beta, "beta", "alpha")
	if _, err := beta.Command(startCtx(t), "alpha", "restart", map[string]any{"delay": 1}); err != nil {
		t.Fatalf("Command(restart): %v", err)
	}
	// The restart first turns the agent inactive (the stop phase); the delay starts there.
	waitFor(t, e2eBound, "alpha to go inactive for the restart", func() bool {
		return statusState(t, root, "alpha") == "inactive"
	})
	inactiveAt := time.Now()
	// Then it comes back active, no earlier than the delay after going inactive.
	waitFor(t, 10*time.Second, "alpha to come back after the restart delay", func() bool {
		s := statusState(t, root, "alpha")
		return s == "idle" || s == "busy"
	})
	if elapsed := time.Since(inactiveAt); elapsed < 950*time.Millisecond {
		t.Errorf("the restart came back %s after going inactive: the 1s delay was not honoured", elapsed)
	}
}

// bytesEqual reports whether two byte slices are equal.
func bytesEqual(a, b []byte) bool {
	return string(a) == string(b)
}

// sameNames reports whether two name lists are equal in content and order.
func sameNames(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
