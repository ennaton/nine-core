package event

import (
	_ "embed"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

//go:embed schema/agent_run.v1.json
var contractJSON []byte

type contract struct {
	Properties map[string]struct {
		Enum []string `json:"enum"`
	} `json:"properties"`
}

func load(t *testing.T) contract {
	t.Helper()
	var c contract
	if err := json.Unmarshal(contractJSON, &c); err != nil {
		t.Fatalf("embedded contract is not valid JSON: %v", err)
	}
	return c
}

// The wire struct is the fast path, the contract is the truth, and this is
// what makes trusting the struct honest: a field added to one and not the
// other is a red build, not a silently dropped column.
func TestWireMatchesContractFields(t *testing.T) {
	c := load(t)
	inStruct := map[string]bool{}
	rt := reflect.TypeOf(wire{})
	for i := 0; i < rt.NumField(); i++ {
		inStruct[strings.Split(rt.Field(i).Tag.Get("json"), ",")[0]] = true
	}
	for field := range c.Properties {
		if !inStruct[field] {
			t.Errorf("contract has %q, wire does not: the strict decoder would refuse every event", field)
		}
	}
	for field := range inStruct {
		if _, ok := c.Properties[field]; !ok {
			t.Errorf("wire has %q, contract does not", field)
		}
	}
}

// The stored enum is the index into the contract's array, so the array's
// order is stored data. This pins it: appending is allowed, moving is not.
func TestEnumOrderIsTheContractOrder(t *testing.T) {
	c := load(t)
	for name, set := range map[string][]string{"agent": Agents, "outcome": Outcomes, "error_kind": ErrorKinds} {
		if got := c.Properties[name].Enum; !reflect.DeepEqual(got, set) {
			t.Errorf("%s: contract %v, stored order %v: a reorder relabels every stored row", name, got, set)
		}
	}
}

const good = `{"event_id":"run-000001","agent":"cursor","occurred_at":"2026-09-08T10:00:00Z","duration_ms":1500,"outcome":"error","error_kind":"network","session_id":"` +
	"0000000000000000000000000000000000000000000000000000000000000001" + `","tokens_in":10}`

func TestDecodeStoresTheIndexAndTheBytes(t *testing.T) {
	r, err := Decode([]byte("tenant-a"), []byte(good))
	if err != nil {
		t.Fatal(err)
	}
	if r.Tenant != "tenant-a" || r.EventID != "run-000001" {
		t.Fatalf("tenant/id: %q %q", r.Tenant, r.EventID)
	}
	if r.Agent != 1 || r.Outcome != 1 || r.ErrorKind == nil || *r.ErrorKind != 3 {
		t.Fatalf("enums: agent=%d outcome=%d error_kind=%v", r.Agent, r.Outcome, r.ErrorKind)
	}
	if len(r.SessionID) != 32 || r.SessionID[31] != 1 || r.RepoHash != nil {
		t.Fatalf("hashes: session=%x repo=%v", r.SessionID, r.RepoHash)
	}
	if !r.OccurredAt.Equal(time.Date(2026, 9, 8, 10, 0, 0, 0, time.UTC)) {
		t.Fatalf("occurred_at: %v", r.OccurredAt)
	}
	if r.TokensIn == nil || *r.TokensIn != 10 || r.TokensOut != nil {
		t.Fatalf("optional ints: in=%v out=%v", r.TokensIn, r.TokensOut)
	}
}

// Each of these is Poison by nine-docs/adr/0001: the bytes will never decode,
// whatever the system does.
func TestDecodeRefusesWhatWillNeverParse(t *testing.T) {
	cases := map[string]struct{ key, value string }{
		"no key, so no tenant":           {"", good},
		"a field the contract lacks":     {"t", strings.Replace(good, `"tokens_in":10`, `"prompt":"secret"`, 1)},
		"an agent the enum lacks":        {"t", strings.Replace(good, `"cursor"`, `"hal9000"`, 1)},
		"a schema version this is not":   {"t", strings.Replace(good, `"error_kind":"network"`, `"error_kind":"solar_flare"`, 1)},
		"a hash that is not 32 bytes":    {"t", strings.Replace(good, "0000000000000000000000000000000000000000000000000000000000000001", "abcd", 1)},
		"a timestamp that is not a time": {"t", strings.Replace(good, "2026-09-08T10:00:00Z", "yesterday", 1)},
		"not json at all":                {"t", "{"},
	}
	for name, c := range cases {
		if _, err := Decode([]byte(c.key), []byte(c.value)); err == nil {
			t.Errorf("%s: decoded", name)
		}
	}
	_, err := Decode([]byte("t"), []byte(strings.Replace(good, `"cursor"`, `"hal9000"`, 1)))
	if !errors.Is(err, ErrUnknownValue) {
		t.Errorf("an unknown enum value should say so: %v", err)
	}
}
