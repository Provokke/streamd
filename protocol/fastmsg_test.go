package protocol

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
)

// frameOf decodes a line with the generic decoder, the reference behaviour.
func frameOf(t testing.TB, line []byte) (Frame, error) {
	var f Frame
	err := json.Unmarshal(line, &f)
	return f, err
}

func sameAsFrame(m MsgFrame, f Frame) bool {
	return f.Op == OpMsg && f.Sub == m.Sub && f.Topic == m.Topic && f.Key == m.Key && f.KeyEnc == m.KeyEnc &&
		f.Value == m.Value && f.Enc == m.Enc && f.Partition != nil && *f.Partition == m.Partition &&
		f.Offset != nil && *f.Offset == m.Offset && f.TS == m.TS
}

var tricky = []string{
	"", "plain", "with \"quotes\" and \\backslash\\", "line\nbreak\r\ttab", "\x00\x01\x1f control", "\x7f del",
	"unicode: héllo wörld ☃ 日本語 😀", "\u2028\u2029 separators", "<html>&amp;</html>", "/slash/", strings.Repeat("x", 5000),
	"\ufffd already replacement", "emoji 𝄞 clef",
}

func TestMsgFrameRoundTrip(t *testing.T) {
	for _, s := range tricky {
		for _, k := range []string{"", s} {
			m := MsgFrame{Sub: "s1", Topic: "t.opic-1", Key: k, Value: s, Partition: 3, Offset: 1 << 40, TS: 1730611200000}
			line := AppendMsg(nil, &m)
			got, ok := ParseMsg(line)
			if !ok {
				t.Fatalf("ParseMsg declined its own output for %q: %s", s, line)
			}
			if got != m {
				t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", got, m)
			}
			f, err := frameOf(t, line)
			if err != nil || !sameAsFrame(m, f) {
				t.Fatalf("generic decoder disagrees: %v %+v\n%s", err, f, line)
			}
		}
	}
}

func TestMsgFrameBinaryPayloads(t *testing.T) {
	var m MsgFrame
	m.SetKey([]byte{0xff, 0xfe})
	m.SetValue([]byte{0x00, 0xc3, 0x28, 0xff})
	m.Topic, m.Sub = "t", "s"
	if m.Enc != EncBase64 || m.KeyEnc != EncBase64 {
		t.Fatalf("binary not base64: %+v", m)
	}
	got, ok := ParseMsg(AppendMsg(nil, &m))
	if !ok {
		t.Fatal("declined")
	}
	k, _ := got.KeyBytes()
	v, _ := got.ValueBytes()
	if string(k) != "\xff\xfe" || string(v) != "\x00\xc3\x28\xff" {
		t.Fatalf("lossy: %q %q", k, v)
	}
}

func TestAppendJSONStringReplacesInvalidUTF8LikeEncodingJSON(t *testing.T) {
	in := "a\xffb\xc3\x28c"
	want, _ := json.Marshal(in)
	// encoding/json also escapes <,>,& and U+2028/9; compare via decode instead.
	var a, b string
	json.Unmarshal(want, &a)
	got := appendJSONString(nil, in)
	if err := json.Unmarshal(got, &b); err != nil || a != b {
		t.Fatalf("got %q (%v), want %q", b, err, a)
	}
}

func TestParseMsgDeclinesAnythingElse(t *testing.T) {
	good := string(AppendMsg(nil, &MsgFrame{Sub: "s", Topic: "t", Value: "v", Partition: 1, Offset: 2, TS: 3}))
	if _, ok := ParseMsg([]byte(good)); !ok {
		t.Fatal("baseline declined")
	}
	bad := []string{
		``, `{}`, `null`, good + " ", " " + good, good + "\n", good[:len(good)-1],
		`{"op":"ok","id":1}`,
		`{"sub":"s","op":"msg","topic":"t","partition":1,"offset":2,"ts":3}`,                    // reordered
		`{"op":"msg","sub":"s","topic":"t","partition":1,"offset":2,"ts":3,"extra":1}`,          // extra field
		`{"op":"msg","sub":"s","topic":"t","partition":1,"offset":2}`,                           // missing ts
		`{"op":"msg","sub":"s","topic":"t","partition":01,"offset":2,"ts":3}`,                   // leading zero
		`{"op":"msg","sub":"s","topic":"t","partition":1.5,"offset":2,"ts":3}`,                  // fraction
		`{"op":"msg","sub":"s","topic":"t","partition":1,"offset":2e3,"ts":3}`,                  // exponent
		`{"op":"msg","sub":"s","topic":"t","partition":-1,"offset":2,"ts":3}`,                   // negative partition
		`{"op":"msg","sub":"s","topic":"t","partition":1,"offset":99999999999999999999,"ts":3}`, // overflow
		`{"op":"msg","sub":"s","topic":"t","value":"a\x","partition":1,"offset":2,"ts":3}`,      // bad escape
		`{"op":"msg","sub":"s","topic":"t","value":"\ud800","partition":1,"offset":2,"ts":3}`,   // lone surrogate
		`{"op":"msg","sub":"s","topic":"t","value":"raw` + "\n" + `newline","partition":1,"offset":2,"ts":3}`,
		`{"op":"msg","sub":"s","topic":"t","value":"\xff","partition":1,"offset":2,"ts":3}`,
		"{\"op\":\"msg\",\"sub\":\"s\",\"topic\":\"t\",\"value\":\"bad\xffutf8\",\"partition\":1,\"offset\":2,\"ts\":3}",
		`{"op":"msg","sub":"s","topic":"t","key":"a","key":"b","partition":1,"offset":2,"ts":3}`, // duplicate
	}
	for _, b := range bad {
		if _, ok := ParseMsg([]byte(b)); ok {
			t.Errorf("accepted %q", b)
		}
	}
	// Escapes and surrogate pairs are understood.
	m, ok := ParseMsg([]byte(`{"op":"msg","sub":"s","topic":"t","value":"\u00e9\ud83d\ude00\n\/","partition":1,"offset":2,"ts":3}`))
	if !ok || m.Value != "é😀\n/" {
		t.Fatalf("escapes: %+v %v", m, ok)
	}
}

func TestParseMsgExtremeIntegers(t *testing.T) {
	m := MsgFrame{Sub: "s", Topic: "t", Partition: 0, Offset: math.MaxInt64, TS: math.MinInt64}
	got, ok := ParseMsg(AppendMsg(nil, &m))
	if !ok || got != m {
		t.Fatalf("%+v %v", got, ok)
	}
}

// FuzzParseMsgDifferential: whenever the fast parser accepts a line, the
// reference decoder must read exactly the same values. Seeds include valid
// frames and near-misses.
func FuzzParseMsgDifferential(f *testing.F) {
	for _, s := range tricky {
		f.Add(AppendMsg(nil, &MsgFrame{Sub: "s", Topic: "t", Key: s, Value: s, Partition: 2, Offset: 7, TS: 9}))
	}
	f.Add([]byte(`{"op":"msg","sub":"s","topic":"t","value":"\u00e9\ud83d\ude00","partition":1,"offset":2,"ts":3}`))
	f.Add([]byte(`{"op":"msg","sub":"s","topic":"t","partition":-0,"offset":2,"ts":3}`))
	f.Add([]byte(`{"op":"msg","sub":"s","topic":"t","partition":1,"offset":2,"ts":3}`))
	f.Fuzz(func(t *testing.T, line []byte) {
		m, ok := ParseMsg(line)
		if !ok {
			return
		}
		ref, err := frameOf(t, line)
		if err != nil {
			t.Fatalf("fast parser accepted what encoding/json rejects (%v): %q", err, line)
		}
		if !sameAsFrame(m, ref) {
			t.Fatalf("disagreement on %q:\n fast %+v\n  ref %+v", line, m, ref)
		}
		// And re-encoding is stable.
		if again, ok := ParseMsg(AppendMsg(nil, &m)); !ok || again != m {
			t.Fatalf("re-encode unstable for %q", line)
		}
	})
}

// FuzzAppendMsgRoundTrip: whatever strings we encode, both decoders agree.
func FuzzAppendMsgRoundTrip(f *testing.F) {
	for _, s := range tricky {
		f.Add(s, s, "sub")
	}
	f.Fuzz(func(t *testing.T, key, value, sub string) {
		m := MsgFrame{Sub: sub, Topic: "t", Partition: 1, Offset: 5, TS: 6}
		m.SetKey([]byte(key)) // base64 when not valid UTF-8, so the round trip is lossless
		m.SetValue([]byte(value))
		line := AppendMsg(nil, &m)
		ref, err := frameOf(t, line)
		if err != nil {
			t.Fatalf("invalid JSON produced for %q/%q: %v\n%s", key, value, err, line)
		}
		if fast, ok := ParseMsg(line); ok {
			if !sameAsFrame(fast, ref) {
				t.Fatalf("fast/ref disagree: %+v vs %+v", fast, ref)
			}
		}
		k, _ := ref.KeyBytes()
		v, _ := ref.ValueBytes()
		if string(k) != key || string(v) != value {
			t.Fatalf("payload not preserved: %q/%q -> %q/%q", key, value, k, v)
		}
	})
}
