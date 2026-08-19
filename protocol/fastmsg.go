package protocol

import (
	"encoding/base64"
	"strconv"
	"unicode/utf8"
)

// MsgFrame is the hot-path form of a server -> client "msg" frame. The server
// encodes it with AppendMsg (no reflection) and the client decodes it with
// ParseMsg, falling back to encoding/json for anything ParseMsg declines. The
// wire bytes are ordinary JSON that a generic Frame decoder reads identically
// (see the differential fuzz test).
type MsgFrame struct {
	Sub, Topic  string
	Key, KeyEnc string
	Value, Enc  string
	Partition   int
	Offset, TS  int64
}

// SetKey stores k, base64-encoding it if it is not valid UTF-8.
func (m *MsgFrame) SetKey(k []byte) { m.Key, m.KeyEnc = encodeBytes(k) }

// SetValue stores v, base64-encoding it if it is not valid UTF-8.
func (m *MsgFrame) SetValue(v []byte) { m.Value, m.Enc = encodeBytes(v) }

// KeyBytes returns the decoded key.
func (m *MsgFrame) KeyBytes() ([]byte, error) { return decodeBytes(m.Key, m.KeyEnc) }

// ValueBytes returns the decoded payload.
func (m *MsgFrame) ValueBytes() ([]byte, error) { return decodeBytes(m.Value, m.Enc) }

func encodeBytes(b []byte) (string, string) {
	if utf8.Valid(b) {
		return string(b), ""
	}
	return base64.StdEncoding.EncodeToString(b), EncBase64
}

func decodeBytes(s, enc string) ([]byte, error) {
	if enc == EncBase64 {
		return base64.StdEncoding.DecodeString(s)
	}
	return []byte(s), nil
}

const hexDigits = "0123456789abcdef"

// appendJSONString appends s as a JSON string. Invalid UTF-8 becomes U+FFFD,
// exactly as encoding/json does, so the output is always valid JSON.
func appendJSONString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	for i := 0; i < len(s); {
		b := s[i]
		if b < utf8.RuneSelf {
			switch {
			case b == '"' || b == '\\':
				dst = append(dst, '\\', b)
			case b == '\n':
				dst = append(dst, '\\', 'n')
			case b == '\r':
				dst = append(dst, '\\', 'r')
			case b == '\t':
				dst = append(dst, '\\', 't')
			case b < 0x20:
				dst = append(dst, '\\', 'u', '0', '0', hexDigits[b>>4], hexDigits[b&0xf])
			default:
				dst = append(dst, b)
			}
			i++
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			dst = append(dst, `\ufffd`...)
		} else {
			dst = append(dst, s[i:i+size]...)
		}
		i += size
	}
	return append(dst, '"')
}

// AppendMsg appends the JSON encoding of m (without a trailing newline).
// Field order is fixed; ParseMsg relies on it.
func AppendMsg(dst []byte, m *MsgFrame) []byte {
	dst = append(dst, `{"op":"msg","sub":`...)
	dst = appendJSONString(dst, m.Sub)
	dst = append(dst, `,"topic":`...)
	dst = appendJSONString(dst, m.Topic)
	if m.Key != "" {
		dst = append(dst, `,"key":`...)
		dst = appendJSONString(dst, m.Key)
	}
	if m.KeyEnc != "" {
		dst = append(dst, `,"kenc":`...)
		dst = appendJSONString(dst, m.KeyEnc)
	}
	if m.Value != "" {
		dst = append(dst, `,"value":`...)
		dst = appendJSONString(dst, m.Value)
	}
	if m.Enc != "" {
		dst = append(dst, `,"enc":`...)
		dst = appendJSONString(dst, m.Enc)
	}
	dst = append(dst, `,"partition":`...)
	dst = strconv.AppendInt(dst, int64(m.Partition), 10)
	dst = append(dst, `,"offset":`...)
	dst = strconv.AppendInt(dst, m.Offset, 10)
	dst = append(dst, `,"ts":`...)
	dst = strconv.AppendInt(dst, m.TS, 10)
	return append(dst, '}')
}

// msgParser is a strict, allocation-light scanner for AppendMsg's exact shape.
type msgParser struct {
	b []byte
	i int
}

func (p *msgParser) lit(s string) bool {
	if len(p.b)-p.i < len(s) || string(p.b[p.i:p.i+len(s)]) != s {
		return false
	}
	p.i += len(s)
	return true
}

// str parses a JSON string starting at the opening quote.
func (p *msgParser) str() (string, bool) {
	if p.i >= len(p.b) || p.b[p.i] != '"' {
		return "", false
	}
	p.i++
	start := p.i
	// Fast path: no escapes.
	for j := start; j < len(p.b); j++ {
		c := p.b[j]
		if c == '"' {
			p.i = j + 1
			return string(p.b[start:j]), true
		}
		if c == '\\' {
			break
		}
		if c < 0x20 {
			return "", false
		}
	}
	// Slow path: unescape.
	var out []byte
	for p.i < len(p.b) {
		c := p.b[p.i]
		switch {
		case c == '"':
			p.i++
			return string(out), true
		case c < 0x20:
			return "", false
		case c != '\\':
			out = append(out, c)
			p.i++
		default:
			if p.i+1 >= len(p.b) {
				return "", false
			}
			e := p.b[p.i+1]
			p.i += 2
			switch e {
			case '"', '\\', '/':
				out = append(out, e)
			case 'b':
				out = append(out, '\b')
			case 'f':
				out = append(out, '\f')
			case 'n':
				out = append(out, '\n')
			case 'r':
				out = append(out, '\r')
			case 't':
				out = append(out, '\t')
			case 'u':
				r, ok := p.hex4()
				if !ok {
					return "", false
				}
				if r >= 0xD800 && r < 0xE000 {
					// Surrogate pair required; anything else: let encoding/json decide.
					if r >= 0xDC00 || !p.lit(`\u`) {
						return "", false
					}
					lo, ok := p.hex4()
					if !ok || lo < 0xDC00 || lo >= 0xE000 {
						return "", false
					}
					r = 0x10000 + (r-0xD800)<<10 + (lo - 0xDC00)
				}
				out = utf8.AppendRune(out, r)
			default:
				return "", false
			}
		}
	}
	return "", false
}

func (p *msgParser) hex4() (rune, bool) {
	if len(p.b)-p.i < 4 {
		return 0, false
	}
	var r rune
	for _, c := range p.b[p.i : p.i+4] {
		r <<= 4
		switch {
		case c >= '0' && c <= '9':
			r |= rune(c - '0')
		case c >= 'a' && c <= 'f':
			r |= rune(c-'a') + 10
		case c >= 'A' && c <= 'F':
			r |= rune(c-'A') + 10
		default:
			return 0, false
		}
	}
	p.i += 4
	return r, true
}

// int64 parses a strict JSON integer (no sign '+', no leading zeros, no fraction/exponent).
func (p *msgParser) int64() (int64, bool) {
	start := p.i
	if p.i < len(p.b) && p.b[p.i] == '-' {
		p.i++
	}
	digits := p.i
	for p.i < len(p.b) && p.b[p.i] >= '0' && p.b[p.i] <= '9' {
		p.i++
	}
	n := p.i - digits
	if n == 0 || n > 19 || (n > 1 && p.b[digits] == '0') {
		return 0, false
	}
	if p.i < len(p.b) { // a following '.', 'e', 'E' would make it a non-integer
		if c := p.b[p.i]; c == '.' || c == 'e' || c == 'E' {
			return 0, false
		}
	}
	v, err := strconv.ParseInt(string(p.b[start:p.i]), 10, 64)
	return v, err == nil
}

// ParseMsg decodes a line produced by AppendMsg. It returns ok=false for
// anything else — including valid JSON in a different shape or order — and the
// caller must then decode with encoding/json. It never accepts input that
// encoding/json would read differently.
func ParseMsg(line []byte) (MsgFrame, bool) {
	var m MsgFrame
	if !utf8.Valid(line) {
		return m, false
	}
	p := msgParser{b: line}
	var ok bool
	if !p.lit(`{"op":"msg","sub":`) {
		return m, false
	}
	if m.Sub, ok = p.str(); !ok || !p.lit(`,"topic":`) {
		return m, false
	}
	if m.Topic, ok = p.str(); !ok {
		return m, false
	}
	if p.lit(`,"key":`) {
		if m.Key, ok = p.str(); !ok {
			return m, false
		}
	}
	if p.lit(`,"kenc":`) {
		if m.KeyEnc, ok = p.str(); !ok {
			return m, false
		}
	}
	if p.lit(`,"value":`) {
		if m.Value, ok = p.str(); !ok {
			return m, false
		}
	}
	if p.lit(`,"enc":`) {
		if m.Enc, ok = p.str(); !ok {
			return m, false
		}
	}
	if !p.lit(`,"partition":`) {
		return m, false
	}
	part, ok := p.int64()
	if !ok || part < 0 || part > 1<<31 || !p.lit(`,"offset":`) {
		return m, false
	}
	m.Partition = int(part)
	if m.Offset, ok = p.int64(); !ok || !p.lit(`,"ts":`) {
		return m, false
	}
	if m.TS, ok = p.int64(); !ok || !p.lit(`}`) || p.i != len(p.b) {
		return m, false
	}
	return m, true
}
