package config

import (
	"fmt"
	"net/url"
	"reflect"
	"strings"
)

// String returns a multi-line summary of the effective configuration,
// suitable for logging at boot. It walks the Config tree via reflection
// and prints each leaf as "Domain.Field=value", with redaction applied:
//
//   - Fields tagged `secret:"true"` are rendered as "***" (or `""` if
//     they are empty in the first place).
//   - String fields whose name contains "url" (case-insensitive) are
//     parsed as URLs; if a password is embedded in the userinfo, it is
//     replaced with "***" before printing. Connection strings like
//     "amqp://user:pass@host/" become "amqp://user:***@host/".
//
// String never accesses external resources (no DNS, no file IO) — it is
// safe to call inside critical sections.
func (c *Config) String() string {
	var sb strings.Builder
	sb.WriteString("Effective config:\n")
	dumpStruct(&sb, "", reflect.ValueOf(c).Elem())
	return sb.String()
}

// dumpStruct walks v depth-first, emitting "prefix.Field=value" lines.
// Nested structs are recursed; primitives are rendered via renderValue.
// Unsupported kinds (slice, map, chan, ...) print their Go-default %v;
// no Config field uses them today, but if one is added the output is
// still informative.
func dumpStruct(sb *strings.Builder, prefix string, v reflect.Value) {
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		field := t.Field(i)
		value := v.Field(i)

		name := field.Name
		if prefix != "" {
			name = prefix + "." + name
		}

		if value.Kind() == reflect.Struct {
			dumpStruct(sb, name, value)
			continue
		}

		fmt.Fprintf(sb, "  %s=%s\n", name, renderValue(field, value))
	}
}

// renderValue renders a single field value with redaction applied.
//
// Redaction precedence:
//  1. Field tag `secret:"true"` wins absolutely.
//  2. Field name containing "url" triggers URL-aware redaction; the
//     password component of any embedded userinfo is replaced.
//  3. Otherwise, the value is printed with %v.
func renderValue(field reflect.StructField, value reflect.Value) string {
	s := fmt.Sprintf("%v", value.Interface())

	if field.Tag.Get("secret") == "true" {
		if s == "" {
			return `""`
		}
		return "***"
	}

	if strings.Contains(strings.ToLower(field.Name), "url") && s != "" {
		if redacted, ok := redactURLPassword(s); ok {
			return redacted
		}
	}

	return s
}

// redactURLPassword returns the input URL with any password in its
// userinfo replaced by "***". Returns (input, false) if the input does
// not parse as a URL or has no password to redact, so the caller can
// fall back to the raw value.
//
// Implementation note: we cannot use url.UserPassword("user", "***")
// because url.URL.String() percent-encodes special characters in the
// userinfo password (asterisks become %2A%2A%2A). Operators reading
// logs need a literal "***" marker, so we splice the URL by hand.
func redactURLPassword(raw string) (string, bool) {
	u, err := url.Parse(raw)
	if err != nil || u.User == nil {
		return raw, false
	}
	if _, hasPass := u.User.Password(); !hasPass {
		return raw, false
	}
	username := u.User.Username()
	// Clear userinfo so u.String() omits it; we re-insert manually.
	u.User = nil
	rest := u.String()
	// rest is "scheme://host/..."; find the "//" separator.
	idx := strings.Index(rest, "//")
	if idx < 0 {
		// Shouldn't happen for a well-formed URL, but bail gracefully.
		return raw, false
	}
	return rest[:idx+2] + username + ":***@" + rest[idx+2:], true
}
