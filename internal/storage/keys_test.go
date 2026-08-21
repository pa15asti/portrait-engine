package storage

import (
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestGenerateInputKey_FormatAndExtension(t *testing.T) {
	cases := map[string]string{
		".jpg":     ".jpg",
		".JPEG":    ".jpg",
		".png":     ".png",
		".gif":     ".jpg", // unsupported -> default
		"":         ".jpg",
		".unknown": ".jpg",
	}
	for in, wantExt := range cases {
		key := GenerateInputKey(in)
		if !strings.HasPrefix(key, InputPrefix) {
			t.Errorf("GenerateInputKey(%q) = %q, missing prefix", in, key)
		}
		if !strings.HasSuffix(key, wantExt) {
			t.Errorf("GenerateInputKey(%q) = %q, want suffix %q", in, key, wantExt)
		}
		if err := ValidateInputKey(key); err != nil {
			t.Errorf("generated key %q failed validation: %v", key, err)
		}
	}
}

func TestUploadID_RoundTrip(t *testing.T) {
	key := GenerateInputKey(".png")
	token := EncodeUploadID(key)
	got, err := DecodeUploadID(token)
	if err != nil {
		t.Fatalf("DecodeUploadID: %v", err)
	}
	if got != key {
		t.Errorf("round-trip mismatch: got %q want %q", got, key)
	}
}

func TestDecodeUploadID_Rejects(t *testing.T) {
	// Not valid base64url.
	if _, err := DecodeUploadID("!!!not base64!!!"); !errors.Is(err, ErrInvalidKey) {
		t.Errorf("expected ErrInvalidKey for bad base64, got %v", err)
	}
	// Valid base64 but a key outside our namespace.
	tok := EncodeUploadID("etc/passwd")
	if _, err := DecodeUploadID(tok); !errors.Is(err, ErrInvalidKey) {
		t.Errorf("expected ErrInvalidKey for foreign key, got %v", err)
	}
}

func TestValidateInputKey_Rejects(t *testing.T) {
	bad := []string{
		"outputs/x.jpg",         // wrong prefix
		"/uploads/x.jpg",        // leading slash
		"uploads/../etc/passwd", // traversal
		"uploads//double.jpg",   // empty segment
		"uploads/a/../../b.jpg", // traversal mid-path
		"uploads/x\x00.jpg",     // control char
		"uploads/./x.jpg",       // non-clean path
	}
	for _, k := range bad {
		if err := ValidateInputKey(k); !errors.Is(err, ErrInvalidKey) {
			t.Errorf("expected %q to be rejected, got %v", k, err)
		}
	}
}

func TestValidateInputKey_Accepts(t *testing.T) {
	if err := ValidateInputKey("uploads/2026/08/" + uuid.NewString() + ".jpg"); err != nil {
		t.Errorf("expected valid key, got %v", err)
	}
}

func TestGenerateOutputKey(t *testing.T) {
	id := uuid.New()
	key := GenerateOutputKey(id, "result.jpg")
	want := OutputPrefix + id.String() + "/result.jpg"
	if key != want {
		t.Errorf("GenerateOutputKey = %q, want %q", key, want)
	}
}
