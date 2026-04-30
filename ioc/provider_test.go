package ioc

import (
	"errors"
	"testing"
)

// TestProviderNameUsesNamer 验证 Namer.Name 优先于反射名称。
func TestProviderNameUsesNamer(t *testing.T) {
	name, err := providerName(&namedTestProvider{name: "custom"})
	if err != nil {
		t.Fatalf("provider name failed: %v", err)
	}
	if name != "custom" {
		t.Fatalf("expected custom name, got %q", name)
	}
}

// TestProviderNameUsesReflection 验证无 Namer 时使用具体类型反射名称。
func TestProviderNameUsesReflection(t *testing.T) {
	name, err := providerName(&reflectedTestProvider{})
	if err != nil {
		t.Fatalf("provider name failed: %v", err)
	}
	if name != TypeNameOf[reflectedTestProvider]() {
		t.Fatalf("expected reflected name %q, got %q", TypeNameOf[reflectedTestProvider](), name)
	}
	if TypeNameOf[*reflectedTestProvider]() != TypeNameOf[reflectedTestProvider]() {
		t.Fatal("expected pointer type name to be normalized")
	}
}

// TestProviderNameRejectsInvalidProviders 验证 nil provider 和空名称会失败。
func TestProviderNameRejectsInvalidProviders(t *testing.T) {
	var nilProvider *reflectedTestProvider
	if _, err := providerName(nilProvider); !errors.Is(err, ErrProviderNil) {
		t.Fatalf("expected nil provider error, got %v", err)
	}

	if _, err := providerName(&namedTestProvider{name: "  "}); !errors.Is(err, ErrProviderNameEmpty) {
		t.Fatalf("expected empty provider name error, got %v", err)
	}
}

// TestAppUseValidatesBeforeWrite 验证 Use 会先完整校验再写入 provider。
func TestAppUseValidatesBeforeWrite(t *testing.T) {
	app := New()
	if err := app.Use(&namedTestProvider{name: "one"}); err != nil {
		t.Fatalf("use first provider failed: %v", err)
	}

	if err := app.Use(&namedTestProvider{name: "two"}, &namedTestProvider{name: "two"}); !errors.Is(err, ErrProviderNameExists) {
		t.Fatalf("expected duplicate provider error, got %v", err)
	}

	if len(app.providerList()) != 1 {
		t.Fatalf("expected failed Use not to append providers, got %d", len(app.providerList()))
	}
	if err := app.Use(&namedTestProvider{name: "one"}); !errors.Is(err, ErrProviderNameExists) {
		t.Fatalf("expected duplicate existing provider error, got %v", err)
	}
}
