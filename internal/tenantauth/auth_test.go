package tenantauth

import "testing"

func TestCredentialResolvesTenantAndOBUScope(t *testing.T) {
	auth, err := New([]CredentialSpec{
		{Token: "device-token-0123456789abcdef0123456789abcdef", TenantID: 17, Role: RoleDevice, OBUIDs: []int{42, 43}},
		{Token: "tenant-token-0123456789abcdef0123456789abcdef", TenantID: 17, Role: RoleTenant},
	})
	if err != nil {
		t.Fatal(err)
	}

	device, ok := auth.Authenticate("Bearer device-token-0123456789abcdef0123456789abcdef")
	if !ok || device.TenantID != 17 || !device.AllowsOBU(42) || device.AllowsOBU(44) {
		t.Fatalf("device principal = %+v, authenticated = %v", device, ok)
	}
	tenant, ok := auth.Authenticate("Bearer tenant-token-0123456789abcdef0123456789abcdef")
	if !ok || tenant.TenantID != 17 || !tenant.AllowsOBU(999) {
		t.Fatalf("tenant principal = %+v, authenticated = %v", tenant, ok)
	}
	if _, ok := auth.Authenticate("Bearer unknown-token-0123456789abcdef0123456789"); ok {
		t.Fatal("unknown API token was authenticated")
	}
}

func TestCredentialConfigurationRejectsWeakAmbiguousAndInvalidScopes(t *testing.T) {
	tests := []struct {
		name  string
		creds []CredentialSpec
	}{
		{name: "empty config"},
		{name: "weak token", creds: []CredentialSpec{{Token: "short", TenantID: 1, Role: RoleTenant}}},
		{name: "non-positive tenant", creds: []CredentialSpec{{Token: "tenant-token-0123456789abcdef0123456789abcdef", TenantID: 0, Role: RoleTenant}}},
		{name: "device without OBU scope", creds: []CredentialSpec{{Token: "device-token-0123456789abcdef0123456789abcdef", TenantID: 1, Role: RoleDevice}}},
		{name: "tenant with OBU scope", creds: []CredentialSpec{{Token: "tenant-token-0123456789abcdef0123456789abcdef", TenantID: 1, Role: RoleTenant, OBUIDs: []int{42}}}},
		{name: "duplicate token", creds: []CredentialSpec{{Token: "same-token-0123456789abcdef0123456789abcd", TenantID: 1, Role: RoleTenant}, {Token: "same-token-0123456789abcdef0123456789abcd", TenantID: 2, Role: RoleTenant}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.creds); err == nil {
				t.Fatal("New() accepted invalid credential configuration")
			}
		})
	}
}

func TestParseRejectsTrailingMalformedJSON(t *testing.T) {
	valid := `[{"token":"tenant-token-0123456789abcdef0123456789abcdef","tenant_id":1,"role":"tenant"}]`
	for _, suffix := range []string{"{}", "{"} {
		if _, err := Parse(valid + suffix); err == nil {
			t.Fatalf("Parse accepted trailing data %q", suffix)
		}
	}
	if _, err := Parse(valid + " "); err != nil {
		t.Fatalf("Parse rejected trailing whitespace: %v", err)
	}
}
