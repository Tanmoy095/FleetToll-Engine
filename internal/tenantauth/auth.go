package tenantauth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	TenantKeysEnv   = "FLEETTOLL_TENANT_API_KEYS"
	ServiceTokenEnv = "AGGREGATOR_GRPC_TOKEN"
	MinTokenBytes   = 32
	RoleTenant      = "tenant"
	RoleDevice      = "device"
)

type CredentialSpec struct {
	Token    string `json:"token"`
	TenantID int64  `json:"tenant_id"`
	Role     string `json:"role"`
	OBUIDs   []int  `json:"obu_ids,omitempty"`
}

type Principal struct {
	TenantID int64
	Role     string
	obuIDs   map[int]struct{}
}

func (p Principal) AllowsOBU(obuID int) bool {
	if p.Role == RoleTenant {
		return true
	}
	_, ok := p.obuIDs[obuID]
	return p.Role == RoleDevice && ok
}

func (p Principal) IsDevice() bool { return p.Role == RoleDevice }

type credential struct {
	digest    [32]byte
	principal Principal
}

type Authenticator struct{ credentials []credential }

func New(specs []CredentialSpec) (Authenticator, error) {
	if len(specs) == 0 {
		return Authenticator{}, errors.New("at least one tenant API credential is required")
	}
	entries := make([]credential, 0, len(specs))
	seen := make(map[[32]byte]struct{}, len(specs))
	for index, spec := range specs {
		if len(spec.Token) < MinTokenBytes {
			return Authenticator{}, fmt.Errorf("credential %d token must contain at least %d bytes", index, MinTokenBytes)
		}
		if spec.TenantID <= 0 {
			return Authenticator{}, fmt.Errorf("credential %d tenant ID must be positive", index)
		}
		principal := Principal{TenantID: spec.TenantID, Role: spec.Role}
		switch spec.Role {
		case RoleTenant:
			if len(spec.OBUIDs) != 0 {
				return Authenticator{}, fmt.Errorf("tenant credential %d must not define OBU IDs", index)
			}
		case RoleDevice:
			if len(spec.OBUIDs) == 0 {
				return Authenticator{}, fmt.Errorf("device credential %d must define at least one OBU ID", index)
			}
			principal.obuIDs = make(map[int]struct{}, len(spec.OBUIDs))
			for _, obuID := range spec.OBUIDs {
				if obuID <= 0 {
					return Authenticator{}, fmt.Errorf("device credential %d OBU IDs must be positive", index)
				}
				if _, exists := principal.obuIDs[obuID]; exists {
					return Authenticator{}, fmt.Errorf("device credential %d contains duplicate OBU ID", index)
				}
				principal.obuIDs[obuID] = struct{}{}
			}
		default:
			return Authenticator{}, fmt.Errorf("credential %d has unsupported role", index)
		}
		digest := sha256.Sum256([]byte(spec.Token))
		if _, exists := seen[digest]; exists {
			return Authenticator{}, fmt.Errorf("credential %d duplicates an existing token", index)
		}
		seen[digest] = struct{}{}
		entries = append(entries, credential{digest: digest, principal: principal})
	}
	return Authenticator{credentials: entries}, nil
}

func Parse(raw string) (Authenticator, error) {
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	var specs []CredentialSpec
	if err := decoder.Decode(&specs); err != nil {
		return Authenticator{}, fmt.Errorf("decode tenant API credential configuration: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return Authenticator{}, errors.New("tenant API credential configuration must contain exactly one valid JSON value")
	}
	return New(specs)
}

func FromEnv() (Authenticator, error) {
	raw := strings.TrimSpace(os.Getenv(TenantKeysEnv))
	if raw == "" {
		return Authenticator{}, fmt.Errorf("%s must be configured", TenantKeysEnv)
	}
	return Parse(raw)
}

func ValidateServiceToken(token string) error {
	if len(token) < MinTokenBytes {
		return fmt.Errorf("%s must contain at least %d bytes", ServiceTokenEnv, MinTokenBytes)
	}
	return nil
}

func ServiceTokenFromEnv() (string, error) {
	token := strings.TrimSpace(os.Getenv(ServiceTokenEnv))
	if err := ValidateServiceToken(token); err != nil {
		return "", err
	}
	return token, nil
}

func (a Authenticator) Authenticate(header string) (Principal, bool) {
	parts := strings.Fields(header)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || len(parts[1]) < MinTokenBytes {
		return Principal{}, false
	}
	got := sha256.Sum256([]byte(parts[1]))
	var matched Principal
	matchCount := 0
	for _, entry := range a.credentials {
		if subtle.ConstantTimeCompare(got[:], entry.digest[:]) == 1 {
			matched = entry.principal
			matchCount++
		}
	}
	return matched, matchCount == 1
}

func (a Authenticator) TenantIDs() []int64 {
	ids := make([]int64, 0, len(a.credentials))
	seen := make(map[int64]struct{}, len(a.credentials))
	for _, entry := range a.credentials {
		if _, exists := seen[entry.principal.TenantID]; exists {
			continue
		}
		seen[entry.principal.TenantID] = struct{}{}
		ids = append(ids, entry.principal.TenantID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

type contextKey struct{}

func WithPrincipal(ctx context.Context, principal Principal) context.Context {
	return context.WithValue(ctx, contextKey{}, principal)
}

func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	principal, ok := ctx.Value(contextKey{}).(Principal)
	return principal, ok
}

func (a Authenticator) HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := a.Authenticate(r.Header.Get("Authorization"))
		if !ok {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r.WithContext(WithPrincipal(r.Context(), principal)))
	})
}

func ServiceTokenHTTPMiddleware(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !validBearerServiceToken(r.Header.Get("Authorization"), token) {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func ServiceTokenUnaryInterceptor(token string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		values, _ := metadata.FromIncomingContext(ctx)
		authorization := values.Get("authorization")
		if len(authorization) != 1 || !validBearerServiceToken(authorization[0], token) {
			return nil, status.Error(codes.Unauthenticated, "valid service credentials are required")
		}
		return handler(ctx, request)
	}
}

func WithServiceToken(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}

func validBearerServiceToken(header, expected string) bool {
	if len(expected) < MinTokenBytes {
		return false
	}
	parts := strings.Fields(header)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return false
	}
	got := sha256.Sum256([]byte(parts[1]))
	want := sha256.Sum256([]byte(expected))
	return subtle.ConstantTimeCompare(got[:], want[:]) == 1
}
