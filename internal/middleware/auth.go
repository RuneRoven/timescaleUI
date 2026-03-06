package middleware

import (
	"context"
	"net/http"

	"github.com/RuneRoven/timescaleUI/internal/auth"
)

type contextKey string

const UserKey contextKey = "user"

// RequireAuth redirects unauthenticated requests to login.
func RequireAuth(sessions *auth.SessionStore) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			sess, err := sessions.Get(r)
			if err != nil || sess == nil {
				http.Redirect(w, r, "/login", http.StatusSeeOther)
				return
			}
			ctx := context.WithValue(r.Context(), UserKey, sess.Username)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// UserFromContext extracts the username from the request context.
func UserFromContext(ctx context.Context) string {
	if u, ok := ctx.Value(UserKey).(string); ok {
		return u
	}
	return ""
}
