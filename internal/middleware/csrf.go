package middleware

import (
	"context"
	"net/http"

	"github.com/gorilla/csrf"
)

// CSRF returns a CSRF protection middleware.
func CSRF(authKey []byte, secure bool) func(http.Handler) http.Handler {
	opts := []csrf.Option{
		csrf.Path("/"),
		csrf.SameSite(csrf.SameSiteStrictMode),
		csrf.HttpOnly(true),
	}
	if !secure {
		opts = append(opts, csrf.Secure(false))
	}
	csrfMiddleware := csrf.Protect(authKey, opts...)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// When not using TLS, mark the request as plaintext so
			// gorilla/csrf skips the Referer header check (which is
			// only meaningful for TLS MITM protection).
			if !secure {
				r = csrf.PlaintextHTTPRequest(r)
			}
			csrfMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				token := csrf.Token(r)
				ctx := context.WithValue(r.Context(), "csrf_token", token)
				next.ServeHTTP(w, r.WithContext(ctx))
			})).ServeHTTP(w, r)
		})
	}
}
