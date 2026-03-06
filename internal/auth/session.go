package auth

import (
	"net/http"
	"time"

	"github.com/gorilla/securecookie"
)

// SessionStore manages encrypted cookie sessions.
type SessionStore struct {
	codec     *securecookie.SecureCookie
	cookieKey string
	ttl       time.Duration
	secure    bool
}

// SessionData holds the session payload.
type SessionData struct {
	Username  string    `json:"u"`
	ExpiresAt time.Time `json:"e"`
}

// NewSessionStore creates a session manager with encrypted cookies.
func NewSessionStore(hashKey, blockKey []byte, ttl time.Duration, secure bool) *SessionStore {
	codec := securecookie.New(hashKey, blockKey)
	codec.MaxAge(int(ttl.Seconds()))
	return &SessionStore{
		codec:     codec,
		cookieKey: "__Host-tsui-session",
		ttl:       ttl,
		secure:    secure,
	}
}

// Create sets the session cookie on the response.
func (s *SessionStore) Create(w http.ResponseWriter, username string) error {
	data := SessionData{
		Username:  username,
		ExpiresAt: time.Now().Add(s.ttl),
	}
	encoded, err := s.codec.Encode(s.cookieKey, data)
	if err != nil {
		return err
	}

	cookie := &http.Cookie{
		Name:     s.cookieKey,
		Value:    encoded,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		MaxAge:   int(s.ttl.Seconds()),
	}
	if s.secure {
		cookie.Secure = true
	} else {
		// __Host- prefix requires Secure; fall back to different name for dev
		cookie.Name = "tsui-session"
	}
	http.SetCookie(w, cookie)
	return nil
}

// Get retrieves the session from the request cookie.
func (s *SessionStore) Get(r *http.Request) (*SessionData, error) {
	cookieName := s.cookieKey
	if !s.secure {
		cookieName = "tsui-session"
	}
	cookie, err := r.Cookie(cookieName)
	if err != nil {
		return nil, err
	}
	var data SessionData
	if err := s.codec.Decode(cookieName, cookie.Value, &data); err != nil {
		return nil, err
	}
	if time.Now().After(data.ExpiresAt) {
		return nil, http.ErrNoCookie
	}
	return &data, nil
}

// Destroy removes the session cookie.
func (s *SessionStore) Destroy(w http.ResponseWriter) {
	name := s.cookieKey
	if !s.secure {
		name = "tsui-session"
	}
	http.SetCookie(w, &http.Cookie{
		Name:     name,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		MaxAge:   -1,
	})
}
