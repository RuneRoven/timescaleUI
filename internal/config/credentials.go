package config

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// Credentials holds encrypted-at-rest admin and DB connection info.
type Credentials struct {
	AdminUser    string `json:"admin_user"`
	AdminHash    string `json:"admin_hash"` // bcrypt hash
	DBHost       string `json:"db_host"`
	DBPort       int    `json:"db_port"`
	DBUser       string `json:"db_user"`
	DBPassword   string `json:"db_password"`
	DBName       string `json:"db_name"`
	DBSSLMode    string `json:"db_sslmode"`
}

// CredentialStore manages encrypted credential persistence.
type CredentialStore struct {
	dataDir string
	key     []byte // 32-byte AES key
}

// NewCredentialStore loads or generates the encryption key and returns a store.
func NewCredentialStore(dataDir, envKey string) (*CredentialStore, error) {
	key, err := resolveKey(dataDir, envKey)
	if err != nil {
		return nil, fmt.Errorf("resolve encryption key: %w", err)
	}
	return &CredentialStore{dataDir: dataDir, key: key}, nil
}

// Exists returns true if credentials have been saved.
func (s *CredentialStore) Exists() bool {
	_, err := os.Stat(s.credPath())
	return err == nil
}

// Save encrypts and persists credentials.
func (s *CredentialStore) Save(creds *Credentials) error {
	plaintext, err := json.Marshal(creds)
	if err != nil {
		return fmt.Errorf("marshal credentials: %w", err)
	}

	ciphertext, err := encrypt(s.key, plaintext)
	if err != nil {
		return fmt.Errorf("encrypt credentials: %w", err)
	}

	if err := os.WriteFile(s.credPath(), ciphertext, 0o600); err != nil {
		return fmt.Errorf("write credentials file: %w", err)
	}
	return nil
}

// Load decrypts and returns stored credentials.
func (s *CredentialStore) Load() (*Credentials, error) {
	ciphertext, err := os.ReadFile(s.credPath())
	if err != nil {
		return nil, fmt.Errorf("read credentials file: %w", err)
	}

	plaintext, err := decrypt(s.key, ciphertext)
	if err != nil {
		return nil, fmt.Errorf("decrypt credentials: %w", err)
	}

	var creds Credentials
	if err := json.Unmarshal(plaintext, &creds); err != nil {
		return nil, fmt.Errorf("unmarshal credentials: %w", err)
	}
	return &creds, nil
}

func (s *CredentialStore) credPath() string {
	return filepath.Join(s.dataDir, "credentials.enc")
}

func resolveKey(dataDir, envKey string) ([]byte, error) {
	if envKey != "" {
		key, err := hex.DecodeString(envKey)
		if err != nil {
			return nil, fmt.Errorf("decode TSUI_ENCRYPTION_KEY hex: %w", err)
		}
		if len(key) != 32 {
			return nil, fmt.Errorf("TSUI_ENCRYPTION_KEY must be 32 bytes (64 hex chars), got %d bytes", len(key))
		}
		return key, nil
	}

	keyPath := filepath.Join(dataDir, ".key")
	if data, err := os.ReadFile(keyPath); err == nil {
		key, err := hex.DecodeString(string(data))
		if err != nil {
			return nil, fmt.Errorf("decode saved key: %w", err)
		}
		if len(key) == 32 {
			return key, nil
		}
	}

	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, fmt.Errorf("generate random key: %w", err)
	}

	if err := os.WriteFile(keyPath, []byte(hex.EncodeToString(key)), 0o600); err != nil {
		return nil, fmt.Errorf("save key file: %w", err)
	}
	return key, nil
}

func encrypt(key, plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

func decrypt(key, ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, fmt.Errorf("ciphertext too short")
	}
	nonce, data := ciphertext[:nonceSize], ciphertext[nonceSize:]
	return gcm.Open(nil, nonce, data, nil)
}
