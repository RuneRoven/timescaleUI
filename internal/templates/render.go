package templates

import (
	"bytes"
	"fmt"
	"html/template"
	"io/fs"
	"log/slog"
	"net/http"
	"strings"
)

// Renderer manages template parsing and rendering.
type Renderer struct {
	templates *template.Template
	logger    *slog.Logger
}

// New parses all templates from the embedded filesystem.
func New(fsys fs.FS, logger *slog.Logger) (*Renderer, error) {
	tmpl := template.New("").Funcs(FuncMap())

	err := fs.WalkDir(fsys, "templates", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".html") {
			return nil
		}
		data, err := fs.ReadFile(fsys, path)
		if err != nil {
			return fmt.Errorf("read template %s: %w", path, err)
		}
		name := strings.TrimPrefix(path, "templates/")
		if _, err := tmpl.New(name).Parse(string(data)); err != nil {
			return fmt.Errorf("parse template %s: %w", name, err)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk templates: %w", err)
	}

	return &Renderer{templates: tmpl, logger: logger}, nil
}

// PageData is the standard data envelope for full page renders.
type PageData struct {
	Title     string
	CSRFToken string
	User      string
	Active    string // active nav item
	Content   any
	Flashes   []Flash
}

// Flash is a one-time notification message.
type Flash struct {
	Type    string // "success", "error", "info", "warning"
	Message string
}

// Page renders a full page with the base layout.
func (r *Renderer) Page(w http.ResponseWriter, status int, page string, data PageData) {
	r.render(w, status, "layouts/base.html", page, data)
}

// Partial renders only a template fragment (for HTMX responses).
func (r *Renderer) Partial(w http.ResponseWriter, status int, name string, data any) {
	var buf bytes.Buffer
	if err := r.templates.ExecuteTemplate(&buf, name, data); err != nil {
		r.logger.Error("render partial", "template", name, "error", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	buf.WriteTo(w)
}

func (r *Renderer) render(w http.ResponseWriter, status int, layout, page string, data PageData) {
	// Clone and associate the page template with the layout
	tmpl, err := r.templates.Clone()
	if err != nil {
		r.logger.Error("clone templates", "error", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	// Define "content" block that the layout calls
	pageContent := r.templates.Lookup(page)
	if pageContent == nil {
		r.logger.Error("template not found", "page", page)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	// Render page into buffer first
	var pageBuf bytes.Buffer
	if err := pageContent.Execute(&pageBuf, data); err != nil {
		r.logger.Error("render page content", "page", page, "error", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	// Set the rendered page content
	data.Content = template.HTML(pageBuf.String())

	var buf bytes.Buffer
	if err := tmpl.ExecuteTemplate(&buf, "layouts/base.html", data); err != nil {
		r.logger.Error("render layout", "layout", layout, "error", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	buf.WriteTo(w)
}

// IsHTMX returns true if the request is an HTMX request.
func IsHTMX(r *http.Request) bool {
	return r.Header.Get("HX-Request") == "true"
}
