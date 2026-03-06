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
	fsys   fs.FS
	funcs  template.FuncMap
	logger *slog.Logger
}

// New validates that all templates parse and returns a Renderer.
func New(fsys fs.FS, logger *slog.Logger) (*Renderer, error) {
	r := &Renderer{
		fsys:   fsys,
		funcs:  FuncMap(),
		logger: logger,
	}

	// Validate all templates parse correctly at startup
	if _, err := r.parseAll(); err != nil {
		return nil, err
	}

	return r, nil
}

func (r *Renderer) parseAll() (*template.Template, error) {
	tmpl := template.New("").Funcs(r.funcs)

	err := fs.WalkDir(r.fsys, "templates", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".html") {
			return nil
		}
		data, err := fs.ReadFile(r.fsys, path)
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

	return tmpl, nil
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
	// Parse a fresh template set for each render to avoid Clone-after-Execute
	tmpl, err := r.parseAll()
	if err != nil {
		r.logger.Error("parse templates", "error", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	pageContent := tmpl.Lookup(page)
	if pageContent == nil {
		r.logger.Error("template not found", "page", page)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	// Render page content with PageData — templates access content via .Content
	var pageBuf bytes.Buffer
	if err := pageContent.Execute(&pageBuf, data); err != nil {
		r.logger.Error("render page content", "page", page, "error", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	// Embed rendered page into layout
	data.Content = template.HTML(pageBuf.String())

	var buf bytes.Buffer
	if err := tmpl.ExecuteTemplate(&buf, "layouts/base.html", data); err != nil {
		r.logger.Error("render layout", "error", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	buf.WriteTo(w)
}

// Partial renders only a template fragment (for HTMX responses).
// For page templates (pages/*), data is wrapped in PageData so templates
// can access content via .Content consistently with full-page renders.
// For actual partials (partials/*), data is passed directly.
func (r *Renderer) Partial(w http.ResponseWriter, status int, name string, data any) {
	tmpl, err := r.parseAll()
	if err != nil {
		r.logger.Error("parse templates for partial", "error", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	// Wrap content in PageData for page templates so .Content.X works
	var templateData any = data
	if strings.HasPrefix(name, "pages/") {
		templateData = PageData{Content: data}
	}

	var buf bytes.Buffer
	if err := tmpl.ExecuteTemplate(&buf, name, templateData); err != nil {
		r.logger.Error("render partial", "template", name, "error", err)
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
