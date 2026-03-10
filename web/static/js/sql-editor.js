// CodeMirror 5 initialization for SQL and JSON editors
(function() {
    'use strict';

    var editors = [];

    function isDark() {
        return document.documentElement.classList.contains('dark');
    }

    function initEditor(textarea) {
        if (!textarea || textarea._cmEditor) return;
        var mode = textarea.getAttribute('data-editor');
        if (!mode) return;

        var isReadonly = textarea.hasAttribute('data-readonly');
        var cmMode = mode === 'json' ? {name: 'javascript', json: true} : 'text/x-pgsql';
        var cm = CodeMirror.fromTextArea(textarea, {
            mode: cmMode,
            theme: isDark() ? 'material-darker' : 'default',
            lineNumbers: true,
            matchBrackets: true,
            lineWrapping: true,
            tabSize: 2,
            indentWithTabs: false,
            readOnly: isReadonly ? 'nocursor' : false,
            cursorBlinkRate: isReadonly ? -1 : 530,
            extraKeys: isReadonly ? {} : {
                'Ctrl-Enter': function() { submitForm(textarea); },
                'Cmd-Enter': function() { submitForm(textarea); },
                Tab: function(cm) {
                    cm.replaceSelection('  ', 'end');
                }
            }
        });

        textarea._cmEditor = cm;
        editors.push(cm);

        if (isReadonly) {
            cm.display.wrapper.classList.add('cm-readonly');
        }

        // Auto-resize: readonly uses auto height, editable uses row-based
        if (isReadonly) {
            cm.setSize(null, null);
        } else {
            var rows = parseInt(textarea.getAttribute('rows')) || 6;
            cm.setSize(null, Math.max(rows * 1.5, 6) + 'em');
        }

        // Sync value back to textarea on changes
        cm.on('change', function() { cm.save(); });
    }

    function submitForm(textarea) {
        var form = textarea.closest('form');
        if (form) {
            if (typeof htmx !== 'undefined' && form.hasAttribute('hx-post')) {
                htmx.trigger(form, 'submit');
            } else {
                form.requestSubmit ? form.requestSubmit() : form.submit();
            }
        }
    }

    function initAll(root) {
        var areas = (root || document).querySelectorAll('textarea[data-editor]');
        for (var i = 0; i < areas.length; i++) {
            initEditor(areas[i]);
        }
    }

    function updateThemes() {
        var theme = isDark() ? 'material-darker' : 'default';
        for (var i = 0; i < editors.length; i++) {
            editors[i].setOption('theme', theme);
        }
    }

    // Make a readonly CM editable (for inline edit toggle)
    function enableEditing(textarea) {
        var cm = textarea._cmEditor;
        if (!cm) return;
        cm.setOption('readOnly', false);
        cm.setOption('cursorBlinkRate', 530);
        cm.setOption('extraKeys', {
            'Ctrl-Enter': function() { submitForm(textarea); },
            'Cmd-Enter': function() { submitForm(textarea); },
            Tab: function(cm) { cm.replaceSelection('  ', 'end'); }
        });
        cm.display.wrapper.classList.remove('cm-readonly');
        var rows = parseInt(textarea.getAttribute('rows')) || 10;
        cm.setSize(null, Math.max(rows * 1.5, 6) + 'em');
        cm.focus();
    }

    function disableEditing(textarea, originalValue) {
        var cm = textarea._cmEditor;
        if (!cm) return;
        if (originalValue !== undefined) cm.setValue(originalValue);
        cm.setOption('readOnly', 'nocursor');
        cm.setOption('cursorBlinkRate', -1);
        cm.setOption('extraKeys', {});
        cm.display.wrapper.classList.add('cm-readonly');
        cm.setSize(null, null);
    }

    // Use document-level listeners (available before body exists)
    // Sync CM values before HTMX sends requests
    document.addEventListener('htmx:configRequest', function() {
        for (var i = 0; i < editors.length; i++) {
            editors[i].save();
        }
    });

    // Init editors in HTMX-swapped content
    document.addEventListener('htmx:afterSettle', function(e) {
        initAll(e.detail.target);
    });

    // Refresh CM when <details> is toggled (CM can't measure hidden elements)
    document.addEventListener('toggle', function(e) {
        if (e.target.tagName === 'DETAILS' && e.target.open) {
            var areas = e.target.querySelectorAll('textarea[data-editor]');
            for (var i = 0; i < areas.length; i++) {
                if (areas[i]._cmEditor) areas[i]._cmEditor.refresh();
            }
        }
    }, true);

    // Watch for class changes on <html> for theme toggles
    new MutationObserver(function() {
        updateThemes();
    }).observe(document.documentElement, {attributes: true, attributeFilter: ['class']});

    // Init on DOM ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', function() { initAll(); });
    } else {
        initAll();
    }

    // Expose for manual use
    window.initSqlEditor = initEditor;
    window.cmEnableEditing = enableEditing;
    window.cmDisableEditing = disableEditing;
})();
