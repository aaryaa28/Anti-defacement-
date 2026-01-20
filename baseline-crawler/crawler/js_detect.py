"""
Detect whether a page requires JavaScript rendering.
Used to escalate React / SPA pages only when necessary.
"""

def needs_js_rendering(html: str) -> bool:
    if not html:
        return True

    h = html.lower()

    # SPA roots (framework-agnostic)
    if (
        '<div id="root"' in h or
        '<div id="app"' in h or
        '<app-root' in h or
        '<main id=' in h
    ):
        return True

    # If DOM is extremely sparse
    if len(h) < 3000:
        return True

    # If body exists but content is minimal
    if h.count("<a ") + h.count("<button") + h.count("<form") < 2:
        return True

    return False

