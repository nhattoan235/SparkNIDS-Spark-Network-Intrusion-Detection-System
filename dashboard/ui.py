"""Small reusable HTML helpers for consistent dashboard emphasis."""

from __future__ import annotations

from html import escape


def metric_cards_html(items: list[tuple[str, str, str]], *, columns: int = 4) -> str:
    """Render compact, color-coded values without trusting labels or values as HTML."""
    cards = []
    for label, value, tone in items:
        tone = tone if tone in {"good", "warning", "bad", "neutral"} else "neutral"
        cards.append(
            f'<div class="metric-card {tone}" role="listitem">'
            f'<span>{escape(str(label))}</span><strong>{escape(str(value))}</strong></div>'
        )
    return f'<div class="metric-grid columns-{max(1, min(4, int(columns)))}" role="list">{"".join(cards)}</div>'
