# UI/UX Improvement Phases

A summary of the visual and UX improvements made to the Tweet-Price Correlation Analyzer.

---

## Phase 1: Data Integrity

**Problem:** The 1D (daily) charts had irregular candle spacing and some assets showed "doubled" candles. This was caused by mixing data from two different sources that used different timestamp conventions (one at midnight UTC, one at 4-5am UTC).

**What we fixed:**
- Cleaned up the database by removing inferior data (zero-volume candles from the old migration)
- Normalized all daily timestamps to midnight UTC
- Re-exported all 7 assets with clean data

**Result:** All daily charts now show evenly-spaced candles with no duplicates.

---

## Phase 2: Silence Line Logic

**Problem:** The dashed "silence lines" connecting tweet markers were missing on some charts (especially MON). This was because:
1. Lines shorter than 20 pixels weren't drawn at all
2. Same-day tweets showed meaningless "0%" labels (because they share the same daily price)

**What we fixed:**
- Replaced the arbitrary 20px threshold with semantic logic (30min+ gaps are meaningful)
- Added three tiers of line styling:
  - **Full glow + labels**: Gaps over 30min with >1% price change
  - **Thin colored line**: Gaps over 30min with <1% change (no labels)
  - **Subtle gray connector**: 15-30min gaps (just visual continuity)
- Gaps under 15min show no line (tweets are essentially continuous)

**Result:** MON and other charts now show connecting lines between all tweet clusters, with labels only appearing when they're informative.

---

## Phase 3: Microcopy & Labels

**Problem:** The interface had verbose labels, technical jargon, and inconsistent styling (hardcoded colors instead of CSS variables).

**What we fixed:**

| Element | Before | After |
|---------|--------|-------|
| Toggle button | "Tweet Markers" | "Tweets" |
| Legend | "Single tweet" / "Multiple tweets" / "Silence gap" | "1 tweet" / "3+ tweets" / "Quiet period" |
| Tooltip timestamp | "12/19/2025, 3:45:00 PM" | "Dec 19, 3:45 PM" |
| Tooltip label | "Price impact:" | "After tweet:" |
| Dismiss hint | Always shown | Mobile only |
| Bottom bar | "Data from X API & GeckoTerminal" | "X + GeckoTerminal" |

Also replaced ~30 hardcoded hex color values with CSS variables for consistency.

**Result:** Cleaner, more concise interface with consistent styling.

---

## Phase 4: Visual Hierarchy & Polish (Planned)

**Problem:** A few visual polish issues remain:
- Bubbles can get too large when zoomed in (up to 40px)
- Labels on steep silence lines can overlap the line itself
- The white ring around bubbles is a bit bright
- Very large gap labels (like "109d") have the same prominence as small ones

**Planned fixes:**
- Cap bubble size at 36px instead of 40px
- Shift labels horizontally for steep lines (>45° angle)
- Soften ring opacity from 50% to 40%
- Use smaller, muted text for gaps over 30 days

**Status:** Plan written, not yet implemented.

---

## Live URLs

- **Production:** https://tweet-price.vercel.app/chart
- **Latest deployment:** Check `vercel ls` for most recent

## Files Changed

- `web/src/components/Chart.tsx` - Main chart component with markers and silence lines
- `web/src/app/chart/page.tsx` - Chart page layout and bottom bar
- `web/src/app/globals.css` - CSS variables and design tokens
- `web/public/static/*/prices_1d.json` - Re-exported daily price data
- `scripts/data/analytics.duckdb` - Cleaned database

