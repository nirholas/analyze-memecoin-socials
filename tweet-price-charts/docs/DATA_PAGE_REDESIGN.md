# Data Page Redesign: The Primitive Table

## Context

Discussion on Dec 19, 2024 about redesigning the `/data` page. The current implementation has several fundamental problems that go beyond just UI polish.

---

## What's Wrong With The Current Page

### Surface-Level Issues
- "Win Rate" label is confusing (what does it mean?)
- "Notable Tweets" shows 3-5 month old data (not notable, just historical)
- Same tweets appear twice (in Notable section AND in table)
- Stats float in a card with no context
- 44 days since last tweet feels like a warning, not information

### The Real Problem

**The page has no purpose.**

It's a data dump pretending to be a product. It shows WHAT happened but not:
- What it MEANS
- What you should DO with it
- Whether the pattern is GOOD or BAD
- How it compares to ANYTHING

Questions users actually have:
- "Should I ape when this founder tweets?" → Not answered
- "Is the alpha decaying over time?" → Not answered
- "What kind of tweets pump hardest?" → Not answered

The page is a spreadsheet, not an insight.

---

## The Constraint

Design within a **full-page data table**. No dashboards, no insight cards, no fancy visualizations. Just a table.

But: make it infinitely deep for power users while instantly shareable on social media.

---

## The Primitive Table Design

### Philosophy

> The insight should emerge from LOOKING at the data, not from reading annotations.

What goes viral on Twitter:
- A screenshot of a terminal with numbers
- A raw spreadsheet where a pattern is visible
- A leaderboard where you can SEE the gap

The pattern IS the insight. Don't annotate it. Let it speak.

### The Design

**One-line summary (plain text, no styling):**
```
@a1lon9 · 102 tweets · 63% hit · +2.7% avg · last: 44d ago
```

**The table (three columns, that's it):**
```
DATE      TWEET                                           24H
────────────────────────────────────────────────────────────────
Sep 13    do you understand? https://t.co/...          +27.8%
Sep 13    pump fun is gonna make crypto cool again     +27.7%
Sep 13    already flipped Rumble in terms of...        +25.1%
Sep 13    the funny thing is that you could...         +25.0%
Sep 9     the teams that figure out how to...          +20.4%
Aug 10    I feel like we're gonna run back Q4...       +20.0%
Oct 25    been grinding the last 24 hours...           +17.9%
Sep 13    ...                                           -1.0%
5mo ago   they said it would be on the 2nd...         -13.5%
5mo ago   the trenches >>> getting a job              -19.3%
4mo ago   see you there! https://t.co/...             -24.8%
```

**Visual encoding:**
- Green text for positive percentages
- Red text for negative percentages
- That's it. No badges, pills, icons, or decorations.

When you screenshot this, you SEE: mostly green. Story told.

---

## What Gets Removed

| Element | Why Remove |
|---------|------------|
| Notable Tweets section | Redundant - same data is in table |
| Stats card with borders | Replace with plain text line |
| "Win Rate" label | Confusing - just say "63% hit" |
| Badges, pills, icons | Decoration, not data |
| ❤️ likes column | Secondary info - hide unless sorting by it |
| Price column | Nobody cares about $0.006 vs $0.004 |
| 1H change column | 24H is what matters |
| Separate Date column styling | Just plain text |

---

## What Remains

1. **Date** - when it happened
2. **Tweet** - what was said
3. **24H change** - what happened (the only number that matters)
4. **Color** - green/red (this is data encoding, not decoration)

---

## Depth Through Manipulation

The table is primitive. Primitives compose infinitely.

**Surface (instant, shareable):**
- See the pattern of green/red
- Read the one-line summary
- Screenshot and post

**Middle (interactive):**
- Click column header → sort
- Type → search/filter
- Click row → opens tweet on Twitter

**Deep (power user):**
- Export to CSV
- Analyze in Excel/Sheets
- Sort by engagement to find viral tweets
- Search for keywords to find patterns ("announcement" tweets vs "shitpost" tweets)

The depth is infinite because the USER controls it. We don't add features—we provide primitives they can combine.

---

## The Screenshot Test

If someone screenshots this table and posts it on Twitter, will people understand the story?

**Current page:** No. It's a mess of cards and numbers with no clear narrative.

**Primitive table:** Yes. You see a list of tweets. Most have green numbers. The founder's tweets pump the coin. Story told.

---

## Implementation Notes

### Summary Line
```tsx
<div className="text-sm text-[var(--text-secondary)] px-4 py-3 border-b border-[var(--border-subtle)]">
  @{founder} · {count} tweets · {hitRate}% hit · {avgReturn > 0 ? '+' : ''}{avgReturn}% avg · last: {daysSince}d ago
</div>
```

### Table Structure
- No `<thead>` or minimal (just "24H" label on the right)
- Rows are just: `date | tweet text | percentage`
- Percentage is colored with CSS: `text-[var(--positive)]` or `text-[var(--negative)]`
- Rows clickable → link to tweet

### Sorting
- Default: by 24H descending (biggest moves first)
- Clickable columns to re-sort
- Maybe keyboard shortcuts: `d` for date, `%` for percentage

### Search
- Simple text input
- Filters tweet text in real-time
- No fancy filters UI

---

## Open Questions

1. **Should engagement (likes) be visible?**
   - Pro: It's signal (viral tweets might have different patterns)
   - Con: Adds visual noise
   - Maybe: Show on hover, or as a 4th column that's muted

2. **Should rows have any background tint?**
   - Very subtle green/red tint could reinforce the pattern
   - But might feel like "design" not "data"
   - Test both

3. **How to handle "no recent tweets"?**
   - Current: "44 days ago" in red feels like error state
   - Better: Just part of the summary line, neutral tone
   - Or: Don't highlight it at all, let the data speak

4. **Mobile layout?**
   - Table might need horizontal scroll
   - Or: Stack date under tweet text on mobile
   - Keep percentage always visible (it's the point)

---

## Next Steps

1. Strip current page down to primitive table
2. Remove Notable Tweets section entirely
3. Replace stats card with one-line summary
4. Test with real data - does the pattern emerge?
5. Screenshot test - post in Discord, does it make sense?

---

*Document created: Dec 19, 2024*
*Context: Discussion about making data page useful vs. just being a data dump*
