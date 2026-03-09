# Proposed Solution

## The Core Fix

The page tries to be a museum before earning the right. Museums work when visitors actively chose to enter. This page needs to **hook first, then show**.

## The Formula

```
HOOK (why should I care?) → CONTEXT (what am I looking at?) → EXPLORATION (the museum)
```

---

## Concrete Changes

### 1. New Hero Section

**Current:**
> "Tweets on charts."
> "See how founder tweets line up with price."

**Proposed:**
> "What happens when founders tweet?"
> "We tracked 4,400+ tweets from 13 token founders. Here's what the data shows."

This:
- Asks a question the visitor might actually wonder
- Gives immediate context (scope of data)
- Implies there's something interesting to discover

### 2. Module Micro-Context

Each module needs 1 sentence explaining WHY it's interesting, not just WHAT it is.

**Impact Explorer:**
> Current: "Every dot is a tweet. Hover to see details, click to open."
> Proposed: "Every dot is a tweet. Green = price went up after. Red = price went down."

**Time Heatmap:**
> Current: "Hour of day (UTC). Brighter = more tweets at that hour."
> Proposed: "Some founders tweet at 3am. Some only during market hours. See their patterns."

**Silences:**
> Current: "Longest gaps between tweets. What happened to price while they were silent?"
> Proposed: Keep this one - it actually asks an interesting question.

### 3. Asset Grid Framing

**Current:** Just shows the grid with "Pick an asset and explore."

**Problem:** FARTCOIN and USELESS undermine credibility.

**Options:**
1. Hide meme tokens by default, show "serious" ones
2. Lean into it: "Yes, we track FARTCOIN. Because the data doesn't care about dignity."
3. Reorder: Put the more credible ones first (JUP, HYPE, WIF)

**Recommendation:** Option 3. Reorder by market cap or tweet count, serious ones bubble up naturally.

### 4. Add Stakes

Somewhere early, establish WHY this matters:

> "Founder tweets can move markets. But how much? And for how long?"

This isn't claiming causation - it's asking a question that justifies the exploration.

---

## What NOT to Do

- Don't add a wall of text explaining everything
- Don't add statistical claims or "win rates"
- Don't over-design - the modules are already good
- Don't hide the data behind explainer pages

The fix is surgical: **add just enough context to make the museum make sense**.

---

## Implementation Estimate

~30 minutes of copy changes:
1. New hero section (10 min)
2. Module micro-context updates (10 min)
3. Asset grid reordering (10 min)

No new components. No architectural changes.
