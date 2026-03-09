/**
 * Formatting utilities for chart labels and annotations
 */

/**
 * Format a time duration (in seconds) as a human-readable string
 * @param seconds - Duration in seconds
 * @returns Formatted string like "30m", "5h", or "3d"
 */
export function formatTimeGap(seconds: number): string {
  const hours = seconds / 3600;
  if (hours < 1) return `${Math.round(seconds / 60)}m`;
  if (hours < 24) return `${Math.round(hours)}h`;
  const days = Math.round(hours / 24);
  return `${days}d`;
}

/**
 * Format a percentage change with sign and appropriate precision
 * @param pct - Percentage value (e.g., -5.25 for -5.25%)
 * @returns Formatted string like "+5.2%" or "-10%"
 */
export function formatPctChange(pct: number): string {
  const sign = pct >= 0 ? '+' : '';
  if (Math.abs(pct) >= 10) return `${sign}${pct.toFixed(0)}%`;
  return `${sign}${pct.toFixed(1)}%`;
}

