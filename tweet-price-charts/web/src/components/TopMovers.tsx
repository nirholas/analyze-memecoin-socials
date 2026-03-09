'use client';

/**
 * TopMovers Component
 * ====================
 * Displays the top performing and worst performing tweets based on 24h price change.
 *
 * Structure:
 * - TopMovers: Container that fetches top 3 pumps and dumps from events
 * - MoverCard: Individual card showing tweet + price impact
 *
 * Props:
 * - events: Array of TweetEvent objects
 * - founder: Twitter handle for building tweet URLs
 *
 * @module components/TopMovers
 */

import { useMemo } from 'react';
import { TweetEvent } from '@/lib/types';

// ============================================================================
// TYPES
// ============================================================================

interface TopMoversProps {
  events: TweetEvent[];
  founder: string;
}

interface MoverCardProps {
  event: TweetEvent;
  founder: string;
  rank: number;
  type: 'pump' | 'dump';
}

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

/**
 * Decode HTML entities in tweet text (&gt; &amp; &lt; etc)
 * Uses a textarea element to leverage browser's native decoding
 */
function decodeHtmlEntities(text: string): string {
  if (typeof document === 'undefined') return text; // SSR safety
  const textarea = document.createElement('textarea');
  textarea.innerHTML = text;
  return textarea.value;
}

/**
 * Format relative time (e.g., "2d ago", "3mo ago")
 */
function formatRelativeTime(timestamp: number): string {
  const now = Date.now() / 1000;
  const diff = now - timestamp;

  const minutes = Math.floor(diff / 60);
  const hours = Math.floor(diff / 3600);
  const days = Math.floor(diff / 86400);
  const months = Math.floor(diff / 2592000);
  const years = Math.floor(diff / 31536000);

  if (years > 0) return `${years}y ago`;
  if (months > 0) return `${months}mo ago`;
  if (days > 0) return `${days}d ago`;
  if (hours > 0) return `${hours}h ago`;
  if (minutes > 0) return `${minutes}m ago`;
  return 'just now';
}

// ============================================================================
// MOVER CARD COMPONENT
// ============================================================================

/**
 * MoverCard - Individual card showing a top-performing or worst-performing tweet
 *
 * Features:
 * - Shows rank badge (1, 2, 3)
 * - Displays truncated tweet text
 * - Shows 24h price change with color coding
 * - Clickable to open tweet on Twitter
 * - Hover effect for interactivity
 */
function MoverCard({ event, founder, rank, type }: MoverCardProps) {
  const change = event.change_24h_pct ?? 0;
  const isPositive = change >= 0;
  const decodedText = decodeHtmlEntities(event.text);
  const relativeTime = formatRelativeTime(event.timestamp);

  // Color scheme based on pump vs dump
  const colorClass = type === 'pump'
    ? 'text-[var(--positive)]'
    : 'text-[var(--negative)]';

  const bgHoverClass = type === 'pump'
    ? 'hover:bg-[var(--positive)]/5'
    : 'hover:bg-[var(--negative)]/5';

  return (
    <a
      href={`https://twitter.com/${founder}/status/${event.tweet_id}`}
      target="_blank"
      rel="noopener noreferrer"
      className={`
        group flex items-start gap-3 p-3 rounded-lg
        bg-[var(--surface-1)] border border-[var(--border-subtle)]
        transition-all duration-200 ${bgHoverClass}
        hover:border-[var(--border-default)] hover:shadow-sm
      `}
    >
      {/* Rank Badge */}
      <div className={`
        flex-shrink-0 w-6 h-6 rounded-full
        flex items-center justify-center text-xs font-bold
        ${type === 'pump' ? 'bg-[var(--positive)]/20 text-[var(--positive)]' : 'bg-[var(--negative)]/20 text-[var(--negative)]'}
      `}>
        {rank}
      </div>

      {/* Content */}
      <div className="flex-1 min-w-0">
        {/* Tweet text - truncated to 2 lines */}
        <p className="text-sm text-[var(--text-primary)] line-clamp-2 group-hover:text-[var(--accent)] transition-colors">
          {decodedText}
        </p>

        {/* Meta row: time + change */}
        <div className="flex items-center justify-between mt-1.5">
          <span className="text-xs text-[var(--text-muted)]">
            {relativeTime}
          </span>
          <span className={`font-mono text-sm font-semibold tabular-nums ${colorClass}`}>
            {isPositive ? '+' : ''}{change.toFixed(1)}%
          </span>
        </div>
      </div>
    </a>
  );
}

// ============================================================================
// TOP MOVERS COMPONENT
// ============================================================================

/**
 * TopMovers - Container showing the top 3 pumps and top 3 dumps
 *
 * Layout:
 * - Two columns on desktop (pumps | dumps)
 * - Stacked on mobile
 * - Each column shows up to 3 cards
 *
 * Data Processing:
 * - Filters events to those with valid 24h change data
 * - Sorts by change_24h_pct (desc for pumps, asc for dumps)
 * - Takes top 3 from each end
 */
export default function TopMovers({ events, founder }: TopMoversProps) {
  // Compute top movers: 3 biggest pumps and 3 biggest dumps
  const { topPumps, topDumps } = useMemo(() => {
    // Filter to events with price data
    const withPriceData = events.filter(e => e.change_24h_pct !== null);

    // Sort by 24h change descending
    const sorted = [...withPriceData].sort((a, b) =>
      (b.change_24h_pct ?? 0) - (a.change_24h_pct ?? 0)
    );

    // Top 3 pumps (biggest positive changes)
    const pumps = sorted.slice(0, 3).filter(e => (e.change_24h_pct ?? 0) > 0);

    // Top 3 dumps (biggest negative changes, from the end)
    const dumps = sorted.slice(-3).reverse().filter(e => (e.change_24h_pct ?? 0) < 0);

    return { topPumps: pumps, topDumps: dumps };
  }, [events]);

  // Don't render if no data
  if (topPumps.length === 0 && topDumps.length === 0) {
    return null;
  }

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
      {/* Top Pumps Column */}
      <div className="space-y-2">
        <h3 className="text-xs font-medium text-[var(--positive)] uppercase tracking-wider flex items-center gap-1.5">
          <span>↑</span> Top Pumps
        </h3>
        <div className="space-y-2">
          {topPumps.map((event, idx) => (
            <MoverCard
              key={event.tweet_id}
              event={event}
              founder={founder}
              rank={idx + 1}
              type="pump"
            />
          ))}
          {topPumps.length === 0 && (
            <p className="text-sm text-[var(--text-muted)] italic py-4">
              No positive moves recorded
            </p>
          )}
        </div>
      </div>

      {/* Top Dumps Column */}
      <div className="space-y-2">
        <h3 className="text-xs font-medium text-[var(--negative)] uppercase tracking-wider flex items-center gap-1.5">
          <span>↓</span> Top Dumps
        </h3>
        <div className="space-y-2">
          {topDumps.map((event, idx) => (
            <MoverCard
              key={event.tweet_id}
              event={event}
              founder={founder}
              rank={idx + 1}
              type="dump"
            />
          ))}
          {topDumps.length === 0 && (
            <p className="text-sm text-[var(--text-muted)] italic py-4">
              No negative moves recorded
            </p>
          )}
        </div>
      </div>
    </div>
  );
}
