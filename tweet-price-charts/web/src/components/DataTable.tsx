'use client';

/**
 * DataTable - Tweet Table with Price & Market Cap Context
 * =======================================================
 * Five columns: Date, Tweet (full text), Price, Market Cap, Next 24H
 *
 * Design rationale:
 * - Users come here to READ tweets, not glance at truncated snippets
 * - price_at_tweet gives context to percentages ("+48.9% @ $0.02" is meaningful)
 * - market_cap_at_tweet shows absolute scale ("$47M" is more intuitive than "$0.0047")
 * - Full tweet text avoids forcing click-through to Twitter
 *
 * Default sort: Latest first (neutral, not cherry-picked)
 * Mobile: Stacked cards with market cap + % pinned right
 */

import { useMemo, useState } from 'react';
import {
  useReactTable,
  getCoreRowModel,
  getSortedRowModel,
  getFilteredRowModel,
  flexRender,
  createColumnHelper,
  SortingState,
} from '@tanstack/react-table';
import { TweetEvent } from '@/lib/types';
import OnlyMentionsToggle from './OnlyMentionsToggle';

interface DataTableProps {
  events: TweetEvent[];
  founder: string;
  assetName: string;
  // Optional filter toggle for assets with keyword filtering
  showOnlyMentionsToggle?: boolean;
  onlyMentions?: boolean;
  onOnlyMentionsChange?: () => void;
}

const columnHelper = createColumnHelper<TweetEvent>();

/**
 * Decode HTML entities in text (&gt; &amp; &lt; etc)
 */
function decodeHtmlEntities(text: string): string {
  if (typeof document === 'undefined') return text;
  const textarea = document.createElement('textarea');
  textarea.innerHTML = text;
  return textarea.value;
}

/**
 * Format market cap in human-readable form ($47M, $1.2B, etc.)
 */
function formatMarketCap(mc: number | null | undefined): string {
  if (mc == null) return '—';
  if (mc >= 1_000_000_000) return `$${(mc / 1_000_000_000).toFixed(1)}B`;
  if (mc >= 1_000_000) return `$${(mc / 1_000_000).toFixed(1)}M`;
  if (mc >= 1_000) return `$${(mc / 1_000).toFixed(1)}K`;
  return `$${mc.toFixed(0)}`;
}

export default function DataTable({
  events,
  founder,
  assetName,
  showOnlyMentionsToggle = false,
  onlyMentions = true,
  onOnlyMentionsChange,
}: DataTableProps) {
  // Default sort: Latest first (neutral view)
  const [sorting, setSorting] = useState<SortingState>([
    { id: 'timestamp', desc: true }
  ]);
  const [globalFilter, setGlobalFilter] = useState('');

  // Five columns: Date, Tweet, Price, Market Cap, Next 24H
  const columns = useMemo(() => [
    columnHelper.accessor('timestamp', {
      header: 'DATE',
      cell: info => {
        const date = new Date(info.getValue() * 1000);
        return (
          <span className="text-[var(--text-secondary)] whitespace-nowrap tabular-nums text-sm">
            {date.toLocaleDateString('en-US', {
              month: 'short',
              day: 'numeric',
              year: 'numeric'
            })}
          </span>
        );
      },
      sortingFn: 'basic',
    }),

    columnHelper.accessor('text', {
      id: 'tweet',
      header: 'TWEET',
      cell: info => {
        const row = info.row.original;
        const decodedText = decodeHtmlEntities(info.getValue());
        return (
          <a
            href={`https://twitter.com/${founder}/status/${row.tweet_id}`}
            target="_blank"
            rel="noopener noreferrer"
            className="block group"
          >
            <p className="text-sm text-[var(--text-primary)] whitespace-pre-wrap break-words group-hover:text-[var(--accent)] transition-colors">
              {decodedText}
            </p>
          </a>
        );
      },
      enableSorting: false,
    }),

    columnHelper.accessor('price_at_tweet', {
      header: 'PRICE',
      cell: info => {
        const price = info.getValue();
        if (price === null) return <span className="text-[var(--text-disabled)]">—</span>;
        // Smart formatting: use precision for tiny prices, fixed for normal prices
        const formatted = price < 0.01 ? price.toPrecision(2) : price.toFixed(2);
        return (
          <span className="text-[var(--text-secondary)] tabular-nums text-sm whitespace-nowrap">
            ${formatted}
          </span>
        );
      },
      sortingFn: 'basic',
    }),

    columnHelper.accessor('market_cap_at_tweet', {
      header: 'MCAP',
      cell: info => {
        const mc = info.getValue();
        if (mc == null) return <span className="text-[var(--text-disabled)]">—</span>;
        return (
          <span className="text-[var(--text-secondary)] tabular-nums text-sm whitespace-nowrap">
            {formatMarketCap(mc)}
          </span>
        );
      },
      sortingFn: 'basic',
    }),

    columnHelper.accessor('change_24h_pct', {
      header: 'NEXT 24H',
      cell: info => {
        const change = info.getValue();
        if (change === null) return <span className="text-[var(--text-disabled)]">—</span>;
        const isPositive = change >= 0;
        return (
          <span className={`font-mono text-sm font-semibold tabular-nums ${isPositive ? 'text-[var(--positive)]' : 'text-[var(--negative)]'}`}>
            {isPositive ? '+' : ''}{change.toFixed(1)}%
          </span>
        );
      },
      sortingFn: 'basic',
    }),
  ], [founder]);

  const table = useReactTable({
    data: events,
    columns,
    state: { sorting, globalFilter },
    onSortingChange: setSorting,
    onGlobalFilterChange: setGlobalFilter,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
  });

  const rows = table.getRowModel().rows;

  return (
    <div className="flex flex-col">
      {/* Search & Export bar */}
      <div className="flex items-center gap-3 px-4 py-3 border-b border-[var(--border-subtle)]">
        <input
          type="text"
          placeholder="Search tweets..."
          value={globalFilter}
          onChange={e => setGlobalFilter(e.target.value)}
          className="flex-1 px-3 py-2 bg-[var(--surface-1)] border border-[var(--border-default)] rounded-lg text-[var(--text-primary)] placeholder-[var(--text-muted)] focus:outline-none focus:border-[var(--accent)] text-base"
        />
        <button
          onClick={() => exportToCSV(events, founder, assetName)}
          className="px-3 py-2.5 min-h-[44px] bg-[var(--surface-1)] border border-[var(--border-default)] text-[var(--text-secondary)] rounded-lg hover:bg-[var(--surface-2)] hover:text-[var(--text-primary)] transition-colors text-sm whitespace-nowrap"
        >
          Export CSV
        </button>
      </div>

      {/* Sort dropdown + filter toggle */}
      <div className="flex items-center gap-4 px-4 py-2 text-xs text-[var(--text-muted)]">
        <div className="flex items-center gap-2">
          <span>Sort:</span>
          <select
            value={
              sorting[0]?.id === 'timestamp'
                ? (sorting[0]?.desc ? 'latest' : 'oldest')
                : sorting[0]?.id === 'price_at_tweet'
                  ? (sorting[0]?.desc ? 'highest-price' : 'lowest-price')
                  : sorting[0]?.id === 'market_cap_at_tweet'
                    ? (sorting[0]?.desc ? 'highest-mcap' : 'lowest-mcap')
                    : (sorting[0]?.desc ? 'biggest-gain' : 'biggest-drop')
            }
            onChange={(e) => {
              const val = e.target.value;
              if (val === 'latest') setSorting([{ id: 'timestamp', desc: true }]);
              else if (val === 'oldest') setSorting([{ id: 'timestamp', desc: false }]);
              else if (val === 'biggest-gain') setSorting([{ id: 'change_24h_pct', desc: true }]);
              else if (val === 'biggest-drop') setSorting([{ id: 'change_24h_pct', desc: false }]);
              else if (val === 'highest-price') setSorting([{ id: 'price_at_tweet', desc: true }]);
              else if (val === 'lowest-price') setSorting([{ id: 'price_at_tweet', desc: false }]);
              else if (val === 'highest-mcap') setSorting([{ id: 'market_cap_at_tweet', desc: true }]);
              else if (val === 'lowest-mcap') setSorting([{ id: 'market_cap_at_tweet', desc: false }]);
            }}
            className="bg-[var(--surface-1)] border border-[var(--border-default)] rounded px-3 py-2.5 text-[var(--text-primary)] text-sm focus:outline-none focus:border-[var(--accent)] min-h-[44px]"
          >
            <option value="latest">Latest</option>
            <option value="oldest">Oldest</option>
            <option value="biggest-gain">Biggest gain</option>
            <option value="biggest-drop">Biggest drop</option>
            <option value="highest-mcap">Highest mcap</option>
            <option value="lowest-mcap">Lowest mcap</option>
          </select>
        </div>

        {/* Only mentions toggle - shown for assets with keyword filtering */}
        {showOnlyMentionsToggle && onOnlyMentionsChange && (
          <OnlyMentionsToggle
            checked={onlyMentions}
            onChange={onOnlyMentionsChange}
          />
        )}
      </div>

      {/* Desktop: Traditional table */}
      <div className="hidden md:block">
        <table className="w-full">
          <thead>
            <tr className="border-b border-[var(--border-subtle)]">
              {table.getHeaderGroups()[0].headers.map(header => (
                <th
                  key={header.id}
                  onClick={header.column.getToggleSortingHandler()}
                  className={`px-4 py-2 text-left text-xs font-medium text-[var(--text-muted)] uppercase tracking-wide ${
                    header.column.getCanSort() ? 'cursor-pointer hover:text-[var(--text-primary)]' : ''
                  }`}
                >
                  <div className="flex items-center gap-1">
                    {flexRender(header.column.columnDef.header, header.getContext())}
                    {header.column.getIsSorted() && (
                      <span>{header.column.getIsSorted() === 'asc' ? '↑' : '↓'}</span>
                    )}
                  </div>
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-[var(--border-subtle)]">
            {rows.map(row => (
              <tr key={row.id} className="hover:bg-[var(--surface-1)] transition-colors">
                {row.getVisibleCells().map(cell => (
                  <td key={cell.id} className="px-4 py-3">
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Mobile: Stacked cards with % pinned right */}
      <div className="md:hidden divide-y divide-[var(--border-subtle)]">
        {rows.map(row => {
          const event = row.original;
          const change = event.change_24h_pct;
          const isPositive = change !== null && change >= 0;
          const decodedText = decodeHtmlEntities(event.text);
          const date = new Date(event.timestamp * 1000);

          return (
            <a
              key={row.id}
              href={`https://twitter.com/${founder}/status/${event.tweet_id}`}
              target="_blank"
              rel="noopener noreferrer"
              className="flex items-start gap-3 px-4 py-3 hover:bg-[var(--surface-1)] transition-colors"
            >
              {/* Left: Tweet text + date */}
              <div className="flex-1 min-w-0">
                <p className="text-sm text-[var(--text-primary)] line-clamp-2">
                  {decodedText}
                </p>
                <p className="text-xs text-[var(--text-muted)] mt-1">
                  {date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })}
                </p>
              </div>

              {/* Right: Market cap + % change stacked */}
              <div className="flex-shrink-0 text-right">
                {/* Market cap at tweet (more intuitive than price) */}
                {event.market_cap_at_tweet != null && (
                  <p className="text-xs text-[var(--text-muted)] tabular-nums">
                    {formatMarketCap(event.market_cap_at_tweet)}
                  </p>
                )}
                {/* % change */}
                {change !== null ? (
                  <span className={`font-mono text-sm font-semibold tabular-nums ${isPositive ? 'text-[var(--positive)]' : 'text-[var(--negative)]'}`}>
                    {isPositive ? '+' : ''}{change.toFixed(1)}%
                  </span>
                ) : (
                  <span className="text-[var(--text-disabled)] text-sm">—</span>
                )}
              </div>
            </a>
          );
        })}
      </div>

      {/* Row count */}
      <div className="px-4 py-3 text-xs text-[var(--text-muted)] border-t border-[var(--border-subtle)]">
        {rows.length === 0 && globalFilter
          ? 'No tweets match your search'
          : `${rows.length} of ${events.length} tweets`}
      </div>
    </div>
  );
}

function exportToCSV(events: TweetEvent[], founder: string, assetName: string) {
  const headers = ['Date', 'Tweet', 'Price', 'Market Cap', 'Change 24h %', 'Tweet URL'];
  const rows = events.map(e => [
    new Date(e.timestamp * 1000).toISOString(),
    `"${e.text.replace(/"/g, '""')}"`,
    e.price_at_tweet?.toFixed(6) ?? '',
    e.market_cap_at_tweet?.toFixed(0) ?? '',
    e.change_24h_pct?.toFixed(2) ?? '',
    `https://twitter.com/${founder}/status/${e.tweet_id}`
  ]);

  const csv = [headers.join(','), ...rows.map(r => r.join(','))].join('\n');
  const blob = new Blob([csv], { type: 'text/csv' });
  const url = URL.createObjectURL(blob);

  const a = document.createElement('a');
  a.href = url;
  a.download = `${assetName.toLowerCase()}_tweets.csv`;
  a.click();
  URL.revokeObjectURL(url);
}
