/**
 * ImpactExplorer Component
 * ========================
 *
 * Interactive scatter plot showing founder tweets vs price changes.
 * Part of the "Tool contract" - helps users see if founder tweets
 * historically coincide with meaningful price moves.
 *
 * Features:
 * - "Biggest moves" filter (15% threshold) to show only outliers
 * - Token logos with colored glow (green=up, red=down)
 * - Glow intensity scales with move magnitude
 * - Hover tooltip matching Chart.tsx style
 * - Filter by token, time range, and impact window (1h/24h)
 *
 * Design decisions:
 * - Default to "Biggest moves" ON to reduce noise
 * - 15% threshold because crypto is volatile (5% was too low)
 * - Logos visible with colored ring, not heavy color overlay
 */
'use client';

import { useState, useEffect, useMemo, useCallback } from 'react';
import { TweetEvent, Asset } from '@/lib/types';
import { loadAssets, loadTweetEvents } from '@/lib/dataLoader';

// =============================================================================
// Types
// =============================================================================

interface DataPoint {
  asset: Asset;
  event: TweetEvent;
  x: number; // normalized 0-1
  y: number; // normalized 0-1
}

interface TooltipData {
  point: DataPoint;
  screenX: number;
  screenY: number;
}

type TimeRange = 'all' | '30d' | '90d' | '1y';
type ImpactMetric = '1h' | '24h';

// =============================================================================
// Constants
// =============================================================================

const TIME_RANGES: { value: TimeRange; label: string }[] = [
  { value: '30d', label: '30 days' },
  { value: '90d', label: '90 days' },
  { value: '1y', label: '1 year' },
  { value: 'all', label: 'All time' },
];

const IMPACT_METRICS: { value: ImpactMetric; label: string }[] = [
  { value: '1h', label: 'After 1h' },
  { value: '24h', label: 'After 24h' },
];

const BIGGEST_MOVES_THRESHOLD = 15; // % threshold for "biggest moves" filter (crypto is volatile, need higher bar)

// Human-readable date formatter
const formatDate = (timestamp: number): string => {
  const date = new Date(timestamp * 1000);
  return date.toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  });
};

// =============================================================================
// Component
// =============================================================================

export default function ImpactExplorer() {
  // ---------------------------------------------------------------------------
  // State
  // ---------------------------------------------------------------------------
  const [assets, setAssets] = useState<Asset[]>([]);
  const [allData, setAllData] = useState<DataPoint[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Filters
  const [selectedAsset, setSelectedAsset] = useState<string>('all');
  const [timeRange, setTimeRange] = useState<TimeRange>('all');
  const [impactMetric, setImpactMetric] = useState<ImpactMetric>('1h');
  const [biggestMovesOnly, setBiggestMovesOnly] = useState<boolean>(true); // Default ON

  // Interaction
  const [tooltip, setTooltip] = useState<TooltipData | null>(null);
  const [hoveredPoint, setHoveredPoint] = useState<DataPoint | null>(null);

  // ---------------------------------------------------------------------------
  // Data Loading
  // ---------------------------------------------------------------------------
  useEffect(() => {
    async function loadData() {
      try {
        setLoading(true);
        setError(null);

        const loadedAssets = await loadAssets();
        setAssets(loadedAssets);

        // Load tweet events for all assets in parallel
        const allPoints: DataPoint[] = [];

        await Promise.all(
          loadedAssets.map(async (asset) => {
            try {
              const tweetsData = await loadTweetEvents(asset.id);

              tweetsData.events.forEach((event) => {
                // Only include events with valid price impact data
                if (event.change_1h_pct !== null && event.change_24h_pct !== null) {
                  allPoints.push({
                    asset,
                    event,
                    x: 0, // Will be normalized later
                    y: 0, // Will be normalized later
                  });
                }
              });
            } catch (e) {
              console.warn(`Failed to load tweets for ${asset.id}:`, e);
            }
          })
        );

        setAllData(allPoints);
      } catch (e) {
        setError(e instanceof Error ? e.message : 'Failed to load data');
      } finally {
        setLoading(false);
      }
    }

    loadData();
  }, []);

  // ---------------------------------------------------------------------------
  // Filtered & Normalized Data
  // ---------------------------------------------------------------------------
  const { filteredData, stats, xDomain, yDomain } = useMemo(() => {
    if (allData.length === 0) {
      return {
        filteredData: [],
        stats: { total: 0, positive: 0, avgImpact: 0 },
        xDomain: [0, 1] as [number, number],
        yDomain: [-50, 50] as [number, number],
      };
    }

    const now = Date.now() / 1000;
    const timeFilters: Record<TimeRange, number> = {
      '30d': now - 30 * 24 * 60 * 60,
      '90d': now - 90 * 24 * 60 * 60,
      '1y': now - 365 * 24 * 60 * 60,
      'all': 0,
    };

    // Get the impact value based on selected metric
    const getImpact = (point: DataPoint) =>
      impactMetric === '1h' ? point.event.change_1h_pct! : point.event.change_24h_pct!;

    // Filter by asset, time, and biggest moves
    let filtered = allData.filter((point) => {
      if (selectedAsset !== 'all' && point.asset.id !== selectedAsset) return false;
      if (point.event.timestamp < timeFilters[timeRange]) return false;
      if (biggestMovesOnly) {
        const impact = getImpact(point);
        if (Math.abs(impact) < BIGGEST_MOVES_THRESHOLD) return false;
      }
      return true;
    });

    if (filtered.length === 0) {
      return {
        filteredData: [],
        stats: { total: 0, positive: 0, avgImpact: 0 },
        xDomain: [0, 1] as [number, number],
        yDomain: [-50, 50] as [number, number],
      };
    }

    // Calculate domains
    const timestamps = filtered.map(p => p.event.timestamp);
    const impacts = filtered.map(getImpact);

    const minTime = Math.min(...timestamps);
    const maxTime = Math.max(...timestamps);

    // Clamp impact values for visualization (outliers still shown at edges)
    const impactRange = 100; // -100% to +100%
    const minImpact = -impactRange;
    const maxImpact = impactRange;

    // Normalize coordinates
    const timeSpan = maxTime - minTime || 1;

    filtered = filtered.map(point => ({
      ...point,
      x: (point.event.timestamp - minTime) / timeSpan,
      y: Math.max(0, Math.min(1, (getImpact(point) - minImpact) / (maxImpact - minImpact))),
    }));

    // Calculate stats
    const positiveCount = impacts.filter(i => i > 0).length;
    const avgImpact = impacts.reduce((a, b) => a + b, 0) / impacts.length;

    return {
      filteredData: filtered,
      stats: {
        total: filtered.length,
        positive: positiveCount,
        avgImpact,
      },
      xDomain: [minTime, maxTime] as [number, number],
      yDomain: [minImpact, maxImpact] as [number, number],
    };
  }, [allData, selectedAsset, timeRange, impactMetric, biggestMovesOnly]);

  // ---------------------------------------------------------------------------
  // Event Handlers
  // ---------------------------------------------------------------------------
  const handlePointHover = useCallback((point: DataPoint | null, e?: React.MouseEvent) => {
    setHoveredPoint(point);
    if (point && e) {
      setTooltip({
        point,
        screenX: e.clientX,
        screenY: e.clientY,
      });
    } else {
      setTooltip(null);
    }
  }, []);

  const handlePointClick = useCallback((point: DataPoint) => {
    // Open tweet in new tab
    window.open(`https://x.com/i/status/${point.event.tweet_id}`, '_blank');
  }, []);

  // ---------------------------------------------------------------------------
  // Render
  // ---------------------------------------------------------------------------
  if (loading) {
    return (
      <div className="bg-[var(--surface-1)] border border-[var(--border-subtle)] rounded-xl p-6">
        <div className="animate-pulse space-y-4">
          <div className="h-6 bg-[var(--surface-2)] rounded w-1/3"></div>
          <div className="h-[300px] bg-[var(--surface-2)] rounded"></div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-[var(--surface-1)] border border-[var(--border-subtle)] rounded-xl p-6">
        <p className="text-[var(--negative)]">Error: {error}</p>
      </div>
    );
  }

  return (
    <div className="bg-[var(--surface-1)] border border-[var(--border-subtle)] rounded-xl overflow-hidden">
      {/* Header */}
      <div className="p-4 border-b border-[var(--border-subtle)]">
        <h3 className="text-lg font-semibold text-[var(--text-primary)] mb-1">
          What price did next
        </h3>
        <p className="text-sm text-[var(--text-secondary)]">
          Every dot is a tweet. Green = price went up after. Red = price went down.
        </p>
      </div>

      {/* Filters */}
      <div className="p-4 border-b border-[var(--border-subtle)] flex flex-wrap gap-3">
        {/* Biggest Moves Toggle */}
        <button
          onClick={() => setBiggestMovesOnly(!biggestMovesOnly)}
          className={`px-3 py-1.5 text-sm font-medium rounded-lg transition-colors ${
            biggestMovesOnly
              ? 'bg-[var(--accent)] text-white'
              : 'bg-[var(--surface-2)] text-[var(--text-secondary)] border border-[var(--border-subtle)] hover:text-[var(--text-primary)]'
          }`}
        >
          {biggestMovesOnly ? '✓ Biggest moves' : 'Biggest moves'}
        </button>

        {/* Token Filter */}
        <select
          value={selectedAsset}
          onChange={(e) => setSelectedAsset(e.target.value)}
          className="px-3 py-1.5 text-sm bg-[var(--surface-2)] border border-[var(--border-subtle)] rounded-lg text-[var(--text-primary)] focus:outline-none focus:border-[var(--accent)]"
        >
          <option value="all">All tokens ({allData.length} tweets)</option>
          {assets.map((asset) => {
            const count = allData.filter(p => p.asset.id === asset.id).length;
            return (
              <option key={asset.id} value={asset.id}>
                {asset.name} ({count})
              </option>
            );
          })}
        </select>

        {/* Time Range Filter */}
        <div className="flex rounded-lg overflow-hidden border border-[var(--border-subtle)]">
          {TIME_RANGES.map((range) => (
            <button
              key={range.value}
              onClick={() => setTimeRange(range.value)}
              className={`px-3 py-1.5 text-sm transition-colors ${
                timeRange === range.value
                  ? 'bg-[var(--surface-3)] text-[var(--text-primary)]'
                  : 'bg-[var(--surface-2)] text-[var(--text-secondary)] hover:text-[var(--text-primary)]'
              }`}
            >
              {range.label}
            </button>
          ))}
        </div>

        {/* Price Change Metric Toggle */}
        <div className="flex rounded-lg overflow-hidden border border-[var(--border-subtle)]">
          {IMPACT_METRICS.map((metric) => (
            <button
              key={metric.value}
              onClick={() => setImpactMetric(metric.value)}
              className={`px-3 py-1.5 text-sm transition-colors ${
                impactMetric === metric.value
                  ? 'bg-[var(--surface-3)] text-[var(--text-primary)]'
                  : 'bg-[var(--surface-2)] text-[var(--text-secondary)] hover:text-[var(--text-primary)]'
              }`}
            >
              {metric.label}
            </button>
          ))}
        </div>
      </div>

      {/* Chart */}
      <div className="relative p-4">
        {/* Y-axis labels */}
        <div className="absolute left-0 top-4 bottom-4 w-12 flex flex-col justify-between text-xs text-[var(--text-muted)] pr-2">
          <span className="text-[var(--positive)]">+100%</span>
          <span>0%</span>
          <span className="text-[var(--negative)]">-100%</span>
        </div>

        {/* Chart area */}
        <div className="ml-12">
          <svg
            viewBox="0 0 800 400"
            className="w-full h-[300px] md:h-[400px]"
            preserveAspectRatio="xMidYMid meet"
          >
            {/* SVG Filters for green/red tinting */}
            <defs>
              <filter id="greenTint" colorInterpolationFilters="sRGB">
                <feColorMatrix
                  type="matrix"
                  values="0.2 0 0 0 0.13
                          0.2 0 0 0 0.77
                          0.2 0 0 0 0.39
                          0   0 0 1 0"
                />
              </filter>
              <filter id="redTint" colorInterpolationFilters="sRGB">
                <feColorMatrix
                  type="matrix"
                  values="0.2 0 0 0 0.94
                          0.2 0 0 0 0.27
                          0.2 0 0 0 0.27
                          0   0 0 1 0"
                />
              </filter>
              {/* Clip path for circular logos */}
              <clipPath id="circleClip">
                <circle cx="10" cy="10" r="10" />
              </clipPath>
            </defs>

            {/* Background */}
            <rect x="0" y="0" width="800" height="400" fill="var(--surface-0)" rx="8" />

            {/* Grid lines */}
            <line x1="0" y1="200" x2="800" y2="200" stroke="var(--border-default)" strokeDasharray="4 4" />
            <line x1="0" y1="100" x2="800" y2="100" stroke="var(--border-subtle)" strokeDasharray="2 2" />
            <line x1="0" y1="300" x2="800" y2="300" stroke="var(--border-subtle)" strokeDasharray="2 2" />

            {/* Zero line label */}
            <text x="4" y="204" fill="var(--text-muted)" fontSize="10">0%</text>

            {/* Data points - token logos with colored glow */}
            {filteredData.map((point, i) => {
              const cx = point.x * 780 + 10;
              const cy = (1 - point.y) * 380 + 10; // Invert Y (higher = top)
              const impact = impactMetric === '1h' ? point.event.change_1h_pct! : point.event.change_24h_pct!;
              const isPositive = impact >= 0;
              const isHovered = hoveredPoint === point;

              // Glow intensity based on outlier magnitude
              const absImpact = Math.abs(impact);
              const glowIntensity = Math.min(0.3 + (absImpact / 100) * 0.5, 0.8); // 0.3-0.8 range

              const size = isHovered ? 28 : 20;
              const glowSize = isHovered ? size + 12 : size + 6;
              const logoPath = point.asset.logo || `/logos/${point.asset.id}.png`;
              const color = isPositive ? '#22C55E' : '#EF4444';

              return (
                <g
                  key={`${point.asset.id}-${point.event.tweet_id}-${i}`}
                  className="cursor-pointer"
                  style={{ transition: 'transform 0.15s ease-out' }}
                  onMouseEnter={(e) => handlePointHover(point, e)}
                  onMouseLeave={() => handlePointHover(null)}
                  onClick={() => handlePointClick(point)}
                >
                  {/* Outer glow - color indicates direction, size indicates magnitude */}
                  <circle
                    cx={cx}
                    cy={cy}
                    r={glowSize / 2}
                    fill={color}
                    opacity={isHovered ? 0.6 : glowIntensity}
                    style={{ filter: isHovered ? 'blur(4px)' : 'blur(3px)' }}
                  />
                  {/* Inner colored ring */}
                  <circle
                    cx={cx}
                    cy={cy}
                    r={size / 2 + 2}
                    fill="none"
                    stroke={color}
                    strokeWidth={isHovered ? 3 : 2}
                    opacity={isHovered ? 1 : 0.8}
                  />
                  {/* White background for logo */}
                  <circle
                    cx={cx}
                    cy={cy}
                    r={size / 2}
                    fill="#1a1a2e"
                  />
                  {/* Token logo - clearly visible */}
                  <image
                    href={logoPath}
                    x={cx - size / 2 + 2}
                    y={cy - size / 2 + 2}
                    width={size - 4}
                    height={size - 4}
                    style={{
                      clipPath: `circle(${(size - 4) / 2}px at ${(size - 4) / 2}px ${(size - 4) / 2}px)`,
                    }}
                    preserveAspectRatio="xMidYMid slice"
                  />
                </g>
              );
            })}

            {/* Empty state */}
            {filteredData.length === 0 && (
              <text x="400" y="200" textAnchor="middle" fill="var(--text-muted)" fontSize="14">
                No data for selected filters
              </text>
            )}
          </svg>

          {/* X-axis labels */}
          <div className="flex justify-between text-xs text-[var(--text-muted)] mt-2 px-2">
            <span>{xDomain[0] ? formatDate(xDomain[0]) : ''}</span>
            <span>{xDomain[1] ? formatDate(xDomain[1]) : ''}</span>
          </div>
        </div>
      </div>

      {/* Simple footer - just tweet count */}
      <div className="p-3 border-t border-[var(--border-subtle)] bg-[var(--surface-0)]">
        <div className="text-center text-sm text-[var(--text-muted)]">
          {stats.total.toLocaleString()} tweets
        </div>
      </div>

      {/* Tooltip - matches Chart.tsx tweet preview style */}
      {tooltip && (
        <div
          className="fixed z-50 pointer-events-none tooltip-enter"
          style={{
            left: tooltip.screenX + 20,
            top: Math.max(tooltip.screenY - 60, 10),
          }}
        >
          <div className="bg-[var(--surface-1)] border border-[var(--border-subtle)] rounded-lg p-3 shadow-xl max-w-xs">
            {/* Header: Avatar + Founder + Date */}
            <div className="flex items-start gap-2 mb-2">
              <img
                src={`/avatars/${tooltip.point.asset.founder}.png`}
                alt={tooltip.point.asset.founder}
                className="w-8 h-8 rounded-full bg-[var(--surface-2)]"
              />
              <div>
                <div className="text-[var(--text-primary)] font-medium text-sm">
                  @{tooltip.point.asset.founder}
                </div>
                <div className="text-[var(--text-muted)] text-xs">
                  {new Date(tooltip.point.event.timestamp * 1000).toLocaleDateString('en-US', {
                    month: 'short',
                    day: 'numeric',
                    hour: 'numeric',
                    minute: '2-digit',
                  })}
                </div>
              </div>
            </div>

            {/* Tweet text */}
            <p className="text-sm text-[var(--text-primary)] line-clamp-3 mb-2">
              {tooltip.point.event.text}
            </p>

            {/* Engagement stats */}
            <div className="flex items-center gap-4 text-xs text-[var(--text-muted)] mb-2">
              <span>❤️ {(tooltip.point.event.likes || 0).toLocaleString()}</span>
              <span>🔁 {(tooltip.point.event.retweets || 0).toLocaleString()}</span>
            </div>

            {/* Price change stats */}
            <div className="pt-2 border-t border-[var(--border-subtle)] flex items-center gap-3 text-xs">
              <span className="text-[var(--text-muted)]">After tweet:</span>
              <span className={tooltip.point.event.change_1h_pct! >= 0 ? 'text-[var(--positive)]' : 'text-[var(--negative)]'}>
                1h: {tooltip.point.event.change_1h_pct! >= 0 ? '+' : ''}{tooltip.point.event.change_1h_pct!.toFixed(1)}%
              </span>
              <span className={tooltip.point.event.change_24h_pct! >= 0 ? 'text-[var(--positive)]' : 'text-[var(--negative)]'}>
                24h: {tooltip.point.event.change_24h_pct! >= 0 ? '+' : ''}{tooltip.point.event.change_24h_pct!.toFixed(1)}%
              </span>
            </div>

            {/* Click hint */}
            <div className="text-xs text-[var(--text-muted)] mt-2 pt-2 border-t border-[var(--border-subtle)]">
              Click to open tweet →
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
