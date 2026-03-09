'use client';

/**
 * ImpactBar Component
 * ====================
 * Visual indicator showing price change magnitude and direction.
 *
 * Design:
 * - Horizontal bar that extends left (negative) or right (positive) from center
 * - Color-coded: green for gains, red for losses
 * - Width proportional to change magnitude (capped at ±50% for visual balance)
 * - Subtle animation on mount and hover
 *
 * Usage:
 *   <ImpactBar change={12.5} />  // Shows +12.5% green bar extending right
 *   <ImpactBar change={-8.3} /> // Shows -8.3% red bar extending left
 *
 * @module components/ImpactBar
 */

// ============================================================================
// TYPES
// ============================================================================

interface ImpactBarProps {
  /** Percentage change value (can be positive or negative) */
  change: number | null;
  /** Optional: Max percentage for scaling (default: 50) */
  maxScale?: number;
  /** Optional: Show percentage text label (default: true) */
  showLabel?: boolean;
  /** Optional: Compact mode for table cells (default: false) */
  compact?: boolean;
}

// ============================================================================
// IMPACT BAR COMPONENT
// ============================================================================

/**
 * ImpactBar - Visual bar showing price change direction and magnitude
 *
 * Features:
 * - Bi-directional: extends left or right from center
 * - Color gradient based on magnitude
 * - Smooth CSS animation
 * - Accessible with aria labels
 */
export default function ImpactBar({
  change,
  maxScale = 50,
  showLabel = true,
  compact = false,
}: ImpactBarProps) {
  // Handle null/undefined change
  if (change === null || change === undefined) {
    return (
      <div className={`flex items-center gap-2 ${compact ? 'w-20' : 'w-32'}`}>
        <span className="text-[var(--text-disabled)] text-sm">—</span>
      </div>
    );
  }

  const isPositive = change >= 0;
  const absChange = Math.abs(change);

  // Calculate bar width as percentage of max scale (capped at 100%)
  const barWidthPercent = Math.min((absChange / maxScale) * 100, 100);

  // Color classes
  const barColorClass = isPositive
    ? 'bg-[var(--positive)]'
    : 'bg-[var(--negative)]';

  const textColorClass = isPositive
    ? 'text-[var(--positive)]'
    : 'text-[var(--negative)]';

  // Container width based on compact mode
  const containerWidth = compact ? 'w-24' : 'w-32';

  return (
    <div
      className={`flex items-center gap-2 ${containerWidth}`}
      role="meter"
      aria-valuenow={change}
      aria-valuemin={-maxScale}
      aria-valuemax={maxScale}
      aria-label={`Price change: ${isPositive ? '+' : ''}${change.toFixed(1)}%`}
    >
      {/* Bar Container - uses CSS grid for bidirectional layout */}
      <div className="flex-1 h-2 bg-[var(--surface-2)] rounded-full overflow-hidden relative">
        {/* Center line indicator */}
        <div className="absolute left-1/2 top-0 bottom-0 w-px bg-[var(--border-subtle)]" />

        {/* The actual bar */}
        <div
          className={`
            absolute top-0 bottom-0 rounded-full
            transition-all duration-500 ease-out
            ${barColorClass}
            ${isPositive ? 'left-1/2' : 'right-1/2'}
          `}
          style={{
            width: `${barWidthPercent / 2}%`,
            opacity: 0.8 + (barWidthPercent / 500), // Slightly more opaque for larger changes
          }}
        />
      </div>

      {/* Percentage Label */}
      {showLabel && (
        <span className={`font-mono text-xs font-medium tabular-nums ${textColorClass} min-w-[3.5rem] text-right`}>
          {isPositive ? '+' : ''}{change.toFixed(1)}%
        </span>
      )}
    </div>
  );
}

// ============================================================================
// INLINE IMPACT INDICATOR (Simpler variant for table cells)
// ============================================================================

interface InlineImpactProps {
  change: number | null;
  size?: 'sm' | 'md';
}

/**
 * InlineImpact - Compact inline indicator for table cells
 *
 * Just shows the colored percentage without the bar visualization.
 * Use this when space is limited or the bar would be too small to be meaningful.
 */
export function InlineImpact({ change, size = 'sm' }: InlineImpactProps) {
  if (change === null || change === undefined) {
    return <span className="text-[var(--text-disabled)]">—</span>;
  }

  const isPositive = change >= 0;
  const colorClass = isPositive
    ? 'text-[var(--positive)]'
    : 'text-[var(--negative)]';

  const sizeClass = size === 'sm' ? 'text-sm' : 'text-base';

  return (
    <span className={`font-mono font-medium tabular-nums ${colorClass} ${sizeClass}`}>
      {isPositive ? '+' : ''}{change.toFixed(1)}%
    </span>
  );
}
