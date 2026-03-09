'use client';

/**
 * OnlyMentionsToggle - Reusable checkbox for filtering tweets
 * ===========================================================
 * Shows only when asset has keyword filtering available.
 * Used on both Chart and Data pages for consistent behavior.
 */

interface OnlyMentionsToggleProps {
  checked: boolean;
  onChange: () => void;
  disabled?: boolean;
  className?: string;
}

export default function OnlyMentionsToggle({
  checked,
  onChange,
  disabled = false,
  className = '',
}: OnlyMentionsToggleProps) {
  return (
    <label className={`flex items-center gap-1.5 cursor-pointer select-none ${disabled ? 'opacity-50 cursor-not-allowed' : ''} ${className}`}>
      <input
        type="checkbox"
        checked={checked}
        onChange={onChange}
        disabled={disabled}
        className="w-3.5 h-3.5 rounded border-[var(--border-subtle)] bg-[var(--surface-0)] text-[var(--accent)] focus:ring-[var(--accent)] focus:ring-offset-0 cursor-pointer disabled:cursor-not-allowed"
      />
      <span className="text-xs text-[var(--text-muted)]">
        Only mentions
      </span>
    </label>
  );
}
