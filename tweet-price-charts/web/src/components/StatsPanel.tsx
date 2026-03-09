'use client';

import { Stats } from '@/lib/types';

interface StatsPanelProps {
  stats: Stats;
  founderName: string;
}

export default function StatsPanel({ stats, founderName }: StatsPanelProps) {
  const { daily_comparison, correlation, current_status } = stats;
  
  // Capitalize first letter of founder name for display
  const displayName = founderName.charAt(0).toUpperCase() + founderName.slice(1);

  return (
    <div className="grid grid-cols-1 md:grid-cols-3 gap-4 p-4">
      {/* Current Status - Hero */}
      <div className="md:col-span-3 bg-gradient-to-r from-[var(--surface-2)] to-[var(--surface-1)] border border-[var(--border-subtle)] rounded-xl p-6">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-lg font-medium text-[var(--text-secondary)] mb-1">
              Current Silence
            </h2>
            <div className="flex items-baseline gap-3">
              <span className="stat-hero text-[var(--negative)]">
                {current_status.days_since_last_tweet}
              </span>
              <span className="text-xl text-[var(--text-secondary)]">days</span>
            </div>
            <p className="text-sm text-[var(--text-muted)] mt-2">
              Last tweet: {current_status.last_tweet_date}
            </p>
          </div>
          
          {current_status.price_change_during_silence !== null && (
            <div className="text-right">
              <p className="text-sm text-[var(--text-secondary)] mb-1">Price Impact</p>
              <span className={`text-4xl font-bold tabular-nums ${
                current_status.price_change_during_silence >= 0 
                  ? 'text-[var(--positive)]' 
                  : 'text-[var(--negative)]'
              }`}>
                {current_status.price_change_during_silence >= 0 ? '+' : ''}
                {current_status.price_change_during_silence.toFixed(1)}%
              </span>
            </div>
          )}
        </div>
      </div>

      {/* Tweet Days */}
      <div className="stat-card" style={{ borderLeftColor: 'var(--positive)' }}>
        <h3 className="stat-label text-[var(--positive)] mb-4 flex items-center gap-2">
          <span className="w-2 h-2 bg-[var(--positive)] rounded-full"></span>
          When {displayName} Tweets
        </h3>
        <div className="space-y-3">
          <div className="flex justify-between items-baseline">
            <span className="text-[var(--text-secondary)]">Avg Return</span>
            <span className={`text-2xl font-bold tabular-nums ${
              daily_comparison.tweet_day_avg_return >= 0 
                ? 'text-[var(--positive)]' 
                : 'text-[var(--negative)]'
            }`}>
              {daily_comparison.tweet_day_avg_return >= 0 ? '+' : ''}
              {daily_comparison.tweet_day_avg_return.toFixed(2)}%
            </span>
          </div>
          <div className="flex justify-between items-baseline">
            <span className="text-[var(--text-secondary)]">Win Rate</span>
            <span className="text-xl font-semibold text-[var(--text-primary)] tabular-nums">
              {daily_comparison.tweet_day_win_rate.toFixed(0)}%
            </span>
          </div>
          <div className="flex justify-between items-baseline">
            <span className="text-[var(--text-muted)]">Sample Size</span>
            <span className="text-[var(--text-secondary)] tabular-nums">
              {daily_comparison.tweet_day_count} days
            </span>
          </div>
        </div>
      </div>

      {/* No-Tweet Days */}
      <div className="stat-card" style={{ borderLeftColor: 'var(--negative)' }}>
        <h3 className="stat-label text-[var(--negative)] mb-4 flex items-center gap-2">
          <span className="w-2 h-2 bg-[var(--negative)] rounded-full"></span>
          When {displayName} is Silent
        </h3>
        <div className="space-y-3">
          <div className="flex justify-between items-baseline">
            <span className="text-[var(--text-secondary)]">Avg Return</span>
            <span className={`text-2xl font-bold tabular-nums ${
              daily_comparison.no_tweet_day_avg_return >= 0 
                ? 'text-[var(--positive)]' 
                : 'text-[var(--negative)]'
            }`}>
              {daily_comparison.no_tweet_day_avg_return >= 0 ? '+' : ''}
              {daily_comparison.no_tweet_day_avg_return.toFixed(2)}%
            </span>
          </div>
          <div className="flex justify-between items-baseline">
            <span className="text-[var(--text-secondary)]">Win Rate</span>
            <span className="text-xl font-semibold text-[var(--text-primary)] tabular-nums">
              {daily_comparison.no_tweet_day_win_rate.toFixed(0)}%
            </span>
          </div>
          <div className="flex justify-between items-baseline">
            <span className="text-[var(--text-muted)]">Sample Size</span>
            <span className="text-[var(--text-secondary)] tabular-nums">
              {daily_comparison.no_tweet_day_count} days
            </span>
          </div>
        </div>
      </div>

      {/* Correlation */}
      <div className="stat-card" style={{ borderLeftColor: 'var(--accent)' }}>
        <h3 className="stat-label text-[var(--accent)] mb-4 flex items-center gap-2">
          <span className="w-2 h-2 bg-[var(--accent)] rounded-full"></span>
          Statistical Correlation
        </h3>
        <div className="space-y-3">
          <div className="flex justify-between items-baseline">
            <span className="text-[var(--text-secondary)]">7d Tweet-Price</span>
            <span className="text-2xl font-bold text-[var(--text-primary)] tabular-nums">
              {correlation.correlation_7d.toFixed(2)}
            </span>
          </div>
          <div className="flex justify-between items-baseline">
            <span className="text-[var(--text-secondary)]">Significant?</span>
            <span className={`text-xl font-semibold ${
              daily_comparison.significant ? 'text-[var(--positive)]' : 'text-[var(--negative)]'
            }`}>
              {daily_comparison.significant ? 'YES' : 'NO'}
            </span>
          </div>
          <div className="flex justify-between items-baseline">
            <span className="text-[var(--text-muted)]">p-value</span>
            <span className="text-[var(--text-secondary)] font-mono tabular-nums">
              {daily_comparison.p_value?.toFixed(4) ?? 'N/A'}
            </span>
          </div>
        </div>
      </div>

      {/* Disclaimer */}
      <div className="md:col-span-3 text-center text-xs text-[var(--text-muted)] py-2">
        Price data from GeckoTerminal. Tweet data from X API. 
        This is not financial advice. DYOR.
      </div>
    </div>
  );
}
