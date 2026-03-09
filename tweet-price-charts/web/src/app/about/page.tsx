import Link from 'next/link';
import ImpactExplorer from '@/components/ImpactExplorer';
import TweetTimeHeatmap from '@/components/TweetTimeHeatmap';
import SilencesExplorer from '@/components/SilencesExplorer';
import AssetGrid from '@/components/AssetGrid';

/**
 * About Page - Tool Contract v0
 * =============================
 *
 * Landing page implementing the "Tool contract":
 * "Use this to see whether a token founder's tweets historically
 * coincide with meaningful short-term price moves."
 *
 * Page structure (intentional hierarchy):
 * 1. Hero - The claim + CTAs ("Show biggest moves", "Pick a token")
 * 2. Orientation strip - Quick context (most dots near 0%, not causation)
 * 3. ImpactExplorer - Main visualization (scatter plot with token logos)
 * 4. AssetGrid - Token picker linking to individual charts
 * 5. Context (optional) - Supporting patterns (tweet times, silences)
 *
 * Design decisions:
 * - Claim upfront: "Most founder tweets do nothing. Some coincide with big moves."
 * - "Biggest moves" filter ON by default to show outliers, not noise
 * - Token logos visible in scatter plot for quick identification
 * - Context modules demoted to optional section (useful but not core)
 *
 * @see /exploration/current-core-problem.md for design rationale
 */
export default function AboutPage() {
  return (
    <div className="min-h-screen bg-[var(--surface-0)] flex flex-col">
      {/* Header */}
      <header className="h-14 md:h-11 border-b border-[var(--border-subtle)] bg-[var(--surface-1)] flex items-center px-4 gap-3">
        <Link
          href="/chart"
          className="px-3 py-1.5 text-xs font-medium text-[var(--text-secondary)] hover:text-[var(--text-primary)] rounded-md transition-colors"
        >
          ← Chart
        </Link>
        <div className="flex-1" />
        <span className="text-xs text-[var(--text-muted)]">tweetcharts.xyz</span>
      </header>

      {/* Main content */}
      <main className="flex-1 max-w-4xl mx-auto px-4 py-8 md:py-12 w-full">
        {/* Hero */}
        <section className="mb-6">
          <h1 className="text-3xl md:text-4xl font-bold text-[var(--text-primary)] mb-3 tracking-tight">
            Most founder tweets do nothing.<br />
            Some coincide with big moves.
          </h1>
          <p className="text-base text-[var(--text-secondary)] mb-5">
            4,537 tweets from 13 token founders. Each dot is a tweet.<br className="hidden md:block" />
            Y-axis = price change after 1h or 24h.
          </p>
          <div className="flex flex-wrap gap-3">
            <a
              href="#chart"
              className="px-5 py-2.5 bg-[var(--accent)] text-white font-medium rounded-lg hover:opacity-90 transition-opacity"
            >
              Show biggest moves
            </a>
            <a
              href="#tokens"
              className="px-5 py-2.5 bg-[var(--surface-2)] text-[var(--text-primary)] font-medium rounded-lg border border-[var(--border-subtle)] hover:bg-[var(--surface-3)] transition-colors"
            >
              Pick a token
            </a>
          </div>
        </section>

        {/* Orientation strip */}
        <section className="mb-10 p-4 bg-[var(--surface-1)] border border-[var(--border-subtle)] rounded-lg">
          <div className="flex flex-col md:flex-row md:items-center gap-2 md:gap-6 text-sm">
            <span className="text-[var(--text-secondary)]">
              <span className="text-[var(--text-muted)]">→</span> Most dots cluster near 0%.
            </span>
            <span className="text-[var(--text-secondary)]">
              <span className="text-[var(--text-muted)]">→</span> Use "Biggest moves" to see rare outliers.
            </span>
            <span className="text-[var(--text-muted)] italic">
              Not causation. Just what happened next.
            </span>
          </div>
        </section>

        {/* Main Chart */}
        <section id="chart" className="mb-10 scroll-mt-20">
          <ImpactExplorer />
        </section>

        {/* Token Grid */}
        <section id="tokens" className="mb-10 scroll-mt-20">
          <AssetGrid />
        </section>

        {/* Context Section (optional patterns) */}
        <section className="mb-10">
          <div className="mb-4">
            <h2 className="text-lg font-semibold text-[var(--text-primary)] mb-1">
              Context (optional)
            </h2>
            <p className="text-sm text-[var(--text-secondary)]">
              These patterns help interpret founder behavior. The main question is still: what did price do next?
            </p>
          </div>
          <div className="space-y-6">
            <TweetTimeHeatmap />
            <SilencesExplorer />
          </div>
        </section>

        {/* Minimal data note */}
        <section className="mb-8">
          <details className="group">
            <summary className="text-sm text-[var(--text-muted)] cursor-pointer hover:text-[var(--text-secondary)] transition-colors">
              Data sources & methodology
            </summary>
            <div className="mt-3 text-sm text-[var(--text-secondary)] space-y-2 pl-4 border-l border-[var(--border-subtle)]">
              <p>
                <strong className="text-[var(--text-primary)]">Tweets:</strong> X API + Nitter historical backfill
              </p>
              <p>
                <strong className="text-[var(--text-primary)]">Prices:</strong> GeckoTerminal, Birdeye, CoinGecko, Hyperliquid
              </p>
              <p>
                <strong className="text-[var(--text-primary)]">Updates:</strong> Hourly via GitHub Actions
              </p>
              <p className="text-[var(--text-muted)] italic">
                Correlation ≠ causation. This is a visualization tool, not financial advice.
              </p>
            </div>
          </details>
        </section>
      </main>

      {/* Footer */}
      <footer className="py-4 text-center text-xs text-[var(--text-muted)] border-t border-[var(--border-subtle)]">
        <a
          href="https://github.com/anthropics/tweet-price"
          className="hover:text-[var(--text-secondary)] transition-colors"
          target="_blank"
          rel="noopener noreferrer"
        >
          View source on GitHub
        </a>
      </footer>
    </div>
  );
}
