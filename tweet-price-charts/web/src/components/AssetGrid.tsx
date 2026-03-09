/**
 * AssetGrid Component
 * ===================
 *
 * Grid of token cards linking to individual chart pages.
 * Acts as the "Pick a token" CTA from the about page hero.
 *
 * Features:
 * - Credibility-based ordering (JUP, HYPE first; FARTCOIN last)
 * - Token logo + name + founder handle
 * - Hover glow effect using token's brand color
 * - Links to /chart?asset={id}
 *
 * Design decision: Manual ordering by credibility rather than
 * alphabetical or tweet count, so serious projects appear first.
 */
'use client';

import { useState, useEffect } from 'react';
import { Asset } from '@/lib/types';
import { loadAssets } from '@/lib/dataLoader';
import Link from 'next/link';
import Image from 'next/image';

// =============================================================================
// Constants
// =============================================================================

// Credibility-based ordering: established projects first, memes last
const ASSET_ORDER: Record<string, number> = {
  jup: 1,      // Jupiter - major Solana DEX
  hype: 2,     // Hyperliquid - HFT/perps platform
  wif: 3,      // WIF - major memecoin
  wld: 4,      // Worldcoin - Sam Altman
  zec: 5,      // Zcash - established crypto
  aster: 6,    // CZ's token
  monad: 7,    // Major L1
  zora: 8,     // NFT platform
  meta: 9,     // META DAO
  believe: 10, // Launchcoin
  xpl: 11,     // Plasma
  pump: 12,    // Pump.fun
  useless: 13, // Meme
  fartcoin: 14, // Meme
};

// =============================================================================
// Component
// =============================================================================

export default function AssetGrid() {
  const [assets, setAssets] = useState<Asset[]>([]);
  const [loading, setLoading] = useState(true);

  // ---------------------------------------------------------------------------
  // Data Loading
  // ---------------------------------------------------------------------------
  useEffect(() => {
    async function loadData() {
      try {
        const loadedAssets = await loadAssets();
        // Sort by credibility order
        const sorted = [...loadedAssets].sort((a, b) => {
          const orderA = ASSET_ORDER[a.id] ?? 99;
          const orderB = ASSET_ORDER[b.id] ?? 99;
          return orderA - orderB;
        });
        setAssets(sorted);
      } catch (e) {
        console.error('Failed to load assets:', e);
      } finally {
        setLoading(false);
      }
    }
    loadData();
  }, []);

  // ---------------------------------------------------------------------------
  // Render
  // ---------------------------------------------------------------------------
  if (loading) {
    return (
      <div className="grid grid-cols-3 md:grid-cols-5 gap-3">
        {[...Array(10)].map((_, i) => (
          <div
            key={i}
            className="aspect-square bg-[var(--surface-1)] rounded-xl animate-pulse"
          />
        ))}
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div className="text-center">
        <p className="text-[var(--text-secondary)]">
          Pick a token and explore the chart.
        </p>
      </div>

      <div className="grid grid-cols-3 md:grid-cols-5 gap-3">
        {assets.map((asset) => (
          <Link
            key={asset.id}
            href={`/chart?asset=${asset.id}`}
            className="group relative aspect-square bg-[var(--surface-1)] border border-[var(--border-subtle)] rounded-xl p-3 flex flex-col items-center justify-center gap-2 hover:border-[var(--border-default)] hover:bg-[var(--surface-2)] transition-all"
            style={{
              // Subtle glow on hover using asset color
              boxShadow: 'none',
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.boxShadow = `0 0 20px ${asset.color}20`;
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.boxShadow = 'none';
            }}
          >
            {/* Logo or color dot */}
            {asset.logo ? (
              <div className="w-10 h-10 relative">
                <Image
                  src={asset.logo}
                  alt={asset.name}
                  fill
                  className="object-contain rounded-full"
                />
              </div>
            ) : (
              <div
                className="w-10 h-10 rounded-full"
                style={{ backgroundColor: asset.color }}
              />
            )}

            {/* Name */}
            <span className="text-sm font-medium text-[var(--text-primary)] text-center">
              {asset.name}
            </span>

            {/* Founder name (smaller) */}
            <span className="text-xs text-[var(--text-muted)] text-center truncate w-full">
              @{asset.founder}
            </span>

            {/* Arrow indicator on hover */}
            <div className="absolute top-2 right-2 opacity-0 group-hover:opacity-100 transition-opacity">
              <span className="text-xs text-[var(--text-muted)]">→</span>
            </div>
          </Link>
        ))}
      </div>
    </div>
  );
}
