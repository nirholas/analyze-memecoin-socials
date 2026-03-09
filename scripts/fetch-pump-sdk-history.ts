#!/usr/bin/env npx tsx
/**
 * Fetch Historical Price Data for pump-sdk (or any Solana token)
 *
 * Usage:
 *   npx tsx scripts/fetch-pump-sdk-history.ts
 *   npx tsx scripts/fetch-pump-sdk-history.ts --mint <MINT_ADDRESS> --timeframe hour --days 30
 *
 * Data sources (cascading fallback):
 *   1. DexScreener → find pool address
 *   2. GeckoTerminal → OHLCV candles for pool
 *   3. Birdeye (if BIRDEYE_API_KEY set) → token OHLCV
 *
 * Outputs: JSON + CSV files to ./data/
 */

const DEFAULTS = {
  mint: "F4ZHCzizwpop2XS8auuAGQ7LZcUTEwQ22m5sRmB2pump", // pump-sdk
  network: "solana",
  timeframe: "day" as "day" | "hour" | "minute",
  aggregate: 1,
  days: 365, // how far back
};

// ─── CLI arg parsing ─────────────────────────────────────────────────────────
const args = process.argv.slice(2);
function arg(name: string, fallback: string): string {
  const idx = args.indexOf(`--${name}`);
  return idx !== -1 && args[idx + 1] ? args[idx + 1] : fallback;
}

const MINT = arg("mint", DEFAULTS.mint);
const NETWORK = arg("network", DEFAULTS.network);
const TIMEFRAME = arg("timeframe", DEFAULTS.timeframe) as "day" | "hour" | "minute";
const AGGREGATE = parseInt(arg("aggregate", String(DEFAULTS.aggregate)), 10);
const DAYS = parseInt(arg("days", String(DEFAULTS.days)), 10);
const BIRDEYE_KEY = process.env.BIRDEYE_API_KEY || "";

// ─── Types ───────────────────────────────────────────────────────────────────
interface Candle {
  timestamp: string; // ISO
  unix: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

interface PoolInfo {
  address: string;
  dex: string;
  baseName: string;
  baseSymbol: string;
  quoteName: string;
  quoteSymbol: string;
  priceUsd: string;
  liquidity: number;
  volume24h: number;
  fdv: number;
  pairUrl: string;
}

// ─── Fetch wrapper ───────────────────────────────────────────────────────────
async function fetchJSON<T>(url: string, headers?: Record<string, string>): Promise<T> {
  const res = await fetch(url, {
    headers: { Accept: "application/json", ...headers },
    signal: AbortSignal.timeout(15000),
  });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText} — ${url}`);
  return res.json() as Promise<T>;
}

// ─── Step 1: Find pool via DexScreener ───────────────────────────────────────
async function findPool(mint: string): Promise<PoolInfo | null> {
  console.log(`\n🔍 Finding pool for ${mint} on DexScreener...`);
  try {
    const data = await fetchJSON<{ pairs?: Array<Record<string, any>> }>(
      `https://api.dexscreener.com/latest/dex/tokens/${mint}`
    );
    if (!data.pairs || data.pairs.length === 0) {
      console.log("   No pairs found on DexScreener");
      return null;
    }

    // Sort by liquidity descending, prefer Solana pairs
    const solanaPairs = data.pairs.filter(
      (p: any) => p.chainId === "solana"
    );
    const sorted = (solanaPairs.length > 0 ? solanaPairs : data.pairs).sort(
      (a: any, b: any) => (b.liquidity?.usd || 0) - (a.liquidity?.usd || 0)
    );
    const best = sorted[0];

    const pool: PoolInfo = {
      address: best.pairAddress,
      dex: best.dexId,
      baseName: best.baseToken?.name || "",
      baseSymbol: best.baseToken?.symbol || "",
      quoteName: best.quoteToken?.name || "",
      quoteSymbol: best.quoteToken?.symbol || "",
      priceUsd: best.priceUsd || "0",
      liquidity: best.liquidity?.usd || 0,
      volume24h: best.volume?.h24 || 0,
      fdv: best.fdv || 0,
      pairUrl: best.url || "",
    };

    console.log(`   ✅ Found: ${pool.baseSymbol}/${pool.quoteSymbol} on ${pool.dex}`);
    console.log(`   Pool: ${pool.address}`);
    console.log(`   Price: $${pool.priceUsd}  |  Liquidity: $${pool.liquidity.toLocaleString()}  |  24h Vol: $${pool.volume24h.toLocaleString()}`);

    return pool;
  } catch (err) {
    console.error("   DexScreener error:", (err as Error).message);
    return null;
  }
}

// ─── Step 2a: GeckoTerminal OHLCV ───────────────────────────────────────────
async function fetchGeckoTerminalOHLCV(
  poolAddress: string,
  network: string,
  timeframe: "day" | "hour" | "minute",
  aggregate: number,
  maxCandles: number,
): Promise<Candle[]> {
  console.log(`\n📊 Fetching OHLCV from GeckoTerminal (${timeframe}, aggregate ${aggregate})...`);

  const allCandles: Candle[] = [];
  const limit = 1000; // GT max per request
  const batchSize = Math.min(limit, maxCandles);
  let remaining = maxCandles;
  let beforeTimestamp: number | null = null;

  // GeckoTerminal uses network slug "solana" directly
  const netSlug = network === "solana" ? "solana" : network;

  while (remaining > 0) {
    const fetchLimit = Math.min(batchSize, remaining);
    let url = `https://api.geckoterminal.com/api/v2/networks/${netSlug}/pools/${poolAddress}/ohlcv/${timeframe}?aggregate=${aggregate}&limit=${fetchLimit}&currency=usd`;
    if (beforeTimestamp) url += `&before_timestamp=${beforeTimestamp}`;

    try {
      const resp = await fetchJSON<{
        data: {
          id: string;
          attributes: {
            ohlcv_list: [number, number, number, number, number, number][];
          };
        };
      }>(url, { Accept: "application/json;version=20230302" });

      const ohlcvList = resp.data?.attributes?.ohlcv_list;
      if (!ohlcvList || ohlcvList.length === 0) break;

      for (const [ts, o, h, l, c, v] of ohlcvList) {
        allCandles.push({
          timestamp: new Date(ts * 1000).toISOString(),
          unix: ts,
          open: o,
          high: h,
          low: l,
          close: c,
          volume: v,
        });
      }

      console.log(`   Batch: ${ohlcvList.length} candles (total: ${allCandles.length})`);

      // GeckoTerminal returns newest first; get the oldest timestamp for pagination
      beforeTimestamp = ohlcvList[ohlcvList.length - 1][0];
      remaining -= ohlcvList.length;

      if (ohlcvList.length < fetchLimit) break; // no more data

      // Rate limit: ~30 req/min
      await new Promise((r) => setTimeout(r, 2100));
    } catch (err) {
      console.error("   GeckoTerminal batch error:", (err as Error).message);
      break;
    }
  }

  // Sort chronologically
  allCandles.sort((a, b) => a.unix - b.unix);
  return allCandles;
}

// ─── Step 2b: Birdeye OHLCV (fallback) ──────────────────────────────────────
async function fetchBirdeyeOHLCV(
  mint: string,
  timeframe: "day" | "hour" | "minute",
  days: number,
): Promise<Candle[]> {
  if (!BIRDEYE_KEY) {
    console.log("\n⚠️  Birdeye: BIRDEYE_API_KEY not set, skipping fallback");
    return [];
  }
  console.log(`\n📊 Fetching OHLCV from Birdeye (${timeframe})...`);

  const tfMap: Record<string, string> = {
    minute: "1m",
    hour: "1H",
    day: "1D",
  };
  const now = Math.floor(Date.now() / 1000);
  const from = now - days * 86400;

  try {
    const data = await fetchJSON<{
      data: { items: Array<{ unixTime: number; o: number; h: number; l: number; c: number; v: number }> };
    }>(
      `https://public-api.birdeye.so/defi/ohlcv?address=${mint}&type=${tfMap[timeframe]}&time_from=${from}&time_to=${now}`,
      {
        "X-API-KEY": BIRDEYE_KEY,
        "x-chain": "solana",
      },
    );

    const candles: Candle[] = (data.data?.items || []).map((i) => ({
      timestamp: new Date(i.unixTime * 1000).toISOString(),
      unix: i.unixTime,
      open: i.o,
      high: i.h,
      low: i.l,
      close: i.c,
      volume: i.v,
    }));

    console.log(`   ✅ ${candles.length} candles from Birdeye`);
    return candles;
  } catch (err) {
    console.error("   Birdeye error:", (err as Error).message);
    return [];
  }
}

// ─── Step 3: DexScreener pairs data (current snapshot + price changes) ──────
async function fetchDexScreenerPairHistory(poolAddress: string): Promise<Record<string, any> | null> {
  try {
    const data = await fetchJSON<{ pairs?: Array<Record<string, any>> }>(
      `https://api.dexscreener.com/latest/dex/pairs/solana/${poolAddress}`
    );
    return data.pairs?.[0] || null;
  } catch {
    return null;
  }
}

// ─── Export helpers ──────────────────────────────────────────────────────────
import { mkdirSync, writeFileSync } from "fs";
import { join } from "path";

function saveJSON(filename: string, data: unknown) {
  const dir = join(process.cwd(), "data");
  mkdirSync(dir, { recursive: true });
  const path = join(dir, filename);
  writeFileSync(path, JSON.stringify(data, null, 2));
  console.log(`   📁 ${path}`);
}

function saveCSV(filename: string, candles: Candle[]) {
  const dir = join(process.cwd(), "data");
  mkdirSync(dir, { recursive: true });
  const header = "timestamp,unix,open,high,low,close,volume";
  const rows = candles.map(
    (c) => `${c.timestamp},${c.unix},${c.open},${c.high},${c.low},${c.close},${c.volume}`
  );
  const path = join(dir, filename);
  writeFileSync(path, [header, ...rows].join("\n"));
  console.log(`   📁 ${path}`);
}

// ─── Main ────────────────────────────────────────────────────────────────────
async function main() {
  console.log("═══════════════════════════════════════════════════");
  console.log("  Historical Price Data Fetcher");
  console.log("═══════════════════════════════════════════════════");
  console.log(`  Mint:      ${MINT}`);
  console.log(`  Network:   ${NETWORK}`);
  console.log(`  Timeframe: ${TIMEFRAME} (aggregate ${AGGREGATE})`);
  console.log(`  Lookback:  ${DAYS} days`);
  console.log("═══════════════════════════════════════════════════");

  // 1. Find pool
  const pool = await findPool(MINT);

  let candles: Candle[] = [];

  // 2. Fetch OHLCV
  if (pool) {
    // Calculate max candles based on timeframe and days
    const candlesPerDay =
      TIMEFRAME === "day" ? 1 / AGGREGATE :
      TIMEFRAME === "hour" ? 24 / AGGREGATE :
      1440 / AGGREGATE; // minute
    const maxCandles = Math.ceil(DAYS * candlesPerDay);

    candles = await fetchGeckoTerminalOHLCV(
      pool.address,
      NETWORK,
      TIMEFRAME,
      AGGREGATE,
      maxCandles,
    );
  }

  // 3. Fallback to Birdeye if GeckoTerminal gave nothing
  if (candles.length === 0) {
    candles = await fetchBirdeyeOHLCV(MINT, TIMEFRAME, DAYS);
  }

  // 4. Current snapshot from DexScreener
  let snapshot: Record<string, any> | null = null;
  if (pool) {
    snapshot = await fetchDexScreenerPairHistory(pool.address);
  }

  // 5. Summary
  console.log("\n═══════════════════════════════════════════════════");
  console.log("  RESULTS");
  console.log("═══════════════════════════════════════════════════");

  if (candles.length > 0) {
    const first = candles[0];
    const last = candles[candles.length - 1];
    const highs = candles.map((c) => c.high);
    const lows = candles.map((c) => c.low).filter((l) => l > 0);
    const totalVol = candles.reduce((s, c) => s + c.volume, 0);

    console.log(`  Candles:   ${candles.length}`);
    console.log(`  Range:     ${first.timestamp.slice(0, 10)} → ${last.timestamp.slice(0, 10)}`);
    console.log(`  Open:      $${first.open}`);
    console.log(`  Close:     $${last.close}`);
    console.log(`  ATH:       $${Math.max(...highs)}`);
    console.log(`  ATL:       $${lows.length ? Math.min(...lows) : 0}`);
    console.log(`  Total Vol: $${totalVol.toLocaleString()}`);

    const change = first.open > 0 ? ((last.close - first.open) / first.open) * 100 : 0;
    console.log(`  Change:    ${change >= 0 ? "+" : ""}${change.toFixed(2)}%`);
  } else {
    console.log("  ⚠️  No candle data retrieved.");
    console.log("     Token may be too new or have no DEX pool tracked by GeckoTerminal.");
    console.log("     Try setting BIRDEYE_API_KEY for Birdeye fallback.");
  }

  // 6. Save files
  const slug = pool ? pool.baseSymbol.toLowerCase().replace(/[^a-z0-9]/g, "-") : MINT.slice(0, 8);
  const ts = new Date().toISOString().slice(0, 10);

  const output = {
    meta: {
      mint: MINT,
      network: NETWORK,
      timeframe: TIMEFRAME,
      aggregate: AGGREGATE,
      fetchedAt: new Date().toISOString(),
      totalCandles: candles.length,
      pool: pool || undefined,
    },
    snapshot: snapshot || undefined,
    candles,
  };

  console.log("\n💾 Saving...");
  saveJSON(`${slug}-ohlcv-${TIMEFRAME}-${ts}.json`, output);
  if (candles.length > 0) {
    saveCSV(`${slug}-ohlcv-${TIMEFRAME}-${ts}.csv`, candles);
  }

  console.log("\n✅ Done!\n");
}

main().catch((err) => {
  console.error("\n❌ Fatal error:", err);
  process.exit(1);
});
