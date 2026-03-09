import { mkdirSync, writeFileSync } from "fs";
import { join } from "path";

const POOL = "AS5KLCFs4HVYPebV8MNt2YzD2Fs2jgb5F2p3xXjxm9Aj";
const LAUNCH_TS = 1772139267; // Feb 26 2026 20:54:27 UTC
const END_2D = LAUNCH_TS + 2 * 86400; // Feb 28

interface Candle {
  timestamp: string;
  unix: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

async function main() {
  const allCandles: Candle[] = [];
  let beforeTs = END_2D + 3600;
  const seen = new Set<number>();

  for (let batch = 0; batch < 10; batch++) {
    const url = `https://api.geckoterminal.com/api/v2/networks/solana/pools/${POOL}/ohlcv/minute?aggregate=1&limit=1000&before_timestamp=${beforeTs}&currency=usd`;
    console.log(`Batch ${batch + 1}: fetching before ${new Date(beforeTs * 1000).toISOString()}...`);
    const res = await fetch(url, {
      headers: { Accept: "application/json;version=20230302" },
      signal: AbortSignal.timeout(15000),
    });
    if (!res.ok) {
      console.log(`HTTP ${res.status}`);
      break;
    }
    const data: any = await res.json();
    const list: [number, number, number, number, number, number][] =
      data?.data?.attributes?.ohlcv_list || [];
    if (list.length === 0) {
      console.log("No more data");
      break;
    }

    let newCount = 0;
    for (const [ts, o, h, l, c, v] of list) {
      if (seen.has(ts)) continue;
      seen.add(ts);
      if (ts >= LAUNCH_TS && ts <= END_2D) {
        allCandles.push({
          timestamp: new Date(ts * 1000).toISOString(),
          unix: ts,
          open: o,
          high: h,
          low: l,
          close: c,
          volume: v,
        });
        newCount++;
      }
    }

    const oldest = list[list.length - 1][0];
    console.log(
      `  → ${list.length} raw, ${newCount} in window | oldest: ${new Date(oldest * 1000).toISOString()}`
    );

    if (oldest <= LAUNCH_TS) {
      console.log("Reached launch time!");
      break;
    }
    beforeTs = oldest;

    await new Promise((r) => setTimeout(r, 2200));
  }

  allCandles.sort((a, b) => a.unix - b.unix);
  console.log(
    `\nTotal 1-min candles in launch window: ${allCandles.length}`
  );
  if (allCandles.length > 0) {
    console.log(
      `First: ${allCandles[0].timestamp} open=$${allCandles[0].open}`
    );
    console.log(
      `Last:  ${allCandles[allCandles.length - 1].timestamp} close=$${allCandles[allCandles.length - 1].close}`
    );
  }

  const dir = join(process.cwd(), "data");
  mkdirSync(dir, { recursive: true });
  const output = {
    meta: {
      mint: "F4ZHCzizwpop2XS8auuAGQ7LZcUTEwQ22m5sRmB2pump",
      pool: POOL,
      timeframe: "1min",
      launchTs: LAUNCH_TS,
      end2dTs: END_2D,
      totalCandles: allCandles.length,
      fetchedAt: new Date().toISOString(),
    },
    candles: allCandles,
  };
  writeFileSync(
    join(dir, "pump-sdk-launch-2d-1min.json"),
    JSON.stringify(output, null, 2)
  );
  const csv = [
    "timestamp,unix,open,high,low,close,volume",
    ...allCandles.map(
      (c) =>
        `${c.timestamp},${c.unix},${c.open},${c.high},${c.low},${c.close},${c.volume}`
    ),
  ].join("\n");
  writeFileSync(join(dir, "pump-sdk-launch-2d-1min.csv"), csv);
  console.log("Saved: data/pump-sdk-launch-2d-1min.json + .csv");
}

main().catch(console.error);
