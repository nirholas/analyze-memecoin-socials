'use client';

import { useState, useEffect } from 'react';

type WorkflowStatus = {
  status: 'queued' | 'in_progress' | 'completed';
  conclusion: 'success' | 'failure' | 'cancelled' | null;
  html_url: string;
};

export default function AdminPage() {
  // Form state
  const [assetId, setAssetId] = useState('');
  const [assetName, setAssetName] = useState('');
  const [founder, setFounder] = useState('');
  const [coingeckoId, setCoingeckoId] = useState('');
  const [network, setNetwork] = useState('');
  const [poolAddress, setPoolAddress] = useState('');
  const [color, setColor] = useState('#3B82F6');
  const [password, setPassword] = useState('');

  // UI state
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [runId, setRunId] = useState<number | null>(null);
  const [runUrl, setRunUrl] = useState<string | null>(null);
  const [workflowStatus, setWorkflowStatus] = useState<WorkflowStatus | null>(null);

  // Poll for workflow status
  useEffect(() => {
    if (!runId) return;

    const pollStatus = async () => {
      try {
        const response = await fetch(`/api/admin/workflow-status?run_id=${runId}`);
        if (response.ok) {
          const data = await response.json();
          setWorkflowStatus(data);

          // Stop polling if completed
          if (data.status === 'completed') {
            return;
          }
        }
      } catch (err) {
        console.error('Error polling status:', err);
      }
    };

    // Poll immediately, then every 5 seconds
    pollStatus();
    const interval = setInterval(pollStatus, 5000);

    return () => clearInterval(interval);
  }, [runId]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    setIsSubmitting(true);
    setRunId(null);
    setRunUrl(null);
    setWorkflowStatus(null);

    try {
      const response = await fetch('/api/admin/add-asset', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          asset_id: assetId.toLowerCase().replace(/\s+/g, '-'),
          asset_name: assetName,
          founder: founder.replace('@', ''),
          coingecko_id: coingeckoId || undefined,
          network: network || undefined,
          pool_address: poolAddress || undefined,
          color,
          password,
        }),
      });

      const data = await response.json();

      if (!response.ok) {
        setError(data.error || 'Failed to trigger workflow');
        return;
      }

      setRunId(data.run_id);
      setRunUrl(data.run_url);
    } catch (err) {
      setError('Network error - please try again');
    } finally {
      setIsSubmitting(false);
    }
  };

  const getStatusDisplay = () => {
    if (!workflowStatus) {
      return { text: 'Starting...', color: 'text-yellow-400', icon: '⏳' };
    }

    if (workflowStatus.status === 'queued') {
      return { text: 'Queued', color: 'text-yellow-400', icon: '⏳' };
    }

    if (workflowStatus.status === 'in_progress') {
      return { text: 'Running...', color: 'text-blue-400', icon: '🔄' };
    }

    if (workflowStatus.conclusion === 'success') {
      return { text: 'Success!', color: 'text-green-400', icon: '✅' };
    }

    if (workflowStatus.conclusion === 'failure') {
      return { text: 'Failed', color: 'text-red-400', icon: '❌' };
    }

    return { text: 'Unknown', color: 'text-gray-400', icon: '❓' };
  };

  return (
    <div className="min-h-screen bg-[#131722] text-white p-8">
      <div className="max-w-xl mx-auto">
        <h1 className="text-2xl font-bold mb-2">🔐 Admin: Add Asset</h1>
        <p className="text-gray-400 mb-8">
          Add a new asset to track. This triggers a GitHub Actions workflow that
          fetches tweets, prices, and deploys automatically.
        </p>

        <form onSubmit={handleSubmit} className="space-y-6">
          {/* Asset ID */}
          <div>
            <label className="block text-sm font-medium text-gray-300 mb-1">
              Asset ID <span className="text-red-400">*</span>
            </label>
            <input
              type="text"
              value={assetId}
              onChange={(e) => setAssetId(e.target.value)}
              placeholder="mytoken"
              className="w-full px-4 py-2 bg-[#1E222D] border border-[#2A2E39] rounded-lg focus:outline-none focus:border-blue-500"
              required
            />
            <p className="text-xs text-gray-500 mt-1">Lowercase, no spaces</p>
          </div>

          {/* Asset Name */}
          <div>
            <label className="block text-sm font-medium text-gray-300 mb-1">
              Display Name <span className="text-red-400">*</span>
            </label>
            <input
              type="text"
              value={assetName}
              onChange={(e) => setAssetName(e.target.value)}
              placeholder="My Token"
              className="w-full px-4 py-2 bg-[#1E222D] border border-[#2A2E39] rounded-lg focus:outline-none focus:border-blue-500"
              required
            />
          </div>

          {/* Founder */}
          <div>
            <label className="block text-sm font-medium text-gray-300 mb-1">
              Founder Twitter <span className="text-red-400">*</span>
            </label>
            <div className="flex">
              <span className="px-3 py-2 bg-[#2A2E39] border border-r-0 border-[#2A2E39] rounded-l-lg text-gray-400">
                @
              </span>
              <input
                type="text"
                value={founder}
                onChange={(e) => setFounder(e.target.value)}
                placeholder="username"
                className="flex-1 px-4 py-2 bg-[#1E222D] border border-[#2A2E39] rounded-r-lg focus:outline-none focus:border-blue-500"
                required
              />
            </div>
          </div>

          {/* Price Source Section */}
          <div className="border border-[#2A2E39] rounded-lg p-4">
            <h3 className="text-sm font-medium text-gray-300 mb-3">
              Price Source (choose one)
            </h3>

            {/* CoinGecko */}
            <div className="mb-4">
              <label className="block text-sm text-gray-400 mb-1">
                CoinGecko ID
              </label>
              <input
                type="text"
                value={coingeckoId}
                onChange={(e) => setCoingeckoId(e.target.value)}
                placeholder="my-token-id"
                className="w-full px-4 py-2 bg-[#1E222D] border border-[#2A2E39] rounded-lg focus:outline-none focus:border-blue-500"
              />
              <p className="text-xs text-gray-500 mt-1">
                For listed tokens. Find on coingecko.com/en/coins/[id]
              </p>
            </div>

            <div className="text-center text-gray-500 text-sm mb-4">— OR —</div>

            {/* DEX Token */}
            <div className="space-y-3">
              <div>
                <label className="block text-sm text-gray-400 mb-1">Network</label>
                <select
                  value={network}
                  onChange={(e) => setNetwork(e.target.value)}
                  className="w-full px-4 py-2 bg-[#1E222D] border border-[#2A2E39] rounded-lg focus:outline-none focus:border-blue-500"
                >
                  <option value="">Select network...</option>
                  <option value="solana">Solana</option>
                  <option value="ethereum">Ethereum</option>
                  <option value="bsc">BSC</option>
                  <option value="base">Base</option>
                </select>
              </div>

              <div>
                <label className="block text-sm text-gray-400 mb-1">
                  Pool Address
                </label>
                <input
                  type="text"
                  value={poolAddress}
                  onChange={(e) => setPoolAddress(e.target.value)}
                  placeholder="0x..."
                  className="w-full px-4 py-2 bg-[#1E222D] border border-[#2A2E39] rounded-lg focus:outline-none focus:border-blue-500 font-mono text-sm"
                />
                <p className="text-xs text-gray-500 mt-1">
                  For DEX tokens. Find on dexscreener.com
                </p>
              </div>
            </div>
          </div>

          {/* Color */}
          <div>
            <label className="block text-sm font-medium text-gray-300 mb-1">
              Brand Color
            </label>
            <div className="flex gap-2">
              <input
                type="color"
                value={color}
                onChange={(e) => setColor(e.target.value)}
                className="w-12 h-10 rounded cursor-pointer"
              />
              <input
                type="text"
                value={color}
                onChange={(e) => setColor(e.target.value)}
                className="flex-1 px-4 py-2 bg-[#1E222D] border border-[#2A2E39] rounded-lg focus:outline-none focus:border-blue-500 font-mono"
              />
            </div>
          </div>

          {/* Password */}
          <div>
            <label className="block text-sm font-medium text-gray-300 mb-1">
              Admin Password <span className="text-red-400">*</span>
            </label>
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder="••••••••"
              className="w-full px-4 py-2 bg-[#1E222D] border border-[#2A2E39] rounded-lg focus:outline-none focus:border-blue-500"
              required
            />
          </div>

          {/* Error */}
          {error && (
            <div className="p-4 bg-red-500/10 border border-red-500/30 rounded-lg text-red-400">
              {error}
            </div>
          )}

          {/* Submit */}
          <button
            type="submit"
            disabled={isSubmitting || (runId !== null && workflowStatus?.status !== 'completed')}
            className="w-full py-3 px-4 bg-blue-600 hover:bg-blue-700 disabled:bg-gray-600 disabled:cursor-not-allowed rounded-lg font-medium transition-colors"
          >
            {isSubmitting ? 'Triggering...' : 'Add Asset'}
          </button>
        </form>

        {/* Status */}
        {runId && (
          <div className="mt-8 p-4 bg-[#1E222D] border border-[#2A2E39] rounded-lg">
            <div className="flex items-center justify-between mb-4">
              <h3 className="font-medium">Workflow Status</h3>
              <span className={`${getStatusDisplay().color}`}>
                {getStatusDisplay().icon} {getStatusDisplay().text}
              </span>
            </div>

            {workflowStatus?.status === 'in_progress' && (
              <div className="mb-4">
                <div className="h-2 bg-[#2A2E39] rounded-full overflow-hidden">
                  <div className="h-full bg-blue-500 rounded-full animate-pulse w-2/3" />
                </div>
                <p className="text-sm text-gray-400 mt-2">
                  This usually takes 2-3 minutes...
                </p>
              </div>
            )}

            {workflowStatus?.conclusion === 'success' && (
              <div className="text-green-400 text-sm mb-4">
                Asset added! The site will auto-deploy in ~2 minutes.
              </div>
            )}

            {workflowStatus?.conclusion === 'failure' && (
              <div className="text-red-400 text-sm mb-4">
                Workflow failed. Check the logs for details.
              </div>
            )}

            {runUrl && (
              <a
                href={runUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="text-blue-400 hover:text-blue-300 text-sm"
              >
                View on GitHub →
              </a>
            )}
          </div>
        )}

        {/* Help */}
        <div className="mt-8 p-4 bg-[#1E222D] border border-[#2A2E39] rounded-lg text-sm text-gray-400">
          <h3 className="font-medium text-gray-300 mb-2">Quick Tips</h3>
          <ul className="space-y-1 list-disc list-inside">
            <li>CoinGecko is easiest - just find the token page and copy the ID from the URL</li>
            <li>For memecoins not on CoinGecko, use the DEX pool address from DexScreener</li>
            <li>The founder&apos;s Twitter is used to fetch their tweets</li>
            <li>After adding, you&apos;ll need to manually add a logo to <code>/logos/[id].png</code></li>
          </ul>
        </div>
      </div>
    </div>
  );
}
