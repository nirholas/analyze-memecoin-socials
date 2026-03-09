import { NextRequest, NextResponse } from 'next/server';

/**
 * POST /api/admin/add-asset
 * Triggers the GitHub Actions workflow to add a new asset.
 */
export async function POST(request: NextRequest) {
  try {
    const body = await request.json();

    // Validate password
    const adminPassword = process.env.ADMIN_PASSWORD;
    if (!adminPassword) {
      return NextResponse.json(
        { error: 'Admin password not configured' },
        { status: 500 }
      );
    }

    if (body.password !== adminPassword) {
      return NextResponse.json({ error: 'Invalid password' }, { status: 401 });
    }

    // Validate required fields
    const { asset_id, asset_name, founder, coingecko_id, network, pool_address, color, refresh_only } = body;

    if (!asset_id) {
      return NextResponse.json({ error: 'asset_id is required' }, { status: 400 });
    }

    if (!refresh_only) {
      if (!asset_name) {
        return NextResponse.json({ error: 'asset_name is required' }, { status: 400 });
      }
      if (!founder) {
        return NextResponse.json({ error: 'founder is required' }, { status: 400 });
      }
      if (!coingecko_id && !pool_address) {
        return NextResponse.json(
          { error: 'Either coingecko_id or pool_address is required' },
          { status: 400 }
        );
      }
    }

    // Validate GitHub token
    const githubToken = process.env.GITHUB_TOKEN;
    const githubRepo = process.env.GITHUB_REPO;

    if (!githubToken || !githubRepo) {
      return NextResponse.json(
        { error: 'GitHub integration not configured' },
        { status: 500 }
      );
    }

    // Trigger GitHub Actions workflow
    const response = await fetch(
      `https://api.github.com/repos/${githubRepo}/actions/workflows/add-asset.yml/dispatches`,
      {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${githubToken}`,
          Accept: 'application/vnd.github.v3+json',
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          ref: 'main',
          inputs: {
            asset_id: asset_id,
            asset_name: asset_name || '',
            founder: founder || '',
            coingecko_id: coingecko_id || '',
            network: network || '',
            pool_address: pool_address || '',
            color: color || '#3B82F6',
            refresh_only: refresh_only ? 'true' : 'false',
          },
        }),
      }
    );

    if (!response.ok) {
      const errorText = await response.text();
      console.error('GitHub API error:', response.status, errorText);
      return NextResponse.json(
        { error: `GitHub API error: ${response.status}` },
        { status: 500 }
      );
    }

    // Get the workflow run ID (need to poll for it)
    // The dispatch endpoint doesn't return the run ID, so we need to fetch recent runs
    await new Promise((resolve) => setTimeout(resolve, 2000)); // Wait for workflow to start

    const runsResponse = await fetch(
      `https://api.github.com/repos/${githubRepo}/actions/workflows/add-asset.yml/runs?per_page=1`,
      {
        headers: {
          Authorization: `Bearer ${githubToken}`,
          Accept: 'application/vnd.github.v3+json',
        },
      }
    );

    let run_id: number | null = null;
    let run_url: string | null = null;

    if (runsResponse.ok) {
      const runsData = await runsResponse.json();
      if (runsData.workflow_runs && runsData.workflow_runs.length > 0) {
        const latestRun = runsData.workflow_runs[0];
        run_id = latestRun.id;
        run_url = latestRun.html_url;
      }
    }

    return NextResponse.json({
      success: true,
      message: 'Workflow triggered successfully',
      run_id,
      run_url,
    });
  } catch (error) {
    console.error('Error triggering workflow:', error);
    return NextResponse.json(
      { error: 'Failed to trigger workflow' },
      { status: 500 }
    );
  }
}
