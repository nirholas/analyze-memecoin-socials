import { NextRequest, NextResponse } from 'next/server';

/**
 * GET /api/admin/workflow-status?run_id=123456
 * Check the status of a GitHub Actions workflow run.
 */
export async function GET(request: NextRequest) {
  const { searchParams } = new URL(request.url);
  const runId = searchParams.get('run_id');

  if (!runId) {
    return NextResponse.json({ error: 'run_id is required' }, { status: 400 });
  }

  const githubToken = process.env.GITHUB_TOKEN;
  const githubRepo = process.env.GITHUB_REPO;

  if (!githubToken || !githubRepo) {
    return NextResponse.json(
      { error: 'GitHub integration not configured' },
      { status: 500 }
    );
  }

  try {
    const response = await fetch(
      `https://api.github.com/repos/${githubRepo}/actions/runs/${runId}`,
      {
        headers: {
          Authorization: `Bearer ${githubToken}`,
          Accept: 'application/vnd.github.v3+json',
        },
      }
    );

    if (!response.ok) {
      return NextResponse.json(
        { error: `GitHub API error: ${response.status}` },
        { status: 500 }
      );
    }

    const data = await response.json();

    return NextResponse.json({
      status: data.status, // 'queued', 'in_progress', 'completed'
      conclusion: data.conclusion, // null, 'success', 'failure', 'cancelled'
      html_url: data.html_url,
      created_at: data.created_at,
      updated_at: data.updated_at,
    });
  } catch (error) {
    console.error('Error fetching workflow status:', error);
    return NextResponse.json(
      { error: 'Failed to fetch workflow status' },
      { status: 500 }
    );
  }
}
