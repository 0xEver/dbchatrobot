CREATE TABLE IF NOT EXISTS videos (
    id UUID PRIMARY KEY,
    creator_id UUID NOT NULL,
    video_created_at TIMESTAMPTZ NOT NULL,
    views_count INTEGER DEFAULT 0,
    likes_count INTEGER DEFAULT 0,
    comments_count INTEGER DEFAULT 0,
    reports_count INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS video_snapshots (
    id UUID PRIMARY KEY,
    video_id UUID REFERENCES videos(id) ON DELETE CASCADE,
    views_count INTEGER DEFAULT 0,
    likes_count INTEGER DEFAULT 0,
    comments_count INTEGER DEFAULT 0,
    reports_count INTEGER DEFAULT 0,
    delta_views_count INTEGER DEFAULT 0,
    delta_likes_count INTEGER DEFAULT 0,
    delta_comments_count INTEGER DEFAULT 0,
    delta_reports_count INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_videos_creator_id ON videos(creator_id);
CREATE INDEX IF NOT EXISTS idx_videos_video_created_at ON videos(video_created_at);
CREATE INDEX IF NOT EXISTS idx_snapshots_video_id ON video_snapshots(video_id);
CREATE INDEX IF NOT EXISTS idx_snapshots_created_at ON video_snapshots(created_at);
