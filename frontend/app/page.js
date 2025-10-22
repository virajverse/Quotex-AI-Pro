"use client";
import { useEffect, useState } from 'react';

export default function Page() {
  const [stats, setStats] = useState({ total_users: '-', active_premium: '-', new_signups_today: '-' });
  useEffect(()=>{ (async()=>{ const s = await fetch('/api/stats').then(r=>r.json()); setStats(s); })(); },[]);
  return (
    <div className="grid md:grid-cols-3 gap-4">
      <div className="card"><div className="card-body"><div className="text-sm text-muted">Total Users</div><div className="text-3xl font-semibold">{String(stats.total_users)}</div></div></div>
      <div className="card"><div className="card-body"><div className="text-sm text-muted">Active Premium</div><div className="text-3xl font-semibold">{String(stats.active_premium)}</div></div></div>
      <div className="card"><div className="card-body"><div className="text-sm text-muted">New Today</div><div className="text-3xl font-semibold">{String(stats.new_signups_today)}</div></div></div>
      <div className="card md:col-span-2"><div className="card-header">Activity</div><div className="card-body text-sm text-muted">Charts coming soon.</div></div>
      <div className="card"><div className="card-header">Quick Tips</div><div className="card-body text-sm text-muted">Use the sidebar to manage users, messages, and broadcasts.</div></div>
    </div>
  );
}
