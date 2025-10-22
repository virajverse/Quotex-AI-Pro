"use client";
import { useEffect, useState } from 'react';

export default function UsersPage() {
  const [q, setQ] = useState('');
  const [users, setUsers] = useState([]);
  const [loading, setLoading] = useState(false);

  async function load() {
    setLoading(true);
    try {
      const res = await fetch('/api/users' + (q ? `?q=${encodeURIComponent(q)}` : ''));
      const j = await res.json();
      setUsers(j.users || []);
    } finally { setLoading(false); }
  }

  useEffect(()=>{ load(); }, []);

  return (
    <div className="grid gap-4">
      <div className="card">
        <div className="card-header flex items-center justify-between">
          <span>Users</span>
          <div className="flex gap-2">
            <input className="form-input px-3 py-2 border rounded-lg" placeholder="Search" value={q} onChange={e=>setQ(e.target.value)} />
            <button className="btn px-4 py-2 bg-black text-white rounded-lg" onClick={load} disabled={loading}>Search</button>
          </div>
        </div>
        <div className="card-body">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-muted">
                  <th className="py-2">Telegram ID</th>
                  <th className="py-2">Name</th>
                  <th className="py-2">Email</th>
                  <th className="py-2">Premium</th>
                  <th className="py-2">Expiry</th>
                  <th className="py-2">Last Login</th>
                </tr>
              </thead>
              <tbody>
                {users.map((u, i)=> (
                  <tr key={i} className="border-t">
                    <td className="py-2">{u.telegram_id ?? ''}</td>
                    <td className="py-2">{u.name ?? ''}</td>
                    <td className="py-2">{u.email ?? ''}</td>
                    <td className="py-2">{u.is_premium ? '✅' : '❌'}</td>
                    <td className="py-2">{u.expires_at ?? ''}</td>
                    <td className="py-2">{u.last_login ?? ''}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
}
