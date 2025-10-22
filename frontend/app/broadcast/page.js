"use client";
import { useState } from 'react';

export default function BroadcastPage() {
  const [text, setText] = useState('');
  const [busy, setBusy] = useState(false);
  const [res, setRes] = useState(null);

  async function send() {
    setBusy(true);
    try {
      const r = await fetch('/api/broadcast', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ text }) });
      const j = await r.json();
      setRes(j);
    } finally { setBusy(false); }
  }

  return (
    <div className="grid gap-4">
      <div className="card">
        <div className="card-header">Broadcast to Premium Users</div>
        <div className="card-body grid gap-3 md:grid-cols-[1fr_auto]">
          <input className="px-3 py-2 border rounded-lg" placeholder="Message to broadcast" value={text} onChange={e=>setText(e.target.value)} />
          <button className="px-4 py-2 bg-black text-white rounded-lg" onClick={send} disabled={busy}>Send</button>
          {res && <div className="md:col-span-2 text-sm text-muted">{JSON.stringify(res)}</div>}
        </div>
      </div>
    </div>
  );
}
