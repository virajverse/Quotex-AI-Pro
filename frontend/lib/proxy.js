const BACKEND_URL = process.env.BACKEND_URL;
const ADMIN_API_KEY = process.env.ADMIN_API_KEY;

if (!BACKEND_URL) {
  console.warn('[config] BACKEND_URL is not set');
}
if (!ADMIN_API_KEY) {
  console.warn('[config] ADMIN_API_KEY is not set');
}

async function forward(path, init = {}) {
  if (!BACKEND_URL || !ADMIN_API_KEY) {
    return new Response(JSON.stringify({ error: 'server not configured' }), { status: 500 });
  }
  const url = new URL(path, BACKEND_URL);
  const headers = new Headers(init.headers || {});
  headers.set('x-admin-key', ADMIN_API_KEY);
  if (!headers.has('content-type') && init.body) headers.set('content-type', 'application/json');
  const res = await fetch(url.toString(), { ...init, headers, cache: 'no-store' });
  const text = await res.text();
  return new Response(text, { status: res.status, headers: { 'content-type': res.headers.get('content-type') || 'application/json' } });
}

export async function get(path) {
  return forward(path, { method: 'GET' });
}

export async function post(path, bodyObj) {
  return forward(path, { method: 'POST', body: JSON.stringify(bodyObj || {}) });
}
