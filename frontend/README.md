# QuotexAI Pro — Admin (Next.js)

## Env Vars (Vercel)
- `BACKEND_URL`: Render backend base URL, e.g. `https://quotex-ai-pro.onrender.com`
- `ADMIN_API_KEY`: Admin key for backend API (kept server-side only)

## Scripts
- `npm run dev` — local dev
- `npm run build && npm start` — production

## Notes
- The browser calls Next.js API routes under `/api/*`, which proxy to the Render backend using server env vars. Secrets never reach the client.
