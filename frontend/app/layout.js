import './globals.css';
import Link from 'next/link';
import { BarChart3, Users, MessageSquare, Megaphone } from 'lucide-react';

export const metadata = {
  title: 'QuotexAI Pro — Admin',
  description: 'Admin panel for QuotexAI Pro'
};

function NavLink({ href, children }) {
  const pathname = typeof window !== 'undefined' ? window.location.pathname : '';
  const active = pathname === href;
  const cls = 'nav-link' + (active ? ' nav-link-active' : '');
  return (
    <Link className={cls} href={href}>{children}</Link>
  );
}

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body>
        <div className="min-h-screen grid grid-cols-[240px_1fr]">
          <aside className="p-4 border-r border-gray-200 bg-[#f7f8fb]">
            <div className="mb-6 text-lg font-semibold">QuotexAI Pro</div>
            <nav className="flex flex-col gap-2">
              <NavLink href="/"><BarChart3 size={18} /> Dashboard</NavLink>
              <NavLink href="/users"><Users size={18} /> Users</NavLink>
              <NavLink href="/messages"><MessageSquare size={18} /> Messages</NavLink>
              <NavLink href="/broadcast"><Megaphone size={18} /> Broadcast</NavLink>
            </nav>
            <div className="mt-8 text-xs text-muted">Admin Panel</div>
          </aside>
          <main className="p-6">
            <div className="topbar">
              <div className="text-xl font-semibold">Admin</div>
              <div className="text-sm text-muted">Telegram bot control</div>
            </div>
            <div className="grid gap-6">{children}</div>
          </main>
        </div>
      </body>
    </html>
  );
}
