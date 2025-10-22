import { get } from '../../../../lib/proxy';

export async function GET() {
  return get('/api/stats');
}
