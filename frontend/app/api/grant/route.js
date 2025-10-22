import { post } from '../../../../lib/proxy';

export async function POST(request) {
  const body = await request.json().catch(() => ({}));
  return post('/api/grant', body);
}
