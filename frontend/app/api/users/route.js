import { get } from '../../../lib/proxy';

export async function GET(request) {
  const { search } = new URL(request.url);
  return get('/api/users' + (search || ''));
}
