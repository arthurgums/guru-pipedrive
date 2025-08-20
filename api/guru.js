// api/guru.js
// Guru (Digital Manager) → Pipedrive (Vercel, Node 20)
// Somente CRIA deals novos no estágio de Onboarding. Nunca atualiza/move.

// ==== ENV OBRIGATÓRIAS ====
// PIPEDRIVE_DOMAIN, PIPEDRIVE_TOKEN, PIPELINE_ID, STAGE_ID_ONBOARD, DEAL_FIELD_SUBSCRIPTION
// ==== OPCIONAIS ====
// PERSON_OWNER_ID, DEAL_OWNER_ID, GURU_API_TOKEN
// ALLOW_CREATE_STATUSES (default: "ativa,iniciada,trial,active,started,trialing")

const {
  PIPEDRIVE_DOMAIN,
  PIPEDRIVE_TOKEN,
  PIPELINE_ID,
  STAGE_ID_ONBOARD,
  PERSON_OWNER_ID,
  DEAL_OWNER_ID,
  DEAL_FIELD_SUBSCRIPTION,
  GURU_API_TOKEN
} = process.env;

// ---------- helpers ----------
async function pdr(path, opts = {}) {
  const url = `https://${PIPEDRIVE_DOMAIN}.pipedrive.com${path}${path.includes('?') ? '&' : '?'}api_token=${PIPEDRIVE_TOKEN}`;
  const res = await fetch(url, opts);
  const json = await res.json().catch(() => ({}));
  if (!res.ok || json.success === false) throw new Error(`Pipedrive error: ${res.status} ${res.statusText} ${JSON.stringify(json)}`);
  return json;
}
const norm = s => (s || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase();
const normPhone = p => (p || '').replace(/\D+/g, '');
async function readRaw(req) {
  const chunks = [];
  for await (const c of req) chunks.push(typeof c === 'string' ? Buffer.from(c) : c);
  return Buffer.concat(chunks).toString('utf8');
}
async function readJsonFromReq(req) {
  if (req.body && typeof req.body === 'object') return req.body;
  if (typeof req.body === 'string') return JSON.parse(req.body);
  const raw = (await readRaw(req)).trim();
  return raw ? JSON.parse(raw) : {};
}
function addDaysUTC(date, days) {
  const d = new Date(date.getTime());
  d.setUTCDate(d.getUTCDate() + days);
  return d;
}
function ymdUTC(d) {
  return d.toISOString().slice(0, 10); // "YYYY-MM-DD"
}

// status permitidos para CRIAR (pt/en)
const ALLOW_CREATE = (process.env.ALLOW_CREATE_STATUSES || 'ativa,iniciada,trial,active,started,trialing')
  .split(',')
  .map(s => norm(s.trim()));

// ---------- handler ----------
export default async function handler(req, res) {
  try {
    if (req.method !== 'POST') return res.status(405).send('Method Not Allowed');

    let sub = {};
    try { sub = await readJsonFromReq(req); }
    catch { return res.status(400).json({ ok: false, error: 'Invalid JSON body' }); }

    // secret opcional
    const providedSecret = sub.api_token || (req.query && req.query.secret) || req.headers['x-webhook-secret'];
    if (GURU_API_TOKEN && providedSecret !== GURU_API_TOKEN) {
      return res.status(401).json({ ok: false, error: 'invalid token' });
    }

    // só eventos de assinatura
    if (sub.webhook_type && sub.webhook_type !== 'subscription') return res.status(204).end();

    // ----- map mínimo do payload -----
    const contact    = sub.last_transaction?.contact || {};
    const subscriber = sub.subscriber || {};

    const email = (subscriber.email || contact.email || '').trim();
    const name  = (subscriber.name  || contact.name  || 'Assinante (sem nome)').trim();
    const phone = (subscriber.phone_number || contact.phone_number || '').trim();
    const cpf   = (subscriber.doc || contact.doc || '').trim();

    const subscriptionCode = sub.subscription_code || sub.id || sub.internal_id || '';
    const planName   = sub.product?.name || sub.next_product?.name || sub.last_transaction?.product?.name || 'Plano';
    const mrr        = Number(sub.current_invoice?.value || sub.last_transaction?.invoice?.value || 0);
    const lastStatus = sub.last_status || 'unknown';

    // expected_close_date = period_end | cycle_end_date | hoje+30
    const expectedCloseRaw =
      sub?.current_invoice?.period_end ||
      sub?.last_transaction?.invoice?.period_end ||
      sub?.dates?.cycle_end_date ||
      null;

    const expectedClose = expectedCloseRaw && String(expectedCloseRaw).trim()
      ? String(expectedCloseRaw).trim()
      : ymdUTC(addDaysUTC(new Date(), 30));

    // —— idempotência por subscription_code → se já existir, NÃO cria
    if (DEAL_FIELD_SUBSCRIPTION && subscriptionCode) {
      const dsearch = await pdr(`/api/v2/deals/search?term=${encodeURIComponent(subscriptionCode)}&fields=custom_fields&exact_match=1`, { method: 'GET' });
      const existingId = dsearch?.data?.items?.[0]?.item?.id || null;
      if (existingId) {
        return res.status(200).json({ ok: true, skipped: true, reason: 'already-exists', dealId: existingId });
      }
    }

    // —— regra: só cria para status "novos"
    if (!ALLOW_CREATE.includes(norm(lastStatus))) {
      return res.status(200).json({ ok: true, skipped: true, reason: 'status-not-allowed', lastStatus });
    }

    // —— upsert da PESSOA (cria se não existir por email)
    let personId = null;
    if (email) {
      const psearch = await pdr(`/api/v2/persons/search?term=${encodeURIComponent(email)}&fields=email&exact_match=1`, { method: 'GET' });
      personId = psearch?.data?.items?.[0]?.item?.id || null;
    }
    if (!personId) {
      const created = await pdr(`/api/v1/persons`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name,
          owner_id: PERSON_OWNER_ID ? Number(PERSON_OWNER_ID) : undefined,
          visible_to: 3,
          email: email ? [{ value: email, primary: true }] : undefined,
          phone: phone ? [{ value: phone, primary: true }] : undefined
          // se tiver campo custom na Pessoa para CPF, adicione aqui
        })
      });
      personId = created?.data?.id;
      if (!personId) throw new Error('Falha ao criar pessoa');
    }

    // —— Criar DEAL (sempre no estágio de onboarding)
    const title = normPhone(phone) ? `(${normPhone(phone)}) (${planName})` : `${planName} – ${name}`;
    const payloadDeal = {
      title,
      person_id: personId,
      pipeline_id: Number(PIPELINE_ID),
      stage_id: Number(STAGE_ID_ONBOARD),
      value: mrr,
      currency: 'BRL',
      owner_id: DEAL_OWNER_ID ? Number(DEAL_OWNER_ID) : undefined,
      status: 'open',
      expected_close_date: expectedClose,
      ...(DEAL_FIELD_SUBSCRIPTION && subscriptionCode ? { [DEAL_FIELD_SUBSCRIPTION]: subscriptionCode } : {})
    };

    const createdDeal = await pdr(`/api/v1/deals`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payloadDeal)
    });

    return res.status(200).json({
      ok: true,
      personId,
      dealId: createdDeal?.data?.id || null,
      status: lastStatus,
      mrr,
      expected_close_date: expectedClose
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ ok: false, error: e.message });
  }
}
