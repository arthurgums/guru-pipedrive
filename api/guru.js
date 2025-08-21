// api/guru.js
// Guru (Digital Manager) → Pipedrive + Klaviyo (Vercel, Node 20)
// - Só CRIA deals no estágio de Onboarding (nunca atualiza/move)
// - Idempotência por subscription_code (se já existir, não cria de novo)
// - expected_close_date = period_end | hoje+30
// - Klaviyo: subscribe em “novos”, unsubscribe em cancelamento (Pipedrive permanece igual)
// - Normalização de telefone para E.164 (se inválido, omite do payload do Klaviyo)

const {
  PIPEDRIVE_DOMAIN,
  PIPEDRIVE_TOKEN,
  PIPELINE_ID,
  STAGE_ID_ONBOARD,
  PERSON_OWNER_ID,
  DEAL_OWNER_ID,
  DEAL_FIELD_SUBSCRIPTION,
  GURU_API_TOKEN,
  KLAVIYO_LIST_ID
} = process.env;

// Aceita KLAVIYO_PRIVATE_KEY ou KLAVIYO_API_KEY para a mesma finalidade
const KLAVIYO_PRIVATE_KEY = process.env.KLAVIYO_PRIVATE_KEY || process.env.KLAVIYO_API_KEY;
const KLAVIYO_REVISION = process.env.KLAVIYO_REVISION || "2025-07-15";

// ----------------- helpers comuns -----------------
async function pdr(path, opts = {}) {
  const url = `https://${PIPEDRIVE_DOMAIN}.pipedrive.com${path}${path.includes("?") ? "&" : "?"}api_token=${PIPEDRIVE_TOKEN}`;
  const res = await fetch(url, opts);
  const text = await res.text();
  let json = {};
  try { json = JSON.parse(text); } catch { json = {}; }
  if (!res.ok || json.success === false) {
    throw new Error(`Pipedrive error: ${res.status} ${res.statusText} ${text}`);
  }
  return { status: res.status, json };
}
const norm = s => (s || "").normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase();
const normPhoneDigits = p => (p || "").replace(/\D+/g, "");

async function readRaw(req) {
  const chunks = [];
  for await (const c of req) chunks.push(typeof c === "string" ? Buffer.from(c) : c);
  return Buffer.concat(chunks).toString("utf8");
}
async function readJsonFromReq(req) {
  if (req.body && typeof req.body === "object") return req.body;
  if (typeof req.body === "string") return JSON.parse(req.body);
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

// Normaliza telefone para E.164. Se inválido → retorna null (para omitir no Klaviyo)
function toE164(phone, countryCode = "55") {
  const digits = normPhoneDigits(phone);
  if (!digits) return null;

  // Se 'digits' já começar com o DDI (ex.: "55..."), mantém; senão prefixa
  const cc = String(countryCode || "").replace(/\D+/g, "") || "55";
  const needsCC = !digits.startsWith(cc);
  const full = (needsCC ? cc : "") + digits;

  const e164 = `+${full}`;
  // Validação básica E.164: + e 8–15 dígitos
  return /^\+[1-9]\d{7,14}$/.test(e164) ? e164 : null;
}

// Status que PODEM criar deal (pt/en)
const ALLOW_CREATE = (process.env.ALLOW_CREATE_STATUSES || "ativa,iniciada,trial,active,started,trialing")
  .split(",").map(s => norm(s.trim()));

// ----------------- KLAVIYO (JSON:API) -----------------
function hasKlaviyoEnv() {
  return Boolean(KLAVIYO_PRIVATE_KEY && KLAVIYO_LIST_ID);
}
async function klFetch(url, payload) {
  const r = await fetch(url, {
    method: "POST",
    headers: {
      "Authorization": `Klaviyo-API-Key ${KLAVIYO_PRIVATE_KEY}`,
      "Content-Type": "application/json",
      "Accept": "application/json",
      "revision": KLAVIYO_REVISION
    },
    body: JSON.stringify(payload)
  });
  const text = await r.text();
  let body = {};
  try { body = JSON.parse(text); } catch { body = { raw: text }; }
  return { ok: r.ok, status: r.status, body };
}

// Assina email (e opcionalmente telefone) na lista
async function klaviyoSubscribe({ email, phone }) {
  if (!hasKlaviyoEnv() || !email) return { skipped: true };
  const payload = {
    data: {
      type: "profile-subscription-bulk-create-job",
      attributes: {
        profiles: {
          data: [
            {
              type: "profile",
              attributes: {
                email,
                ...(phone ? { phone_number: phone } : {}),
                subscriptions: {
                  email: { marketing: { consent: "SUBSCRIBED" } }
                  // Se quiser SMS também: sms: { marketing: { consent: "SUBSCRIBED" } }
                }
              }
            }
          ]
        }
      },
      relationships: {
        list: { data: { type: "list", id: KLAVIYO_LIST_ID } }
      }
    }
  };
  return klFetch("https://a.klaviyo.com/api/profile-subscription-bulk-create-jobs/", payload);
}

// Cancela assinatura de email na lista (sem tocar no Pipedrive)
async function klaviyoUnsubscribe({ email }) {
  if (!hasKlaviyoEnv() || !email) return { skipped: true };
  const payload = {
    data: {
      type: "profile-subscription-bulk-delete-job",
      attributes: {
        profiles: {
          data: [
            {
              type: "profile",
              attributes: {
                email,
                subscriptions: {
                  email: { marketing: { consent: "UNSUBSCRIBED" } }
                }
              }
            }
          ]
        }
      },
      relationships: {
        list: { data: { type: "list", id: KLAVIYO_LIST_ID } }
      }
    }
  };
  return klFetch("https://a.klaviyo.com/api/profile-subscription-bulk-delete-jobs/", payload);
}

// ----------------- handler -----------------
export default async function handler(req, res) {
  try {
    if (req.method !== "POST") return res.status(405).send("Method Not Allowed");

    let sub = {};
    try { sub = await readJsonFromReq(req); }
    catch { return res.status(400).json({ ok: false, error: "Invalid JSON body" }); }

    // Secret opcional
    const providedSecret = sub.api_token || (req.query && req.query.secret) || req.headers["x-webhook-secret"];
    if (GURU_API_TOKEN && providedSecret !== GURU_API_TOKEN) {
      return res.status(401).json({ ok: false, error: "invalid token" });
    }

    // Só eventos de assinatura
    if (sub.webhook_type && sub.webhook_type !== "subscription") return res.status(204).end();

    // ----- map mínimo do payload -----
    const contact    = sub.last_transaction?.contact || {};
    const subscriber = sub.subscriber || {};

    const email = (subscriber.email || contact.email || "").trim();
    const fullName = (subscriber.name || contact.name || "Assinante (sem nome)").trim();

    // Normalização de telefone → E.164 (usa phone_local_code como DDI quando houver)
    const phoneRaw = (subscriber.phone_number || contact.phone_number || "").trim();
    const country  = (subscriber.phone_local_code || contact.phone_local_code || "55").replace(/\D+/g, "") || "55";
    const phone    = toE164(phoneRaw, country); // se inválido → null (será omitido no Klaviyo)

    const subscriptionCode = sub.subscription_code || sub.id || sub.internal_id || "";
    const planName   = sub.product?.name || sub.next_product?.name || sub.last_transaction?.product?.name || "Plano";
    const mrr        = Number(sub.current_invoice?.value || sub.last_transaction?.invoice?.value || 0);
    const lastStatus = sub.last_status || "unknown";
    const ls         = norm(lastStatus);

    // expected_close_date = period_end | cycle_end_date | hoje+30
    const expectedCloseRaw =
      sub?.current_invoice?.period_end ||
      sub?.last_transaction?.invoice?.period_end ||
      sub?.dates?.cycle_end_date ||
      null;

    const expectedClose = expectedCloseRaw && String(expectedCloseRaw).trim()
      ? String(expectedCloseRaw).trim()
      : ymdUTC(addDaysUTC(new Date(), 30));

    // —— Cancelamento: remove do Klaviyo e sai (não mexe no Pipedrive)
    if (["cancelada","cancelado","cancelled","canceled"].includes(ls)) {
      const kl = await klaviyoUnsubscribe({ email });
      return res.status(200).json({ ok: true, skipped: true, reason: "cancel-received", klaviyo: kl });
    }

    // —— “Novos” (status permitidos): assina no Klaviyo (se tiver email)
    let klaviyo = {};
    if (email && ALLOW_CREATE.includes(ls)) {
      try { klaviyo = await klaviyoSubscribe({ email, phone }); }
      catch (e) { klaviyo = { ok: false, error: e.message }; }
    }

    // —— Idempotência por subscription_code → se já existir, NÃO cria deal
    if (DEAL_FIELD_SUBSCRIPTION && subscriptionCode) {
      const srch = await pdr(`/api/v2/deals/search?term=${encodeURIComponent(subscriptionCode)}&fields=custom_fields&exact_match=1`, { method: "GET" });
      const existingId = srch.json?.data?.items?.[0]?.item?.id || null;
      if (existingId) {
        return res.status(200).json({ ok: true, skipped: true, reason: "already-exists", dealId: existingId, klaviyo });
      }
    }

    // —— Só cria para status “novos”
    if (!ALLOW_CREATE.includes(ls)) {
      return res.status(200).json({ ok: true, skipped: true, reason: "status-not-allowed", lastStatus, klaviyo });
    }

    // —— Upsert da PESSOA (cria se não existir por email)
    let personId = null;
    if (email) {
      const psearch = await pdr(`/api/v2/persons/search?term=${encodeURIComponent(email)}&fields=email&exact_match=1`, { method: "GET" });
      personId = psearch.json?.data?.items?.[0]?.item?.id || null;
    }
    if (!personId) {
      const created = await pdr(`/api/v1/persons`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: fullName,
          owner_id: PERSON_OWNER_ID ? Number(PERSON_OWNER_ID) : undefined,
          visible_to: 3,
          email: email ? [{ value: email, primary: true }] : undefined,
          phone: phoneRaw ? [{ value: phoneRaw, primary: true }] : undefined // mantém no Pipedrive como foi recebido
        })
      });
      personId = created.json?.data?.id;
      if (!personId) throw new Error("Falha ao criar pessoa");
    }

    // —— Criar DEAL (sempre no estágio de onboarding)
    const phoneDigits = normPhoneDigits(phoneRaw);
    const title = phoneDigits ? `(${phoneDigits}) (${planName})` : `${planName} – ${fullName}`;
    const payloadDeal = {
      title,
      person_id: personId,
      pipeline_id: Number(PIPELINE_ID),
      stage_id: Number(STAGE_ID_ONBOARD),
      value: mrr,
      currency: "BRL",
      owner_id: DEAL_OWNER_ID ? Number(DEAL_OWNER_ID) : undefined,
      status: "open",
      expected_close_date: expectedClose,
      ...(DEAL_FIELD_SUBSCRIPTION && subscriptionCode ? { [DEAL_FIELD_SUBSCRIPTION]: subscriptionCode } : {})
    };

    const createdDeal = await pdr(`/api/v1/deals`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payloadDeal)
    });

    return res.status(200).json({
      ok: true,
      personId,
      dealId: createdDeal.json?.data?.id || null,
      status: lastStatus,
      mrr,
      expected_close_date: expectedClose,
      klaviyo
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ ok: false, error: e.message });
  }
}
