// api/guru.js
// Guru (Digital Manager) → Pipedrive + Klaviyo (Vercel, Node 20)
// - Só CRIA deals no estágio de Onboarding (nunca atualiza/move)
// - Idempotência por subscription_code (se já existir, não cria de novo)
// - expected_close_date = (period_end | cycle_end_date) + 1 dia  |  fallback: mesmo dia do mês seguinte baseado no início do ciclo
// - Klaviyo: subscribe em “novos”, unsubscribe em cancelamento (Pipedrive permanece igual)

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
const normPhone = p => (p || "").replace(/\D+/g, "");
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
function ymdUTC(d) {
  return d.toISOString().slice(0, 10); // "YYYY-MM-DD"
}

// Mesmo dia do mês seguinte (se não existir, usa o último dia do mês)
function sameDayNextMonthUTC(date) {
  const y = date.getUTCFullYear();
  const m = date.getUTCMonth(); // 0-11
  const d = date.getUTCDate();
  const firstNext = new Date(Date.UTC(y, m + 1, 1));
  const daysInNext = new Date(Date.UTC(firstNext.getUTCFullYear(), firstNext.getUTCMonth() + 1, 0)).getUTCDate();
  const day = Math.min(d, daysInNext);
  return new Date(Date.UTC(firstNext.getUTCFullYear(), firstNext.getUTCMonth(), day));
}
function parseDateUTC(s) {
  if (!s) return null;
  const t = String(s).trim();
  if (!t) return null;
  const iso = /^\d{4}-\d{2}-\d{2}$/.test(t) ? `${t}T00:00:00Z` : t;
  const d = new Date(iso);
  return isNaN(d) ? null : d;
}
function addDaysUTC(date, days) {
  const d = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
  d.setUTCDate(d.getUTCDate() + days);
  return d;
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
  if (!hasKlaviyoEnv() || !email) return { attempted: false, ok: false, reason: !hasKlaviyoEnv() ? "missing-klaviyo-config" : "missing-email" };
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
  const r = await klFetch("https://a.klaviyo.com/api/profile-subscription-bulk-create-jobs/", payload);
  return { attempted: true, ok: r.ok, status: r.status, body: r.body };
}

// Cancela assinatura de email na lista (sem tocar no Pipedrive)
async function klaviyoUnsubscribe({ email }) {
  if (!hasKlaviyoEnv() || !email) return { attempted: false, ok: false, reason: !hasKlaviyoEnv() ? "missing-klaviyo-config" : "missing-email" };
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
  const r = await klFetch("https://a.klaviyo.com/api/profile-subscription-bulk-delete-jobs/", payload);
  return { attempted: true, ok: r.ok, status: r.status, body: r.body };
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
    const email      = (subscriber.email || contact.email || "").trim();
    const fullName   = (subscriber.name || contact.name || "Assinante (sem nome)").trim();
    const phone      = (subscriber.phone_number || contact.phone_number || "").trim();

    const subscriptionCode = sub.subscription_code || sub.id || sub.internal_id || "";
    const planName   = sub.product?.name || sub.next_product?.name || sub.last_transaction?.product?.name || "Plano";
    const mrr        = Number(sub.current_invoice?.value || sub.last_transaction?.invoice?.value || 0);
    const lastStatus = sub.last_status || "unknown";
    const ls         = norm(lastStatus);

    // ciclo atual (para só criar no ciclo 1)
    const cycle = Number(sub.current_invoice?.cycle ?? sub.last_transaction?.invoice?.cycle ?? 0);
    const isFirstCycle = cycle === 1;

    // CANCELAMENTO (não mexe no Pipedrive)
    const cancelish =
      ["cancelada","cancelado","cancelled","canceled"].includes(ls) ||
      String(sub.cancel_at_cycle_end || "") === "1" ||
      Boolean(sub.dates?.canceled_at) ||
      Boolean((sub.cancel_reason || "").trim());

    // expected_close_date = (period_end | cycle_end_date) + 1 dia  |  fallback: mesmo dia do mês seguinte baseado no início do ciclo
    const periodEndRaw =
      sub?.current_invoice?.period_end ||
      sub?.last_transaction?.invoice?.period_end ||
      sub?.dates?.cycle_end_date ||
      null;

    const baseStartDate =
      parseDateUTC(sub?.current_invoice?.period_start) ||
      parseDateUTC(sub?.dates?.cycle_start_date) ||
      parseDateUTC(sub?.current_invoice?.charge_at) ||
      parseDateUTC(sub?.last_transaction?.invoice?.period_start) ||
      new Date();

    const periodEndDate = parseDateUTC(periodEndRaw);
    const expectedCloseDate = periodEndDate ? addDaysUTC(periodEndDate, 1) : sameDayNextMonthUTC(baseStartDate);
    const expectedClose = ymdUTC(expectedCloseDate);

    // ---------- resultado unificado ----------
    const out = {
      ok: true,
      meta: {
        status: lastStatus,
        cycle,
        subscription_code: subscriptionCode || null,
        email: email || null,
        plan: planName,
        expected_close_date: expectedClose
      },
      klaviyo: {
        attempted: false,
        action: null,
        ok: false,
        reason: null,
        status: null
      },
      pipedrive: {
        attempted: false,
        action: null,
        ok: false,
        reason: null,
        dealId: null,
        personId: null
      }
    };

    // ---------- KLAVIYO ----------
    try {
      if (cancelish) {
        out.klaviyo.action = "unsubscribe";
        const r = await klaviyoUnsubscribe({ email });
        Object.assign(out.klaviyo, r);
      } else if (ALLOW_CREATE.includes(ls)) {
        out.klaviyo.action = "subscribe";
        const r = await klaviyoSubscribe({ email, phone });
        Object.assign(out.klaviyo, r);
      } else {
        out.klaviyo.action = "none";
        out.klaviyo.reason = "status-not-allowed";
      }
    } catch (e) {
      out.klaviyo.attempted = true;
      out.klaviyo.ok = false;
      out.klaviyo.action = out.klaviyo.action || "subscribe";
      out.klaviyo.reason = e.message || "klaviyo-error";
    }

    // ---------- PIPEDRIVE ----------
    try {
      if (cancelish) {
        out.pipedrive.action = "none";
        out.pipedrive.reason = "cancellation-event";
      } else if (!subscriptionCode) {
        out.pipedrive.action = "none";
        out.pipedrive.reason = "missing-subscription-code";
      } else if (!ALLOW_CREATE.includes(ls)) {
        out.pipedrive.action = "none";
        out.pipedrive.reason = "status-not-allowed";
      } else if (!isFirstCycle) {
        out.pipedrive.action = "none";
        out.pipedrive.reason = "not-first-cycle";
      } else {
        // Idempotência
        if (DEAL_FIELD_SUBSCRIPTION) {
          const srch = await pdr(`/api/v2/deals/search?term=${encodeURIComponent(subscriptionCode)}&fields=custom_fields&exact_match=1`, { method: "GET" });
          const existingId = srch.json?.data?.items?.[0]?.item?.id || null;
          if (existingId) {
            out.pipedrive.action = "none";
            out.pipedrive.reason = "already-exists";
            out.pipedrive.dealId = existingId;
          } else {
            // Upsert da pessoa por email
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
                  phone: phone ? [{ value: phone, primary: true }] : undefined
                })
              });
              personId = created.json?.data?.id;
              if (!personId) throw new Error("person-create-failed");
            }

            const title = normPhone(phone) ? `(${normPhone(phone)}) (${planName})` : `${planName} – ${fullName}`;
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
              ...(DEAL_FIELD_SUBSCRIPTION ? { [DEAL_FIELD_SUBSCRIPTION]: subscriptionCode } : {})
            };

            const createdDeal = await pdr(`/api/v1/deals`, {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(payloadDeal)
            });

            out.pipedrive.attempted = true;
            out.pipedrive.action = "create-deal";
            out.pipedrive.ok = true;
            out.pipedrive.personId = personId;
            out.pipedrive.dealId = createdDeal.json?.data?.id || null;
          }
        } else {
          out.pipedrive.action = "none";
          out.pipedrive.reason = "missing-deal-field-subscription";
        }
      }
    } catch (e) {
      out.pipedrive.attempted = true;
      out.pipedrive.ok = false;
      out.pipedrive.action = out.pipedrive.action || "create-deal";
      out.pipedrive.reason = e.message || "pipedrive-error";
    }

    return res.status(200).json(out);
  } catch (e) {
    console.error(e);
    return res.status(500).json({ ok: false, error: e.message });
  }
}
