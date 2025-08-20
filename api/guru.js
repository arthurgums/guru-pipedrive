// api/guru.js
// Guru (Digital Manager) → Pipedrive + Klaviyo (Vercel, Node 20)
// - Só CRIA deals (Onboarding). Nunca atualiza/move.
// - Idempotência por subscription_code
// - expected_close_date = period_end | hoje+30
// - Klaviyo: subscribe em novos, unsubscribe em cancelados
// - Com diagnóstico detalhado (logs e retorno)

const {
  PIPEDRIVE_DOMAIN,
  PIPEDRIVE_TOKEN,
  PIPELINE_ID,
  STAGE_ID_ONBOARD,
  PERSON_OWNER_ID,
  DEAL_OWNER_ID,
  DEAL_FIELD_SUBSCRIPTION,
  GURU_API_TOKEN,
  KLAVIYO_API_KEY,
  KLAVIYO_LIST_ID
} = process.env;

const KLAVIYO_BASE = "https://a.klaviyo.com";

// ----------------- helpers comuns -----------------
const norm = (s) => (s || "").normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase();
const normPhone = (p) => (p || "").replace(/\D+/g, "");
function addDaysUTC(date, days) { const d = new Date(date.getTime()); d.setUTCDate(d.getUTCDate() + days); return d; }
function ymdUTC(d) { return d.toISOString().slice(0, 10); } // YYYY-MM-DD
function safeJson(t) { try { return JSON.parse(t); } catch { return t; } }
function redactUrl(u){ return (u || "").replace(/api_token=[^&]+/, "api_token=***"); }
function trimText(t, max=2000){ if(!t) return t; return t.length>max ? t.slice(0,max)+"…(truncated)" : t; }

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

// HTTP genérico com diagnóstico
async function httpJson(url, opts) {
  const res = await fetch(url, opts);
  const text = await res.text();
  let data; try { data = JSON.parse(text); } catch { data = null; }
  return { ok: res.ok, status: res.status, statusText: res.statusText, url, text, data };
}

// Pipedrive request com montagem de URL + diag
async function pdCall(path, opts = {}) {
  const url = `https://${PIPEDRIVE_DOMAIN}.pipedrive.com${path}${path.includes("?") ? "&" : "?"}api_token=${PIPEDRIVE_TOKEN}`;
  const r = await httpJson(url, opts);
  const diag = { ok: r.ok && (r.data?.success !== false), status: r.status, statusText: r.statusText, url: redactUrl(r.url), body: r.data ?? trimText(r.text) };
  // log enxuto
  console.log("[Pipedrive]", opts.method || "GET", r.status, redactUrl(r.url));
  if (!diag.ok) console.error("[Pipedrive ERROR]", diag);
  return { diag, data: r.data };
}

async function klaviyoSubscribe({ email, phone, listId }) {
  if (!KLAVIYO_API_KEY || !listId || !email) return { skipped: true };

  // 1) Moderno (recomendado)
  const modern = await fetch(`https://a.klaviyo.com/api/profile-subscription-bulk-create-jobs/`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Klaviyo-API-Key ${KLAVIYO_API_KEY}`,
      "revision": "2025-07-15"
    },
    body: JSON.stringify({
      data: {
        type: "profile-subscription-bulk-create-job",
        attributes: {
          list_id: listId,
          profiles: {
            data: [
              {
                type: "profile",
                attributes: {
                  email,
                  ...(phone ? { phone_number: phone } : {})
                  // Se quiser, pode adicionar: properties: { source: "guru" }
                }
              }
            ]
          }
        }
      }
    })
  });
  if (modern.ok) {
    return { ok: true, method: "bulk-create", status: modern.status };
  }
  const modernText = await modern.text();

  // 2) Legacy (v2) como fallback
  const legacy = await fetch(`https://a.klaviyo.com/api/v2/list/${listId}/subscribe`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      api_key: KLAVIYO_API_KEY,
      profiles: [{ email, ...(phone ? { phone_number: phone } : {}) }]
    })
  });
  if (legacy.ok) {
    return { ok: true, method: "v2", status: legacy.status };
  }
  const legacyText = await legacy.text();
  throw new Error(`Klaviyo subscribe failed. modern=${modern.status} ${modernText} | legacy=${legacy.status} ${legacyText}`);
}

async function klaviyoUnsubscribe({ email, listId }) {
  if (!KLAVIYO_API_KEY || !listId || !email) return { ok:false, skipped:true, reason:"missing_key_list_or_email" };

  // T1: endpoint simples (v2)
  try {
    const res = await fetch(`${KLAVIYO_BASE}/api/v2/list/${listId}/subscribe`, {
      method: "DELETE",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ api_key: KLAVIYO_API_KEY, emails: [email] })
    });
    const txt = await res.text();
    const body = safeJson(txt);
    const diag = { ok: res.ok, method: "v2/unsubscribe", status: res.status, body };
    console.log("[Klaviyo UNSUB v2]", res.status, body && typeof body === "string" ? trimText(body) : "");
    if (res.ok) return diag;
  } catch (e) {
    console.error("[Klaviyo UNSUB v2 error]", e.message);
  }

  // T2: moderno (bulk-delete)
  const res2 = await fetch(`${KLAVIYO_BASE}/api/profile-subscription-bulk-delete-jobs/`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Authorization": `Klaviyo-API-Key ${KLAVIYO_API_KEY}`,
      "revision": "2025-07-15"
    },
    body: JSON.stringify({ data: { type: "profile-subscription-bulk-delete-job", attributes: { list_id: listId, emails: [email] } } })
  });
  const txt2 = await res2.text();
  const body2 = safeJson(txt2);
  const diag2 = { ok: res2.ok, method: "bulk-delete", status: res2.status, body: body2 };
  console.log("[Klaviyo UNSUB bulk]", res2.status, typeof body2 === "string" ? trimText(body2) : "");
  return diag2;
}

// Status permitidos para CRIAR (pt/en)
const ALLOW_CREATE = (process.env.ALLOW_CREATE_STATUSES || "ativa,iniciada,trial,active,started,trialing")
  .split(",").map((s) => norm(s.trim()));

// ----------------- handler -----------------
export default async function handler(req, res) {
  try {
    if (req.method !== "POST") return res.status(405).send("Method Not Allowed");

    let sub = {};
    try { sub = await readJsonFromReq(req); }
    catch { return res.status(400).json({ ok: false, error: "Invalid JSON body" }); }

    // Alguns provedores mandam wrapper { payload: {...} }
    if (sub && sub.payload && typeof sub.payload === "object") sub = sub.payload;

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
    const [firstName, ...rest] = fullName.split(" ");
    const lastName = rest.join(" ") || undefined;
    const phone = (subscriber.phone_number || contact.phone_number || "").trim();

    const subscriptionCode = sub.subscription_code || sub.id || sub.internal_id || "";
    const planName   = sub.product?.name || sub.next_product?.name || sub.last_transaction?.product?.name || "Plano";
    const mrr        = Number(sub.current_invoice?.value || sub.last_transaction?.invoice?.value || 0);
    const lastStatus = sub.last_status || "unknown";

    const expectedCloseRaw =
      sub?.current_invoice?.period_end ||
      sub?.last_transaction?.invoice?.period_end ||
      sub?.dates?.cycle_end_date || null;
    const expectedClose = expectedCloseRaw && String(expectedCloseRaw).trim()
      ? String(expectedCloseRaw).trim()
      : ymdUTC(addDaysUTC(new Date(), 30));

    // ===== DIAGNÓSTICO =====
    const diag = { klaviyo: null, pipedrive: {} };

    // —— Klaviyo: subscribe em novos / unsubscribe em cancelados
    const ls = norm(lastStatus);
    if (email && ALLOW_CREATE.includes(ls)) {
      try { diag.klaviyo = await klaviyoSubscribe({ email, phone, firstName, lastName, listId: KLAVIYO_LIST_ID }); }
      catch (e) { diag.klaviyo = { ok:false, error:e.message }; }
    } else if (email && ["cancelada","cancelado","cancelled","canceled"].includes(ls)) {
      try { diag.klaviyo = await klaviyoUnsubscribe({ email, listId: KLAVIYO_LIST_ID }); }
      catch (e) { diag.klaviyo = { ok:false, error:e.message }; }
    }

    // —— Idempotência por subscription_code → se já existir, NÃO cria
    if (DEAL_FIELD_SUBSCRIPTION && subscriptionCode) {
      const dsearch = await pdCall(`/api/v2/deals/search?term=${encodeURIComponent(subscriptionCode)}&fields=custom_fields&exact_match=1`, { method: "GET" });
      diag.pipedrive.deals_search = dsearch.diag;
      const existingId = dsearch?.data?.data?.items?.[0]?.item?.id || null;
      if (existingId) {
        return res.status(200).json({
          ok: true,
          skipped: true,
          reason: "already-exists",
          dealId: existingId,
          klaviyo: diag.klaviyo,
          diag
        });
      }
    }

    // —— Só cria para status “novos”
    if (!ALLOW_CREATE.includes(ls)) {
      return res.status(200).json({
        ok: true,
        skipped: true,
        reason: "status-not-allowed",
        lastStatus,
        klaviyo: diag.klaviyo,
        diag
      });
    }

    // —— Upsert da PESSOA (cria se não existir por email)
    let personId = null;
    if (email) {
      const psearch = await pdCall(`/api/v2/persons/search?term=${encodeURIComponent(email)}&fields=email&exact_match=1`, { method: "GET" });
      diag.pipedrive.persons_search = psearch.diag;
      personId = psearch?.data?.data?.items?.[0]?.item?.id || null;
    }
    if (!personId) {
      const created = await pdCall(`/api/v1/persons`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: fullName,
          owner_id: PERSON_OWNER_ID ? Number(PERSON_OWNER_ID) : undefined,
          visible_to: 3,
          email: email ? [{ value: email, primary: true }] : undefined,
          phone: phone ? [{ value: phone, primary: true }] : undefined
          // se tiver campo custom na Pessoa para CPF, adicione aqui
        })
      });
      diag.pipedrive.persons_create = created.diag;
      if (!created.diag.ok) {
        return res.status(502).json({ ok:false, error:"Failed to create person in Pipedrive", diag });
      }
      personId = created?.data?.data?.id || null;
      if (!personId) {
        return res.status(502).json({ ok:false, error:"Missing personId after create", diag });
      }
    }

    // —— Criar DEAL (sempre no estágio de onboarding)
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
      ...(DEAL_FIELD_SUBSCRIPTION && subscriptionCode ? { [DEAL_FIELD_SUBSCRIPTION]: subscriptionCode } : {})
    };

    const createdDeal = await pdCall(`/api/v1/deals`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payloadDeal)
    });
    diag.pipedrive.deals_create = { ...createdDeal.diag, requestBody: payloadDeal };

    if (!createdDeal.diag.ok) {
      return res.status(502).json({ ok:false, error:"Failed to create deal in Pipedrive", diag });
    }
    const dealId = createdDeal?.data?.data?.id || null;
    if (!dealId) {
      return res.status(502).json({ ok:false, error:"Missing dealId after create", diag });
    }

    return res.status(200).json({
      ok: true,
      personId,
      dealId,
      status: lastStatus,
      mrr,
      expected_close_date: expectedClose,
      klaviyo: diag.klaviyo,
      diag
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ ok: false, error: e.message });
  }
}
