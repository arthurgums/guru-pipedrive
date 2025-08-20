// api/guru.js
// Webhook Guru (Digital Manager) → Pipedrive (Vercel, Node 20)

// === OBRIGATÓRIAS ===
// PIPEDRIVE_DOMAIN, PIPEDRIVE_TOKEN, PIPELINE_ID, STAGE_ID_ONBOARD, STAGE_ID_CHURN, DEAL_FIELD_SUBSCRIPTION
//
// === OPCIONAIS ===
// STAGE_ID_PEND, PERSON_OWNER_ID, DEAL_OWNER_ID, GURU_API_TOKEN
// ALLOW_CREATE_STATUSES  (default: "ativa,iniciada,trial,active,started,trialing")
//
// === CAMPOS EXTRAS OPCIONAIS (use as KEYS dos fields no Pipedrive) ===
// DEAL_FIELD_NOME_PRODUTO
// DEAL_FIELD_ORIGEM                (ex.: "Guru")
// DEAL_FIELD_ID_ORIGEM             (ex.: subscription_code)
// DEAL_FIELD_CANAL_ORIGEM          (ex.: "Assinatura")
// DEAL_FIELD_ID_CANAL_ORIGEM       (ex.: "guru_subscription")
// DEAL_FIELD_CPF, DEAL_FIELD_WHATSAPP, DEAL_FIELD_TELEFONE
// DEAL_FIELD_LINK_RASTREIO, DEAL_FIELD_LINK
// DEAL_FIELD_UTM_SOURCE, DEAL_FIELD_UTM_MEDIUM, DEAL_FIELD_UTM_CAMPAIGN, DEAL_FIELD_UTM_CONTENT, DEAL_FIELD_UTM_TERM
// DEAL_FIELD_INVOICE_STATUS, DEAL_FIELD_INVOICE_CODE, DEAL_FIELD_INVOICE_URL, DEAL_FIELD_PAYMENT_METHOD
// DEAL_FIELD_CYCLE_START_DATE, DEAL_FIELD_CYCLE_END_DATE, DEAL_FIELD_NEXT_CYCLE_DATE
// DEAL_FIELD_TRIAL_START_DATE, DEAL_FIELD_TRIAL_END_DATE
// DEAL_FIELD_CARD_BRAND, DEAL_FIELD_CARD_LAST4

const {
  PIPEDRIVE_DOMAIN,
  PIPEDRIVE_TOKEN,
  PIPELINE_ID,
  STAGE_ID_ONBOARD,
  STAGE_ID_PEND,
  STAGE_ID_CHURN,
  PERSON_OWNER_ID,
  DEAL_OWNER_ID,
  DEAL_FIELD_SUBSCRIPTION,
  GURU_API_TOKEN
} = process.env;

// ---------------- utils ----------------
async function pdr(path, opts = {}) {
  const url = `https://${PIPEDRIVE_DOMAIN}.pipedrive.com${path}${path.includes("?") ? "&" : "?"}api_token=${PIPEDRIVE_TOKEN}`;
  const res = await fetch(url, opts);
  const json = await res.json().catch(() => ({}));
  if (!res.ok || json.success === false) throw new Error(`Pipedrive error: ${res.status} ${res.statusText} ${JSON.stringify(json)}`);
  return json;
}
const numOrNull = v => (v!==undefined && v!==null && v!=="" && !Number.isNaN(Number(v))) ? Number(v) : null;
const norm = s => (s || "").normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase();
const normPhone = p => (p||"").replace(/\D+/g,"");
const setField = (key, value) => (key && value!==undefined && value!==null && value!=="") ? { [key]: value } : {};
const get = (obj, path, def) => path.split(".").reduce((o,k)=> (o && o[k]!==undefined ? o[k] : undefined), obj) ?? def;

function shortProductName(name){
  const s = (name||"").toLowerCase();
  if (s.includes("greens")) return "greens";
  if (s.includes("natural fire") || s.includes("fire")) return "fire";
  if (s.includes("premium")) return "premium";
  return (name||"").trim();
}

function resolveStage({ lastStatus, invoiceStatus }) {
  const onboard = numOrNull(STAGE_ID_ONBOARD);
  const churn   = numOrNull(STAGE_ID_CHURN);
  const pend    = numOrNull(STAGE_ID_PEND);

  const ls = norm(lastStatus);
  if (["cancelled","canceled","cancelada","cancelado"].includes(ls)) return churn ?? onboard;

  const inv = norm(invoiceStatus);
  if (["unpaid","overdue","pending","atrasada","pendente"].includes(inv)) return pend ?? onboard;

  return onboard;
}

// status que PODEM CRIAR deal (pt/en)
const ALLOW_CREATE = (process.env.ALLOW_CREATE_STATUSES || "ativa,iniciada,trial,active,started,trialing")
  .split(",").map(s => norm(s.trim()));

// ---- leitura robusta do corpo (evita "Invalid JSON") ----
async function readRaw(req) {
  const chunks = [];
  for await (const c of req) chunks.push(typeof c === "string" ? Buffer.from(c) : c);
  return Buffer.concat(chunks).toString("utf8");
}
async function readJsonFromReq(req) {
  if (req.body && typeof req.body === "object") return req.body;        // já veio parseado
  if (typeof req.body === "string") return JSON.parse(req.body);        // string JSON
  const raw = (await readRaw(req)).trim();                               // stream cru
  return raw ? JSON.parse(raw) : {};
}

// ---------------- handler ----------------
export default async function handler(req, res) {
  try {
    if (req.method !== "POST") return res.status(405).send("Method Not Allowed");

    let body = {};
    try { body = await readJsonFromReq(req); }
    catch { return res.status(400).json({ ok:false, error:"Invalid JSON body" }); }

    // Secret (body.api_token, ?secret=, header x-webhook-secret)
    const providedSecret = body.api_token || (req.query && req.query.secret) || req.headers["x-webhook-secret"];
    if (GURU_API_TOKEN && providedSecret !== GURU_API_TOKEN) {
      return res.status(401).json({ ok:false, error:"invalid token" });
    }

    if (body.webhook_type && body.webhook_type !== "subscription") return res.status(204).end();

    // ---------- map payload ----------
    const sub = body;
    const contact    = sub.last_transaction?.contact || {};
    const subscriber = sub.subscriber || {};
    const source     = sub.last_transaction?.source || {};

    const email = (subscriber.email || contact.email || "").trim();
    const name  = (subscriber.name  || contact.name  || "Assinante (sem nome)").trim();
    const phone = (subscriber.phone_number || contact.phone_number || "").trim();
    const cpf   = (subscriber.doc || contact.doc || "").trim();

    const subscriptionCode = sub.subscription_code || sub.id || sub.internal_id || "";
    const planName   = sub.product?.name || sub.next_product?.name || sub.last_transaction?.product?.name || "Plano";
    const mrr        = Number(get(sub, "current_invoice.value", get(sub, "last_transaction.invoice.value", get(sub, "last_transaction.product.unit_value", 0))));
    const invoiceSt  = sub.current_invoice?.status || sub.last_transaction?.invoice?.status || "";
    const invoiceCode= sub.current_invoice?.code || "";
    const invoiceUrl = sub.current_invoice?.payment_url || sub.last_transaction?.payment?.billet?.url || sub.last_transaction?.payment?.pix?.qrcode?.url || "";
    const lastStatus = sub.last_status || "unknown";

    // datas principais
    const nextCycle  = sub.dates?.next_cycle_at || sub.current_invoice?.charge_at || null;
    const cycleStart = sub.dates?.cycle_start_date || sub.last_transaction?.invoice?.period_start || null;
    const cycleEnd   = sub.dates?.cycle_end_date   || sub.last_transaction?.invoice?.period_end   || null;
    const trialStart = sub.trial_started_at || null;
    const trialEnd   = sub.trial_finished_at || null;

    // pagamento
    const paymentMethod = sub.payment_method || sub.last_transaction?.payment?.method || sub.credit_card?.brand || "";

    // UTM
    const utm = {
      source:   source.utm_source   || "",
      medium:   source.utm_medium   || "",
      campaign: source.utm_campaign || "",
      content:  source.utm_content  || "",
      term:     source.utm_term     || ""
    };

    // ---------- upsert Person ----------
    let personId = null;
    if (email) {
      const search = await pdr(`/api/v2/persons/search?term=${encodeURIComponent(email)}&fields=email&exact_match=1`, { method:"GET" });
      personId = search?.data?.items?.[0]?.item?.id || null;
    }
    if (!personId) {
      const created = await pdr(`/api/v1/persons`, {
        method: "POST",
        headers: { "Content-Type":"application/json" },
        body: JSON.stringify({
          name,
          owner_id: PERSON_OWNER_ID ? Number(PERSON_OWNER_ID) : undefined,
          visible_to: 3,
          email: email ? [{ value: email, primary:true }] : undefined,
          phone: phone ? [{ value: phone, primary:true }] : undefined
          // se tiver campo custom p/ CPF na pessoa: "hash_cpf": cpf || undefined
        })
      });
      personId = created?.data?.id;
      if (!personId) throw new Error("Falha ao criar pessoa");
    }

    // ---------- idempotência por subscription_code ----------
    let dealId = null;
    if (DEAL_FIELD_SUBSCRIPTION && subscriptionCode) {
      const dsearch = await pdr(`/api/v2/deals/search?term=${encodeURIComponent(subscriptionCode)}&fields=custom_fields&exact_match=1`, { method:"GET" });
      dealId = dsearch?.data?.items?.[0]?.item?.id || null;
    }

    // regra: só cria se status “novo”
    const ls = norm(lastStatus);
    const canCreate = ALLOW_CREATE.includes(ls);
    if (!dealId && !canCreate) {
      return res.status(200).json({ ok:true, skipped:true, reason:"creation-not-allowed-for-status", lastStatus });
    }

    const desiredStage = resolveStage({ lastStatus, invoiceStatus: invoiceSt });

    // ---------- campos extras opcionais ----------
    const phoneClean   = normPhone(phone);
    const titleForCard = phoneClean ? `(${phoneClean}) (${shortProductName(planName)})` : `${planName} – ${name}`;

    const extraFields = {
      // origem básica
      ...setField(process.env.DEAL_FIELD_ORIGEM, "Guru"),
      ...setField(process.env.DEAL_FIELD_ID_ORIGEM, subscriptionCode),
      ...setField(process.env.DEAL_FIELD_CANAL_ORIGEM, "Assinatura"),
      ...setField(process.env.DEAL_FIELD_ID_CANAL_ORIGEM, "guru_subscription"),

      // produto
      ...setField(process.env.DEAL_FIELD_NOME_PRODUTO, planName),

      // pessoa/contato
      ...setField(process.env.DEAL_FIELD_CPF, cpf),
      ...setField(process.env.DEAL_FIELD_WHATSAPP, phoneClean),
      ...setField(process.env.DEAL_FIELD_TELEFONE, phoneClean),

      // links
      ...setField(process.env.DEAL_FIELD_LINK_RASTREIO, get(sub, "last_transaction.shipment.tracking", "") || get(sub, "last_transaction.shipment.tracking_url", "")),
      ...setField(process.env.DEAL_FIELD_LINK, sub.public_url || get(sub, "last_transaction.checkout_url", "")),

      // utm
      ...setField(process.env.DEAL_FIELD_UTM_SOURCE, utm.source),
      ...setField(process.env.DEAL_FIELD_UTM_MEDIUM, utm.medium),
      ...setField(process.env.DEAL_FIELD_UTM_CAMPAIGN, utm.campaign),
      ...setField(process.env.DEAL_FIELD_UTM_CONTENT, utm.content),
      ...setField(process.env.DEAL_FIELD_UTM_TERM, utm.term),

      // fatura/pagamento
      ...setField(process.env.DEAL_FIELD_INVOICE_STATUS, invoiceSt),
      ...setField(process.env.DEAL_FIELD_INVOICE_CODE, invoiceCode),
      ...setField(process.env.DEAL_FIELD_INVOICE_URL, invoiceUrl),
      ...setField(process.env.DEAL_FIELD_PAYMENT_METHOD, paymentMethod),

      // datas
      ...setField(process.env.DEAL_FIELD_CYCLE_START_DATE, cycleStart),
      ...setField(process.env.DEAL_FIELD_CYCLE_END_DATE, cycleEnd),
      ...setField(process.env.DEAL_FIELD_NEXT_CYCLE_DATE, nextCycle),
      ...setField(process.env.DEAL_FIELD_TRIAL_START_DATE, trialStart),
      ...setField(process.env.DEAL_FIELD_TRIAL_END_DATE, trialEnd),

      // cartão (se quiser)
      ...setField(process.env.DEAL_FIELD_CARD_BRAND, get(sub, "credit_card.brand", "")),
      ...setField(process.env.DEAL_FIELD_CARD_LAST4, get(sub, "credit_card.last_four", "") || get(sub, "last_transaction.payment.creditcard.last_digits", ""))
    };

    // ---------- criar/atualizar deal ----------
    if (!dealId) {
      const payload = {
        title: titleForCard,
        person_id: personId,
        pipeline_id: Number(PIPELINE_ID),
        value: mrr,
        currency: "BRL",
        owner_id: DEAL_OWNER_ID ? Number(DEAL_OWNER_ID) : undefined,
        status: "open",
        ...(desiredStage != null ? { stage_id: desiredStage } : {}),
        ...(DEAL_FIELD_SUBSCRIPTION && subscriptionCode ? { [DEAL_FIELD_SUBSCRIPTION]: subscriptionCode } : {}),
        ...extraFields
      };
      const createdDeal = await pdr(`/api/v1/deals`, {
        method: "POST",
        headers: { "Content-Type":"application/json" },
        body: JSON.stringify(payload)
      });
      dealId = createdDeal?.data?.id || null;
    } else {
      const payload = {
        value: mrr,
        ...(desiredStage != null ? { stage_id: desiredStage } : {}),
        ...(DEAL_FIELD_SUBSCRIPTION && subscriptionCode ? { [DEAL_FIELD_SUBSCRIPTION]: subscriptionCode } : {}),
        ...extraFields
      };
    await pdr(`/api/v1/deals/${dealId}`, {
        method: "PUT",
        headers: { "Content-Type":"application/json" },
        body: JSON.stringify(payload)
      });
    }

    return res.status(200).json({
      ok: true,
      personId,
      dealId,
      status: lastStatus,
      invoiceStatus: invoiceSt,
      mrr,
      nextCycle
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ ok:false, error: e.message });
  }
}
