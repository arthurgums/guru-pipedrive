// api/guru.js
// Vercel Node.js Function — recebe webhook da Guru e upserta no Pipedrive

const {
  PIPEDRIVE_DOMAIN,
  PIPEDRIVE_TOKEN,
  PIPELINE_ID,
  STAGE_ID_ONBOARD,
  STAGE_ID_PEND,        // opcional
  STAGE_ID_CHURN,
  PERSON_OWNER_ID,      // opcional
  DEAL_OWNER_ID,        // opcional
  DEAL_FIELD_SUBSCRIPTION,
  GURU_API_TOKEN        // opcional (segredo do webhook)
} = process.env;

// ---- Helpers ---------------------------------------------------------------
async function pdr(path, opts = {}) {
  const url = `https://${PIPEDRIVE_DOMAIN}.pipedrive.com${path}${path.includes("?") ? "&" : "?"}api_token=${PIPEDRIVE_TOKEN}`;
  const res = await fetch(url, opts);
  const json = await res.json().catch(() => ({}));
  if (!res.ok || json.success === false) {
    throw new Error(`Pipedrive error: ${res.status} ${res.statusText} ${JSON.stringify(json)}`);
  }
  return json;
}

function numOrNull(v){
  return (v!==undefined && v!==null && v!=="" && !Number.isNaN(Number(v))) ? Number(v) : null;
}

function resolveStage({ lastStatus, invoiceStatus }) {
  const onboard = numOrNull(STAGE_ID_ONBOARD);
  const churn   = numOrNull(STAGE_ID_CHURN);
  const pend    = numOrNull(STAGE_ID_PEND); // pode ser null

  if (lastStatus === "cancelled") return churn ?? onboard;
  if (["unpaid","overdue","pending"].includes((invoiceStatus || "").toLowerCase())) {
    return pend ?? onboard; // fallback para Onboarding se não existir PEND
  }
  return onboard;
}

// ---- Handler ---------------------------------------------------------------
export default async function handler(req, res) {
  try {
    if (req.method !== "POST") return res.status(405).send("Method Not Allowed");

    const body = req.body || {};
    if (!body || typeof body !== "object") {
      return res.status(400).json({ ok: false, error: "Invalid JSON" });
    }

    // Validação opcional do segredo
    if (GURU_API_TOKEN && body.api_token && body.api_token !== GURU_API_TOKEN) {
      return res.status(401).json({ ok: false, error: "invalid token" });
    }

    // Apenas eventos de assinatura
    if (body.webhook_type && body.webhook_type !== "subscription") {
      return res.status(204).end();
    }

    // ----- Mapear payload da Guru -----
    const sub        = body;
    const contact    = sub.last_transaction?.contact || {};
    const subscriber = sub.subscriber || {};

    const email = (subscriber.email || contact.email || "").trim();
    const name  = (subscriber.name  || contact.name  || "Assinante (sem nome)").trim();
    const phone = (subscriber.phone_number || contact.phone_number || "").trim();
    const cpf   = (subscriber.doc || contact.doc || "").trim();

    const subscriptionCode = sub.subscription_code || sub.id || "";
    const planName   = sub.product?.name || sub.next_product?.name || "Plano";
    const mrr        = Number(sub.current_invoice?.value || sub.last_transaction?.invoice?.value || 0);
    const invoiceSt  = sub.current_invoice?.status || "";
    const lastStatus = sub.last_status || "unknown";
    const nextCharge = sub.dates?.next_cycle_at || sub.current_invoice?.charge_at || null;

    // ----- Upsert Pessoa -----
    let personId = null;
    if (email) {
      const search = await pdr(`/api/v2/persons/search?term=${encodeURIComponent(email)}&fields=email&exact_match=1`, { method: "GET" });
      personId = search?.data?.items?.[0]?.item?.id || null;
    }
    if (!personId) {
      const created = await pdr(`/api/v1/persons`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name,
          owner_id: PERSON_OWNER_ID ? Number(PERSON_OWNER_ID) : undefined,
          visible_to: 3,
          email: email ? [{ value: email, primary: true }] : undefined,
          phone: phone ? [{ value: phone, primary: true }] : undefined
          // Se tiver campo custom para CPF: "hash_cpf": cpf || undefined
        })
      });
      personId = created?.data?.id;
      if (!personId) throw new Error("Falha ao criar pessoa");
    }

    // ----- Idempotência por subscription_code no Deal -----
    let dealId = null;
    if (DEAL_FIELD_SUBSCRIPTION && subscriptionCode) {
      const dsearch = await pdr(`/api/v2/deals/search?term=${encodeURIComponent(subscriptionCode)}&fields=custom_fields&exact_match=1`, { method: "GET" });
      dealId = dsearch?.data?.items?.[0]?.item?.id || null;
    }

    const desiredStage = resolveStage({ lastStatus, invoiceStatus: invoiceSt });

    if (!dealId) {
      // Criar Deal
      const payload = {
        title: `${planName} – ${name}`,
        person_id: personId,
        pipeline_id: Number(PIPELINE_ID),
        value: mrr,
        currency: "BRL",
        owner_id: DEAL_OWNER_ID ? Number(DEAL_OWNER_ID) : undefined,
        status: "open",
        ...(desiredStage != null ? { stage_id: desiredStage } : {}),
        ...(DEAL_FIELD_SUBSCRIPTION && subscriptionCode ? { [DEAL_FIELD_SUBSCRIPTION]: subscriptionCode } : {})
        // Campos custom adicionais se quiser
        // "hash_status_assinatura": lastStatus,
        // "hash_proxima_cobranca": nextCharge,
        // "hash_plano": planName,
        // "hash_cpf": cpf
      };
      const createdDeal = await pdr(`/api/v2/deals`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      dealId = createdDeal?.data?.id || null;
    } else {
      // Atualizar Deal
      const payload = {
        value: mrr,
        ...(desiredStage != null ? { stage_id: desiredStage } : {}),
        ...(DEAL_FIELD_SUBSCRIPTION && subscriptionCode ? { [DEAL_FIELD_SUBSCRIPTION]: subscriptionCode } : {})
      };
      await pdr(`/api/v2/deals/${dealId}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
    }

    return res.status(200).json({
      ok: true,
      personId,
      dealId,
      status: lastStatus,
      invoiceStatus: invoiceSt,
      nextCharge
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ ok: false, error: e.message });
  }
}
