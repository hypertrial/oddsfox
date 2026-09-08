"use strict";
const $ = id => document.getElementById(id);
const node = (tag, text, cls) => { const el = document.createElement(tag); if (text !== undefined) el.textContent = text; if (cls) el.className = cls; return el; };
const pretty = x => JSON.stringify(x, null, 2);
function notice(text, error = false) { $("notice").replaceChildren(node("div", text, error ? "error" : "panel")); }
async function api(path, body) {
  const response = await fetch(path, body === undefined ? {} : {method:"POST",headers:{"Content-Type":"application/json","X-Oddsfox-Token":$("token").value},body:JSON.stringify(body)});
  const data = await response.json(); if (!response.ok) throw new Error(typeof data.detail === "string" ? data.detail : pretty(data.detail)); return data;
}
function action(label, callback, reload = true) { const b = node("button",label); b.addEventListener("click",async()=>{b.disabled=true;try{await callback();if(reload)await refresh();}catch(e){notice(e.message,true);}finally{b.disabled=false;}});return b; }
function details(label, data) { const el=node("details");el.append(node("summary",label),node("pre",typeof data === "string" ? data : pretty(data)));return el; }
function link(label,path){const a=node("a",label);a.href=path;return a;}
async function refresh(){
  const data=await api("/api/report");
  $("metrics").replaceChildren(...[[data.contracts.length,"Captured contracts"],[data.interpretations.length,"Current interpretations"],[data.assertions.length,"Accepted claims"],[data.status.pending_review,"Awaiting review"]].map(([n,label])=>{const el=node("div",undefined,"metric");el.append(node("strong",n),node("span",label));return el;}));
  $("contract-list").replaceChildren();
  for(const c of data.contracts){
    const el=node("article",undefined,"card"), meta=c.data.metadata;
    el.append(node("span",({kalshi:"Kalshi",polymarket:"Polymarket International"}[c.data.platform]||c.data.platform),"badge"),node("span",meta.capture_status,"badge warning"),node("h3",meta.title||c.logical));
    const last=data.freshness.find(r=>r.logical===c.logical);el.append(node("p",last?`Last retrieval: ${last.retrieved} · ${last.success?"successful":"FAILED — historical capture retained"}`:"No retrieval evidence","small muted"));
    const evidence=node("details");evidence.append(node("summary","Captured evidence & version"),node("p",c.id,"small"));
    for(const [name,id] of Object.entries(c.data.text_artifacts)){const p=node("p");p.append(link(name,`/api/artifacts/${id}`));evidence.append(p);}evidence.append(details("Referenced material",c.data.references));el.append(evidence);
    const interpretation=data.interpretations.find(i=>i.data.ir.contract_version_id===c.id);
    if(interpretation){
      el.append(node("p",`Source: ${interpretation.data.ir.observation.source || "unresolved"} · ${interpretation.data.ir.observation.measurement_method || "unknown method"}`,"small muted"));
      const review=data.reviews.find(r=>r.logical===interpretation.id);
      el.append(node("p",review?(review.data.approved?"REVIEWED":"REJECTED / WITHDRAWN"):interpretation.data.assessment,"badge"),details("Semantic interpretation & evidence",interpretation.data.ir),link("Canonical IR JSON",`/api/interpretations/${interpretation.id}/ir`));
      const quotes=node("details");quotes.append(node("summary","Source excerpts for each semantic field"));quotes.addEventListener("toggle",async()=>{if(!quotes.open || quotes.dataset.loaded)return;try{const rows=await api(`/api/interpretations/${interpretation.id}/evidence`);for(const row of rows){quotes.append(node("p",`${row.field}: ${pretty(row.value)}`,"small"));for(const source of row.sources)quotes.append(node("blockquote",source.text));}quotes.dataset.loaded="true";}catch(e){notice(e.message,true);}});el.append(quotes);
      const reviewBox=node("details");reviewBox.append(node("summary","Review this exact interpretation"),node("p","Check each populated field against its quotation. Approval covers this IR and its exact dependencies; solver results do not establish language accuracy."));
      const rationale=node("input");rationale.placeholder="Rationale and unresolved issues";const label=node("label","Review rationale");label.append(rationale);reviewBox.append(label);
      const attestation=node("input");attestation.type="checkbox";const attestLabel=node("label",undefined,"inline");attestLabel.append(attestation,node("span","I checked the complete governing material and the exact canonical observation definition."));reviewBox.append(attestLabel);
      const actions=node("div",undefined,"actions");for(const approve of [true,false])actions.append(action(approve?"Approve interpretation":"Reject / withdraw approval",()=>api(`/api/reviews/${interpretation.id}`,{reviewer:$("reviewer").value,rationale:rationale.value,approve,governing_material_complete:attestation.checked})));reviewBox.append(actions);el.append(reviewBox);
    }else el.append(node("p","No interpretation yet. Start a candidate or run your configured local model.","muted"));
    const edit=node("details");edit.append(node("summary",interpretation?"Correct interpretation":"Create interpretation candidate"));const input=node("textarea");input.rows=12;input.setAttribute("aria-label","Semantic IR JSON");input.value=interpretation?pretty(interpretation.data.ir):"";edit.append(input);
    edit.append(action("Load empty schema",async()=>{input.value=pretty(await api(`/api/contracts/${c.id}/draft`));},false),action("Save candidate",()=>api("/api/interpret",{ir:JSON.parse(input.value),derivations:interpretation?.data.derivations||{}})));el.append(edit);
    for(const model of data.models)el.append(action(`Compile with ${model}`,()=>api("/api/compile",{contract_version_id:c.id,model_name:model})));
    $("contract-list").append(el);
  }
  if(!data.contracts.length)$("contract-list").append(node("div","Your research set starts here. Import captured market JSON or retrieve a bounded list of native IDs above.","empty"));
  $("comparison-list").replaceChildren();
  if(data.near_match_coverage&&!data.near_match_coverage.complete)$("comparison-list").append(node("p",`Legacy near-match view covers ${data.near_match_coverage.processed} of ${data.near_match_coverage.total} interpretations. Use event details for indexed cross-venue suggestions.`,"condition"));
  const titles=Object.fromEntries(data.contracts.map(c=>[c.id,c.data.metadata.title||c.logical]));
  for(const claim of data.comparisons){const accepted=data.assertions.some(a=>a.logical===claim.claim_id);const el=node("article",undefined,"card");el.append(node("span",accepted?"ACCEPTED":"PROVISIONAL",accepted?"badge success":"badge warning"),node("span",claim.scope === "OBSERVED_EVENT" ? "OBSERVED EVENTS" : "SETTLEMENT OUTCOMES","badge"),node("h3",`${titles[claim.a]} ${claim.relation} ${titles[claim.b]}`),node("p",`${claim.proof.state} · settlement ${claim.settlement.state}`));for(const condition of claim.conditions)el.append(node("p",condition,"condition"));el.append(node("p",claim.constraint.expression.replaceAll(claim.a,"A").replaceAll(claim.b,"B")),details("Premises, proof, source evidence & dependencies",claim));$("comparison-list").append(el);}
  for(const mismatch of data.near_matches){const el=node("article",undefined,"card");el.append(node("span","NEAR-MATCH / NOT COMPARABLE","badge warning"),node("h3",`${titles[mismatch.a]} · ${titles[mismatch.b]}`),node("p",mismatch.reason),details("Observation differences",mismatch.differences));$("comparison-list").append(el);}
  if(!data.comparisons.length)$("comparison-list").append(node("div","No eligible shared observation yet. Incomplete semantics and different sources, times or methods remain separate; review the contract details.","empty"));
  $("activity-list").replaceChildren(details("Jobs, failures, freshness and pending review",data.status));
}
$("refresh").addEventListener("click",()=>refresh().catch(e=>notice(e.message,true)));
for(const [id,fn]of[["capture",()=>api("/api/capture",{platform:$("venue").value,native_ids:$("ids").value.split(",").map(x=>x.trim()).filter(Boolean)})],["import",()=>api("/api/import",{platform:$("venue").value,payload:$("payload").value})],["publish",()=>api("/api/publish",{})],["register",()=>api("/api/registry",{canonical_id:$("registry-name").value,observation:JSON.parse($("registry-json").value),reviewer:$("reviewer").value,rationale:$("registry-rationale").value})]])$(id).addEventListener("click",async()=>{try{await fn();notice("Action recorded. Check processing and review state below.");await refresh();}catch(e){notice(e.message,true);}});
refresh().catch(e=>notice(e.message,true));
