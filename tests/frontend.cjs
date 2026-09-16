const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const {test} = require('node:test');

class Element {
  constructor(tag) { this.tag=tag; this.children=[]; this.listeners={}; this.dataset={}; this.value=''; }
  append(...children) { this.children.push(...children); }
  replaceChildren(...children) { this.children=children; }
  addEventListener(name, callback) { this.listeners[name]=callback; }
  setAttribute(name, value) { if (name === 'aria-label') this.ariaLabel = value; }
  contains(node) { return walk(this).includes(node); }
  querySelectorAll(selector) { return walk(this).slice(1).filter(n => selector==='[data-focus-key]' ? n.dataset.focusKey : n.tag==='details' && (selector!=='details[open]' || n.open)); }
}
function walk(el) { return [el, ...el.children.flatMap(child => typeof child === 'object' ? walk(child) : [])]; }

test('loading a draft preserves the visible editor and saving still refreshes', async () => {
  const elements={}, requests=[];
  const document={getElementById: id => elements[id] ??= new Element('div'), createElement: tag => new Element(tag)};
  const draft={contract_version_id:'a', observation:{source:null}};
  const report={contracts:[{id:'a', logical:'kalshi:a', data:{platform:'kalshi',metadata:{title:'A'},text_artifacts:{},references:[]}}],interpretations:[],assertions:[],reviews:[],freshness:[],comparisons:[],near_matches:[],models:[],status:{pending_review:0}};
  const fetch=async (path, options) => {
    requests.push({path,options});
    return {ok:true, json:async () => path.endsWith('/draft') ? draft : report};
  };
  vm.runInNewContext(fs.readFileSync('src/oddsfox/static/app.js','utf8'), {document,fetch});
  await new Promise(resolve => setImmediate(resolve));
  const find=text => walk(elements['contract-list']).find(el => el.textContent===text);
  await find('Load empty schema').listeners.click();
  const editor=walk(elements['contract-list']).find(el => el.tag==='textarea');
  assert.equal(find('Load empty schema').type, 'button');
  assert.equal(find('Load empty schema').className, 'secondary');
  assert.equal(find('Save candidate').type, 'button');
  assert.deepEqual(JSON.parse(editor.value),draft);
  const edited={...draft,observation:{source:'Edited source'}};
  editor.value=JSON.stringify(edited);
  await find('Save candidate').listeners.click();
  const saved=requests.find(request => request.path==='/api/interpret');
  assert.deepEqual(JSON.parse(saved.options.body),{ir:edited,derivations:{}});
  assert.equal(requests.filter(request => request.path==='/api/report').length,2);
});

test('event browser paginates, resets filters, and authenticates pause without running inference', async () => {
  Object.defineProperty(Element.prototype, 'firstChild', {get(){return this.children[0];},configurable:true});
  const elements={}, requests=[];
  const document={hidden:false,getElementById:id=>elements[id]??=new Element('div'),createElement:tag=>new Element(tag)};
  document.getElementById('filter-qualification').value='qualified';
  document.getElementById('token').value='fixture-session';
  let paused=false;
  const fetch=async(path, options)=>{
    requests.push({path,options});
    if(path==='/api/sync/pause')paused=JSON.parse(options.body).paused;
    const data=path.startsWith('/api/events?') ? {total:31,items:[],next_offset:path.includes('offset=30')?null:30} :
      {counts:[],analysis:[],models:[],lanes:[],categories:[],coverage_notes:[],formal_verification:{groups:0},venues:[{venue:'kalshi',paused,data:{state:'complete'}}]};
    return {ok:true,json:async()=>data};
  };
  vm.runInNewContext(fs.readFileSync('src/oddsfox/static/events.js','utf8'), {document,fetch,URLSearchParams,Intl,setInterval:()=>{}});
  const settle=()=>new Promise(resolve=>setImmediate(resolve));
  await settle();
  assert(requests.some(r=>r.path.includes('limit=30')));
  elements.next.listeners.click();await settle();
  assert(requests.some(r=>r.path.includes('offset=30')));
  elements['filter-qualification'].value='unknown';
  elements['filter-qualification'].listeners.change();await settle();
  assert(requests.at(-2).path.includes('offset=0'));
  assert(requests.at(-2).path.includes('qualification=unknown'));
  await elements.pause.listeners.click();
  const mutation=requests.find(r=>r.path==='/api/sync/pause');
  assert.equal(mutation.options.headers['X-Oddsfox-Token'],'fixture-session');
  assert.deepEqual(JSON.parse(mutation.options.body),{paused:true});
  assert.equal(elements.pause.textContent,'Resume sync');
});

test('correcting a normalized interpretation preserves its derivation records', async()=>{
 const elements={},requests=[];
 const document={getElementById:id=>elements[id]??=new Element('div'),createElement:tag=>new Element(tag)};
 const ir={contract_version_id:'a',observation:{source:'Canonical'},field_evidence:{'/observation/source':{derivation_ref:'rule'}}};
 const derivations={rule:{pointer:'/observation/source',value:'Canonical',source_spans:[]}};
 const report={contracts:[{id:'a',logical:'a',data:{platform:'kalshi',metadata:{},text_artifacts:{},references:[]}}],interpretations:[{id:'i',data:{ir,derivations}}],assertions:[],reviews:[],freshness:[],comparisons:[],near_matches:[],models:[],status:{pending_review:0}};
 const fetch=async(path,options)=>{requests.push({path,options});return {ok:true,json:async()=>report};};
 vm.runInNewContext(fs.readFileSync('src/oddsfox/static/app.js','utf8'),{document,fetch});
 await new Promise(setImmediate);
 await walk(elements['contract-list']).find(e=>e.textContent==='Save candidate').listeners.click();
 const body=JSON.parse(requests.find(r=>r.path==='/api/interpret').options.body);
 assert.deepEqual(body.derivations,derivations);
});

test('event refresh withdraws stale details without focus and ignores responses after close', async()=>{
 const elements={};let timer,accepted=true,release=null,focused=0;
 const document={hidden:false,getElementById:id=>elements[id]??=new Element('div'),createElement:tag=>new Element(tag)};
 Element.prototype.focus=function(){focused++;document.activeElement=this;};Element.prototype.scrollIntoView=()=>{};
 document.getElementById('filter-qualification').value='qualified';
 const fetch=async path=>{
   let data;
   if(path==='/api/events/e'){
     if(release===true)await new Promise(resolve=>release=resolve);
     data={venue:'kalshi',title:'Event',data:{volume:{amount:'100001'},active_markets:[]},assertions:accepted?[{id:'claim',data:{relation:'IMPLIES'}}]:[],contracts:[{id:'c',logical:'c',data:{metadata:{title:'Market evidence'},text_artifacts:{},references:[]}}]};
   }else if(path.startsWith('/api/events?'))data={total:0,items:[],next_offset:null};
   else data={counts:[],analysis:[],models:[],venues:[],categories:[],lanes:[],formal_verification:{}};
   return {ok:true,json:async()=>data};
 };
 const context=vm.createContext({document,fetch,URLSearchParams,Intl,setInterval:fn=>timer=fn});
 vm.runInContext(fs.readFileSync('src/oddsfox/static/events.js','utf8'),context);
 await new Promise(setImmediate);await vm.runInContext('detail("e")',context);
  assert.equal(walk(elements['event-detail']).find(e=>e.textContent==='Close details').dataset.focusKey,'button:Close details');
  assert.equal(walk(elements['event-detail']).find(e=>e.textContent==='Close details').className,'secondary');
  assert.equal(walk(elements['event-detail']).find(e=>e.textContent==='Close details').type,'button');
 const label=()=>walk(elements['event-detail']).find(e=>e.tag==='h3'&&e.textContent.startsWith('Accepted')).textContent;
 assert.equal(label(),'Accepted formal relationships · 1');
 const evidence=walk(elements['event-detail']).find(n=>n.dataset.key==='contract:c');evidence.open=true;
 const summary=evidence.children[0];summary.focus();const beforeFocus=focused;
 timer();await new Promise(setImmediate);
 assert.equal(walk(elements['event-detail']).find(n=>n.dataset.key==='contract:c'),evidence);
 assert.equal(evidence.open,true);assert.equal(document.activeElement,summary);assert.equal(focused,beforeFocus);
 accepted=false;timer();await new Promise(setImmediate);
 assert.equal(label(),'Accepted formal relationships · 0');
 const updated=walk(elements['event-detail']).find(n=>n.dataset.key==='contract:c');
 assert.equal(updated.open,true);assert.equal(document.activeElement,updated.children[0]);
 release=true;timer();await new Promise(setImmediate);
 await walk(elements['event-detail']).find(e=>e.textContent==='Close details').listeners.click();
 release();await new Promise(setImmediate);
 assert.equal(elements['event-detail'].hidden,true);
});

test('event and research venue selectors expose exactly the two supported venues', () => {
  for (const [file,id] of [['index.html','filter-venue'],['research.html','venue']]) {
    const html=fs.readFileSync(`src/oddsfox/static/${file}`,'utf8');
    const select=html.match(new RegExp(`<select id="${id}">([\\s\\S]*?)</select>`));
    assert(select, `missing ${id} selector`);
    const options=[...select[1].matchAll(/<option value="([^"]*)">([^<]*)<\/option>/g)];
    assert.deepEqual(options.map(m=>m[1]).filter(Boolean).sort(),['kalshi','polymarket']);
    assert(options.some(m=>m[1]==='polymarket'&&m[2]==='Polymarket International'));
    assert(!/Polymarket US|polymarket_us|three venues/.test(html));
    assert(html.includes('class="skip"') && html.includes('href="#main-content"'));
    assert(html.includes('/assets/logo.png'));
    assert(!html.includes('innerHTML'));
  }
  const research=fs.readFileSync('src/oddsfox/static/research.html','utf8');
  assert(/<textarea id="payload"/.test(research));
  assert(/<textarea id="registry-json"/.test(research));
});

test('event polling uses a 10s interval and skips refresh while the document is hidden', async () => {
  Object.defineProperty(Element.prototype, 'firstChild', {get(){return this.children[0];},configurable:true});
  const elements={}, requests=[];
  let poll;
  const document={hidden:true,getElementById:id=>elements[id]??=new Element('div'),createElement:tag=>new Element(tag)};
  document.getElementById('filter-qualification').value='qualified';
  const fetch=async path=>{
    requests.push(path);
    if(path.startsWith('/api/events?'))return {ok:true,json:async()=>({total:0,items:[],next_offset:null})};
    return {ok:true,json:async()=>({counts:[],analysis:[],models:[],venues:[],categories:[],lanes:[],formal_verification:{}})};
  };
  vm.runInNewContext(fs.readFileSync('src/oddsfox/static/events.js','utf8'), {document,fetch,URLSearchParams,Intl,setInterval:(fn,ms)=>{poll={fn,ms};}});
  await new Promise(setImmediate);
  assert.equal(poll.ms, 10000);
  const afterLoad=requests.length;
  assert.ok(afterLoad >= 2);
  poll.fn();
  await new Promise(setImmediate);
  assert.equal(requests.length, afterLoad);
  document.hidden=false;
  poll.fn();
  await new Promise(setImmediate);
  assert.ok(requests.length > afterLoad);
});

test('event cards keep the understand action and render hostile titles as text', async () => {
  Object.defineProperty(Element.prototype, 'firstChild', {get(){return this.children[0];},configurable:true});
  const elements={};
  const document={hidden:false,getElementById:id=>elements[id]??=new Element('div'),createElement:tag=>new Element(tag)};
  document.getElementById('filter-qualification').value='qualified';
  const title='<img src=x onerror=alert(1)>';
  const fetch=async path=>{
    if(path.startsWith('/api/events?'))return {ok:true,json:async()=>({total:1,items:[{id:'evt',venue:'polymarket',title,category:'sports',analysis_status:'ready',data:{volume:{amount:'100001',basis:'notional'},active_markets:[{}]}}],next_offset:null})};
    return {ok:true,json:async()=>({counts:[],analysis:[],models:[],venues:[],categories:[],lanes:[],formal_verification:{}})};
  };
  vm.runInNewContext(fs.readFileSync('src/oddsfox/static/events.js','utf8'), {document,fetch,URLSearchParams,Intl,setInterval:()=>{}});
  await new Promise(setImmediate);
  const card=elements['event-list'].children[0];
  assert.equal(walk(card).find(e=>e.tag==='h3').textContent, title);
  assert.equal(walk(card).find(e=>e.textContent==='Understand this event').dataset.focusKey, 'button:Understand this event');
  assert.equal(walk(card).find(e=>e.textContent==='Understand this event').type, 'button');
  assert.equal(walk(card).find(e=>e.textContent==='Understand this event').ariaLabel, 'Understand this event: '+title);
  assert(walk(card).some(e=>e.textContent==='Polymarket International'));
});

test('review actions distinguish reject from approve and color review status', async () => {
  const elements={};
  const document={getElementById:id=>elements[id]??=new Element('div'),createElement:tag=>new Element(tag)};
  const ir={contract_version_id:'a',observation:{source:'Canonical',measurement_method:'instantaneous'}};
  const report={
    contracts:[{id:'a',logical:'a',data:{platform:'kalshi',metadata:{title:'A'},text_artifacts:{},references:[]}}],
    interpretations:[{id:'i',data:{ir,derivations:{},assessment:'SUPPORTED'}}],
    assertions:[],
    reviews:[{logical:'i',data:{approved:false}}],
    freshness:[],
    comparisons:[],
    near_matches:[],
    models:[],
    status:{pending_review:1},
  };
  const fetch=async()=>({ok:true,json:async()=>report});
  vm.runInNewContext(fs.readFileSync('src/oddsfox/static/app.js','utf8'),{document,fetch});
  await new Promise(setImmediate);
  const nodes=walk(elements['contract-list']);
  const approve=nodes.find(e=>e.textContent==='Approve interpretation');
  const reject=nodes.find(e=>e.textContent==='Reject / withdraw approval');
  const status=nodes.find(e=>e.textContent==='REJECTED / WITHDRAWN');
  assert.equal(approve.type,'button');
  assert.notEqual(approve.className,'danger');
  assert.notEqual(approve.className,'secondary');
  assert.equal(reject.type,'button');
  assert.equal(reject.className,'danger');
  assert.equal(status.className,'badge warning');
});

test('approved review status uses the success badge', async () => {
  const elements={};
  const document={getElementById:id=>elements[id]??=new Element('div'),createElement:tag=>new Element(tag)};
  const ir={contract_version_id:'a',observation:{source:'Canonical',measurement_method:'instantaneous'}};
  const report={
    contracts:[{id:'a',logical:'a',data:{platform:'kalshi',metadata:{title:'A'},text_artifacts:{},references:[]}}],
    interpretations:[{id:'i',data:{ir,derivations:{},assessment:'SUPPORTED'}}],
    assertions:[],
    reviews:[{logical:'i',data:{approved:true}}],
    freshness:[],
    comparisons:[],
    near_matches:[],
    models:[],
    status:{pending_review:0},
  };
  const fetch=async()=>({ok:true,json:async()=>report});
  vm.runInNewContext(fs.readFileSync('src/oddsfox/static/app.js','utf8'),{document,fetch});
  await new Promise(setImmediate);
  const status=walk(elements['contract-list']).find(e=>e.textContent==='REVIEWED');
  assert.equal(status.className,'badge success');
});

test('event detail scroll respects reduced motion', async () => {
  const elements={};
  const seen=[];
  const document={hidden:false,getElementById:id=>elements[id]??=new Element('div'),createElement:tag=>new Element(tag)};
  document.getElementById('filter-qualification').value='qualified';
  Element.prototype.focus=function(){document.activeElement=this;};
  Element.prototype.scrollIntoView=function(opts){seen.push(opts.behavior);};
  const fetch=async path=>{
    if(path==='/api/events/e')return {ok:true,json:async()=>({venue:'kalshi',title:'Event',data:{volume:{amount:'100001'},active_markets:[]},assertions:[],contracts:[]})};
    if(path.startsWith('/api/events?'))return {ok:true,json:async()=>({total:0,items:[],next_offset:null})};
    return {ok:true,json:async()=>({counts:[],analysis:[],models:[],venues:[],categories:[],lanes:[],formal_verification:{}})};
  };
  const context=vm.createContext({document,fetch,URLSearchParams,Intl,setInterval:()=>{},matchMedia:query=>({matches:query==='(prefers-reduced-motion: reduce)'})});
  vm.runInContext(fs.readFileSync('src/oddsfox/static/events.js','utf8'),context);
  await new Promise(setImmediate);
  await vm.runInContext('detail("e")',context);
  assert.equal(seen.at(-1),'auto');
});

function eventListFetch(handler){
  Object.defineProperty(Element.prototype, 'firstChild', {get(){return this.children[0];},configurable:true});
  const elements={};
  const document={hidden:false,getElementById:id=>elements[id]??=new Element('div'),createElement:tag=>new Element(tag)};
  document.getElementById('filter-qualification').value='qualified';
  const fetch=async path=>{
    if(path.startsWith('/api/events?') || path.startsWith('/api/events/')) return handler(path);
    return {ok:true,json:async()=>({counts:[],analysis:[],models:[],venues:[],categories:[],lanes:[],formal_verification:{}})};
  };
  return {elements,document,fetch};
}

test('event next click during an in-flight refresh still requests the next page', async () => {
  const requests=[];
  let firstComplete=false;
  let release;
  const {elements,document,fetch}=eventListFetch(async path=>{
    const offset=new URLSearchParams(path.split('?')[1]).get('offset');
    requests.push(offset);
    if(firstComplete && typeof release!=='function' && release!==true){
      await new Promise(resolve=>{release=resolve;});
    }
    firstComplete=true;
    return {ok:true,json:async()=>({total:31,items:[],next_offset:offset==='0'?30:null})};
  });
  let timer;
  vm.runInNewContext(fs.readFileSync('src/oddsfox/static/events.js','utf8'), {document,fetch,URLSearchParams,Intl,setInterval:fn=>{timer=fn;}});
  const settle=()=>new Promise(resolve=>setImmediate(resolve));
  await settle();
  assert.ok(requests.includes('0'));
  timer();
  await settle();
  while(typeof release!=='function')await settle();
  elements.next.listeners.click();
  release();
  await settle();
  await settle();
  assert.ok(requests.includes('30'));
});

test('event page offset clamps when the requested page no longer exists', async () => {
  const {elements,document,fetch}=eventListFetch(async path=>{
    const offset=Number(new URLSearchParams(path.split('?')[1]).get('offset'));
    if(offset===0){
      return {ok:true,json:async()=>({total:5,items:[{id:'e',venue:'kalshi',title:'T',category:'sports',analysis_status:'ready',data:{volume:{amount:'1',basis:'n'},active_markets:[]}}],next_offset:null})};
    }
    return {ok:true,json:async()=>({total:5,items:[],next_offset:null})};
  });
  const context=vm.createContext({document,fetch,URLSearchParams,Intl,setInterval:()=>{}});
  vm.runInContext(fs.readFileSync('src/oddsfox/static/events.js','utf8'),context);
  const settle=()=>new Promise(resolve=>setImmediate(resolve));
  await settle();
  await vm.runInContext('offset=30;refresh()',context);
  await settle();
  await settle();
  assert.equal(elements['page-label'].textContent,'1–1 of 5');
});

test('event page offset clamps when offset equals total', async () => {
  const item={id:'e',venue:'kalshi',title:'T',category:'sports',analysis_status:'ready',data:{volume:{amount:'1',basis:'n'},active_markets:[]}};
  const {elements,document,fetch}=eventListFetch(async path=>{
    const offset=Number(new URLSearchParams(path.split('?')[1]).get('offset'));
    if(offset===0){
      return {ok:true,json:async()=>({total:30,items:Array.from({length:30},()=>item),next_offset:null})};
    }
    return {ok:true,json:async()=>({total:30,items:[],next_offset:null})};
  });
  const context=vm.createContext({document,fetch,URLSearchParams,Intl,setInterval:()=>{}});
  vm.runInContext(fs.readFileSync('src/oddsfox/static/events.js','utf8'),context);
  const settle=()=>new Promise(resolve=>setImmediate(resolve));
  await settle();
  await vm.runInContext('offset=30;refresh()',context);
  await settle();
  await settle();
  assert.equal(elements['page-label'].textContent,'1–30 of 30');
});

test('event page offset does not clamp in a loop when total is zero', async () => {
  const {elements,document,fetch}=eventListFetch(async()=>({ok:true,json:async()=>({total:0,items:[],next_offset:null})}));
  const context=vm.createContext({document,fetch,URLSearchParams,Intl,setInterval:()=>{}});
  vm.runInContext(fs.readFileSync('src/oddsfox/static/events.js','utf8'),context);
  const settle=()=>new Promise(resolve=>setImmediate(resolve));
  await settle();
  await vm.runInContext('offset=30;refresh()',context);
  await settle();
  await settle();
  assert.equal(elements['page-label'].textContent,'0 events');
});

test('failed event detail does not stay selected for polling', async () => {
  const requests=[];
  let timer;
  const {elements,document,fetch}=eventListFetch(async path=>{
    requests.push(path);
    if(path==='/api/events/missing')return {ok:false,json:async()=>({detail:'unknown event'})};
    if(path.startsWith('/api/events?'))return {ok:true,json:async()=>({total:0,items:[],next_offset:null})};
    return {ok:true,json:async()=>({counts:[],analysis:[],models:[],venues:[],categories:[],lanes:[],formal_verification:{}})};
  });
  const context=vm.createContext({document,fetch,URLSearchParams,Intl,setInterval:fn=>{timer=fn;}});
  vm.runInContext(fs.readFileSync('src/oddsfox/static/events.js','utf8'),context);
  const settle=()=>new Promise(resolve=>setImmediate(resolve));
  await settle();
  await vm.runInContext('detail("missing")',context).catch(()=>{});
  const before=requests.filter(path=>path==='/api/events/missing').length;
  assert.equal(before,1);
  timer();
  await settle();
  assert.equal(requests.filter(path=>path==='/api/events/missing').length,1);
});

test('failed poll of a selected event keeps it selected', async () => {
  const requests=[];
  let available=true;
  let timer;
  const {elements,document,fetch}=eventListFetch(async path=>{
    requests.push(path);
    if(path==='/api/events/e'){
      if(!available)return {ok:false,json:async()=>({detail:'unknown event'})};
      return {ok:true,json:async()=>({venue:'kalshi',title:'Event',data:{volume:{amount:'100001'},active_markets:[]},assertions:[],contracts:[]})};
    }
    if(path.startsWith('/api/events?'))return {ok:true,json:async()=>({total:0,items:[],next_offset:null})};
    return {ok:true,json:async()=>({counts:[],analysis:[],models:[],venues:[],categories:[],lanes:[],formal_verification:{}})};
  });
  const context=vm.createContext({document,fetch,URLSearchParams,Intl,setInterval:fn=>{timer=fn;}});
  vm.runInContext(fs.readFileSync('src/oddsfox/static/events.js','utf8'),context);
  const settle=()=>new Promise(resolve=>setImmediate(resolve));
  await settle();
  await vm.runInContext('detail("e")',context);
  available=false;
  timer();
  await settle();
  const afterFailure=requests.filter(path=>path==='/api/events/e').length;
  assert.equal(afterFailure,2);
  timer();
  await settle();
  assert.equal(requests.filter(path=>path==='/api/events/e').length,afterFailure+1);
});

test('research empty-state is omitted when only near-matches exist', async () => {
  const elements={};
  const document={getElementById:id=>elements[id]??=new Element('div'),createElement:tag=>new Element(tag)};
  const report={
    contracts:[],
    interpretations:[],
    assertions:[],
    reviews:[],
    freshness:[],
    comparisons:[],
    near_matches:[{a:'a',b:'b',reason:'different source',differences:{source:{a:'x',b:'y'}}}],
    models:[],
    status:{pending_review:0},
  };
  const fetch=async()=>({ok:true,json:async()=>report});
  vm.runInNewContext(fs.readFileSync('src/oddsfox/static/app.js','utf8'),{document,fetch});
  await new Promise(setImmediate);
  const texts=walk(elements['comparison-list']).map(e=>e.textContent).filter(Boolean);
  assert.ok(texts.some(text=>text==='NEAR-MATCH / NOT COMPARABLE'));
  assert.ok(!texts.some(text=>text.includes('No eligible shared observation yet')));
});

test('research comparison coverage banner appears when the report is truncated', async () => {
  const elements={};
  const document={getElementById:id=>elements[id]??=new Element('div'),createElement:tag=>new Element(tag)};
  const report={
    contracts:[],
    interpretations:[],
    assertions:[],
    reviews:[],
    freshness:[],
    comparisons:[],
    near_matches:[],
    comparison_coverage:{processed:250,total:251,complete:false},
    models:[],
    status:{pending_review:0},
  };
  const fetch=async()=>({ok:true,json:async()=>report});
  vm.runInNewContext(fs.readFileSync('src/oddsfox/static/app.js','utf8'),{document,fetch});
  await new Promise(setImmediate);
  const texts=walk(elements['comparison-list']).map(e=>e.textContent).filter(Boolean);
  assert.ok(texts.some(text=>text.includes('covers 250 of 251 stored rows')));
  assert.ok(texts.some(text=>text.includes('No eligible shared observation yet')));
});

test('research comparison coverage banner is omitted when complete', async () => {
  const elements={};
  const document={getElementById:id=>elements[id]??=new Element('div'),createElement:tag=>new Element(tag)};
  const report={
    contracts:[],
    interpretations:[],
    assertions:[],
    reviews:[],
    freshness:[],
    comparisons:[],
    near_matches:[],
    comparison_coverage:{processed:2,total:2,complete:true},
    models:[],
    status:{pending_review:0},
  };
  const fetch=async()=>({ok:true,json:async()=>report});
  vm.runInNewContext(fs.readFileSync('src/oddsfox/static/app.js','utf8'),{document,fetch});
  await new Promise(setImmediate);
  const texts=walk(elements['comparison-list']).map(e=>e.textContent).filter(Boolean);
  assert.ok(!texts.some(text=>text.includes('covers')));
  assert.ok(texts.some(text=>text.includes('No eligible shared observation yet')));
});

test('research truncated coverage still shows near-matches without empty-state', async () => {
  const elements={};
  const document={getElementById:id=>elements[id]??=new Element('div'),createElement:tag=>new Element(tag)};
  const report={
    contracts:[],
    interpretations:[],
    assertions:[],
    reviews:[],
    freshness:[],
    comparisons:[],
    near_matches:[{a:'a',b:'b',reason:'different source',differences:{source:{a:'x',b:'y'}}}],
    comparison_coverage:{processed:250,total:251,complete:false},
    models:[],
    status:{pending_review:0},
  };
  const fetch=async()=>({ok:true,json:async()=>report});
  vm.runInNewContext(fs.readFileSync('src/oddsfox/static/app.js','utf8'),{document,fetch});
  await new Promise(setImmediate);
  const texts=walk(elements['comparison-list']).map(e=>e.textContent).filter(Boolean);
  assert.ok(texts.some(text=>text.includes('covers 250 of 251 stored rows')));
  assert.ok(texts.some(text=>text==='NEAR-MATCH / NOT COMPARABLE'));
  assert.ok(!texts.some(text=>text.includes('No eligible shared observation yet')));
});

test('event details deep-link each contract version into the research workspace', async () => {
  Object.defineProperty(Element.prototype, 'firstChild', {get(){return this.children[0];},configurable:true});
  const elements={};
  const document={hidden:false,getElementById:id=>elements[id]??=new Element('div'),createElement:tag=>new Element(tag)};
  document.getElementById('filter-qualification').value='qualified';
  document.getElementById('token').value='fixture-session';
  const detail={
    id:'event-1',venue:'kalshi',title:'Example',qualification:'qualified',
    data:{volume:{amount:'1',basis:'face',window:'lifetime',observed_at:'t',reason:''},active_markets:[],stale_reason:'',combination:null},
    explanation:null,suggestions:null,assertions:[],
    contracts:[{id:'contract-abc',logical:'kalshi:x',data:{metadata:{title:'Child'},text_artifacts:{rules:'art'},references:[]}}],
  };
  const fetch=async(path)=>({
    ok:true,
    json:async()=>path.startsWith('/api/events/')&&!path.includes('?')?detail:
      path.startsWith('/api/events?')?{total:1,items:[{id:'event-1',venue:'kalshi',title:'Example',category:'X',data:{volume:{amount:'1',basis:'face'},active_markets:[]},analysis_status:'pending'}],next_offset:null}:
      {counts:[],analysis:[],models:[],lanes:[],categories:[],coverage_notes:[],formal_verification:{groups:0},venues:[]},
  });
  vm.runInNewContext(fs.readFileSync('src/oddsfox/static/events.js','utf8'),{document,fetch,URLSearchParams,Intl,setInterval:()=>{},encodeURIComponent});
  const settle=()=>new Promise(resolve=>setImmediate(resolve));
  await settle();
  const understand=walk(elements['event-list']).find(el=>el.textContent==='Understand this event');
  await understand.listeners.click();
  await settle();
  const link=walk(elements['event-detail']).find(el=>el.tag==='a'&&el.textContent==='Open this contract in the review workspace');
  assert.equal(link.href,'/research?contract=contract-abc');
});

test('research focuses the requested contract and reports a stale target', async () => {
  Element.prototype.focus=function(){this.focused=true;};
  Element.prototype.scrollIntoView=function(){this.scrolled=true;};
  const report={
    contracts:[{id:'a',logical:'kalshi:a',data:{platform:'kalshi',metadata:{title:'A',capture_status:'captured'},text_artifacts:{},references:[]}}],
    interpretations:[],assertions:[],reviews:[],consensus_approvals:[],freshness:[],comparisons:[],near_matches:[],models:[],status:{pending_review:0},
  };
  const run=async search=>{
    const elements={};
    const document={getElementById:id=>elements[id]??=new Element('div'),createElement:tag=>new Element(tag)};
    const fetch=async()=>({ok:true,json:async()=>report});
    vm.runInNewContext(fs.readFileSync('src/oddsfox/static/app.js','utf8'),{document,fetch,location:{search},URLSearchParams,matchMedia:()=>({matches:true})});
    await new Promise(setImmediate);
    return elements;
  };
  const found=await run('?contract=a');
  const card=walk(found['contract-list']).find(el=>el.dataset&&el.dataset.contractId==='a');
  assert.equal(card.id,'contract-a');
  assert.equal(card.tabIndex,-1);
  assert.equal(card.focused,true);
  const missing=await run('?contract=missing');
  assert.ok(walk(missing.notice).some(el=>String(el.textContent||'').includes('missing or stale')));
});

test('research deep-link focuses an encoded contract identifier', async () => {
  Element.prototype.focus=function(){this.focused=true;};
  Element.prototype.scrollIntoView=function(){this.scrolled=true;};
  const report={
    contracts:[{id:'kalshi:a',logical:'kalshi:a',data:{platform:'kalshi',metadata:{title:'A',capture_status:'captured'},text_artifacts:{},references:[]}}],
    interpretations:[],assertions:[],reviews:[],consensus_approvals:[],freshness:[],comparisons:[],near_matches:[],models:[],status:{pending_review:0},
  };
  const elements={};
  const document={getElementById:id=>elements[id]??=new Element('div'),createElement:tag=>new Element(tag)};
  const fetch=async()=>({ok:true,json:async()=>report});
  vm.runInNewContext(fs.readFileSync('src/oddsfox/static/app.js','utf8'),{document,fetch,location:{search:'?contract='+encodeURIComponent('kalshi:a')},URLSearchParams,matchMedia:()=>({matches:true})});
  await new Promise(setImmediate);
  const card=walk(elements['contract-list']).find(el=>el.dataset&&el.dataset.contractId==='kalshi:a');
  assert.equal(card.id,'contract-kalshi:a');
  assert.equal(card.tabIndex,-1);
  assert.equal(card.focused,true);
});

test('consensus status is not REVIEWED and human review remains available', async () => {
  const report={
    contracts:[{id:'a',logical:'kalshi:a',data:{platform:'kalshi',metadata:{title:'A'},text_artifacts:{},references:[]}}],
    interpretations:[{id:'i',data:{ir:{contract_version_id:'a',observation:{source:'Canonical',measurement_method:'instantaneous'}},derivations:{},assessment:'SUPPORTED'}}],
    assertions:[],
    reviews:[],
    consensus_approvals:[{logical:'i',data:{acceptance_basis:'LOCAL_MODEL_CONSENSUS'}}],
    freshness:[],comparisons:[],near_matches:[],models:[],status:{pending_review:1},
    allow_consensus:true,
  };
  const run=async extra=>{
    const elements={};
    const document={getElementById:id=>elements[id]??=new Element('div'),createElement:tag=>new Element(tag)};
    const fetch=async()=>({ok:true,json:async()=>({...report,...extra})});
    vm.runInNewContext(fs.readFileSync('src/oddsfox/static/app.js','utf8'),{document,fetch});
    await new Promise(setImmediate);
    return walk(elements['contract-list']);
  };
  const nodes=await run({});
  const status=nodes.find(e=>e.textContent==='LOCAL MODEL CONSENSUS');
  assert.equal(status.className,'badge success');
  assert.ok(!nodes.some(e=>e.textContent==='REVIEWED'));
  assert.ok(nodes.some(e=>e.textContent==='Approve interpretation'));
  assert.ok(nodes.some(e=>e.textContent==='Reject / withdraw approval'));
  assert.ok(nodes.some(e=>String(e.textContent||'').includes('Acceptance basis: LOCAL MODEL CONSENSUS')));
  const vetoed=await run({reviews:[{logical:'i',data:{approved:false}}]});
  assert.ok(vetoed.some(e=>e.textContent==='REJECTED / WITHDRAWN'));
  assert.ok(!vetoed.some(e=>e.textContent==='LOCAL MODEL CONSENSUS'));
  assert.ok(!vetoed.some(e=>String(e.textContent||'').includes('LOCAL MODEL CONSENSUS')));
  assert.ok(!vetoed.some(e=>e.textContent==='REVIEWED'));
  const unpublished=await run({allow_consensus:false});
  assert.ok(!unpublished.some(e=>e.textContent==='LOCAL MODEL CONSENSUS'));
  assert.ok(!unpublished.some(e=>String(e.textContent||'').includes('Acceptance basis: LOCAL MODEL CONSENSUS')));
  assert.ok(unpublished.some(e=>String(e.textContent||'').includes('Consensus publication is off')));
});

test('suggested matches stay UNREVIEWED and never look accepted', async () => {
  Object.defineProperty(Element.prototype, 'firstChild', {get(){return this.children[0];},configurable:true});
  const elements={};
  const document={hidden:false,getElementById:id=>elements[id]??=new Element('div'),createElement:tag=>new Element(tag)};
  document.getElementById('filter-qualification').value='qualified';
  const detail={
    id:'event-1',venue:'kalshi',title:'Example',qualification:'qualified',
    data:{volume:{amount:'1',basis:'face',window:'lifetime',observed_at:'t',reason:''},active_markets:[],stale_reason:'',combination:null},
    explanation:null,
    suggestions:{data:{coverage:'1 candidate',omitted_pending_explanations:[],matches:[{relationship:'possible_match',reason:'similar wording',citations:[]}]}},
    assertions:[],
    contracts:[{id:'kalshi:x',logical:'kalshi:x',data:{metadata:{title:'Child'},text_artifacts:{rules:'art'},references:[]}}],
  };
  const fetch=async(path)=>({
    ok:true,
    json:async()=>path.startsWith('/api/events/')&&!path.includes('?')?detail:
      path.startsWith('/api/events?')?{total:1,items:[{id:'event-1',venue:'kalshi',title:'Example',category:'X',data:{volume:{amount:'1',basis:'face'},active_markets:[]},analysis_status:'pending'}],next_offset:null}:
      {counts:[],analysis:[],models:[],lanes:[],categories:[],coverage_notes:[],formal_verification:{groups:0},venues:[]},
  });
  vm.runInNewContext(fs.readFileSync('src/oddsfox/static/events.js','utf8'),{document,fetch,URLSearchParams,Intl,setInterval:()=>{},encodeURIComponent});
  const settle=()=>new Promise(resolve=>setImmediate(resolve));
  await settle();
  await walk(elements['event-list']).find(el=>el.textContent==='Understand this event').listeners.click();
  await settle();
  const texts=walk(elements['event-detail']).map(e=>e.textContent).filter(Boolean);
  assert.ok(texts.some(text=>text==='possible_match · UNREVIEWED'));
  assert.ok(!texts.some(text=>text==='ACCEPTED'||text==='REVIEWED'));
  const link=walk(elements['event-detail']).find(el=>el.tag==='a'&&el.textContent==='Open this contract in the review workspace');
  assert.equal(link.href,'/research?contract='+encodeURIComponent('kalshi:x'));
});
