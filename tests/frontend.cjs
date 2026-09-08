const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const {test} = require('node:test');

class Element {
  constructor(tag) { this.tag=tag; this.children=[]; this.listeners={}; this.dataset={}; this.value=''; }
  append(...children) { this.children.push(...children); }
  replaceChildren(...children) { this.children=children; }
  addEventListener(name, callback) { this.listeners[name]=callback; }
  setAttribute() {}
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
