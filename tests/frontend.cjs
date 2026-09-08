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
  assert.deepEqual(JSON.parse(saved.options.body),{ir:edited});
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
