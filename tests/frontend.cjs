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
