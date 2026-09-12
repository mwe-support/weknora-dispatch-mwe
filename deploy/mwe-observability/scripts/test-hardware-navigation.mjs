import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';

const path = process.argv[2] || new URL('../grafana/dashboards/knowledgebase-overview.json', import.meta.url);
const dashboard = JSON.parse(readFileSync(path, 'utf8').replace(/^\uFEFF/, ''));
const panels = dashboard.panels.flatMap(p => [p, ...(p.panels || [])]);
const panel = panels.find(p => p.id === 23);
for (const range of [{from:'now-6h',to:'now'}, {from:'1789153575425',to:'1789175175425'}]) {
  const state = {...range, refresh:'5s', 'var-doc_page':'3', 'var-log_container':'WeKnora-app'};
  const calls = [];
  const button = {dataset:{container:'WeKnora-frontend'}, setAttribute(){}, addEventListener(event, callback){this[event] = callback;}};
  const root = {dataset:{}, querySelectorAll(){return [button];}};
  const context = {element:{querySelector(){return root;}}, grafana:{replaceVariables(){return 'WeKnora-app';}, locationService:{partial(update){calls.push(JSON.parse(JSON.stringify(update)));Object.assign(state,update);}}}};
  vm.runInNewContext(`(()=>{${panel.options.afterRender || ''}})()`, {context});
  assert.equal(typeof button.click, 'function', 'log navigation must update only its variable, not freeze the time range in a link');
  button.click();
  assert.deepEqual(calls,[{'var-log_container':'WeKnora-frontend'}]);
  assert.deepEqual(state,{...range,refresh:'5s','var-doc_page':'3','var-log_container':'WeKnora-frontend'});
}
assert.ok(!panel.options.content.includes('${__from}'));
assert.ok(panels.find(p => p.id===21).targets.every(t=>t.expr.includes('job="integrations/host_network"')));
assert.ok(!panels.filter(p=>[14,15,16,17].includes(p.id)).some(p=>p.title.includes('实时')));
console.log('PASS: relative and historical ranges, refresh, and unrelated filters survive log navigation; host network source is explicit.');
