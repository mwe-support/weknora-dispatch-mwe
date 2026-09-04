// Run from the repository root with GO_EXE (optional) and Go cache env configured.
// Standard-library isolation runs the actual Sheet implementation, types and tests.
// It excludes unrelated database/CGO dependencies, not any Sheet algorithm.
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { spawnSync } from 'node:child_process';

const root = process.cwd();
const sourcePath = path.join(root, 'internal/datasource/connector/tencentdocs/sheet.go');
const source = fs.readFileSync(sourcePath, 'utf8');
const output = path.resolve(process.argv[2] ?? 'sheet-empty-ablation-results.json');
const scratchBase = fs.realpathSync(os.tmpdir());
const scratch = fs.mkdtempSync(path.join(scratchBase, 'weknora-sheet-ablation-'));
const replaceOnce = (text, from, to) => {
  if (text.split(from).length !== 2) throw new Error(`Ablation anchor changed: ${from}`);
  return text.replace(from, to);
};
const noPlaceholders = text => {
  text = replaceOnce(text, '\t\t\t\tif cell == (SheetCell{}) {\n\t\t\t\t\tcontinue\n\t\t\t\t}', '');
  return replaceOnce(text, '\t\t\t\tif cell == (SheetCell{}) {\n\t\t\t\t\temptyPlaceholderCount++\n\t\t\t\t\tcontinue\n\t\t\t\t}', '');
};
const noRows = text => replaceOnce(text,
  'if lastUsedCol < 0 {\n\t\t\t\t\tcontinue\n\t\t\t\t}',
  'if lastUsedCol < 0 {\n\t\t\t\t\tlastUsedCol = 0\n\t\t\t\t}');
const dropOutside = text => replaceOnce(text,
  '\t\t\t\t\treturn nil, fmt.Errorf(\n\t\t\t\t\t\t"get Sheet cells %s returned out-of-range cell (%d,%d), requested rows %d-%d cols 0-%d (0-based)",\n\t\t\t\t\t\tsheet.ID, cell.Row, cell.Col, rowRange.start, rowRange.end, sheet.ColCount-1,\n\t\t\t\t\t)',
  '\t\t\t\t\tcontinue');
const arms = [
  ['A_none', noRows(noPlaceholders(source))],
  ['B_rows_only', noPlaceholders(source)],
  ['C_placeholders_only', noRows(source)],
  ['D_combined', source],
  ['E_drop_all_outside', dropOutside(source)],
  ['F_text_only_empty', source.replaceAll('cell == (SheetCell{})', 'strings.TrimSpace(cell.StringValue) == "" && strings.TrimSpace(cell.Formula) == ""')],
];
const results = [];
try {
  const dir = path.dirname(sourcePath);
  fs.writeFileSync(path.join(scratch, 'go.mod'), 'module sheetablation\n\ngo 1.26.0\n');
  for (const name of ['types.go', 'sheet_empty_cells_test.go']) fs.copyFileSync(path.join(dir, name), path.join(scratch, name));
  // Compile-time assertions reference the transport client, which is not part of
  // this isolated Sheet test. Keep the real transport-independent interfaces.
  const client = fs.readFileSync(path.join(dir, 'client.go'), 'utf8').replace(/^var _ (Client|SheetClient) = .*\r?\n/gm, '');
  fs.writeFileSync(path.join(scratch, 'client.go'), client);
  const connector = fs.readFileSync(path.join(dir, 'connector.go'), 'utf8');
  const helper = connector.match(/func firstNonEmpty\(values \.\.\.string\) string \{[\s\S]*?\n\}/)?.[0];
  if (!helper) throw new Error('firstNonEmpty helper not found');
  fs.writeFileSync(path.join(scratch, 'helper.go'), 'package tencentdocs\n' + helper + '\n');
  fs.mkdirSync(path.join(scratch, 'testdata'));
  fs.copyFileSync(path.join(dir, 'testdata/sheet_empty_ablation.json'), path.join(scratch, 'testdata/sheet_empty_ablation.json'));
  for (const [arm, code] of arms) {
    const replacement = path.join(scratch, 'sheet.go');
    fs.writeFileSync(replacement, code);
    const run = spawnSync(process.env.GO_EXE ?? 'go', [
      'test', '.',
      '-run', '^TestSheet(Ablation|EmptyPlaceholderRegression|EmptyPlaceholderOrder)$', '-count=1', '-v',
    ], { cwd: scratch, encoding: 'utf8', timeout: 180000, maxBuffer: 16 * 1024 * 1024 });
    if (run.error) throw run.error;
    const cases = run.stdout.split(/\r?\n/).filter(line => line.includes('ABLATION_JSON '))
      .map(line => JSON.parse(line.slice(line.indexOf('ABLATION_JSON ') + 14)));
    if (cases.length !== 13) throw new Error(`${arm}: expected 13 cases\n${run.stdout}\n${run.stderr}`);
    const passed = cases.filter(c => c.pass).length;
    results.push({ arm, exit_code: run.status, passed, total: cases.length, cases });
    console.log(`${arm}: ${passed}/${cases.length}; ${cases.filter(c => !c.expect_rejection).map(c => `${c.case.split('/')[1]}=${c.error ? 'ERROR' : c.output_rows}`).join(', ')}`);
  }
  fs.writeFileSync(output, JSON.stringify({ generated_at: new Date().toISOString(), scope: 'actual Sheet code with standard-library isolation; not whole-package integration', results }, null, 2) + '\n');
  const selected = results.find(r => r.arm === 'D_combined');
  if (selected.passed !== 13 || selected.exit_code !== 0) process.exitCode = 1;
} finally {
  if (path.dirname(fs.realpathSync(scratch)) !== scratchBase || !path.basename(scratch).startsWith('weknora-sheet-ablation-')) {
    throw new Error('Refusing to remove an unexpected temporary directory');
  }
  fs.rmSync(scratch, { recursive: true, force: true });
}
