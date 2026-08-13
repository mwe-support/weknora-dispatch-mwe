import { readdir, readFile, stat } from 'node:fs/promises'
import { resolve, relative, sep } from 'node:path'

const projectRoot = resolve(import.meta.dirname, '..')
const distRoot = resolve(process.argv[2] ?? resolve(projectRoot, 'frontend', 'dist'))
const referencePattern = /(?:^|["'`(=\s])\/?(assets\/[A-Za-z0-9_./@+-]+\.[A-Za-z0-9]+)(?=$|["'`),?\s#])/gm
const scannableExtensions = new Set(['.css', '.html', '.js', '.mjs'])

async function walk(directory) {
  const entries = await readdir(directory, { withFileTypes: true })
  const files = []
  for (const entry of entries) {
    const fullPath = resolve(directory, entry.name)
    if (entry.isDirectory()) files.push(...await walk(fullPath))
    else if (entry.isFile()) files.push(fullPath)
  }
  return files
}

function extension(filePath) {
  const fileName = filePath.split(sep).at(-1) ?? ''
  const dot = fileName.lastIndexOf('.')
  return dot === -1 ? '' : fileName.slice(dot)
}

const files = await walk(distRoot)
const relativeFiles = new Set(files.map((filePath) => relative(distRoot, filePath).split(sep).join('/')))
const entrypoints = files.filter((filePath) => extension(filePath) === '.html')

if (entrypoints.length === 0) {
  throw new Error(`frontend dist has no HTML entrypoint: ${distRoot}`)
}

const missing = new Map()
for (const filePath of files) {
  if (!scannableExtensions.has(extension(filePath))) continue
  const content = await readFile(filePath, 'utf8')
  for (const match of content.matchAll(referencePattern)) {
    const assetPath = match[1]
    if (relativeFiles.has(assetPath)) continue
    const source = relative(distRoot, filePath).split(sep).join('/')
    if (!missing.has(assetPath)) missing.set(assetPath, new Set())
    missing.get(assetPath).add(source)
  }
}

const emptyFiles = []
for (const filePath of files) {
  if (!scannableExtensions.has(extension(filePath))) continue
  if ((await stat(filePath)).size === 0) {
    emptyFiles.push(relative(distRoot, filePath).split(sep).join('/'))
  }
}

if (missing.size > 0 || emptyFiles.length > 0) {
  const details = []
  for (const [assetPath, sources] of [...missing].sort()) {
    details.push(`missing ${assetPath} (referenced by ${[...sources].sort().join(', ')})`)
  }
  for (const filePath of emptyFiles.sort()) details.push(`empty ${filePath}`)
  throw new Error(`frontend dist integrity check failed:\n${details.join('\n')}`)
}

const cssCount = files.filter((filePath) => extension(filePath) === '.css').length
const jsCount = files.filter((filePath) => ['.js', '.mjs'].includes(extension(filePath))).length
console.log(`frontend dist integrity check passed: ${files.length} files, ${entrypoints.length} HTML, ${jsCount} JS, ${cssCount} CSS`)
