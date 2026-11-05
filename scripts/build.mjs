import { execFileSync } from 'node:child_process'
import { readdirSync, readFileSync, rmSync, writeFileSync, existsSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { join, dirname } from 'node:path'

const root = fileURLToPath(new URL('../', import.meta.url))
rmSync(join(root, 'dist'), { recursive: true, force: true })
execFileSync(process.execPath, [fileURLToPath(import.meta.resolve('typescript/bin/tsc')), '-p', join(root, 'tsconfig.build.json')], { stdio: 'inherit' })
// Source uses bundler-compatible imports for direct embedding. Published JS and
// declarations use explicit extensions so native Node ESM works without a loader.
function fix(directory) {
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name)
    if (entry.isDirectory()) {
      fix(path)
      continue
    }
    if (!path.endsWith('.js') && !path.endsWith('.d.ts'))
      continue
    const content = readFileSync(path, 'utf8').replace(/(from\s*|import\s*|import\()(['"])(\.{1,2}(?:\/[^'"]+)?)\2/g, (match, prefix, quote, specifier) => {
      if (/\.(js|json)$/.test(specifier))
        return match
      const target = existsSync(join(dirname(path), `${specifier}.js`)) ? `${specifier}.js` : `${specifier}/index.js`
      return `${prefix}${quote}${target}${quote}`
    })
    writeFileSync(path, content)
  }
}
fix(join(root, 'dist'))
