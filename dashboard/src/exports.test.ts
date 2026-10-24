import { fileURLToPath } from 'node:url'
import ts from 'typescript'
import { describe, expect, it } from 'vitest'

// The engine's types reach a host under their own names: each one the
// root and `/core` export is the generated declaration, not a name of the
// core's that happens to match and wins over it.

const file = (path: string) => fileURLToPath(new URL(path, import.meta.url))
const contract = file('./core/contract/types.ts')
const entries = { root: file('./index.ts'), core: file('./core/index.ts') }

const program = ts.createProgram([contract, ...Object.values(entries)], {
  module: ts.ModuleKind.ESNext,
  moduleResolution: ts.ModuleResolutionKind.Bundler,
  target: ts.ScriptTarget.ES2022,
  noEmit: true,
})
const checker = program.getTypeChecker()

function exportsOf(path: string): Map<string, ts.Symbol> {
  const source = program.getSourceFile(path)
  const module = source && checker.getSymbolAtLocation(source)
  if (!module) throw new Error(`no module at ${path}`)
  return new Map(checker.getExportsOfModule(module).map((s) => [s.name, s]))
}

const resolved = (s: ts.Symbol) => (s.flags & ts.SymbolFlags.Alias ? checker.getAliasedSymbol(s) : s)
const declaredIn = (s: ts.Symbol) => resolved(s).declarations?.map((d) => d.getSourceFile().fileName) ?? []

describe('the engine types', () => {
  const generated = exportsOf(contract)

  it('are many', () => {
    expect(generated.size).toBeGreaterThan(20)
  })

  for (const [entry, path] of Object.entries(entries)) {
    it(`reach ${entry} unchanged`, () => {
      const exported = exportsOf(path)
      const wrong = [...generated.keys()].filter((name) => {
        const s = exported.get(name)
        return !s || !declaredIn(s).every((f) => f === contract)
      })
      expect(wrong).toEqual([])
    })
  }
})
